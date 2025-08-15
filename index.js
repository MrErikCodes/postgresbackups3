#!/usr/bin/env node
import { config } from "dotenv";
config();
import { spawn } from "node:child_process";
import { createReadStream } from "node:fs";
import * as fsp from "node:fs/promises";
import { tmpdir, cpus } from "node:os";
import { join } from "node:path";
import {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";

/** ===== Config via environment ===== */
const env = process.env;

// Required
[
  "PGHOST",
  "PGUSER",
  "PGPASSWORD",
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET",
  "BACKUP_PASSWORD",
].forEach((k) => {
  if (!env[k]) {
    console.error(`Missing required env: ${k}`);
    process.exit(2);
  }
});

// Optional
const PGPORT = env.PGPORT || "5432";
const PG_DUMP_PATH = env.PG_DUMP_PATH || "pg_dump";
const PSQL_PATH = env.PSQL_PATH || "psql";
const SEVEN_Z_PATH = env.SEVEN_Z_PATH || "7z";
const PG_LIST_DBNAME = env.PG_LIST_DBNAME || "postgres";
const PGDATABASES = (env.PGDATABASES || "").trim();

// R2
const R2_ENDPOINT =
  env.R2_ENDPOINT || `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
const R2_FORCE_PATH_STYLE =
  String(env.R2_FORCE_PATH_STYLE || "false").toLowerCase() === "true";

const R2_PREFIX = env.R2_PREFIX || "db-backups/";
const BACKUP_RETENTION_DAYS = Number(env.BACKUP_RETENTION_DAYS || 7);
const ARCHIVE_FORMAT = (env.ARCHIVE_FORMAT || "7z").toLowerCase(); // "7z" or "zip"
const ext = ARCHIVE_FORMAT === "zip" ? "zip" : "7z";

// Parallelism
const DEFAULT_CONC = Math.max(2, cpus()?.length || 4);
const CONCURRENCY = Number(env.CONCURRENCY || DEFAULT_CONC);

// Date (UTC) for filenames
const now = new Date();
const yyyy = now.getUTCFullYear();
const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
const dd = String(now.getUTCDate()).padStart(2, "0");
const dateStr = `${yyyy}-${mm}-${dd}`;

/** R2 S3-compatible client */
const s3 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  forcePathStyle: R2_FORCE_PATH_STYLE,
  credentials: {
    accessKeyId: env.R2_ACCESS_KEY_ID,
    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
  },
});

/** ---------- helpers ---------- */
function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ["ignore", "pipe", "pipe"], ...opts });
    let err = "";
    p.stdout.on("data", (d) => process.stdout.write(d));
    p.stderr.on("data", (d) => {
      process.stderr.write(d);
      err += d.toString();
    });
    p.on("error", reject);
    p.on("close", (code) =>
      code === 0
        ? resolve()
        : reject(new Error(`${cmd} exited ${code}: ${err.trim()}`))
    );
  });
}

async function getDatabases() {
  if (PGDATABASES) {
    return PGDATABASES.split(/[, \n]+/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  const sql = `
    SELECT datname
    FROM pg_database
    WHERE datallowconn
      AND datistemplate = false
      AND datname NOT IN ('template0','template1')
      AND datname NOT ILIKE 'rdsadmin'
      AND datname NOT ILIKE 'azure_maintenance'
    ORDER BY datname;
  `.trim();

  const args = [
    "-h",
    env.PGHOST,
    "-p",
    String(PGPORT),
    "-U",
    env.PGUSER,
    "-d",
    PG_LIST_DBNAME,
    "-Atc",
    sql,
  ];

  const out = await new Promise((resolve, reject) => {
    const p = spawn(PSQL_PATH, args, {
      env: { ...env, PGPASSWORD: env.PGPASSWORD },
    });
    let stdout = "",
      stderr = "";
    p.stdout.on("data", (d) => (stdout += d.toString()));
    p.stderr.on("data", (d) => (stderr += d.toString()));
    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve(stdout);
      else reject(new Error(`psql exited ${code}: ${stderr.trim()}`));
    });
  });

  return out
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

async function dumpDatabase(dbName, outPath) {
  const args = [
    "-h",
    env.PGHOST,
    "-p",
    String(PGPORT),
    "-U",
    env.PGUSER,
    "-d",
    dbName,
    "-Fc",
    "-f",
    outPath,
  ];
  await run(PG_DUMP_PATH, args, {
    env: { ...env, PGPASSWORD: env.PGPASSWORD },
  });
}

async function createEncryptedArchive(inputFile, outArchive) {
  const args = ["a", `-t${ext}`, "-mx=9"];
  if (ext === "7z") args.push("-mhe=on");
  else args.push("-mem=AES256");
  // ⚠️ Password appears in process args during the run.
  args.push(`-p${env.BACKUP_PASSWORD}`, outArchive, inputFile);
  await run(SEVEN_Z_PATH, args);
}

async function uploadToR2(filePath, bucket, key, contentType, meta = {}) {
  const Body = createReadStream(filePath);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body,
      ContentType: contentType,
      Metadata: meta,
    })
  );
}

async function pruneOldBackups(bucket, dbPrefix, retentionDays) {
  const cutoff = new Date(now.getTime() - retentionDays * 24 * 60 * 60 * 1000);
  let ContinuationToken;
  let pending = [];

  do {
    const page = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: dbPrefix,
        ContinuationToken,
      })
    );

    for (const o of page.Contents || []) {
      if (!o.Key || !o.Key.endsWith(`.${ext}`)) continue;
      if (o.LastModified && o.LastModified < cutoff) {
        pending.push({ Key: o.Key });
        if (pending.length === 1000) {
          await s3.send(
            new DeleteObjectsCommand({
              Bucket: bucket,
              Delete: { Objects: pending },
            })
          );
          pending = [];
        }
      }
    }

    ContinuationToken = page.IsTruncated
      ? page.NextContinuationToken
      : undefined;
  } while (ContinuationToken);

  if (pending.length) {
    await s3.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: { Objects: pending },
      })
    );
  }
}

/** Simple promise pool */
async function runPool(items, worker, concurrency) {
  const results = [];
  let idx = 0;
  const errors = [];
  const runNext = async () => {
    const myIdx = idx++;
    if (myIdx >= items.length) return;
    const item = items[myIdx];
    try {
      results[myIdx] = await worker(item);
    } catch (e) {
      errors.push({ item, error: e });
    } finally {
      await runNext();
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, runNext)
  );
  if (errors.length) {
    // surface the first error but log all
    for (const { item, error } of errors) {
      console.error(`Task failed for ${item}:`, error.message);
    }
    throw errors[0].error;
  }
  return results;
}

/** Per-database task */
async function backupOneDatabase(dbName, rootWorkdir) {
  const dbWorkdir = await fsp.mkdtemp(join(rootWorkdir, `${dbName}-`));
  const dumpFile = join(dbWorkdir, `${dbName}.${dateStr}.dump`);
  const archiveFile = join(dbWorkdir, `${dateStr}.${ext}`);
  const keyPrefix = `${R2_PREFIX}${dbName}/`;
  const objectKey = `${keyPrefix}${dateStr}.${ext}`;

  console.log(`\n==> [${dbName}] Starting backup`);
  console.log(`[${dbName}] 1/4 Dumping → ${dumpFile}`);
  await dumpDatabase(dbName, dumpFile);

  console.log(
    `[${dbName}] 2/4 Archiving (${ext.toUpperCase()}) → ${archiveFile}`
  );
  await createEncryptedArchive(dumpFile, archiveFile);

  console.log(`[${dbName}] 3/4 Uploading → r2://${env.R2_BUCKET}/${objectKey}`);
  await uploadToR2(
    archiveFile,
    env.R2_BUCKET,
    objectKey,
    ext === "zip" ? "application/zip" : "application/x-7z-compressed",
    { database: dbName, created_at: now.toISOString() }
  );

  console.log(
    `[${dbName}] 4/4 Pruning old backups (> ${BACKUP_RETENTION_DAYS} days)`
  );
  await pruneOldBackups(env.R2_BUCKET, keyPrefix, BACKUP_RETENTION_DAYS);

  // cleanup
  await fsp.rm(dbWorkdir, { recursive: true, force: true }).catch(() => {});
  console.log(`✅ [${dbName}] Done`);
}

async function main() {
  const dbs = await getDatabases();
  if (!dbs.length) {
    console.error("No databases found to back up.");
    process.exit(3);
  }
  console.log(
    `Backing up ${dbs.length} database(s) with concurrency=${CONCURRENCY}`
  );

  const rootWorkdir = await fsp.mkdtemp(join(tmpdir(), "pgbkp-"));
  try {
    await runPool(
      dbs,
      (dbName) => backupOneDatabase(dbName, rootWorkdir),
      CONCURRENCY
    );
    console.log("\n🎉 All databases backed up successfully.");
  } finally {
    await fsp.rm(rootWorkdir, { recursive: true, force: true }).catch(() => {});
  }
}

main().catch((e) => {
  console.error("❌ Backup failed:", e.message);
  process.exit(1);
});
