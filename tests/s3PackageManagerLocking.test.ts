import type {Logger, Manifest} from '@verdaccio/types';

import {createS3Client} from '../src/s3Client.js';
import S3DatabasePostgres from '../src/s3DatabasePostgres.js';
import S3PackageManager from '../src/s3PackageManager.js';
import type {S3Config} from '../src/types.js';

import {CreateBucketCommand, PutObjectCommand} from '@aws-sdk/client-s3';
import {randomBytes, randomUUID} from 'node:crypto';
import {setTimeout as sleep} from 'node:timers/promises';
import pg from 'pg';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';

// Needs real S3 and Postgres, e.g. MinIO and a scratch database
const ENDPOINT = process.env.VERDACCIO_S3_STORAGE_TEST_S3_ENDPOINT;
const BASE_PG_URL = process.env.VERDACCIO_S3_STORAGE_TEST_PG_URL;
// A database of its own, since other suites reset tables in the base one
const PG_URL = BASE_PG_URL && withDatabase(BASE_PG_URL, 'verdaccio_s3_locking');

function withDatabase(url: string, database: string): string {
  const parsed = new URL(url);
  parsed.pathname = `/${database}`;
  return parsed.toString();
}

async function ensureDatabase(url: string, database: string) {
  const admin = new pg.Client({connectionString: url});
  await admin.connect();
  try {
    await admin.query(`CREATE DATABASE ${database}`);
  } catch (err) {
    // 42P04: it already exists
    if ((err as {code?: string}).code !== '42P04') throw err;
  } finally {
    await admin.end();
  }
}

const logger = {
  error: () => {},
  warn: () => {},
  info: () => {},
  debug: () => {},
  trace: () => {},
} as unknown as Logger;

const config = {
  bucket: 'verdaccio-s3-storage-test',
  keyPrefix: `locking-${randomUUID()}/`,
  endpoint: ENDPOINT,
  region: 'us-east-1',
  s3ForcePathStyle: true,
  accessKeyId: process.env.VERDACCIO_S3_STORAGE_TEST_S3_ACCESS_KEY ?? 'minio',
  secretAccessKey: process.env.VERDACCIO_S3_STORAGE_TEST_S3_SECRET_KEY ?? 'minio12345',
} as S3Config;

type Handler = (manifest: Manifest) => Promise<Manifest>;

// One registry process: its own pools, index and S3 client
function replica() {
  const pool = new pg.Pool({connectionString: PG_URL});
  const lockPool = new pg.Pool({connectionString: PG_URL});
  const db = new S3DatabasePostgres(pool, logger, lockPool);
  const s3 = createS3Client(config);
  return {
    db,
    pm: (name: string) => new S3PackageManager(config, name, logger, s3, db),
    close: () => Promise.all([pool.end(), lockPool.end()]),
  };
}

// What @verdaccio/store's generateRevision produces
const nextRev = (rev: string) =>
  `${(Number(rev.split('-')[0]) || 0) + 1}-${randomBytes(8).toString('hex')}`;

// Mirrors @verdaccio/store Storage.updatePackage: the plugin's update, then writePackage
async function verdaccioUpdate(
  pm: S3PackageManager,
  name: string,
  handler: Handler
): Promise<Manifest> {
  const updated = await pm.updatePackage(name, handler);
  updated._rev = nextRev(updated._rev);
  await pm.savePackage(name, updated);
  return updated;
}

// Mirrors Verdaccio's addVersion plus the tarball attachment, pausing to overlap
const addVersion =
  (version: string, pause = 100): Handler =>
  async (data) => {
    await sleep(pause);
    data.versions[version] = {name: data.name, version} as never;
    data._attachments[`${data.name}-${version}.tgz`] = {
      shasum: version,
      version,
    } as never;
    data['dist-tags'].latest = version;
    return data;
  };

function initial(name: string): Manifest {
  return {
    name,
    versions: {'1.0.0': {name, version: '1.0.0'}},
    'dist-tags': {latest: '1.0.0'},
    _attachments: {[`${name}-1.0.0.tgz`]: {shasum: '1', version: '1.0.0'}},
    _uplinks: {},
    _distfiles: {},
    time: {},
    _rev: '1-0',
  } as unknown as Manifest;
}

describe.skipIf(!ENDPOINT || !PG_URL)('S3PackageManager locking (real S3 + Postgres)', () => {
  let a: ReturnType<typeof replica>;
  let b: ReturnType<typeof replica>;
  let admin: pg.Pool;

  beforeAll(async () => {
    await ensureDatabase(BASE_PG_URL!, 'verdaccio_s3_locking');
    a = replica();
    b = replica();
    admin = new pg.Pool({connectionString: PG_URL, max: 1});
    await createS3Client(config)
      .send(new CreateBucketCommand({Bucket: config.bucket}))
      .catch((err: {name?: string}) => {
        if (err.name !== 'BucketAlreadyOwnedByYou' && err.name !== 'BucketAlreadyExists') {
          throw err;
        }
      });
    await Promise.all([a.db.init(), b.db.init()]);
  });

  afterAll(async () => {
    await Promise.all([a.close(), b.close(), admin.end()]);
  });

  async function indexed(name: string) {
    const {rows} = await admin.query<{versions: string[]; rev: string}>(
      'SELECT versions, rev FROM verdaccio_manifests WHERE name = $1',
      [name]
    );
    return rows[0];
  }

  async function expectConsistent(name: string, versions: string[]) {
    const stored = await a.pm(name).readPackage(name);
    expect(Object.keys(stored.versions).sort()).toEqual(versions);
    const row = await indexed(name);
    expect([...row.versions].sort()).toEqual(versions);
    expect(row.rev).toBe(stored._rev);
  }

  test('two replicas publishing one package keep both versions', async () => {
    const name = `race-${randomUUID()}`;
    await a.pm(name).createPackage(name, initial(name));
    await Promise.all([
      verdaccioUpdate(a.pm(name), name, addVersion('1.0.1')),
      verdaccioUpdate(b.pm(name), name, addVersion('1.0.2')),
    ]);
    await expectConsistent(name, ['1.0.0', '1.0.1', '1.0.2']);
  });

  test('many writers across replicas and within one lose nothing', async () => {
    const name = `race-${randomUUID()}`;
    await b.pm(name).createPackage(name, initial(name));
    const versions = Array.from({length: 8}, (_, i) => `2.0.${i}`);
    await Promise.all(
      versions.map((version, i) =>
        verdaccioUpdate((i % 2 ? a : b).pm(name), name, addVersion(version, 20))
      )
    );
    await expectConsistent(name, ['1.0.0', ...versions].sort());
  });

  test('a save based on a stale read does not clobber a newer manifest', async () => {
    const name = `stale-${randomUUID()}`;
    await a.pm(name).createPackage(name, initial(name));
    const stale = await a.pm(name).readPackage(name);
    await verdaccioUpdate(b.pm(name), name, addVersion('1.0.1', 0));

    stale._rev = nextRev(stale._rev);
    await a.pm(name).savePackage(name, stale);
    await expectConsistent(name, ['1.0.0', '1.0.1']);
  });

  test('a cached manifest is replaced once another process writes a newer one', async () => {
    const name = `cache-${randomUUID()}`;
    await a.pm(name).createPackage(name, initial(name));
    const first = await a.pm(name).readPackage(name);
    // Mutating what a read returns must not reach the next read
    first.versions['9.9.9'] = {name, version: '9.9.9'} as never;
    expect(Object.keys((await a.pm(name).readPackage(name)).versions)).toEqual(['1.0.0']);

    // Another process: writes S3 and the index without touching this cache
    const newer = {...initial(name), _rev: '7-other'} as Manifest;
    newer.versions['1.0.1'] = {name, version: '1.0.1'} as never;
    await createS3Client(config).send(
      new PutObjectCommand({
        Bucket: config.bucket,
        Key: `${config.keyPrefix}${name}/package.json`,
        Body: JSON.stringify(newer),
      })
    );
    await admin.query('UPDATE verdaccio_manifests SET rev = $2 WHERE name = $1', [name, '7-other']);
    const read = await a.pm(name).readPackage(name);
    expect(read._rev).toBe('7-other');
    expect(Object.keys(read.versions).sort()).toEqual(['1.0.0', '1.0.1']);
  });

  test('concurrent first publishes create the package once', async () => {
    const name = `create-${randomUUID()}`;
    const results = await Promise.allSettled([
      a.pm(name).createPackage(name, initial(name)),
      b.pm(name).createPackage(name, initial(name)),
    ]);
    expect(results.map((r) => r.status).sort()).toEqual(['fulfilled', 'rejected']);
    await expectConsistent(name, ['1.0.0']);
  });
});
