import type {Logger, Manifest} from '@verdaccio/types';

import {createS3Client} from '../src/s3Client.js';
import S3PackageManager from '../src/s3PackageManager.js';
import type {ManifestIndex, S3Config} from '../src/types.js';

import {CreateBucketCommand} from '@aws-sdk/client-s3';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {beforeAll, describe, expect, test} from 'vitest';

// Set to run against real S3, e.g. MinIO at http://localhost:9000 with minio/minio12345
const ENDPOINT = process.env.VERDACCIO_S3_STORAGE_TEST_S3_ENDPOINT;

const logger = {
  error: () => {},
  warn: () => {},
  info: () => {},
  debug: () => {},
  trace: () => {},
} as unknown as Logger;

const config = {
  bucket: 'verdaccio-s3-storage-test',
  keyPrefix: `test-${randomUUID()}/`,
  endpoint: ENDPOINT,
  region: 'us-east-1',
  s3ForcePathStyle: true,
  accessKeyId: process.env.VERDACCIO_S3_STORAGE_TEST_S3_ACCESS_KEY ?? 'minio',
  secretAccessKey: process.env.VERDACCIO_S3_STORAGE_TEST_S3_SECRET_KEY ?? 'minio12345',
} as S3Config;

function manifest(name: string): Manifest {
  return {
    name,
    versions: {},
    'dist-tags': {},
    _attachments: {},
    _uplinks: {},
    _distfiles: {},
    _rev: '1-0',
  } as unknown as Manifest;
}

function storage(index?: ManifestIndex): S3PackageManager {
  return new S3PackageManager(config, `pkg-${randomUUID()}`, logger, createS3Client(config), index);
}

// Records the calls the package storage makes to its manifest index
function recordingIndex() {
  const calls: string[] = [];
  const index: ManifestIndex = {
    record: (name) => {
      calls.push(`record ${name}`);
      return Promise.resolve();
    },
    forget: (name) => {
      calls.push(`forget ${name}`);
      return Promise.resolve();
    },
    withPackageLock: (name, fn) => {
      calls.push(`lock ${name}`);
      return fn(index);
    },
    revision: () => Promise.resolve(null),
  };
  return {calls, index};
}

function signal(): AbortSignal {
  return new AbortController().signal;
}

async function read(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks);
}

describe.skipIf(!ENDPOINT)('S3PackageManager (real S3)', () => {
  beforeAll(async () => {
    await createS3Client(config)
      .send(new CreateBucketCommand({Bucket: config.bucket}))
      .catch((err: {name?: string}) => {
        if (err.name !== 'BucketAlreadyOwnedByYou' && err.name !== 'BucketAlreadyExists') {
          throw err;
        }
      });
  });

  test('creates, reads and saves a package manifest', async () => {
    const pm = storage();
    expect(await pm.hasPackage()).toBe(false);
    await pm.createPackage('a', manifest('a'));
    expect(await pm.hasPackage()).toBe(true);
    expect(await pm.readPackage('a')).toEqual(manifest('a'));

    await pm.savePackage('a', {...manifest('a'), _rev: '2-0'});
    expect((await pm.readPackage('a'))._rev).toBe('2-0');
  });

  test('createPackage fails with 409 when the package exists', async () => {
    const pm = storage();
    await pm.createPackage('a', manifest('a'));
    await expect(pm.createPackage('a', manifest('a'))).rejects.toMatchObject({
      code: 409,
    });
  });

  test('readPackage of a missing package fails with 404', async () => {
    await expect(storage().readPackage('missing')).rejects.toMatchObject({
      code: 404,
    });
  });

  test('updatePackage returns the handler result without writing it', async () => {
    const pm = storage();
    await pm.createPackage('a', manifest('a'));
    const updated = await pm.updatePackage('a', (data) => Promise.resolve({...data, _rev: '9-0'}));
    expect(updated._rev).toBe('9-0');
    expect((await pm.readPackage('a'))._rev).toBe('1-0');
    await expect(
      storage().updatePackage('missing', (data) => Promise.resolve(data))
    ).rejects.toMatchObject({
      code: 404,
    });
  });

  test('writes a tarball, closing only once it is stored', async () => {
    const pm = storage();
    const content = Buffer.alloc(6 * 1024 * 1024, 7);
    const stream = await pm.writeTarball('a-1.0.0.tgz', {signal: signal()});
    await once(stream, 'open');
    const closed = once(stream, 'close').then(() => pm.hasTarball('a-1.0.0.tgz'));
    await pipeline(Readable.from([content.subarray(0, 1000), content.subarray(1000)]), stream);
    expect(await closed).toBe(true);

    const tarball = await pm.readTarball('a-1.0.0.tgz', {signal: signal()});
    const [length] = await Promise.all([once(tarball, 'content-length'), once(tarball, 'open')]);
    expect(length).toEqual([content.length]);
    expect((await read(tarball)).equals(content)).toBe(true);
  });

  test('an aborted upload stores nothing', async () => {
    const pm = storage();
    const controller = new AbortController();
    const stream = await pm.writeTarball('a-1.0.0.tgz', {
      signal: controller.signal,
    });
    await once(stream, 'open');
    stream.write(Buffer.alloc(1024));
    const failed = once(stream, 'error');
    controller.abort();
    const [err] = (await failed) as [Error];
    expect(err.name).toBe('AbortError');
    expect(await pm.hasTarball('a-1.0.0.tgz')).toBe(false);
  });

  test('reading a missing tarball fails with 404', async () => {
    const tarball = await storage().readTarball('missing.tgz', {
      signal: signal(),
    });
    const [err] = (await once(tarball, 'error')) as [Error];
    expect(err).toMatchObject({code: 404});
  });

  test('deletes files and removes the package', async () => {
    const pm = storage();
    await pm.createPackage('a', manifest('a'));
    const stream = await pm.writeTarball('a-1.0.0.tgz', {signal: signal()});
    await pipeline(Readable.from([Buffer.from('tarball')]), stream);

    await pm.deletePackage('a-1.0.0.tgz');
    expect(await pm.hasTarball('a-1.0.0.tgz')).toBe(false);
    expect(await pm.hasPackage()).toBe(true);

    await pm.removePackage();
    expect(await pm.hasPackage()).toBe(false);
    await pm.removePackage();
  });

  test('keeps the manifest index in step with saves and removals', async () => {
    const {calls, index} = recordingIndex();
    const pm = storage(index);
    await pm.createPackage('a', manifest('a'));
    await pm.savePackage('a', {...manifest('a'), _rev: '2-0'});
    await pm.savePackage('a', {...manifest('a'), _rev: '2-1'});
    await pm.deletePackage('a-1.0.0.tgz');
    await pm.deletePackage('package.json');
    await pm.removePackage();
    const name = calls[0].split(' ')[1];
    expect(calls).toEqual([
      `lock ${name}`,
      `record ${name}`,
      `lock ${name}`,
      `record ${name}`,
      `lock ${name}`,
      `lock ${name}`,
      `lock ${name}`,
      `forget ${name}`,
      `lock ${name}`,
      `forget ${name}`,
    ]);
  });

  test("with an index, updatePackage stores the result and skips Verdaccio's echo", async () => {
    const {calls, index} = recordingIndex();
    const pm = storage(index);
    await pm.createPackage('a', manifest('a'));
    const updated = await pm.updatePackage('a', (data) => Promise.resolve({...data, readme: 'hi'}));
    expect(updated._rev).toMatch(/^2-[0-9a-f]{16}$/);
    expect(await pm.readPackage('a')).toEqual(updated);

    const written = updated._rev;
    updated._rev = '3-0';
    await pm.savePackage('a', updated);
    expect(updated._rev).toBe(written);
    expect((await pm.readPackage('a'))._rev).toBe(written);
    expect(calls.filter((c) => c.startsWith('record'))).toHaveLength(2);
  });

  test('a failed index update leaves no stale manifest cached', async () => {
    let rev: string | null = null;
    let failRecord = false;
    const index: ManifestIndex = {
      record: (_name, m) => {
        if (failRecord) return Promise.reject(new Error('postgres down'));
        rev = (m as Manifest)._rev;
        return Promise.resolve();
      },
      forget: () => Promise.resolve(),
      withPackageLock: (_name, fn) => fn(index),
      revision: () => Promise.resolve(rev),
    };
    const pm = storage(index);
    await pm.createPackage('a', manifest('a'));
    expect((await pm.readPackage('a'))._rev).toBe('1-0');

    failRecord = true;
    await expect(pm.savePackage('a', {...manifest('a'), _rev: '2-0'})).rejects.toThrow(
      'postgres down'
    );
    expect((await pm.readPackage('a'))._rev).toBe('2-0');
  });
});
