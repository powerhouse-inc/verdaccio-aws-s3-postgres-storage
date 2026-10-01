import {HEADERS, type VerdaccioError, type pluginUtils} from '@verdaccio/core';
import type {Logger, Manifest} from '@verdaccio/types';

import addTrailingSlash from './addTrailingSlash.js';
import {deleteKeyPrefix} from './deleteKeyPrefix.js';
import {convertS3Error, create409Error, is404Error} from './s3Errors.js';
import type {ManifestIndex, ManifestIndexWriter, S3Config} from './types.js';

import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import type {ObjectCannedACL, S3Client} from '@aws-sdk/client-s3';
import {Upload} from '@aws-sdk/lib-storage';
import debugCore from 'debug';
import {randomBytes} from 'node:crypto';
import {PassThrough, Writable, addAbortSignal} from 'node:stream';
import type {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';

const debug = debugCore('verdaccio:plugin:aws-s3-storage:package');

const pkgFileName = 'package.json';

// Manifests updatePackage already stored, with the stored `_rev`, so Verdaccio's follow-up save is skipped
const stored = new WeakMap<object, string>();

// Raw manifests by S3 key, served while their `_rev` matches the index's
const manifestCache = new Map<string, {rev: string; body: string}>();
const MANIFEST_CACHE_MAX = 2_000;

function cacheManifest(key: string, rev: unknown, body: string): void {
  if (typeof rev !== 'string' || !rev) return;
  manifestCache.delete(key);
  manifestCache.set(key, {rev, body});
  if (manifestCache.size > MANIFEST_CACHE_MAX) {
    manifestCache.delete(manifestCache.keys().next().value!);
  }
}

// Keys @verdaccio/store's normalizePackage fills before every write
const OBJECT_KEYS = [
  'versions',
  'dist-tags',
  '_distfiles',
  '_attachments',
  '_uplinks',
  'time',
] as const;

// Same shape as @verdaccio/store's generateRevision: "<counter>-<hex>"
const revCounter = (rev: unknown) => (typeof rev === 'string' ? Number(rev.split('-')[0]) || 0 : 0);
const nextRevision = (rev: unknown) => `${revCounter(rev) + 1}-${randomBytes(8).toString('hex')}`;

function normalize(manifest: Manifest): Manifest {
  const m = manifest as unknown as Record<string, unknown>;
  for (const key of OBJECT_KEYS) {
    const value = m[key];
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      m[key] = {};
    }
  }
  return manifest;
}

export default class S3PackageManager implements pluginUtils.StorageHandler {
  public config: S3Config;
  public logger: Logger;
  private readonly packageName: string;
  private readonly s3: S3Client;
  private readonly packagePath: string;
  private readonly tarballACL: ObjectCannedACL;

  public constructor(
    config: S3Config,
    packageName: string,
    logger: Logger,
    s3: S3Client,
    private readonly index?: ManifestIndex
  ) {
    this.config = config;
    this.packageName = packageName;
    this.logger = logger;
    this.s3 = s3;
    this.tarballACL = (config.tarballACL || 'private') as ObjectCannedACL;

    const packageAccess = this.config.getMatchedPackagesSpec
      ? this.config.getMatchedPackagesSpec(packageName)
      : undefined;
    if (packageAccess) {
      const storage = packageAccess.storage;
      const packageCustomFolder = addTrailingSlash(storage);
      this.packagePath = `${this.config.keyPrefix}${packageCustomFolder}${this.packageName}`;
    } else {
      this.packagePath = `${this.config.keyPrefix}${this.packageName}`;
    }

    debug(
      'init package=%o path=%o bucket=%o acl=%o',
      packageName,
      this.packagePath,
      config.bucket,
      this.tarballACL
    );
  }

  // Without an index Verdaccio writes the returned manifest itself; with one
  // the whole read-modify-write runs under the package lock.
  public async updatePackage(
    name: string,
    handleUpdate: (manifest: Manifest) => Promise<Manifest>
  ): Promise<Manifest> {
    debug('updatePackage name=%o', name);
    if (!this.index) return handleUpdate(await this.readPackage(name));
    return this.index.withPackageLock(this.packageName, async (index) => {
      const manifest = normalize(await handleUpdate((await this.readFromS3(name)).manifest));
      manifest._rev = nextRevision(manifest._rev);
      await this.write(manifest, index);
      stored.set(manifest, manifest._rev);
      return manifest;
    });
  }

  // A primary-key lookup in the index replaces the S3 read and its parse
  // whenever the cached copy is still the indexed revision
  public async readPackage(name: string): Promise<Manifest> {
    const key = this.key(pkgFileName);
    const rev = await this.index?.revision(this.packageName);
    const cached = manifestCache.get(key);
    if (rev && cached?.rev === rev) return JSON.parse(cached.body) as Manifest;
    const {manifest, body} = await this.readFromS3(name);
    if (rev && manifest._rev === rev) cacheManifest(key, rev, body);
    return manifest;
  }

  private async readFromS3(name: string): Promise<{manifest: Manifest; body: string}> {
    const key = this.key(pkgFileName);
    debug('readPackage name=%o key=%o', name, key);
    let body: string;
    try {
      const response = await this.s3.send(
        new GetObjectCommand({Bucket: this.config.bucket, Key: key})
      );
      body = (await response.Body?.transformToString()) ?? '';
    } catch (err) {
      throw convertS3Error(err);
    }
    try {
      return {manifest: JSON.parse(body) as Manifest, body};
    } catch (err) {
      this.logger.error(
        {key, err},
        'aws-s3-storage: invalid package.json at @{key}: @{err.message}'
      );
      throw err;
    }
  }

  public hasPackage(): Promise<boolean> {
    return this.exists(this.key(pkgFileName));
  }

  public async createPackage(name: string, manifest: Manifest): Promise<void> {
    debug('createPackage name=%o', name);
    await this.locked(async (index) => {
      if (await this.hasPackage()) throw create409Error();
      await this.write(manifest, index);
    });
  }

  // With an index, a save whose revision isn't ahead of the stored one is stale and dropped
  public async savePackage(name: string, manifest: Manifest): Promise<void> {
    debug('savePackage name=%o', name);
    // Verdaccio bumps `_rev` before the echo; restore the revision that was written
    const persisted = stored.get(manifest);
    if (persisted !== undefined) {
      stored.delete(manifest);
      manifest._rev = persisted;
      return;
    }
    await this.locked(async (index) => {
      if (index) {
        const current = await this.storedRevision();
        if (current !== null && revCounter(manifest._rev) <= revCounter(current)) {
          this.logger.warn(
            {name, rev: manifest._rev, current},
            'aws-s3-storage: dropped stale save of @{name} (rev @{rev}, stored @{current})'
          );
          return;
        }
      }
      await this.write(manifest, index);
    });
  }

  public async deletePackage(fileName: string): Promise<void> {
    const key = this.key(fileName);
    debug('deletePackage key=%o', key);
    await this.locked(async (index) => {
      try {
        await this.s3.send(new DeleteObjectCommand({Bucket: this.config.bucket, Key: key}));
      } catch (err) {
        throw convertS3Error(err);
      }
      if (fileName === pkgFileName) await index?.forget(this.packageName);
    });
  }

  public async removePackage(): Promise<void> {
    await this.locked(async (index) => {
      await index?.forget(this.packageName);
      const prefix = addTrailingSlash(this.packagePath);
      debug('removePackage prefix=%o', prefix);
      try {
        await deleteKeyPrefix(this.s3, {
          Bucket: this.config.bucket,
          Prefix: prefix,
        });
      } catch (err) {
        if (!is404Error(err as VerdaccioError)) throw err;
      }
    });
  }

  public hasTarball(fileName: string): Promise<boolean> {
    return this.exists(this.key(fileName));
  }

  // Emits "open" once the upload starts and "close" only after S3 has stored the object
  public writeTarball(fileName: string, {signal}: {signal: AbortSignal}): Promise<Writable> {
    const key = this.key(fileName);
    debug('writeTarball key=%o acl=%o', key, this.tarballACL);
    const body = new PassThrough();
    const upload = new Upload({
      client: this.s3,
      params: {
        Bucket: this.config.bucket,
        Key: key,
        Body: body,
        ACL: this.tarballACL,
      },
    });
    const uploaded = upload.done().then(
      () => undefined,
      (err: unknown) => {
        throw convertS3Error(err);
      }
    );
    let stored = false;

    const stream = new Writable({
      write(chunk: Buffer, _encoding, callback) {
        if (body.write(chunk)) callback();
        else body.once('drain', () => callback());
      },
      final(callback) {
        body.end();
        uploaded.then(
          () => {
            stored = true;
            debug('writeTarball key=%o stored', key);
            callback();
          },
          (err: Error) => callback(err)
        );
      },
      destroy: (err, callback) => {
        if (!stored) {
          body.destroy();
          void upload.abort().catch(() => undefined);
        }
        callback(err);
      },
    });
    uploaded.catch((err: Error) => stream.destroy(err));
    addAbortSignal(signal, stream);
    setImmediate(() => {
      if (!stream.destroyed) stream.emit('open');
    });
    return Promise.resolve(stream);
  }

  // Emits "open" once S3 answers; a missing object fails with a 404 error
  public readTarball(fileName: string, {signal}: {signal: AbortSignal}): Promise<Readable> {
    const key = this.key(fileName);
    debug('readTarball key=%o', key);
    const stream = addAbortSignal(signal, new PassThrough());
    this.s3
      .send(new GetObjectCommand({Bucket: this.config.bucket, Key: key}), {
        abortSignal: signal,
      })
      .then(
        (response) => {
          if (stream.destroyed) {
            (response.Body as Readable | undefined)?.destroy();
            return;
          }
          if (response.ContentLength) {
            stream.emit(HEADERS.CONTENT_LENGTH, response.ContentLength);
          }
          stream.emit('open');
          pipeline(response.Body as Readable, stream).catch((err: unknown) =>
            stream.destroy(convertS3Error(err))
          );
        },
        (err: unknown) => {
          debug('readTarball key=%o failed: %o', key, err);
          stream.destroy(convertS3Error(err));
        }
      );
    return Promise.resolve(stream);
  }

  // Runs fn under the package lock when there is an index, else directly
  private locked<T>(fn: (index: ManifestIndexWriter | undefined) => Promise<T>): Promise<T> {
    return this.index ? this.index.withPackageLock(this.packageName, fn) : fn(undefined);
  }

  // S3 first, so the index never names a manifest S3 doesn't hold
  private async write(manifest: Manifest, index: ManifestIndexWriter | undefined): Promise<void> {
    const body = JSON.stringify(manifest, null, '  ');
    try {
      await this.s3.send(
        new PutObjectCommand({
          Body: body,
          Bucket: this.config.bucket,
          Key: this.key(pkgFileName),
        })
      );
    } catch (err) {
      throw convertS3Error(err);
    }
    // A failed index update must not leave the old body cached under the old revision
    manifestCache.delete(this.key(pkgFileName));
    await index?.record(this.packageName, manifest);
    cacheManifest(this.key(pkgFileName), manifest._rev, body);
  }

  private async storedRevision(): Promise<string | null> {
    try {
      return (await this.readFromS3(this.packageName)).manifest._rev;
    } catch (err) {
      if (is404Error(err as VerdaccioError)) return null;
      throw err;
    }
  }

  private key(fileName: string): string {
    return `${this.packagePath}/${fileName}`;
  }

  private async exists(key: string): Promise<boolean> {
    try {
      await this.s3.send(new HeadObjectCommand({Bucket: this.config.bucket, Key: key}));
      return true;
    } catch (err) {
      const converted = convertS3Error(err);
      if (is404Error(converted)) return false;
      throw converted;
    }
  }
}
