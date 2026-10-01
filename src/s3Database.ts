import type {pluginUtils, searchUtils} from '@verdaccio/core';
import type {Config, Logger, Token, TokenFilter} from '@verdaccio/types';

import addTrailingSlash from './addTrailingSlash.js';
import {createS3Client} from './s3Client.js';
import S3DatabasePostgres from './s3DatabasePostgres.js';
import S3PackageManager from './s3PackageManager.js';
import setConfigValue from './setConfigValue.js';
import type {S3Config} from './types.js';

import type {S3Client} from '@aws-sdk/client-s3';
import debugCore from 'debug';
import pg from 'pg';

const debug = debugCore('verdaccio:plugin:aws-s3-storage:database');

// Light: the package list, tokens and secret; locks hold one per write
const DEFAULT_POOL_MAX = 2;
const DEFAULT_LOCK_POOL_MAX = 4;

function poolSize(value: number | string | undefined, fallback: number) {
  const n = Number(setConfigValue(value === undefined ? undefined : String(value)));
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

export default class S3Database implements Omit<
  pluginUtils.Storage<S3Config>,
  keyof pluginUtils.Plugin<S3Config>
> {
  public logger: Logger;
  public config: S3Config;
  private s3: S3Client;
  private database: S3DatabasePostgres;

  public constructor(config: Config, options: {logger: Logger; config: Config}) {
    this.logger = options.logger;
    if (!config) {
      throw new Error('s3 storage missing config. Add `store.s3-storage` to your config file');
    }
    // verdaccio 7+ passes plugin config directly, older versions nest it under config.store
    const store = config.store as Record<string, Partial<S3Config>> | undefined;
    const pluginConfig = store?.['aws-s3-storage'] ?? {};
    this.config = Object.assign({}, config, pluginConfig) as S3Config;

    if (!this.config.bucket) {
      throw new Error('s3 storage requires a bucket');
    }

    if (!this.config.postgresUrl) {
      throw new Error('s3 storage requires a postgresUrl');
    }

    this.config.bucket = setConfigValue(this.config.bucket);
    this.config.keyPrefix = setConfigValue(this.config.keyPrefix);
    this.config.endpoint = setConfigValue(this.config.endpoint);
    this.config.region = setConfigValue(this.config.region);
    this.config.accessKeyId = setConfigValue(this.config.accessKeyId);
    this.config.secretAccessKey = setConfigValue(this.config.secretAccessKey);
    this.config.sessionToken = setConfigValue(this.config.sessionToken);
    this.config.proxy = setConfigValue(this.config.proxy);
    this.config.postgresUrl = setConfigValue(this.config.postgresUrl);

    const configKeyPrefix = this.config.keyPrefix;
    this.config.keyPrefix = addTrailingSlash(configKeyPrefix);

    debug(
      'initialized bucket=%o keyPrefix=%o region=%o',
      this.config.bucket,
      this.config.keyPrefix,
      this.config.region
    );
    this.logger.trace(
      {
        bucket: this.config.bucket,
        keyPrefix: this.config.keyPrefix,
        region: this.config.region,
      },
      'aws-s3-storage: plugin initialized bucket=@{bucket} keyPrefix=@{keyPrefix} region=@{region}'
    );

    this.s3 = createS3Client(this.config);

    const pool = new pg.Pool({
      connectionString: this.config.postgresUrl,
      max: poolSize(this.config.postgresPoolMax, DEFAULT_POOL_MAX),
    });
    const lockPool = new pg.Pool({
      connectionString: this.config.postgresUrl,
      max: poolSize(this.config.postgresLockPoolMax, DEFAULT_LOCK_POOL_MAX),
    });
    // An idle client losing its connection must not crash the process
    for (const p of [pool, lockPool]) {
      p.on('error', (err) => {
        this.logger.warn({err}, 'aws-s3-storage: postgres pool error: @{err.message}');
      });
    }
    this.database = new S3DatabasePostgres(pool, this.logger, lockPool);
  }

  public async init(): Promise<void> {
    await this.database.init();
  }

  public async getSecret(): Promise<string> {
    return this.database.getSecret();
  }

  public async setSecret(secret: string): Promise<void> {
    await this.database.setSecret(secret);
  }

  public async add(name: string): Promise<void> {
    await this.database.add(name);
  }

  public async remove(name: string): Promise<void> {
    await this.database.remove(name);
  }

  public async get(): Promise<string[]> {
    return this.database.get();
  }

  // Matches published package names; cached uplink packages aren't listed. Verdaccio
  // pages the results itself, so only the first from + size matches are returned.
  public async search(query: searchUtils.SearchQuery): Promise<searchUtils.SearchItem[]> {
    const limit = query.size === undefined ? null : (query.from ?? 0) + query.size;
    const names = await this.database.search(query.text, limit);
    return names.map((name) => ({
      package: {name},
      verdaccioPrivate: true,
      verdaccioPkgCached: false,
      score: {
        final: 1,
        detail: {quality: 1, popularity: 1, maintenance: 0},
      },
    }));
  }

  public getPackageStorage(packageName: string): S3PackageManager {
    debug('getPackageStorage package=%o bucket=%o', packageName, this.config.bucket);
    this.logger.trace(
      {packageName, bucket: this.config.bucket},
      'aws-s3-storage: [getPackageStorage] creating storage for package=@{packageName} bucket=@{bucket}'
    );
    return new S3PackageManager(this.config, packageName, this.logger, this.s3, this.database);
  }

  public async saveToken(token: Token): Promise<void> {
    await this.database.saveToken(token);
  }

  public async deleteToken(user: string, tokenKey: string): Promise<void> {
    await this.database.deleteToken(user, tokenKey);
  }

  public async readTokens(filter: TokenFilter): Promise<Token[]> {
    return this.database.readTokens(filter);
  }
}
