import {generateRandomSecretKey} from '@verdaccio/config';
import type {Logger, Token, TokenFilter} from '@verdaccio/types';

import type {ManifestIndex, ManifestIndexWriter} from './types.js';

import debugCore from 'debug';
import type {Pool, PoolClient} from 'pg';

const debug = debugCore('verdaccio:plugin:aws-s3-storage:database:postgres');

// Serializes schema setup across instances starting against the same database
const MIGRATION_LOCK_ID = 7_446_517_003;
// First key of the per-package advisory lock; the second is hashtext(name)
const PACKAGE_LOCK_CLASS = 744_651_700;
// Longest a writer waits for another replica's lock on the same package
const PACKAGE_LOCK_TIMEOUT = '60s';

type Query = <R extends object>(text: string, values?: unknown[]) => Promise<{rows: R[]}>;

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS verdaccio_packages (
    name       text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS verdaccio_tokens (
    "user" text NOT NULL,
    key    text NOT NULL,
    token  jsonb NOT NULL,
    PRIMARY KEY ("user", key)
  )`,
  `CREATE TABLE IF NOT EXISTS verdaccio_secret (
    id     integer PRIMARY KEY CHECK (id = 1),
    secret text NOT NULL
  )`,
  // Versions and dist-tags of each package published here, as last saved
  `CREATE TABLE IF NOT EXISTS verdaccio_manifests (
    name       text PRIMARY KEY,
    versions   jsonb NOT NULL,
    dist_tags  jsonb NOT NULL,
    rev        text,
    updated_at timestamptz NOT NULL DEFAULT now()
  )`,
];

interface IndexedManifest {
  _rev?: string;
  versions?: Record<string, unknown>;
  'dist-tags'?: Record<string, string>;
  _attachments?: Record<string, unknown>;
}

export default class S3DatabasePostgres implements ManifestIndex {
  public logger: Logger;
  private pool: Pool;
  private lockPool: Pool;
  private schema: Promise<void> | undefined;
  // Tail of each package's in-process queue, so waiters hold no connection
  private queues = new Map<string, Promise<void>>();

  // Lock transactions use `lockPool`, so code run under a lock can still query `pool`
  public constructor(pool: Pool, logger: Logger, lockPool: Pool = pool) {
    this.pool = pool;
    this.lockPool = lockPool;
    this.logger = logger;
  }

  // Verdaccio 6 never calls init(), so every query waits for the schema first
  public init(): Promise<void> {
    this.schema ??= this.migrate().catch((err: unknown) => {
      this.schema = undefined;
      throw err;
    });
    return this.schema;
  }

  private async migrate(): Promise<void> {
    debug('migrate: applying schema');
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
      for (const statement of SCHEMA) await client.query(statement);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
    this.logger.trace('aws-s3-storage: [init] postgres schema ready');
  }

  // Stores a secret on first read, so every instance signs with the same one
  public async getSecret(): Promise<string> {
    await this.query(
      'INSERT INTO verdaccio_secret (id, secret) VALUES (1, $1) ON CONFLICT (id) DO NOTHING',
      [generateRandomSecretKey()]
    );
    const result = await this.query<{secret: string}>(
      'SELECT secret FROM verdaccio_secret WHERE id = 1'
    );
    return result.rows[0]?.secret ?? '';
  }

  public async setSecret(secret: string): Promise<void> {
    await this.query(
      `INSERT INTO verdaccio_secret (id, secret) VALUES (1, $1)
       ON CONFLICT (id) DO UPDATE SET secret = EXCLUDED.secret`,
      [secret]
    );
  }

  public async add(name: string): Promise<void> {
    debug('add package=%o', name);
    await this.query(
      'INSERT INTO verdaccio_packages (name) VALUES ($1) ON CONFLICT (name) DO NOTHING',
      [name]
    );
  }

  public async remove(name: string): Promise<void> {
    debug('remove package=%o', name);
    await this.query('DELETE FROM verdaccio_packages WHERE name = $1', [name]);
  }

  public async get(): Promise<string[]> {
    const result = await this.query<{name: string}>(
      'SELECT name FROM verdaccio_packages ORDER BY name COLLATE "C"'
    );
    return result.rows.map((row) => row.name);
  }

  // Names containing text, in name order; a null limit returns every match
  public async search(text: string, limit: number | null): Promise<string[]> {
    const result = await this.query<{name: string}>(
      `SELECT name FROM verdaccio_packages WHERE strpos(name, $1) > 0
       ORDER BY name COLLATE "C" LIMIT $2`,
      [text, limit]
    );
    return result.rows.map((row) => row.name);
  }

  public record(name: string, manifest: object): Promise<void> {
    return recordWith(this.query.bind(this), name, manifest);
  }

  public forget(name: string): Promise<void> {
    return forgetWith(this.query.bind(this), name);
  }

  public async revision(name: string): Promise<string | null> {
    const result = await this.query<{rev: string | null}>(
      'SELECT rev FROM verdaccio_manifests WHERE name = $1',
      [name]
    );
    return result.rows[0]?.rev ?? null;
  }

  public async withPackageLock<T>(
    name: string,
    fn: (index: ManifestIndexWriter) => Promise<T>
  ): Promise<T> {
    const previous = this.queues.get(name) ?? Promise.resolve();
    let release!: () => void;
    const tail = previous.then(() => new Promise<void>((resolve) => (release = resolve)));
    this.queues.set(name, tail);
    await previous;
    try {
      return await this.lockedTransaction(name, fn);
    } finally {
      release();
      if (this.queues.get(name) === tail) this.queues.delete(name);
    }
  }

  // Transaction-scoped lock, so it holds through a transaction-mode pooler
  private async lockedTransaction<T>(
    name: string,
    fn: (index: ManifestIndexWriter) => Promise<T>
  ): Promise<T> {
    await this.init();
    const client = await this.lockPool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL lock_timeout = '${PACKAGE_LOCK_TIMEOUT}'`);
      await client.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
        PACKAGE_LOCK_CLASS,
        name,
      ]);
      debug('lock package=%o', name);
      const query = clientQuery(client);
      const result = await fn({
        record: (n, manifest) => recordWith(query, n, manifest),
        forget: (n) => forgetWith(query, n),
      });
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  public async saveToken(token: Token): Promise<void> {
    await this.query(
      `INSERT INTO verdaccio_tokens ("user", key, token) VALUES ($1, $2, $3)
       ON CONFLICT ("user", key) DO UPDATE SET token = EXCLUDED.token`,
      [token.user, token.key, JSON.stringify(token)]
    );
  }

  public async deleteToken(user: string, tokenKey: string): Promise<void> {
    await this.query('DELETE FROM verdaccio_tokens WHERE "user" = $1 AND key = $2', [
      user,
      tokenKey,
    ]);
  }

  public async readTokens(filter: TokenFilter): Promise<Token[]> {
    const result = await this.query<{token: Token}>(
      'SELECT token FROM verdaccio_tokens WHERE "user" = $1 ORDER BY key COLLATE "C"',
      [filter.user]
    );
    return result.rows.map((row) => row.token);
  }

  private async query<R extends object>(text: string, values?: unknown[]): Promise<{rows: R[]}> {
    await this.init();
    return this.pool.query<R>(text, values);
  }
}

const clientQuery =
  (client: PoolClient): Query =>
  (text, values) =>
    client.query(text, values);

// Only published packages are indexed; metadata cached from an uplink isn't
async function recordWith(query: Query, name: string, manifest: object): Promise<void> {
  const m = manifest as IndexedManifest;
  if (Object.keys(m._attachments ?? {}).length === 0) {
    await forgetWith(query, name);
    return;
  }
  await query(
    `INSERT INTO verdaccio_manifests (name, versions, dist_tags, rev)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (name) DO UPDATE SET
       versions = EXCLUDED.versions, dist_tags = EXCLUDED.dist_tags,
       rev = EXCLUDED.rev, updated_at = now()`,
    [
      name,
      JSON.stringify(Object.keys(m.versions ?? {})),
      JSON.stringify(m['dist-tags'] ?? {}),
      m._rev ?? null,
    ]
  );
}

async function forgetWith(query: Query, name: string): Promise<void> {
  await query('DELETE FROM verdaccio_manifests WHERE name = $1', [name]);
}
