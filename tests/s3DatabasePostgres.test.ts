import type {Logger, Token} from '@verdaccio/types';

import S3DatabasePostgres from '../src/s3DatabasePostgres.js';

import {PGlite} from '@electric-sql/pglite';
import pg from 'pg';
import type {Pool} from 'pg';
import {afterEach, describe, expect, test} from 'vitest';

// Set to run against a real server, e.g. postgres://postgres:postgres@localhost:5432/verdaccio
const PG_URL = process.env.VERDACCIO_S3_STORAGE_TEST_PG_URL;

const logger = {
  trace: () => {},
  debug: () => {},
  warn: () => {},
} as unknown as Logger;

// A one-connection pool over PGlite; connect() waits until the previous client is released
function pglitePool(db: PGlite): Pool {
  let queue = Promise.resolve();
  const query = (text: string, values?: unknown[]) => db.query(text, values);
  return {
    query,
    connect: () => {
      let release!: () => void;
      const released = new Promise<void>((resolve) => (release = resolve));
      const acquired = queue.then(() => ({query, release}));
      queue = queue.then(() => released);
      return acquired;
    },
  } as unknown as Pool;
}

type Backend = {pool: () => Promise<Pool>; close: () => Promise<void>};

function pgliteBackend(): Backend {
  const db = new PGlite();
  const pool = pglitePool(db);
  return {pool: () => Promise.resolve(pool), close: () => db.close()};
}

function postgresBackend(url: string): Backend {
  const pools: Pool[] = [];
  const admin = new pg.Pool({connectionString: url, max: 1});
  let reset: Promise<void> | undefined;
  return {
    pool: async () => {
      reset ??= admin
        .query('DROP TABLE IF EXISTS verdaccio_packages, verdaccio_tokens, verdaccio_secret')
        .then(() => undefined);
      await reset;
      const pool = new pg.Pool({connectionString: url});
      pools.push(pool);
      return pool;
    },
    close: async () => {
      await Promise.all([...pools, admin].map((pool) => pool.end()));
    },
  };
}

function token(user: string, key: string, extra: Partial<Token> = {}): Token {
  return {
    user,
    key,
    token: `${user}-${key}`,
    readonly: false,
    created: 1700000000000,
    ...extra,
  };
}

const backends: [string, () => Backend][] = [['pglite', pgliteBackend]];
if (PG_URL) backends.push(['postgres', () => postgresBackend(PG_URL)]);

describe.each(backends)('S3DatabasePostgres (%s)', (_, createBackend) => {
  let backend: Backend;

  async function database(): Promise<S3DatabasePostgres> {
    const db = new S3DatabasePostgres(await backend.pool(), logger);
    await db.init();
    return db;
  }

  afterEach(async () => {
    await backend.close();
  });

  test('init is idempotent', async () => {
    backend = createBackend();
    const db = await database();
    await db.init();
    expect(await db.get()).toEqual([]);
  });

  test('add, get and remove packages', async () => {
    backend = createBackend();
    const db = await database();
    await db.add('@scope/b');
    await db.add('a');
    await db.add('a');
    expect(await db.get()).toEqual(['@scope/b', 'a']);

    await db.remove('a');
    await db.remove('missing');
    expect(await db.get()).toEqual(['@scope/b']);
  });

  test('search returns matching names in order, up to the limit', async () => {
    backend = createBackend();
    const db = await database();
    for (const name of ['@scope/ledger', 'ledger-ui', 'other', 'a-ledger']) await db.add(name);
    expect(await db.search('ledger', null)).toEqual(['@scope/ledger', 'a-ledger', 'ledger-ui']);
    expect(await db.search('ledger', 2)).toEqual(['@scope/ledger', 'a-ledger']);
    expect(await db.search('%', null)).toEqual([]);
    expect(await db.search('', 1)).toEqual(['@scope/ledger']);
  });

  test('indexes published manifests and drops uplink-cached ones', async () => {
    backend = createBackend();
    const db = await database();
    const pool = await backend.pool();
    const published = {
      _rev: '3-a',
      versions: {'1.0.0': {}, '1.1.0': {}},
      'dist-tags': {latest: '1.1.0'},
      _attachments: {'a-1.1.0.tgz': {}},
    };
    await db.record('a', published);
    const rows = await pool.query<{
      versions: string[];
      dist_tags: Record<string, string>;
      rev: string;
    }>("SELECT versions, dist_tags, rev FROM verdaccio_manifests WHERE name = 'a'");
    expect(rows.rows).toEqual([
      {
        versions: ['1.0.0', '1.1.0'],
        dist_tags: {latest: '1.1.0'},
        rev: '3-a',
      },
    ]);

    await db.record('a', {...published, _attachments: {}});
    await db.record('b', published);
    await db.forget('b');
    const left = await pool.query('SELECT name FROM verdaccio_manifests');
    expect(left.rows).toEqual([]);
  });

  test('getSecret stores a secret on first read and keeps it', async () => {
    backend = createBackend();
    const db = await database();
    const secret = await db.getSecret();
    expect(secret).toHaveLength(32);
    expect(await db.getSecret()).toBe(secret);

    const replacement = 'x'.repeat(32);
    await db.setSecret(replacement);
    expect(await db.getSecret()).toBe(replacement);
  });

  test('saves, replaces, reads and deletes tokens per user', async () => {
    backend = createBackend();
    const db = await database();
    await db.saveToken(token('alice', 'k2', {cidr: ['10.0.0.0/8'], created: '2026-09-30'}));
    await db.saveToken(token('alice', 'k1'));
    await db.saveToken(token('bob', 'k1'));
    await db.saveToken(token('alice', 'k1', {readonly: true}));

    expect(await db.readTokens({user: 'alice'})).toEqual([
      token('alice', 'k1', {readonly: true}),
      token('alice', 'k2', {cidr: ['10.0.0.0/8'], created: '2026-09-30'}),
    ]);

    await db.deleteToken('alice', 'k1');
    expect(await db.readTokens({user: 'alice'})).toEqual([
      token('alice', 'k2', {cidr: ['10.0.0.0/8'], created: '2026-09-30'}),
    ]);
    expect(await db.readTokens({user: 'bob'})).toEqual([token('bob', 'k1')]);
    expect(await db.readTokens({user: 'carol'})).toEqual([]);
  });

  test('queries set up the schema when init was never called', async () => {
    backend = createBackend();
    const db = new S3DatabasePostgres(await backend.pool(), logger);
    expect(await db.getSecret()).toHaveLength(32);
    await db.add('a');
    expect(await db.get()).toEqual(['a']);
  });

  test('a failed schema setup is retried by the next query', async () => {
    backend = createBackend();
    const pool = await backend.pool();
    const connect = pool.connect.bind(pool);
    let failures = 1;
    const flaky = Object.assign(Object.create(pool) as Pool, {
      query: pool.query.bind(pool),
      connect: () => (failures-- > 0 ? Promise.reject(new Error('connection refused')) : connect()),
    });
    const db = new S3DatabasePostgres(flaky, logger);
    await expect(db.add('a')).rejects.toThrow('connection refused');
    await db.add('a');
    expect(await db.get()).toEqual(['a']);
  });

  test('instances starting together share one schema, secret and package list', async () => {
    backend = createBackend();
    const instances = await Promise.all(
      Array.from({length: 4}, async () => new S3DatabasePostgres(await backend.pool(), logger))
    );
    await Promise.all(instances.map((db) => db.init()));

    const secrets = await Promise.all(instances.map((db) => db.getSecret()));
    expect(new Set(secrets).size).toBe(1);

    await Promise.all(instances.map((db, i) => db.add(`pkg-${i}`)));
    for (const db of instances) {
      expect(await db.get()).toEqual(['pkg-0', 'pkg-1', 'pkg-2', 'pkg-3']);
    }
  });
});
