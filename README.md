# @powerhousedao/verdaccio-s3-storage

Verdaccio 7 storage plugin that keeps package files in S3 and the registry state (package list, secret, tokens) in Postgres, so several Verdaccio instances can share one registry.

Forked from [verdaccio-aws-s3-storage](https://github.com/verdaccio/verdaccio-aws-s3-storage) 12.1.3 (MIT). Changes from upstream:

- **Verdaccio 7 API.** The package storage and the package list use the promise-based storage API of Verdaccio 7 (`@verdaccio/store` 9). Upstream still uses the callback API, which Verdaccio 7.0.0-next-7.29 crashes on.
- **Postgres backend.** `postgresUrl` stores the registry state in Postgres, and is required. Upstream's DynamoDB backend, and its single JSON file in the bucket that every instance reads once and overwrites, are removed.
- **Search** returns published packages whose name contains the query text.

## Install

```bash
npm install @powerhousedao/verdaccio-s3-storage
```

Or copy the built package into Verdaccio's `plugins` folder as `@powerhousedao/verdaccio-s3-storage`, as the [Dockerfile](Dockerfile) does.

## Configuration

```yaml
store:
  '@powerhousedao/verdaccio-s3-storage':
    bucket: registry
    keyPrefix: prod # optional
    endpoint: https://nbg1.your-objectstorage.com # optional
    region: eu-central # optional
    s3ForcePathStyle: true # optional, for MinIO and most S3-compatible stores
    accessKeyId: S3_ACCESS_KEY_ID # optional, uses the AWS credential chain if omitted
    secretAccessKey: S3_SECRET_ACCESS_KEY # optional
    postgresUrl: DATABASE_URL
    postgresPoolMax: 2 # optional: reads and writes outside a package lock
    postgresLockPoolMax: 4 # optional: one connection per concurrent manifest write
```

A value that names a set environment variable is replaced by that variable, as in upstream.

## Postgres

The plugin creates its tables on first use, under an advisory lock so instances starting together don't race:

| Table                 | Holds                                                                                             |
| --------------------- | ------------------------------------------------------------------------------------------------- |
| `verdaccio_packages`  | names of packages published to this registry                                                      |
| `verdaccio_tokens`    | npm tokens, per user and key                                                                      |
| `verdaccio_secret`    | the signing secret, stored by the first instance to start                                         |
| `verdaccio_manifests` | versions, dist-tags and revision of each package published here, written with every manifest save |

Package manifests, tarballs and dist-tags stay in S3.

## Tests

```bash
pnpm build && pnpm test
```

See [LOCAL_DEV.md](LOCAL_DEV.md) for the Docker setup with LocalStack and Postgres.

The verifier test loads the built package. The S3 package storage tests run against real S3, and the Postgres tests also run against a real server, when these are set:

```bash
VERDACCIO_S3_STORAGE_TEST_S3_ENDPOINT=http://localhost:9000   # MinIO, minio / minio12345
VERDACCIO_S3_STORAGE_TEST_S3_ACCESS_KEY=test                  # optional, e.g. for LocalStack
VERDACCIO_S3_STORAGE_TEST_S3_SECRET_KEY=test
VERDACCIO_S3_STORAGE_TEST_PG_URL=postgres://postgres:postgres@localhost:5432/verdaccio
```

Without them, the Postgres tests run on PGlite and the S3 tests are skipped. CI sets them against Postgres and LocalStack.
