# Local Development

Requires Node.js 24 (`.nvmrc`), pnpm via corepack, and Docker for the end-to-end setup.

```bash
corepack enable && corepack install
pnpm install
pnpm type-check
pnpm lint
pnpm build
pnpm test
pnpm format
```

## Running Verdaccio with Docker

`docker compose up --build` starts:

- LocalStack (S3 only) on port 4566, with the bucket `verdaccio-storage`;
- Postgres 16 on port 5432 (`verdaccio` / `verdaccio`);
- Verdaccio on http://localhost:4873, with this plugin built into the image and `conf/config.yaml`.

The plugin creates its tables on first start.

```bash
docker compose logs verdaccio | grep "verdaccio-s3-storage"   # plugin loaded
pnpm verdaccioctl ping -r http://localhost:4873
pnpm verdaccioctl login -u test -p test1234 -r http://localhost:4873
pnpm e2e:registry --pm npm --pm pnpm                          # registry battery
pnpm e2e:ui:run                                               # Cypress UI tests
```

Inspect the stored state:

```bash
aws --endpoint-url http://localhost:4566 s3 ls s3://verdaccio-storage --recursive
docker compose exec postgres psql -U verdaccio -c 'select * from verdaccio_manifests'
```

Rebuild after code changes with `docker compose up --build -d verdaccio`. `docker compose down -v` stops everything and wipes the data.

## Debug logging

Set `DEBUG=verdaccio:plugin*` (the compose file does) for all plugin namespaces, or e.g. `DEBUG=verdaccio:plugin:aws-s3-storage*` for one.
