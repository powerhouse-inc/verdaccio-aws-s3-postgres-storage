import type {Config} from '@verdaccio/types';

/** Writes to the index of published manifests kept beside the files in S3. */
export interface ManifestIndexWriter {
  record(name: string, manifest: object): Promise<void>;
  forget(name: string): Promise<void>;
}

/** Keeps an index of published manifests beside the files in S3. */
export interface ManifestIndex extends ManifestIndexWriter {
  /** The indexed manifest's `_rev`, or null when it isn't indexed. */
  revision(name: string): Promise<string | null>;
  // Runs fn holding the package's lock; writes through `index` commit with it
  withPackageLock<T>(name: string, fn: (index: ManifestIndexWriter) => Promise<T>): Promise<T>;
}

export interface S3Config extends Config {
  bucket: string;
  keyPrefix: string;
  endpoint?: string;
  region?: string;
  s3ForcePathStyle?: boolean;
  tarballACL?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  proxy?: string;
  // Keeps the package list, secret, tokens and manifest index in Postgres
  postgresUrl: string;
  /** Connections for reads and index writes outside a lock (default 2) */
  postgresPoolMax?: number | string;
  /** Connections holding package locks, one per concurrent write (default 4) */
  postgresLockPoolMax?: number | string;
}
