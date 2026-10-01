import {verifyPlugin} from '@verdaccio/plugin-verifier';

import {mkdirSync, mkdtempSync, rmSync, symlinkSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {afterAll, describe, expect, test} from 'vitest';

const PLUGIN = '@powerhousedao/verdaccio-s3-storage';

// A plugins folder holding this package under its scoped name, as Verdaccio would find it
const pluginsFolder = mkdtempSync(join(tmpdir(), 'vs3-plugins-'));
mkdirSync(join(pluginsFolder, '@powerhousedao'));
symlinkSync(resolve(import.meta.dirname, '..'), join(pluginsFolder, PLUGIN), 'dir');

afterAll(() => rmSync(pluginsFolder, {recursive: true, force: true}));

describe('Plugin loading verification', () => {
  test('should be loadable by verdaccio as a storage plugin', async () => {
    const result = await verifyPlugin({
      pluginPath: PLUGIN,
      category: 'storage',
      pluginsFolder,
      pluginConfig: {
        bucket: 'test-bucket',
        keyPrefix: 'test/',
        region: 'us-east-1',
        postgresUrl: 'postgres://verdaccio@localhost:5432/verdaccio',
      },
    });

    expect(result.error).toBeUndefined();
    expect(result.success).toBe(true);
    expect(result.pluginsLoaded).toBe(1);
  });
});
