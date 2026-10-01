import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    globals: true,
    environment: 'node',
    server: {
      deps: {
        inline: ['@verdaccio/core', '@verdaccio/streams'],
      },
    },
    coverage: {
      exclude: ['node_modules', '_storage', 'fixtures'],
    },
  },
});
