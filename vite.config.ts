import {resolve} from 'path';
import {defineConfig} from 'vite';
import dts from 'vite-plugin-dts';

export default defineConfig({
  plugins: [
    dts({
      rollupTypes: true,
      tsconfigPath: './tsconfig.json',
    }),
  ],
  build: {
    lib: {
      entry: {
        index: resolve(__dirname, 'src/index.ts'),
        s3DatabasePostgres: resolve(__dirname, 'src/s3DatabasePostgres.ts'),
      },
      formats: ['es'],
      fileName: (_format, name) => `${name}.js`,
    },
    rollupOptions: {
      output: {
        exports: 'named',
      },
      external: [
        // Node builtins
        /^node:/,
        'stream',
        'path',
        'crypto',
        // AWS SDK
        /^@aws-sdk\//,
        /^@smithy\//,
        // Verdaccio
        /^@verdaccio\//,
        // Other deps
        'debug',
        'pg',
        'http-errors',
      ],
    },
    outDir: 'lib',
    sourcemap: true,
    minify: false,
  },
});
