import { defineConfig } from 'wxt';
import { buildManifest } from './src/platform/manifest';

// https://wxt.dev/api/config.html
export default defineConfig({
  modules: ['@wxt-dev/module-svelte'],
  manifestVersion: 3,
  // Explicit imports everywhere: easier to read, and the same code runs under Vitest.
  imports: false,
  manifest: ({ browser, mode }) => buildManifest({ browser, mode }),
  zip: {
    artifactTemplate: 'MusicFetch-{{version}}-{{browser}}.zip',
    sourcesTemplate: 'MusicFetch-{{version}}-sources.zip',
  },
  vite: () => ({
    build: {
      // Readable bundles for store review; the engine wasm is the size that matters.
      minify: false,
      sourcemap: false,
    },
  }),
});
