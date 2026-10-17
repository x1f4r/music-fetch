import { svelte } from '@sveltejs/vite-plugin-svelte';
import { svelteTesting } from '@testing-library/svelte/vite';
import { defineConfig } from 'vitest/config';
import { WxtVitest } from 'wxt/testing/vitest-plugin';

// Unit tests: WXT's fake browser (storage, runtime, tabs) and aliases, the Svelte
// compiler, jsdom for components.
export default defineConfig({
  plugins: [WxtVitest(), svelte(), svelteTesting()],
  test: {
    environment: 'jsdom',
    include: ['tests/unit/**/*.test.ts'],
    restoreMocks: true,
  },
});
