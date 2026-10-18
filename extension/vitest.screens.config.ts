import { defineConfig } from 'vitest/config';

// Screenshots of every demo state (tests/screens). Run after `npm run build:chrome`.
export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/screens/**/*.screens.ts'],
    testTimeout: 1_800_000,
    hookTimeout: 240_000,
  },
});
