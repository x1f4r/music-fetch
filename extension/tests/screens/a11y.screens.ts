import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join, resolve } from 'node:path';
import puppeteer, { type Browser } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DEMO_STATES, demoUrl } from '../../src/ui/demo/states';
import { OUTPUT, tempProfile } from '../e2e/support';

/**
 * An accessibility audit of every demo state with axe-core (WCAG 2.1 A/AA rules: names,
 * roles, ARIA, contrast, landmarks), light and dark. Writes `.screens/a11y.json`; fails on
 * serious or critical findings. Run with the screenshots: `npm run screens`.
 */
const CHROMIUM = process.env.CHROMIUM_BIN ?? '/usr/bin/chromium';
const OUT = resolve(process.env.SCREENS_DIR ?? '.screens');
const ONLY = (process.env.SCREENS ?? '').split(',').filter(Boolean);
const AXE = readFileSync(createRequire(import.meta.url).resolve('axe-core/axe.min.js'), 'utf8');

interface Finding {
  state: string;
  theme: string;
  id: string;
  impact: string | null;
  help: string;
  targets: string[];
}

describe.skipIf(!existsSync(CHROMIUM) || !existsSync(OUTPUT.chromium))('accessibility', () => {
  let browser: Browser;
  let id = '';
  const findings: Finding[] = [];

  beforeAll(async () => {
    mkdirSync(OUT, { recursive: true });
    browser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: true,
      pipe: true,
      protocolTimeout: 60_000,
      enableExtensions: [OUTPUT.chromium],
      userDataDir: tempProfile('a11y'),
      args: ['--no-first-run', '--no-default-browser-check', '--mute-audio', '--disable-gpu', '--use-angle=swiftshader', '--lang=en-US'],
    });
    const deadline = Date.now() + 120_000;
    while (!id && Date.now() < deadline) {
      id = [...(await browser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!id) await new Promise((done) => setTimeout(done, 300));
    }
    if (!id) throw new Error('the extension did not load');
  });

  afterAll(async () => {
    writeFileSync(join(OUT, 'a11y.json'), JSON.stringify(findings, null, 1));
    await browser?.close();
  });

  it('finds nothing serious in any state', async () => {
    for (const state of DEMO_STATES.filter((s) => ONLY.length === 0 || ONLY.includes(s.id))) {
      for (const theme of ['light', 'dark'] as const) {
        const page = await browser.newPage();
        try {
          await page.setViewport({ width: state.width, height: state.height });
          await page.emulateMediaFeatures([{ name: 'prefers-color-scheme', value: theme }]);
          await page.goto(`chrome-extension://${id}/${demoUrl(state, { theme, still: true })}`);
          await page.waitForSelector('html[data-demo-ready]', { timeout: 15_000 });
          await page.evaluate(AXE);
          const result = (await page.evaluate(async () => {
            const axe = (globalThis as unknown as { axe: { run(context: unknown, options: unknown): Promise<unknown> } }).axe;
            return axe.run(document, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'best-practice'] }, resultTypes: ['violations'] });
          })) as { violations: { id: string; impact: string | null; help: string; nodes: { target: string[] }[] }[] };
          for (const v of result.violations) {
            findings.push({ state: state.id, theme, id: v.id, impact: v.impact, help: v.help, targets: v.nodes.slice(0, 6).map((n) => n.target.join(' ')) });
          }
        } finally {
          await page.close().catch(() => undefined);
        }
      }
    }
    const serious = findings.filter((f) => f.impact === 'serious' || f.impact === 'critical');
    expect(serious.map((f) => `${f.state}/${f.theme} ${f.id}: ${f.targets.join(', ')}`)).toEqual([]);
  });
});
