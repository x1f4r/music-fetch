import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { join, resolve } from 'node:path';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import webExt from 'web-ext';
import { DEMO_STATES, demoUrl } from '../../src/ui/demo/states';
import { OUTPUT, startPageServer } from '../e2e/support';

/**
 * Demo states rendered by Firefox, to compare with Chromium's (`.screens/firefox-*.png`).
 * WebDriver BiDi can't navigate a tab into the extension, so each state loads in a
 * same-origin iframe of its size inside the welcome tab (as in the e2e test).
 */
const FIREFOX = process.env.FIREFOX_BIN ?? '/usr/bin/firefox';
const OUT = resolve(process.env.SCREENS_DIR ?? '.screens');
const DEFAULT = ['popup-idle', 'popup-result-menu', 'popup-candidates', 'panel-mix', 'sidebar-narrow', 'app-library-select', 'app-session-mix', 'mobile-result'];
const ONLY = (process.env.FIREFOX_SCREENS ?? DEFAULT.join(',')).split(',').filter(Boolean);
const FRAME = 'mf-screens-frame';

async function freePort(): Promise<number> {
  return new Promise((done, fail) => {
    const probe = createServer();
    probe.once('error', fail);
    probe.listen(0, '127.0.0.1', () => {
      const address = probe.address();
      probe.close(() => (typeof address === 'object' && address ? done(address.port) : fail(new Error('no port'))));
    });
  });
}

describe.skipIf(!existsSync(FIREFOX) || !existsSync(OUTPUT.firefox))('demo states in Firefox', () => {
  let runner: { exit(): Promise<void> } | null = null;
  let browser: Browser;
  let server: Awaited<ReturnType<typeof startPageServer>>;
  let tab: Page;
  const problems: string[] = [];

  beforeAll(async () => {
    mkdirSync(OUT, { recursive: true });
    server = await startPageServer();
    const port = await freePort();
    process.env.MOZ_REMOTE_ALLOW_SYSTEM_ACCESS = '1';
    runner = await webExt.cmd.run(
      {
        sourceDir: OUTPUT.firefox,
        target: ['firefox-desktop'],
        firefox: FIREFOX,
        args: [`--remote-debugging-port=${port}`, '-headless', '--window-size=1400,1000'],
        pref: { 'media.volume_scale': '0.0', 'browser.shell.checkDefaultBrowser': false, 'datareporting.policy.dataSubmissionEnabled': false },
        startUrl: [server.url],
        noReload: true,
        noInput: true,
        browserConsole: false,
      },
      { shouldExitProgram: false },
    );
    browser = await puppeteer.connect({ browserWSEndpoint: `ws://127.0.0.1:${port}/session`, protocol: 'webDriverBiDi' });
    const target = await browser.waitForTarget((t) => /^moz-extension:\/\/[^/]+\/app\.html#\/welcome/.test(t.url()), { timeout: 90_000 });
    tab = (await target.asPage())!;
  });

  afterAll(async () => {
    writeFileSync(join(OUT, 'firefox.json'), JSON.stringify(problems, null, 1));
    await browser?.disconnect();
    await runner?.exit();
    await server?.close();
  });

  it('renders the chosen states', async () => {
    for (const state of DEMO_STATES.filter((s) => ONLY.includes(s.id))) {
      for (const theme of ['light', 'dark'] as const) {
        await tab.evaluate(
          async (id, src, w, h) => {
            document.getElementById(id)?.remove();
            document.body.style.margin = '0';
            const frame = document.createElement('iframe');
            frame.id = id;
            frame.src = src;
            Object.assign(frame.style, { position: 'fixed', left: '0', top: '0', width: `${w}px`, height: `${h}px`, border: '0', zIndex: '9999', background: 'white' });
            const loaded = new Promise((done) => frame.addEventListener('load', done, { once: true }));
            document.body.append(frame);
            await loaded;
          },
          FRAME,
          `/${demoUrl(state, { theme, still: true })}`,
          state.width,
          state.height,
        );
        const ready = await tab
          .waitForFunction((id) => (document.getElementById(id) as HTMLIFrameElement | null)?.contentDocument?.documentElement.dataset.demoReady ?? false, { timeout: 30_000, polling: 100 }, FRAME)
          .then((h) => h.jsonValue() as Promise<string | false>)
          .catch(() => 'never');
        if (ready !== '1') problems.push(`${state.id}/${theme}: ${String(ready)}`);
        await tab.screenshot({ path: join(OUT, `firefox-${state.id}-${theme}.png`) as `${string}.png`, clip: { x: 0, y: 0, width: state.width, height: state.height } });
      }
    }
    expect(problems).toEqual([]);
  });
});
