import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { join, resolve } from 'node:path';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import webExt from 'web-ext';
import { DEMO_STATES, demoUrl } from '../../src/ui/demo/states';
import { OUTPUT, startPageServer } from '../e2e/support';

/**
 * Demo states laid out by Firefox: each reaches its state, nothing overflows sideways, and
 * the popup's stage slots sit where the design puts them (design spec §4), as in Chromium.
 * WebDriver BiDi can't navigate a tab into the extension or screenshot extension pages, so
 * each state loads in a same-origin iframe of its size inside the welcome tab (as in the
 * e2e test) and is measured there. Findings go to `.screens/firefox.json`.
 */
const FIREFOX = process.env.FIREFOX_BIN ?? '/usr/bin/firefox';
const OUT = resolve(process.env.SCREENS_DIR ?? '.screens');
const DEFAULT = ['popup-idle', 'popup-nomatch', 'popup-blocked', 'popup-result-menu', 'popup-candidates', 'panel-listen', 'panel-mix', 'sidebar-narrow', 'app-library-select', 'app-session-mix', 'app-settings-listening', 'app-narrow-library', 'mobile-result'];
/** Top of each stage slot in the 380 × 560 popup (design spec §4). */
const POPUP_SLOTS: Record<string, number> = { '.top': 10, '.chip-slot': 52, '.listen': 196, '.state:not(.two)': 312, '.sources': 352, '.recovery': 404, '.footer': 512 };
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
  const measured: unknown[] = [];

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
    writeFileSync(join(OUT, 'firefox.json'), JSON.stringify({ problems, measured }, null, 1));
    await browser?.disconnect();
    await runner?.exit();
    await server?.close();
  });

  it('lays out the chosen states like the design', async () => {
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
        const layout = await tab.evaluate(
          (id, slots) => {
            const doc = (document.getElementById(id) as HTMLIFrameElement).contentDocument!;
            const root = doc.querySelector('main, .app') as HTMLElement | null;
            const tops: Record<string, number | null> = {};
            for (const selector of Object.keys(slots)) {
              const el = doc.querySelector(`.popup > ${selector}`) as HTMLElement | null;
              tops[selector] = el ? Math.round(el.getBoundingClientRect().top) : null;
            }
            return { overflow: root ? root.scrollWidth - root.clientWidth : 0, tops };
          },
          FRAME,
          POPUP_SLOTS,
        );
        if (layout.overflow > 1) problems.push(`${state.id}/${theme}: ${layout.overflow}px sideways overflow`);
        for (const [selector, top] of Object.entries(layout.tops)) {
          if (top !== null && Math.abs(top - POPUP_SLOTS[selector]!) > 1) problems.push(`${state.id}/${theme}: ${selector} at ${top}, design ${POPUP_SLOTS[selector]}`);
        }
        measured.push({ state: state.id, theme, ...layout });
      }
    }
    expect(problems).toEqual([]);
  });
});
