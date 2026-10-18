import { existsSync } from 'node:fs';
import { createServer } from 'node:net';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import webExt from 'web-ext';
import { HEADLESS, OUTPUT, startPageServer, STEP_MS, text } from './support';

const FIREFOX = process.env.FIREFOX_BIN ?? '/usr/bin/firefox';

/**
 * Firefox's WebDriver BiDi can't navigate a web tab into the extension's process, can't
 * resize extension tabs, and sends no navigation or realm events for them. So the test
 * drives one extension tab (the welcome tab opened on install) and loads the other
 * pages into a same-origin iframe of the size they get in the browser, reading and
 * clicking through `contentDocument`.
 */
const FRAME = 'mf-e2e-frame';

async function mount(tab: Page, path: string, width: number, height: number): Promise<void> {
  await tab.evaluate(
    async (id, src, w, h) => {
      document.getElementById(id)?.remove();
      const frame = document.createElement('iframe');
      frame.id = id;
      frame.src = src;
      Object.assign(frame.style, { position: 'fixed', left: '0', top: '0', width: `${w}px`, height: `${h}px`, border: '0', zIndex: '9999' });
      const loaded = new Promise((done) => frame.addEventListener('load', done, { once: true }));
      document.body.append(frame);
      await loaded;
    },
    FRAME,
    path,
    width,
    height,
  );
}

/** Text of the first match inside the frame, once it exists. */
async function frameText(tab: Page, selector: string, timeout = STEP_MS): Promise<string> {
  const handle = await tab.waitForFunction(
    (id, sel) => (document.getElementById(id) as HTMLIFrameElement | null)?.contentDocument?.querySelector(sel)?.textContent?.trim() || false,
    { timeout, polling: 100 },
    FRAME,
    selector,
  );
  return (await handle.jsonValue()) as string;
}

async function frameCount(tab: Page, selector: string): Promise<number> {
  return tab.evaluate((id, sel) => (document.getElementById(id) as HTMLIFrameElement).contentDocument!.querySelectorAll(sel).length, FRAME, selector);
}

async function frameClick(tab: Page, selector: string): Promise<void> {
  await tab.evaluate(
    (id, sel) => ((document.getElementById(id) as HTMLIFrameElement).contentDocument!.querySelector(sel) as HTMLElement).click(),
    FRAME,
    selector,
  );
}

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

describe.skipIf(!existsSync(FIREFOX) || !existsSync(OUTPUT.firefox))('Firefox', () => {
  let runner: { exit(): Promise<void> } | null = null;
  let browser: Browser;
  let server: Awaited<ReturnType<typeof startPageServer>>;
  /** The welcome tab: an extension page the test opens the others from. */
  let tab: Page;

  beforeAll(async () => {
    server = await startPageServer();
    const port = await freePort();
    // WebDriver BiDi only drives moz-extension:// pages with system access, which the
    // browser reads from its environment (web-ext passes ours on).
    process.env.MOZ_REMOTE_ALLOW_SYSTEM_ACCESS = '1';
    // web-ext installs the build as a temporary add-on, like `npm run dev:firefox`.
    runner = await webExt.cmd.run(
      {
        sourceDir: OUTPUT.firefox,
        target: ['firefox-desktop'],
        firefox: FIREFOX,
        args: [`--remote-debugging-port=${port}`, ...(HEADLESS ? ['-headless'] : [])],
        pref: {
          'media.volume_scale': '0.0',
          'browser.shell.checkDefaultBrowser': false,
          'datareporting.policy.dataSubmissionEnabled': false,
        },
        startUrl: [server.url],
        noReload: true,
        noInput: true,
        browserConsole: false,
      },
      { shouldExitProgram: false },
    );
    browser = await puppeteer.connect({ browserWSEndpoint: `ws://127.0.0.1:${port}/session`, protocol: 'webDriverBiDi' });
  });

  afterAll(async () => {
    await browser?.disconnect();
    await runner?.exit();
    await server?.close();
  });

  it('opens the welcome tab on install', async () => {
    const target = await browser.waitForTarget((t) => /^moz-extension:\/\/[^/]+\/app\.html#\/welcome/.test(t.url()), { timeout: STEP_MS });
    tab = (await target.asPage())!;
    expect(await text(tab, '.welcome h1')).toBe('Where do you listen?');
  });

  it('renders the popup and runs a mock session through the event-page host', async () => {
    await mount(tab, '/popup.html', 380, 560);
    expect(await frameText(tab, '.first-run h1')).toBe('Where do you listen?');
    await frameClick(tab, '.first-run .acts .btn.text'); // Skip
    expect(await frameText(tab, '#stage-state')).toBe("Identify what's playing");
    // Held open until Stop, so "Listening" can't pass before the test looks.
    await tab.evaluate(() => localStorage.setItem('mf.mock.next', 'hold'));
    await frameClick(tab, '.listen .disc');
    await tab.waitForFunction(
      (id) => (document.getElementById(id) as HTMLIFrameElement).contentDocument?.querySelector('#stage-state')?.textContent?.includes('Listening'),
      { timeout: STEP_MS, polling: 100 },
      FRAME,
    );
    await frameClick(tab, '.listen .disc'); // Stop
    expect((await frameText(tab, '.hero .ttl', STEP_MS)).length).toBeGreaterThan(0);
    expect(await frameText(tab, '.split .main')).toBe('Open in Spotify');
  });

  it('renders the sidebar page and the library', async () => {
    await mount(tab, '/sidepanel.html', 360, 800);
    await frameText(tab, '.panel .list .row');
    expect(await frameCount(tab, '.panel .list .row')).toBeGreaterThanOrEqual(4);
    await mount(tab, '/app.html#/library', 1280, 800);
    expect(await frameText(tab, '.app .day.pinned')).toBe('Pinned');
    expect(await frameCount(tab, '.app .list .row')).toBeGreaterThanOrEqual(4);
  });
});
