import { existsSync } from 'node:fs';
import { join } from 'node:path';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ARTIFACTS, HEADLESS, OUTPUT, popupFlow, startPageServer, tempProfile, text } from './support';

const CHROMIUM = process.env.CHROMIUM_BIN ?? '/usr/bin/chromium';

describe.skipIf(!existsSync(CHROMIUM) || !existsSync(OUTPUT.chromium))('Chromium', () => {
  let browser: Browser;
  let server: Awaited<ReturnType<typeof startPageServer>>;
  let id = '';

  beforeAll(async () => {
    server = await startPageServer();
    browser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: HEADLESS,
      pipe: true,
      enableExtensions: [OUTPUT.chromium],
      userDataDir: tempProfile('chromium'),
      defaultViewport: null,
      args: ['--no-first-run', '--no-default-browser-check', '--mute-audio', '--disable-gpu', '--window-size=1280,860'],
    });
    // The extension finishes loading after launch returns (slowly on a busy machine), and
    // its service worker may already be idle again: poll the browser's extension list.
    const deadline = Date.now() + 180_000;
    while (!id && Date.now() < deadline) {
      id = [...(await browser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!id) await new Promise((done) => setTimeout(done, 500));
    }
    if (!id) throw new Error('the extension did not load');
  });

  afterAll(async () => {
    await browser?.close();
    await server?.close();
  });

  it('opens the welcome tab on install with the provider panel', async () => {
    const target = await browser.waitForTarget((t) => t.url().startsWith(`chrome-extension://${id}/app.html#/welcome`), { timeout: 30_000 });
    const welcome = (await target.asPage())!;
    expect(await text(welcome, '.welcome h1')).toBe('Where do you listen?');
    await welcome.screenshot({ path: join(ARTIFACTS, 'chromium-0-welcome.png') });
    await welcome.close();
  });

  it('renders the popup from the toolbar action and runs a mock session', async () => {
    const page = await browser.newPage();
    await page.goto(server.url);
    const extension = (await browser.extensions()).get(id)!;
    let popup: Page;
    try {
      await extension.triggerAction(page);
      const target = await browser.waitForTarget((t) => t.url() === `chrome-extension://${id}/popup.html`, { timeout: 10_000 });
      popup = (await target.asPage())!;
    } catch {
      // No action UI (e.g. headless without a toolbar): the same page in a tab.
      popup = await browser.newPage();
      await popup.setViewport({ width: 380, height: 560 });
      await popup.goto(`chrome-extension://${id}/popup.html`);
    }
    const result = await popupFlow(popup, 'chromium');
    expect(result.title.length).toBeGreaterThan(0);
    expect(result.openIn).toBe('Open in Spotify');

    // The engine runs in the offscreen document, not in the service worker.
    expect(browser.targets().some((t) => t.url() === `chrome-extension://${id}/offscreen.html`)).toBe(true);
    type Chrome = { chrome: { storage: { sync: { get(key: string): Promise<unknown> } } } };
    const decision = await popup.evaluate(() => (globalThis as unknown as Chrome).chrome.storage.sync.get('providerDecision'));
    expect(decision).toEqual({ providerDecision: { provider: 'spotify', skipped: true } });
  });

  it('shows the session in the side panel and the library', async () => {
    const panel = await browser.newPage();
    await panel.setViewport({ width: 360, height: 800 });
    await panel.goto(`chrome-extension://${id}/sidepanel.html`);
    await panel.waitForSelector('.panel .list .row', { timeout: 15_000 });
    expect(await panel.$$eval('.panel .list .row', (rows) => rows.length)).toBeGreaterThanOrEqual(4);
    await panel.screenshot({ path: join(ARTIFACTS, 'chromium-5-panel.png') });

    const app = await browser.newPage();
    await app.goto(`chrome-extension://${id}/app.html#/library`);
    await app.waitForSelector('.app .list .row', { timeout: 15_000 });
    expect(await app.$$eval('.app .list .row', (rows) => rows.length)).toBeGreaterThanOrEqual(4);
    expect(await text(app, '.app .day.pinned')).toBe('Pinned');
    await app.screenshot({ path: join(ARTIFACTS, 'chromium-6-library.png') });
  });

  it('declares no content scripts and no broad host access', async () => {
    const page = await browser.newPage();
    await page.goto(`chrome-extension://${id}/manifest.json`);
    const manifest = JSON.parse(await page.$eval('body', (b) => b.textContent ?? '{}')) as Record<string, unknown>;
    expect(manifest.name).toBe('Music Fetch');
    expect(manifest.content_scripts).toBeUndefined();
    expect(manifest.host_permissions).not.toContain('<all_urls>');
  });
});
