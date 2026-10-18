import { existsSync } from 'node:fs';
import { join } from 'node:path';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ARTIFACTS, HEADLESS, tempProfile } from './support';
import { writeFileSync } from 'node:fs';
import { E2E_OUTPUT, RUNTIME_E2E, echoed, eventsOf, micFile, rpc, sleep, startFixtureServer, trackEvents, waitForSession, type FixtureServer } from './runtime-support';

const CHROMIUM = process.env.CHROMIUM_BIN ?? '/usr/bin/chromium';
const offscreenLog: string[] = [];

/** The offscreen document's console, for a failing run (it is no page puppeteer can open). */
function watchOffscreen(browser: Browser) {
  browser.on('targetchanged', async (target) => {
    if (!target.url().includes('/offscreen.html') || (target as unknown as { mfWatched?: boolean }).mfWatched) return;
    (target as unknown as { mfWatched?: boolean }).mfWatched = true;
    try {
      const cdp = await target.createCDPSession();
      cdp.on('Runtime.consoleAPICalled', (e: { type: string; args: { value?: unknown; description?: string }[] }) =>
        offscreenLog.push(`${e.type} ${e.args.map((a) => a.value ?? a.description).join(' ')}`),
      );
      cdp.on('Runtime.exceptionThrown', (e: { exceptionDetails: { text: string; exception?: { description?: string } } }) =>
        offscreenLog.push(`exception ${e.exceptionDetails.exception?.description ?? e.exceptionDetails.text}`),
      );
      await cdp.send('Runtime.enable');
    } catch (error) {
      offscreenLog.push(`no CDP session: ${String(error)}`);
    }
  });
}

/**
 * The runtime in Chromium: the offscreen engine host and its Worker, tab capture, the
 * fake microphone, the header rules and the capture coordinator, against local pages
 * that play synthetic songs. Headful (Xvfb) so the toolbar action opens a real popup:
 * tab capture needs the extension to be invoked on the tab.
 */
describe.skipIf(!RUNTIME_E2E || !existsSync(CHROMIUM) || !existsSync(E2E_OUTPUT.chromium))('Chromium runtime', () => {
  let browser: Browser;
  let server: FixtureServer;
  let id = '';
  let app: Page;

  beforeAll(async () => {
    server = await startFixtureServer();
    browser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: HEADLESS,
      pipe: true,
      enableExtensions: [E2E_OUTPUT.chromium],
      userDataDir: tempProfile('rt-chromium'),
      defaultViewport: null,
      args: [
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-gpu',
        '--window-size=1280,860',
        // Output goes to the runner's private null sink (PULSE_SINK). Not --disable-audio-output:
        // without an output device the captured tab audio comes out garbled. No fake media
        // devices either: they replace tab capture too (the microphone test has its own browser).
        '--autoplay-policy=no-user-gesture-required',
      ],
    });
    watchOffscreen(browser);
    const deadline = Date.now() + 180_000;
    while (!id && Date.now() < deadline) {
      id = [...(await browser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!id) await sleep(500);
    }
    if (!id) throw new Error('the extension did not load');
    // Skip the first-run provider panel once, in the welcome tab.
    const welcome = await (await browser.waitForTarget((t) => t.url().startsWith(`chrome-extension://${id}/app.html#/welcome`), { timeout: 60_000 })).asPage();
    await welcome!.waitForSelector('.welcome', { timeout: 30_000 });
    await welcome!.evaluate(() => (globalThis as unknown as { chrome: { storage: { sync: { set(v: unknown): Promise<void> } } } }).chrome.storage.sync.set({ providerDecision: { provider: 'spotify', skipped: true } }));
    await welcome!.close();
    app = await browser.newPage();
    await app.goto(`chrome-extension://${id}/app.html#/library`);
    await trackEvents(app);
  });

  afterAll(async () => {
    writeFileSync(join(ARTIFACTS, 'rt-chromium-offscreen.log'), offscreenLog.join('\n'));
    await browser?.close();
    await server?.close();
  });

  /** A tab playing `path`, once its media plays. */
  async function playingTab(path: string): Promise<Page> {
    const page = await browser.newPage();
    await page.goto(`${server.origin}${path}`);
    await page.waitForFunction(() => {
      const el = document.querySelector('audio, video') as HTMLMediaElement | null;
      return el && !el.paused && el.currentTime > 0.3;
    }, { timeout: 20_000 });
    return page;
  }

  /** The toolbar action on `page`: the popup, which also invokes the extension on that tab. */
  async function openPopup(page: Page): Promise<Page> {
    await page.bringToFront();
    const extension = (await browser.extensions()).get(id)!;
    await extension.triggerAction(page);
    const target = await browser.waitForTarget((t) => t.url() === `chrome-extension://${id}/popup.html`, { timeout: 15_000 });
    const popup = (await target.asPage())!;
    await popup.waitForSelector('.listen .disc', { timeout: 20_000 });
    return popup;
  }

  async function listenFrom(popup: Page, source: 'This tab' | 'Microphone') {
    await popup.evaluate((label) => {
      const button = [...document.querySelectorAll<HTMLButtonElement>('.sources button')].find((b) => b.textContent?.trim() === label);
      button?.click();
    }, source);
    await popup.click('.listen .disc');
  }

  async function tabIdOf(url: string): Promise<number> {
    return app.evaluate(async (u) => {
      const tabs = await (globalThis as unknown as { chrome: { tabs: { query(q: unknown): Promise<{ id: number; url: string }[]> } } }).chrome.tabs.query({});
      return tabs.find((t) => t.url === u)!.id;
    }, url);
  }

  it('identifies a tab with tab capture in the offscreen document, and the tab stays audible', async () => {
    const page = await playingTab('/pages/generic.html?src=/media/song-a.wav&autoplay');
    const popup = await openPopup(page);
    await listenFrom(popup, 'This tab');
    await popup
      .waitForFunction(() => document.querySelector('.hero .ttl')?.textContent?.includes('Test Signal A'), { timeout: 45_000 })
      .catch(async (error: unknown) => {
        await popup.screenshot({ path: join(ARTIFACTS, 'rt-chromium-tab-failed.png') }).catch(() => undefined);
        const [newest] = await rpc<{ id: string }[]>(app, 'mf-engine', 'library', [{ kinds: [], search: null, pinned_only: false, limit: 1, offset: 0 }]);
        throw new Error(`no Test Signal A: ${newest ? await eventsOf(app, newest.id) : 'no session'} · sources ${JSON.stringify(await rpc(app, 'mf-host', 'sources'))}`, { cause: error });
      });
    await popup.screenshot({ path: join(ARTIFACTS, 'rt-chromium-tab-result.png') });
    expect(browser.targets().some((t) => t.url() === `chrome-extension://${id}/offscreen.html`)).toBe(true);
    await popup.close();
    await page.close();
  });

  it('lands the result when the popup closes mid-session, with a notification', async () => {
    const page = await playingTab('/pages/generic.html?src=/media/song-c.wav&autoplay');
    const popup = await openPopup(page);
    await listenFrom(popup, 'This tab');
    await popup.waitForFunction(() => document.querySelector('#stage-state')?.textContent?.includes('Listening'), { timeout: 15_000 });
    await app.close();
    await popup.close();
    // Nothing is connected now; the offscreen host keeps capturing and identifying.
    await sleep(15_000);
    app = await browser.newPage();
    await app.goto(`chrome-extension://${id}/app.html#/library`);
    await trackEvents(app);
    const [newest] = await rpc<{ id: string; title: string; status: string }[]>(app, 'mf-engine', 'library', [{ kinds: [], search: null, pinned_only: false, limit: 1, offset: 0 }]);
    expect(newest).toMatchObject({ title: 'Test Signal C', status: 'Matched' });
    // What the background posted (e2e builds record it): CI runners have no notification
    // daemon, so the browser's own list (notifications.getAll) may already be empty there.
    const posted = await app.evaluate(async () => {
      const stored = await (globalThis as unknown as { chrome: { storage: { session: { get(k: string): Promise<Record<string, unknown>> } } } }).chrome.storage.session.get('mfE2eNotifications');
      return ((stored.mfE2eNotifications as { id: string }[] | undefined) ?? []).map((n) => n.id);
    });
    expect(posted).toContain(`mf-${newest!.id}`);
    await page.close();
  });

  it('recycles the engine Worker without losing the library', async () => {
    const query = [{ kinds: [], search: null, pinned_only: false, limit: 50, offset: 0 }];
    const before = await rpc<unknown[]>(app, 'mf-engine', 'library', query);
    const state = await rpc<{ worker: string; workerStarts: number }>(app, 'mf-host', 'state');
    const recycled = await rpc<{ worker: string }>(app, 'mf-host', 'recycle');
    expect(recycled.worker).toBe('down');
    const after = await rpc<unknown[]>(app, 'mf-engine', 'library', query);
    expect(after).toEqual(before);
    expect((await rpc<{ workerStarts: number }>(app, 'mf-host', 'state')).workerStarts).toBe(state.workerStarts + 1);
  });

  it('listens to the microphone in the offscreen document after the one-time grant', async () => {
    // Fake media devices replace every capture source, so this test gets its own browser.
    const micBrowser = await puppeteer.launch({
      executablePath: CHROMIUM,
      headless: HEADLESS,
      pipe: true,
      enableExtensions: [E2E_OUTPUT.chromium],
      userDataDir: tempProfile('rt-chromium-mic'),
      defaultViewport: null,
      args: [
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-gpu',
        '--window-size=1280,860',
        '--use-fake-ui-for-media-stream',
        '--use-fake-device-for-media-stream',
        `--use-file-for-fake-audio-capture=${micFile('b')}`,
      ],
    });
    let micId = '';
    while (!micId) {
      micId = [...(await micBrowser.extensions()).values()].find((e) => e.name === 'Music Fetch')?.id ?? '';
      if (!micId) await sleep(500);
    }
    const welcome = await (await micBrowser.waitForTarget((t) => t.url().startsWith(`chrome-extension://${micId}/app.html#/welcome`), { timeout: 60_000 })).asPage();
    await welcome!.evaluate(() => (globalThis as unknown as { chrome: { storage: { sync: { set(v: unknown): Promise<void> } } } }).chrome.storage.sync.set({ providerDecision: { provider: 'spotify', skipped: true } }));
    const grant = await micBrowser.newPage();
    await grant.goto(`chrome-extension://${micId}/app.html#/grant-mic`);
    await grant.waitForSelector('.grant .acts .btn.filled', { timeout: 15_000 });
    await grant.click('.grant .acts .btn.filled');
    await grant.waitForFunction(() => /allowed|this tab only/i.test(document.querySelector('.grant h1')?.textContent ?? ''), { timeout: 15_000 }).catch(() => undefined);
    const page = await micBrowser.newPage();
    await page.goto(`${server.origin}/blank`);
    await page.bringToFront();
    await (await micBrowser.extensions()).get(micId)!.triggerAction(page);
    const popup = (await (await micBrowser.waitForTarget((t) => t.url() === `chrome-extension://${micId}/popup.html`, { timeout: 15_000 })).asPage())!;
    await popup.waitForSelector('.listen .disc', { timeout: 20_000 });
    await listenFrom(popup, 'Microphone');
    await popup.waitForFunction(() => document.querySelector('.hero .ttl')?.textContent?.includes('Test Signal B'), { timeout: 45_000 });
    await micBrowser.close();
  });

  it('rewrites the engine’s requests with the header rules and leaves the page’s alone', async () => {
    await app.evaluate((origin) => (globalThis as unknown as { chrome: { storage: { local: { set(v: unknown): Promise<void> } } } }).chrome.storage.local.set({ mfE2eEcho: origin }), server.origin);
    const installed = await app.evaluate(() => (globalThis as unknown as { chrome: { runtime: { sendMessage(m: unknown): Promise<{ value: { rules: number } }> } } }).chrome.runtime.sendMessage({ type: 'mf:net.ensure' }));
    expect(installed.value.rules).toBeGreaterThan(10);
    // The user has their own cookie on the site.
    const page = await browser.newPage();
    await page.goto(`${server.origin}/set-cookie`);
    const probe = async (rule: string, method = 'GET') => echoed((await rpc<{ body: string }>(app, 'mf-host', 'netProbe', [`${server.origin}/echo/${rule}`, { method, ...(method === 'POST' ? { body: '{}' } : {}) }])).body).headers;

    const shazam = await probe('shazam', 'POST');
    expect(shazam['user-agent']).toMatch(/^Dalvik\/2\.1\.0 \(Linux; U; Android/);
    expect(shazam.origin).toBeUndefined();
    expect((await probe('crawler'))['user-agent']).toMatch(/^facebookexternalhit\/1\.1/);
    expect((await probe('youtube')).cookie).toBe('SOCS=CAI; CONSENT=YES+1');
    const ytm = await probe('ytmusic', 'POST');
    expect(ytm.origin).toBe('https://music.youtube.com');
    expect(ytm.referer).toBe('https://music.youtube.com/');
    expect((await probe('neutral'))['user-agent']).toMatch(/^MusicFetch\/1\.1\.0 /);
    // Without a rule nothing is added, and the user's cookie never goes along.
    expect((await probe('plain')).cookie).toBeUndefined();

    // The same URLs from the user's tab: untouched.
    const fromPage = await page.evaluate(async (origin) => {
      const r = await fetch(`${origin}/echo/youtube`, { credentials: 'include' });
      return ((await r.json()) as { headers: Record<string, string> }).headers;
    }, server.origin);
    expect(fromPage.cookie).toBe('user=private-session');
    expect(fromPage['user-agent']).not.toMatch(/Dalvik|facebookexternalhit|MusicFetch/);
    await page.goto(`${server.origin}/echo/shazam`);
    const navigated = JSON.parse(await page.$eval('body', (b) => b.textContent ?? '{}')) as { headers: Record<string, string> };
    expect(navigated.headers['user-agent']).not.toMatch(/Dalvik/);
    await page.close();
  });

  it('listens a fake YouTube mix through at 4× with tab capture, past a pre-roll ad, and restores the player', async () => {
    const url = `${server.origin}/pages/youtube.html?preroll=3`;
    const page = await browser.newPage();
    await page.goto(url);
    // Invoking the extension on the tab allows tab capture (the popup's "Analyze this page").
    const popup = await openPopup(page);
    await popup.close();
    const tabId = await tabIdOf(url);
    const started = Date.now();
    const session = await rpc<string>(app, 'mf-engine', 'analyze', [url, 'Mix', { tab_id: tabId, url, title: 'Fake Mix', facts: null }]);
    const result = await waitForSession(app, session, (s) => s !== 'Running', 120_000);
    const where = `${JSON.stringify(result.segments)} ${await eventsOf(app, session)}`;
    expect(result.status, where).toBe('Completed');
    expect(result.segments.map((s) => s.title), where).toEqual(['Test Signal A', 'Test Signal B', 'Test Signal C']);
    expect(Math.abs(result.segments[0]!.end_ms - 20_000), where).toBeLessThanOrEqual(2000);
    expect(Math.abs(result.segments[1]!.end_ms - 40_000), where).toBeLessThanOrEqual(2000);
    // 60 s of media at 4×: well under a minute, ad included.
    expect(Date.now() - started).toBeLessThan(60_000);
    const player = await page.evaluate(() => {
      const v = document.querySelector('video')!;
      return { rate: v.playbackRate, muted: v.muted, pitch: v.preservesPitch, quality: (window as unknown as { fakeQuality(): string }).fakeQuality() };
    });
    expect(player).toEqual({ rate: 1, muted: false, pitch: true, quality: 'hd720' });
    await page.close();
  });

  it('falls back to muted element capture on a tab the extension was not invoked on', async () => {
    const url = `${server.origin}/pages/youtube.html`;
    const page = await browser.newPage();
    await page.goto(url);
    const tabId = await tabIdOf(url);
    const session = await rpc<string>(app, 'mf-engine', 'analyze', [url, 'Mix', { tab_id: tabId, url, title: 'Fake Mix', facts: null }]);
    const result = await waitForSession(app, session, (s) => s !== 'Running', 120_000);
    expect(result.segments.map((s) => s.title), `${JSON.stringify(result.segments)} ${await eventsOf(app, session)}`).toEqual(['Test Signal A', 'Test Signal B', 'Test Signal C']);
    expect(await page.evaluate(() => document.querySelector('video')!.muted)).toBe(false);
    await page.close();
  });

  it('checks a SoundCloud-like page with survey windows through its detached player', async () => {
    const url = `${server.origin}/pages/soundcloud.html`;
    const page = await browser.newPage();
    await page.goto(url);
    const popup = await openPopup(page);
    await popup.close();
    const tabId = await tabIdOf(url);
    const session = await rpc<string>(app, 'mf-engine', 'analyze', [url, 'Auto', { tab_id: tabId, url, title: 'Test Signal B', facts: null }]);
    const result = await waitForSession(app, session, (s) => s !== 'Running', 90_000);
    expect(result.segments.map((s) => s.title), `${JSON.stringify(result.segments)} ${await eventsOf(app, session)}`).toEqual(['Test Signal B']);
    await page.close();
  });

  it('refuses browser pages with the capture reason', async () => {
    const page = await browser.newPage();
    await page.goto('chrome://version');
    await page.bringToFront();
    const error = await rpc(app, 'mf-engine', 'startListening', ['Tab', 'Auto', { auto_stop: true }]).catch((e: Error) => e.message);
    expect(String(error)).toMatch(/CantCapture|NeedsInvocation/);
    await page.close();
  });
});
