import { existsSync } from 'node:fs';
import { createServer } from 'node:net';
import puppeteer, { type Browser, type Page } from 'puppeteer-core';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import webExt from 'web-ext';
import { HEADLESS } from './support';
import { E2E_OUTPUT, RUNTIME_E2E, echoed, rpc, sleep, startFixtureServer, waitForSession, type FixtureServer } from './runtime-support';

const FIREFOX = process.env.FIREFOX_BIN ?? '/usr/bin/firefox';
const FRAME = 'mf-rt-frame';

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

type Api = {
  tabs: { query(q: unknown): Promise<{ id: number; url?: string; active: boolean }[]>; update(id: number, p: unknown): Promise<unknown>; create(p: unknown): Promise<{ id: number }> };
  storage: { local: { set(v: unknown): Promise<void> }; sync: { set(v: unknown): Promise<void> } };
  runtime: { sendMessage(m: unknown): Promise<{ value: { rules: number } }> };
};

/**
 * The runtime in Firefox: the event page hosts the engine Worker; "This tab" is element
 * capture through the injected agent; the microphone is captured by the page that asked;
 * the keepalive holds the event page through a job with no traffic. Firefox's BiDi can't
 * open the toolbar popup or navigate a web tab into the extension, so the tests drive one
 * extension tab (the welcome tab) and call the host from it.
 */
describe.skipIf(!RUNTIME_E2E || !existsSync(FIREFOX) || !existsSync(E2E_OUTPUT.firefox))('Firefox runtime', () => {
  let runner: { exit(): Promise<void> } | null = null;
  let browser: Browser;
  let server: FixtureServer;
  let tab: Page;

  beforeAll(async () => {
    server = await startFixtureServer();
    const port = await freePort();
    process.env.MOZ_REMOTE_ALLOW_SYSTEM_ACCESS = '1';
    runner = await webExt.cmd.run(
      {
        sourceDir: E2E_OUTPUT.firefox,
        target: ['firefox-desktop'],
        firefox: FIREFOX,
        args: [`--remote-debugging-port=${port}`, ...(HEADLESS ? ['-headless'] : [])],
        pref: {
          'browser.shell.checkDefaultBrowser': false,
          'datareporting.policy.dataSubmissionEnabled': false,
          // Audible autoplay for the fixture pages; the output goes to a private null sink.
          'media.autoplay.default': 0,
          'media.autoplay.blocking_policy': 0,
          // A fake microphone, allowed without a prompt.
          'media.navigator.streams.fake': true,
          'media.navigator.permission.disabled': true,
          'permissions.default.microphone': 1,
        },
        startUrl: [`${server.origin}/pages/generic.html?src=/media/song-a.wav&autoplay`],
        noReload: true,
        noInput: true,
        browserConsole: false,
      },
      { shouldExitProgram: false },
    );
    browser = await puppeteer.connect({ browserWSEndpoint: `ws://127.0.0.1:${port}/session`, protocol: 'webDriverBiDi' });
    const target = await browser.waitForTarget((t) => /^moz-extension:\/\/[^/]+\/app\.html#\/welcome/.test(t.url()), { timeout: 60_000 });
    tab = (await target.asPage())!;
    await tab.waitForSelector('.welcome', { timeout: 30_000 });
    await tab.evaluate(() => (globalThis as unknown as { browser: Api }).browser.storage.sync.set({ providerDecision: { provider: 'spotify', skipped: true } }));
  });

  afterAll(async () => {
    await browser?.disconnect();
    await runner?.exit();
    await server?.close();
  });

  /** Make the tab showing `prefix` the active one (what "This tab" means). */
  async function activate(prefix: string): Promise<number> {
    return tab.evaluate(async (p) => {
      const api = (globalThis as unknown as { browser: Api }).browser;
      const found = (await api.tabs.query({})).find((t) => t.url?.startsWith(p));
      if (!found) throw new Error(`no tab ${p}`);
      await api.tabs.update(found.id, { active: true });
      return found.id;
    }, prefix);
  }

  async function openTab(url: string): Promise<number> {
    return tab.evaluate(async (u) => (await (globalThis as unknown as { browser: Api }).browser.tabs.create({ url: u, active: true })).id, url);
  }

  it('identifies a tab through element capture in the page', async () => {
    await activate(`${server.origin}/pages/generic.html`);
    const id = await rpc<string>(tab, 'mf-engine', 'startListening', ['Tab', 'Auto', { auto_stop: true }]);
    await sleep(2000);
    const sources = await rpc<{ sessionId: string; via: string; frames: number }[]>(tab, 'mf-host', 'sources');
    expect(sources.find((s) => s.sessionId === id)).toMatchObject({ via: 'element' });
    const result = await waitForSession(tab, id, (s) => s !== 'Running', 45_000);
    expect(result).toMatchObject({ status: 'Matched', title: 'Test Signal A' });
  });

  it('listens a fake YouTube mix through at 4×, muted, and restores the player', async () => {
    const url = `${server.origin}/pages/youtube.html?preroll=3`;
    const tabId = await openTab(url);
    await sleep(1500);
    const session = await rpc<string>(tab, 'mf-engine', 'analyze', [url, 'Mix', { tab_id: tabId, url, title: 'Fake Mix', facts: null }]);
    const result = await waitForSession(tab, session, (s) => s !== 'Running', 120_000);
    expect(result.status).toBe('Completed');
    expect(result.segments.map((s) => s.title)).toEqual(['Test Signal A', 'Test Signal B', 'Test Signal C']);
    expect(Math.abs(result.segments[0]!.end_ms - 20_000)).toBeLessThanOrEqual(2000);
    expect(Math.abs(result.segments[1]!.end_ms - 40_000)).toBeLessThanOrEqual(2000);
  });

  it('keeps the event page alive through a job with no traffic', async () => {
    const before = await rpc<{ instance: string }>(tab, 'mf-host', 'state');
    const session = await rpc<string>(tab, 'mf-engine', 'analyze', [`${server.origin}/blank?mf-e2e-hold=75000`, 'Auto', null]);
    // Longer than Firefox's 30 s idle timeout, twice over; nothing talks to the event page.
    await sleep(80_000);
    const result = await waitForSession(tab, session, (s) => s !== 'Running', 20_000);
    expect(result.status).toBe('NoMatch');
    const after = await rpc<{ instance: string }>(tab, 'mf-host', 'state');
    expect(after.instance).toBe(before.instance);
  });

  it('captures the microphone in the page that asked, not in the event page', async () => {
    await tab.evaluate(
      (id, src) => {
        document.getElementById(id)?.remove();
        const frame = document.createElement('iframe');
        frame.id = id;
        frame.src = src;
        frame.allow = 'microphone';
        Object.assign(frame.style, { position: 'fixed', left: '0', top: '0', width: '380px', height: '560px', border: '0', zIndex: '9999' });
        document.body.append(frame);
      },
      FRAME,
      '/popup.html',
    );
    const inFrame = <T,>(fn: string) =>
      tab.waitForFunction(
        (id, code) => {
          const doc = (document.getElementById(id) as HTMLIFrameElement | null)?.contentDocument;
          return doc ? new Function('doc', code)(doc) : false;
        },
        { timeout: 30_000, polling: 100 },
        FRAME,
        fn,
      ) as unknown as Promise<T>;
    await inFrame("return doc.querySelector('.listen .disc')");
    await tab.evaluate((id) => {
      const doc = (document.getElementById(id) as HTMLIFrameElement).contentDocument!;
      [...doc.querySelectorAll<HTMLButtonElement>('.sources button')].find((b) => b.textContent?.trim() === 'Microphone')?.click();
      doc.querySelector<HTMLElement>('.listen .disc')!.click();
    }, FRAME);
    await sleep(2500);
    const sources = await rpc<{ via: string; frames: number }[]>(tab, 'mf-host', 'sources');
    expect(sources.some((s) => s.via === 'mic' && s.frames > 0)).toBe(true);
    await inFrame("return doc.querySelector('.hero .ttl')");
  });

  it('rewrites the engine’s requests with the same DNR rules as Chromium', async () => {
    await tab.evaluate((origin) => (globalThis as unknown as { browser: Api }).browser.storage.local.set({ mfE2eEcho: origin }), server.origin);
    const installed = await tab.evaluate(() => (globalThis as unknown as { browser: Api }).browser.runtime.sendMessage({ type: 'mf:net.ensure' }));
    expect(installed.value.rules).toBeGreaterThan(10);
    const probe = async (rule: string, method = 'GET') => echoed((await rpc<{ body: string }>(tab, 'mf-host', 'netProbe', [`${server.origin}/echo/${rule}`, { method, ...(method === 'POST' ? { body: '{}' } : {}) }])).body).headers;
    const shazam = await probe('shazam', 'POST');
    expect(shazam['user-agent']).toMatch(/^Dalvik\/2\.1\.0/);
    expect(shazam.origin).toBeUndefined();
    expect((await probe('youtube')).cookie).toBe('SOCS=CAI; CONSENT=YES+1');
    expect((await probe('neutral'))['user-agent']).toMatch(/^MusicFetch\//);
    expect((await probe('ytmusic', 'POST')).origin).toBe('https://music.youtube.com');
  });
});
