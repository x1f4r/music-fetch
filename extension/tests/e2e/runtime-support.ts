import { existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { Page } from 'puppeteer-core';
import { encodeWav, longVersion, renderSong, SYNTHETIC_SONGS } from '../../src/engine/host-mock-audio';
import { handleRecognizer, SongIndex, type RecognizerQuery, type SignatureTools } from './stub-recognizer';

/**
 * Shared by the runtime end-to-end tests (engine host, audio sources, header rules,
 * capture coordinator). They run the `--mode e2e` builds, which may script pages on
 * 127.0.0.1 and know the local echo server; everything they touch is local: fixture
 * pages, synthetic songs, the echo server. No media site is contacted.
 */
export const E2E_OUTPUT = {
  chromium: resolve('.output/chrome-mv3-e2e'),
  firefox: resolve('.output/firefox-mv3-e2e'),
};

/**
 * The runtime tests play audio: they run only when asked (`MF_RUNTIME_E2E=1`, set by
 * `npm run e2e:runtime` and `legion-runtime.sh`, which also routes the audio to a private
 * null sink), never as part of `npm run e2e`.
 */
export const RUNTIME_E2E = process.env.MF_RUNTIME_E2E === '1';

export const SAMPLE_RATE = 48_000;
const [A, B, C, D, E, F] = SYNTHETIC_SONGS as unknown as [typeof SYNTHETIC_SONGS[0], ...typeof SYNTHETIC_SONGS[0][]];

function concat(parts: Float32Array[]): Float32Array {
  const out = new Float32Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

/** Deterministic noise: an "ad" that matches no song. */
function noise(ms: number, amplitude = 0.08): Float32Array {
  let state = 12345;
  return new Float32Array(Math.round((ms / 1000) * SAMPLE_RATE)).map(() => {
    state = (Math.imul(state, 1103515245) + 12345) >>> 0;
    return ((state / 2 ** 32) * 2 - 1) * amplitude;
  });
}

/** The media the fixture pages play, rendered once per run. */
export function renderMedia(): Map<string, Uint8Array> {
  const media = new Map<string, Uint8Array>();
  for (const [letter, song] of [
    ['a', A],
    ['b', B],
    ['c', C],
    ['d', D],
    ['e', E],
    ['f', F],
  ] as const) {
    media.set(`song-${letter}.wav`, encodeWav(renderSong(song!, SAMPLE_RATE, 32_000), SAMPLE_RATE));
  }
  // The fake YouTube mix: A, B and C for 20 s each (truth: boundaries at 20 s and 40 s).
  const mix = concat([renderSong(A, SAMPLE_RATE, 20_000, 0), renderSong(B!, SAMPLE_RATE, 20_000, 3000), renderSong(C!, SAMPLE_RATE, 20_000, 9000)]);
  media.set('mix.wav', encodeWav(mix, SAMPLE_RATE));
  // For the real engine, whose mix pipeline expects songs of minutes: 90 s each
  // (boundaries at 1:30 and 3:00; youtube.html?src=/media/mix-long.wav&length=270&marks=0,90,180).
  const long = (song: typeof A, from: number) => renderSong(longVersion(song, 120), SAMPLE_RATE, LONG_SONG_MS, from);
  media.set('mix-long.wav', encodeWav(concat([long(A, 0), long(B!, 3000), long(C!, 9000)]), SAMPLE_RATE));
  media.set('ad.wav', encodeWav(noise(8000), SAMPLE_RATE));
  return media;
}

/** Song length of the long fake mix (`mix-long.wav`). */
export const LONG_SONG_MS = 90_000;
/** The fake YouTube page playing the long mix, with its tracklist at the song changes. */
export const LONG_MIX_QUERY = `src=/media/mix-long.wav&length=${(3 * LONG_SONG_MS) / 1000}&marks=0,${LONG_SONG_MS / 1000},${(2 * LONG_SONG_MS) / 1000}`;

export interface FixtureServer {
  origin: string;
  /** Requests the echo endpoints saw, newest last. */
  echoes: EchoRecord[];
  /** What the stub recognizer was asked (only with the engine module built). */
  recognizer: RecognizerQuery[];
  close(): Promise<void>;
}

/**
 * The engine module's signature tools for the stub recognizer, when the module is built
 * (`npm run build:engine`). The e2e builds then run the real engine, which sends its
 * Shazam requests to the fixture server (`useStubRecognizer`).
 */
export async function signatureTools(): Promise<SignatureTools | null> {
  const pkg = resolve('src/engine/wasm');
  if (!existsSync(join(pkg, 'mfcore_wasm_bg.wasm'))) return null;
  const module = (await import(pathToFileURL(join(pkg, 'mfcore_wasm.js')).href)) as { initSync(o: { module: BufferSource }): unknown; Engine: SignatureTools };
  module.initSync({ module: readFileSync(join(pkg, 'mfcore_wasm_bg.wasm')) });
  return module.Engine;
}

/**
 * Point the engine at the fixture server's stub recognizer and restart its Worker so it
 * takes the setting (e2e builds read it at every Worker start). Returns the engine kind.
 */
export async function useStubRecognizer(page: Page, origin: string): Promise<'wasm' | 'mock'> {
  await page.evaluate((o) => {
    const api = ((globalThis as unknown as { browser?: unknown }).browser ?? (globalThis as unknown as { chrome: unknown }).chrome) as { storage: { local: { set(v: unknown): Promise<void> } } };
    return api.storage.local.set({ mfE2eRecognizer: o });
  }, origin);
  await rpc(page, 'mf-host', 'recycle');
  return (await rpc<{ kind: 'wasm' | 'mock' }>(page, 'mf-engine', 'hello')).kind;
}

export interface EchoRecord {
  rule: string;
  method: string;
  headers: Record<string, string | string[] | undefined>;
}

function serveRange(req: IncomingMessage, res: ServerResponse, body: Uint8Array, type: string) {
  const range = /bytes=(\d+)-(\d*)/.exec(req.headers.range ?? '');
  if (range) {
    const start = Number(range[1]);
    const end = range[2] ? Math.min(Number(range[2]), body.length - 1) : body.length - 1;
    res.writeHead(206, { 'content-type': type, 'accept-ranges': 'bytes', 'content-range': `bytes ${start}-${end}/${body.length}`, 'content-length': end - start + 1 });
    res.end(body.subarray(start, end + 1));
    return;
  }
  res.writeHead(200, { 'content-type': type, 'accept-ranges': 'bytes', 'content-length': body.length });
  res.end(body);
}

/** Fixture pages, the synthetic media (with Range support for seeking) and the echo server, on 127.0.0.1. */
export async function startFixtureServer(): Promise<FixtureServer> {
  const media = renderMedia();
  const pages = resolve('tests/fixtures/pages');
  const echoes: EchoRecord[] = [];
  const tools = await signatureTools();
  const songs = tools ? new SongIndex(tools) : null;
  const recognizer: RecognizerQuery[] = [];
  const server: Server = createServer((req, res) => {
    if (songs && handleRecognizer(songs, recognizer, req, res)) return;
    const url = new URL(req.url ?? '/', 'http://127.0.0.1');
    const path = url.pathname;
    if (path.startsWith('/media/')) {
      const body = media.get(path.slice('/media/'.length));
      if (!body) return void res.writeHead(404).end();
      return serveRange(req, res, body, 'audio/wav');
    }
    if (path.startsWith('/pages/') || path === '/watch') {
      const file = path === '/watch' ? 'youtube.html' : path.slice('/pages/'.length);
      try {
        const html = readFileSync(join(pages, file.replace(/[^a-z0-9.-]/gi, '')));
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        return void res.end(html);
      } catch {
        return void res.writeHead(404).end();
      }
    }
    if (path.startsWith('/echo/')) {
      const record: EchoRecord = { rule: path.slice('/echo/'.length), method: req.method ?? 'GET', headers: req.headers };
      echoes.push(record);
      req.resume();
      req.on('end', () => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(record));
      });
      return;
    }
    if (path === '/set-cookie') {
      // The user's own session on this site: engine requests must never carry it.
      res.writeHead(200, { 'set-cookie': 'user=private-session; Path=/; SameSite=Lax', 'content-type': 'text/html' });
      return void res.end('<!doctype html><title>cookie set</title><p>cookie set</p>');
    }
    if (path === '/' || path === '/blank') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return void res.end('<!doctype html><title>Music Fetch test page</title><h1>A page with no music</h1>');
    }
    res.writeHead(404).end();
  });
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('no port');
  return {
    origin: `http://127.0.0.1:${address.port}`,
    echoes,
    recognizer,
    close: () =>
      new Promise((done) => {
        server.closeAllConnections?.();
        server.close(() => done());
      }),
  };
}

/** A WAV on disk for Chromium's fake microphone. */
export function micFile(letter: 'a' | 'b' | 'c' | 'd' | 'e' | 'f'): string {
  const dir = mkdtempSync(join(tmpdir(), 'mf-e2e-mic-'));
  const path = join(dir, `song-${letter}.wav`);
  writeFileSync(path, renderMedia().get(`song-${letter}.wav`)!);
  return path;
}

/**
 * Call the engine host from an extension page over the raw wire protocol (`ENGINE_PORT`
 * or `HOST_PORT`), the way the UI's RemoteEngineClient does.
 */
export async function rpc<T = unknown>(page: Page, port: 'mf-engine' | 'mf-host', method: string, args: unknown[] = [], timeoutMs = 60_000): Promise<T> {
  return (await page.evaluate(
    async (portName: string, name: string, params: unknown[], timeout: number) => {
      type Port = { postMessage(m: unknown): void; disconnect(): void; onMessage: { addListener(f: (m: never) => void): void } };
      const api = ((globalThis as unknown as { browser?: unknown }).browser ?? (globalThis as unknown as { chrome: unknown }).chrome) as {
        runtime: { sendMessage(m: unknown): Promise<unknown>; connect(o: { name: string }): Port };
      };
      await api.runtime.sendMessage({ type: 'mf:host.ensure' });
      const port = api.runtime.connect({ name: portName });
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`no answer to ${name}`)), timeout);
        port.onMessage.addListener((m: { mf?: number; t?: string; id?: number; ok?: boolean; value?: unknown; error?: { code: string; message: string; reason?: string } }) => {
          if (m?.mf !== 1 || m.t !== 'res' || m.id !== 1) return;
          clearTimeout(timer);
          port.disconnect();
          if (m.ok) resolve(m.value);
          else reject(new Error(`${m.error?.code}: ${m.error?.message}${m.error?.reason ? ` [${m.error.reason}]` : ''}`));
        });
        port.postMessage({ mf: 1, t: 'req', id: 1, method: name, args: params });
      });
    },
    port,
    method,
    args,
    timeoutMs,
  )) as T;
}

/**
 * Keep a port to the engine open in `page` and record every session's events there, so
 * a failing test can say why a session failed (`eventsOf`).
 */
export async function trackEvents(page: Page): Promise<void> {
  await page.evaluate(async () => {
    const w = globalThis as unknown as { __mfEvents?: Record<string, unknown[]>; browser?: unknown; chrome?: unknown };
    if (w.__mfEvents) return;
    w.__mfEvents = {};
    const api = (w.browser ?? w.chrome) as { runtime: { sendMessage(m: unknown): Promise<unknown>; connect(o: { name: string }): { onMessage: { addListener(f: (m: never) => void): void } } } };
    await api.runtime.sendMessage({ type: 'mf:host.ensure' });
    const port = api.runtime.connect({ name: 'mf-engine' });
    port.onMessage.addListener((m: { t?: string; topic?: string; payload?: { sessionId: string; event?: unknown; status?: unknown } }) => {
      if (m?.t !== 'evt' || !m.payload?.sessionId) return;
      const list = (w.__mfEvents![m.payload.sessionId] ??= []);
      const e = m.topic === 'capture' ? { capture: m.payload.status } : m.payload.event;
      // Levels are many and say little here.
      if (!(e && typeof e === 'object' && 'Level' in e)) list.push(e);
    });
  });
}

/** The recorded events of `id` (names, plus the payload of failures and notices). */
export async function eventsOf(page: Page, id: string): Promise<string> {
  return page.evaluate((sid) => {
    const list = ((globalThis as unknown as { __mfEvents?: Record<string, unknown[]> }).__mfEvents ?? {})[sid] ?? [];
    return list
      .map((e) => (typeof e === 'string' ? e : JSON.stringify(e).slice(0, 160)))
      .join(' | ');
  }, id);
}

export interface SessionSummary {
  status: string;
  title: string;
  segments: { title: string | null; start_ms: number; end_ms: number }[];
}

/** Poll a session until `done(status)` holds. */
export async function waitForSession(page: Page, id: string, done: (status: string) => boolean, timeoutMs = 60_000): Promise<SessionSummary> {
  const deadline = Date.now() + timeoutMs;
  let last: SessionSummary | null = null;
  while (Date.now() < deadline) {
    const detail = await rpc<{ entry: { status: string; title: string }; segments: { track: { title: string } | null; start_ms: number; end_ms: number }[] } | null>(page, 'mf-engine', 'session', [id]).catch(() => null);
    if (detail) {
      last = {
        status: detail.entry.status,
        title: detail.entry.title,
        segments: detail.segments.map((s) => ({ title: s.track?.title ?? null, start_ms: s.start_ms, end_ms: s.end_ms })),
      };
      if (done(last.status)) return last;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`session ${id} did not finish: ${JSON.stringify(last)}`);
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** The echo server's view of a request, from the body a probe got back. */
export function echoed(body: string): EchoRecord {
  return JSON.parse(body) as EchoRecord;
}
