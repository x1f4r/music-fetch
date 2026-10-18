// @vitest-environment node
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { PcmChunk } from '../../src/audio/types';
import type { CaptureHost, CaptureTarget, Coverage } from '../../src/capture/types';
import type { EngineListener } from '../../src/engine/client';
import { checkEventContract, isTerminal } from '../../src/engine/events';
import type { BackendEnv } from '../../src/engine/host-backend';
import { longVersion, renderSong, SYNTHETIC_SONGS } from '../../src/engine/host-mock-audio';
import { pageMedia, surveyWindows, WasmBackend, type WasmModule } from '../../src/engine/host-wasm-backend';
import type { SessionEvent } from '../../src/engine/types';
import { startStubRecognizer, type SignatureTools, type StubRecognizer } from '../e2e/stub-recognizer';

/**
 * The real engine (the wasm module `npm run build:engine` builds) behind the Worker's
 * backend interface, in Node: in-memory storage, the stub recognizer for Shazam, the
 * synthetic test songs as audio. Skipped when the module has not been built.
 */
const pkg = resolve('src/engine/wasm');
const built = existsSync(resolve(pkg, 'mfcore_wasm_bg.wasm'));
const RATE = 48_000;
const [A, B, C] = SYNTHETIC_SONGS;

describe.skipIf(!built)('the wasm engine behind the backend', () => {
  let wasm: WasmModule & { initSync(o: { module: BufferSource }): unknown; Engine: SignatureTools };
  let stub: StubRecognizer;
  let backend: WasmBackend;
  const events = new Map<string, SessionEvent[]>();
  let pushTo: ((id: string, chunk: PcmChunk) => void) | null = null;
  const pushed: number[][] = [];

  /** A fake page player: plays `audio` (48 kHz) into the session at `rate` when asked. */
  function fakeCapture(audio: Float32Array, durationMs: number): CaptureHost & { asked: string[] } {
    const asked: string[] = [];
    const play = async (session: string, from: number, to: number, rate: number): Promise<Coverage> => {
      const block = 4_800;
      let seq = 0;
      for (let ms = from; ms < to; ms += (block / RATE) * 1000 * rate) {
        // At `rate` with pitch not preserved the capture holds `rate`× the media per block.
        const start = Math.floor((ms / 1000) * RATE);
        const media = audio.subarray(start, start + Math.round(block * rate));
        const samples = new Float32Array(block);
        for (let i = 0; i < block; i++) samples[i] = media[Math.floor(i * rate)] ?? 0;
        pushTo?.(session, { samples, sampleRate: RATE, speed: rate, mediaMs: ms, wallMs: 0, continuous: seq > 0, seq: seq++ });
        await new Promise((r) => setTimeout(r, 0));
      }
      return [[from, to]];
    };
    return {
      asked,
      async attach(session, url): Promise<CaptureTarget> {
        asked.push(`attach ${url}`);
        return { session, tabId: 1, url, title: 'Fake page', durationMs, maxRate: 4, canSeek: true, live: false, adapter: 'generic', via: 'tab', facts: null };
      },
      async listenThrough(session, from, to, rate) {
        asked.push(`listenThrough ${from}-${to} @${rate}`);
        return play(session, from, to, rate);
      },
      async windows(session, windows, rate) {
        asked.push(`windows ${windows.map(([a, b]) => `${a}-${b}`).join(',')} @${rate}`);
        for (const [a, b] of windows) await play(session, a, b, rate);
        return windows;
      },
      async release(session) {
        asked.push(`release ${session}`);
      },
    };
  }

  async function start(capture: CaptureHost) {
    const env: BackendEnv = { target: 'chromium', android: false, lowPower: false, cpus: 8, capture };
    backend = await WasmBackend.create(wasm, env, { storage: 'memory', shazam_base_url: stub.origin, test_pacing_ms: 20, offline_catalog: true, log_level: process.env.MF_LOG ?? 'warn' });
    pushTo = (id, chunk) => {
      pushed.push([chunk.mediaMs ?? -1, chunk.samples.length, chunk.speed, chunk.continuous ? 1 : 0]);
      backend.pushPcm(id, chunk);
    };
    const listener: EngineListener = { sessionEvent: (id, event) => events.set(id, [...(events.get(id) ?? []), event]) };
    backend.subscribe(listener);
  }

  async function ended(id: string, timeoutMs = 60_000): Promise<SessionEvent[]> {
    const deadline = Date.now() + timeoutMs;
    while (!(events.get(id) ?? []).some(isTerminal)) {
      if (Date.now() > deadline) throw new Error(`no terminal event (queries ${JSON.stringify(stub.queries)}): ${JSON.stringify((events.get(id) ?? []).filter((e) => typeof e === "string" || !("Level" in e || "Pitch" in e)))}`);
      await new Promise((r) => setTimeout(r, 20));
    }
    return events.get(id)!;
  }

  beforeAll(async () => {
    const module = (await import(/* @vite-ignore */ resolve(pkg, 'mfcore_wasm.js'))) as typeof wasm;
    module.initSync({ module: readFileSync(resolve(pkg, 'mfcore_wasm_bg.wasm')) });
    wasm = module;
    stub = await startStubRecognizer(module.Engine);
  });

  afterAll(async () => {
    await backend?.shutdown();
    await stub?.close();
  });

  it('identifies a test song heard live from "This tab"', async () => {
    await start(fakeCapture(new Float32Array(0), 0));
    expect(await backend.hello()).toMatchObject({ kind: 'wasm' });
    const id = await backend.startLive('Tab', 'Auto', { auto_stop: true }, { kind: 'tab', sampleRate: RATE, label: 'test', directive: null });
    expect((await backend.activeSessions()).map((s) => s.id)).toEqual([id]);
    const audio = renderSong(A!, RATE, 14_000, 2_000);
    for (let i = 0, seq = 0; i < audio.length; i += 960, seq++) {
      backend.pushPcm(id, { samples: audio.subarray(i, i + 960), sampleRate: RATE, speed: 1, mediaMs: null, wallMs: 0, continuous: true, seq });
      if (seq % 10 === 0) await new Promise((r) => setTimeout(r, 0));
    }
    const stream = await ended(id);
    expect(checkEventContract(stream)).toEqual([]);
    const last = stream.at(-1) as { Finished?: { outcome: { Matched?: { track: { title: string } } } } };
    expect(last.Finished?.outcome.Matched?.track.title, JSON.stringify(stub.queries)).toBe('Alpha Signal');
    expect(stub.queries.length).toBeGreaterThan(0);
    const [entry] = await backend.library({ kinds: [], search: null, pinned_only: false, limit: 1, offset: 0 });
    expect(entry).toMatchObject({ id, kind: 'SystemAudio', status: 'Matched', title: 'Alpha Signal' });
    expect(await backend.activeSessions()).toEqual([]);
  }, 90_000);

  it('analyzes a page it captures at 4× as a mix of three songs, with its tracklist', async () => {
    // Like the e2e tests' fake YouTube mix: three songs of 90 s and a tracklist.
    const SONG_MS = 90_000;
    const long = (song: (typeof SYNTHETIC_SONGS)[number], from: number) => renderSong(longVersion(song, 120), RATE, SONG_MS, from);
    const mix = new Float32Array([...long(A!, 0), ...long(B!, 3000), ...long(C!, 9000)]);
    const capture = fakeCapture(mix, 3 * SONG_MS);
    await backend?.shutdown();
    await start(capture);
    const url = 'https://www.youtube.com/watch?v=abcdefghijk';
    const artist = 'Music Fetch Test Signals';
    const site = {
      title: 'Fake Mix',
      artist: 'Test Channel',
      durationMs: 3 * SONG_MS,
      description: `Tracklist:\n0:00 ${artist} - Alpha Signal\n1:30 ${artist} - Bravo Signal\n3:00 ${artist} - Charlie Signal`,
      chapters: [
        { title: 'Alpha Signal', startMs: 0 },
        { title: 'Bravo Signal', startMs: SONG_MS },
        { title: 'Charlie Signal', startMs: 2 * SONG_MS },
      ],
    };
    const id = await backend.analyze(url, 'Mix', { tab_id: 1, url, title: 'Fake Mix', facts: { site } });
    const stream = await ended(id, 120_000);
    const brief = stream.filter((e) => typeof e === 'string' || !('Level' in e || 'Pitch' in e));
    expect(checkEventContract(stream), JSON.stringify(brief).slice(0, 2000)).toEqual([]);
    expect(capture.asked[0]).toBe(`attach ${url}`);
    expect(capture.asked[1]).toBe(`listenThrough 0-${3 * SONG_MS} @4`);
    const detail = await backend.session(id);
    const plays = detail!.segments.filter((s) => s.track);
    const where = `${JSON.stringify(stub.queries.map((q) => q.song))} ${JSON.stringify(plays.map((s) => [s.track!.title, s.start_ms, s.end_ms, s.tier]))}`;
    expect(plays.map((s) => s.track!.title), where).toEqual(['Alpha Signal', 'Bravo Signal', 'Charlie Signal']);
    expect(Math.abs(plays[0]!.end_ms - SONG_MS), where).toBeLessThanOrEqual(2_000);
    expect(Math.abs(plays[1]!.end_ms - 2 * SONG_MS), where).toBeLessThanOrEqual(2_000);
    expect(plays.every((s) => s.tier === 'Confirmed'), where).toBe(true);
    expect(detail!.entry.status).toBe('Completed');
  }, 180_000);

  it('plans survey windows and reads the page facts as hints', () => {
    expect(surveyWindows(240_000)).toEqual([[113_000, 127_000], [41_000, 55_000], [185_000, 199_000]]);
    expect(surveyWindows(10_000)).toEqual([[0, 10_000], [0, 10_000], [0, 10_000]]);
    const page = pageMedia('https://example.com/v', {
      tab_id: 3,
      url: 'https://example.com/v',
      title: 'Tab title',
      facts: { site: { title: 'Set', artist: 'DJ', durationMs: 3_600_000, description: '0:00 A - B', chapters: [{ title: 'One', startMs: 0.4 }] } },
    });
    expect(page).toMatchObject({ title: 'Set', uploader: 'DJ', duration_ms: 3_600_000, description: '0:00 A - B', chapters: [{ start_ms: 0, title: 'One' }] });
  });

  it('rejects bad input with the core’s error codes', async () => {
    await expect(backend.analyze('not a link', 'Auto', null)).rejects.toMatchObject({ code: 'InvalidInput' });
    await expect(backend.searchLyrics('   ')).rejects.toMatchObject({ code: 'InvalidInput' });
  });
});
