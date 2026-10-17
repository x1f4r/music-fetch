import { describe, expect, it, vi } from 'vitest';
import { checkEventContract, eventName } from '@/src/engine/events';
import { MockEngine, MOCK_DIRECTIVES, routeLink, type MockDirective } from '@/src/engine/mock/engine';
import { memoryPersistence } from '@/src/engine/mock/persistence';
import { fromRecording, parseRecording } from '@/src/engine/mock/scripts';
import { libraryQuery, type SessionEvent } from '@/src/engine/types';
import recording from '../fixtures/hum-no-index.jsonl?raw';

function engine(options: { directive?: MockDirective; persistence?: ReturnType<typeof memoryPersistence>; seed?: boolean } = {}) {
  let next: string | null = options.directive ?? null;
  return new MockEngine({
    timeScale: 0,
    persistence: options.persistence,
    seed: options.seed,
    directive: () => {
      const value = next;
      next = null;
      return value;
    },
  });
}

async function finished(mock: MockEngine, id: string): Promise<SessionEvent[]> {
  await vi.waitFor(
    () => {
      const events = mock.eventsOf(id);
      const last = events.at(-1);
      if (!last || !['Finished', 'Failed', 'Canceled'].includes(eventName(last))) throw new Error('still running');
    },
    { timeout: 3000, interval: 5 },
  );
  return mock.eventsOf(id);
}

describe('the mock engine', () => {
  it.each(['match', 'nomatch', 'quiet', 'silent', 'ratelimited', 'offline', 'fail'] as const)(
    'keeps the event contract for a live session (%s)',
    async (directive) => {
      const mock = engine({ directive });
      const id = await mock.startListening('Tab', 'Auto', { auto_stop: true });
      const events = await finished(mock, id);
      expect(checkEventContract(events)).toEqual([]);
    },
  );

  it.each([
    ['a Spotify track', 'https://open.spotify.com/track/7qiZfU4dY1lWllzX7mPBI3'],
    ['an album', 'https://music.apple.com/us/album/1989/1440935467'],
    ['a video', 'https://www.youtube.com/watch?v=abc'],
    ['a mix', 'https://www.mixcloud.com/someone/a-long-mix/'],
    ['a failing link', 'https://www.youtube.com/watch?v=abc&mf-mock=fail'],
  ])('keeps the event contract for %s', async (_name, url) => {
    const mock = engine();
    const id = await mock.analyze(url, 'Auto', null);
    const events = await finished(mock, id);
    expect(checkEventContract(events)).toEqual([]);
  });

  it('keeps the contract for hums with and without the melody library', async () => {
    const mock = engine();
    const missing = await finished(mock, await mock.startListening('Microphone', 'Hum', { auto_stop: true }));
    expect(checkEventContract(missing)).toEqual([]);
    expect(missing).toContainEqual({ Notice: { notice: 'MelodyIndexMissing' } });

    await finished(mock, await mock.installMelodyIndex());
    expect((await mock.melodyIndexStatus()).installed).toBe(true);
    const hum = await finished(mock, await mock.startListening('Microphone', 'Hum', { auto_stop: true }));
    expect(checkEventContract(hum)).toEqual([]);
    const id = (hum[0] as { Started: { session_id: string } }).Started.session_id;
    const detail = await mock.session(id);
    expect(detail?.entry.status).toBe('Candidates');
    expect(detail?.candidates.map((c) => c.tier)).toEqual(['Strong', 'Possible', 'LongShot']);
  });

  it('folds events into the library like the core store', async () => {
    const mock = engine();
    const id = await mock.analyze('https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M', 'Auto', { tab_id: 1, url: null, title: 'Today’s hits', facts: null });
    await finished(mock, id);
    const [entry] = await mock.library(libraryQuery());
    expect(entry).toMatchObject({ id, kind: 'Link', status: 'Completed', title: 'Today’s hits', track_count: 5 });
    const detail = await mock.session(id);
    expect(detail?.segments).toHaveLength(5);
    expect(detail?.source?.service).toBe('Spotify');
  });

  it('stitches segments: a removed segment leaves the library', async () => {
    const mock = engine();
    const id = await mock.analyze('https://www.youtube.com/watch?v=mix&mf-mock=mix', 'Auto', null);
    const events = await finished(mock, id);
    expect(events.some((e) => eventName(e) === 'SegmentRemoved')).toBe(true);
    const detail = await mock.session(id);
    expect(detail?.segments.map((s) => s.id)).not.toContain('seg-5b');
    expect(detail?.segments).toHaveLength(10);
  });

  it('cancels, deletes, pins and corrects', async () => {
    const mock = engine();
    const changed = vi.fn();
    mock.subscribe({ libraryChanged: changed });
    const id = await mock.startListening('Microphone', 'Auto', { auto_stop: true });
    await mock.cancelSession(id);
    expect(mock.eventsOf(id).at(-1)).toBe('Canceled');
    expect((await mock.session(id))?.entry.status).toBe('Canceled');

    const done = await mock.startListening('Microphone', 'Auto', { auto_stop: true });
    await finished(mock, done);
    await mock.setPinnedMany([done], true);
    expect(await mock.library(libraryQuery({ pinned_only: true }))).toHaveLength(1);

    const [segment] = (await mock.session(done))!.segments;
    const rejected = await mock.rejectSegment(done, segment!.id);
    expect(rejected).toMatchObject({ kind: 'Unidentified', track: null });
    expect(rejected.alternates).toHaveLength(1);
    const corrected = await mock.correctSegment(done, segment!.id, rejected.alternates[0]!);
    expect(corrected).toMatchObject({ kind: 'Track', tier: 'Confirmed', confidence: 1 });

    await mock.deleteSessions([id, done]);
    expect(await mock.library(libraryQuery())).toHaveLength(0);
    expect(changed).toHaveBeenCalled();
  });

  it('saves captures made offline and identifies them on retry', async () => {
    const mock = engine({ directive: 'offline' });
    const id = await mock.startListening('Microphone', 'Auto', { auto_stop: true });
    const events = await finished(mock, id);
    expect(events.at(-1)).toEqual({ Finished: { outcome: 'Waiting' } });
    expect((await mock.session(id))?.entry.status).toBe('Waiting');
    await expect(mock.retryWaiting()).resolves.toBe(1);
    expect((await mock.session(id))?.entry.status).toBe('Matched');
    await expect(mock.retryWaiting()).resolves.toBe(0);
  });

  it('persists the library and fails sessions a crash left running', async () => {
    const persistence = memoryPersistence();
    const first = engine({ persistence, seed: true });
    expect(await first.library(libraryQuery())).toHaveLength(3);
    const running = await first.startListening('Microphone', 'Auto', { auto_stop: true });
    const second = engine({ persistence });
    expect((await second.session(running))?.entry.status).toBe('Failed');
    expect(await second.library(libraryQuery())).toHaveLength(4);
  });

  it('exports tracklists', async () => {
    const mock = engine();
    const id = await mock.analyze('https://www.mixcloud.com/someone/a-long-mix/', 'Auto', null);
    await finished(mock, id);
    const text = await mock.exportSession(id, 'Text');
    expect(text.split('\n')[0]).toMatch(/^\[00:00\] .+ - .+$/);
    expect((await mock.exportSession(id, 'Csv')).split('\n')[0]).toBe('start,end,artist,title,album,isrc,tier');
    expect(await mock.exportSession(id, 'Cue')).toContain('TRACK 01 AUDIO');
    expect(JSON.parse(await mock.exportSession(id, 'Json')).entry.id).toBe(id);
  });

  it('rejects input the core would reject', async () => {
    const mock = engine();
    await expect(mock.analyze('   ', 'Auto', null)).rejects.toMatchObject({ code: 'InvalidInput' });
    await expect(mock.analyze('ftp://example.com/x', 'Auto', null)).rejects.toMatchObject({ code: 'UnsupportedLink' });
    await expect(mock.analyze('https://example.com/?mf-mock=unsupported', 'Auto', null)).rejects.toMatchObject({ code: 'UnsupportedLink' });
    await expect(mock.searchLyrics(' ')).rejects.toMatchObject({ code: 'InvalidInput' });
  });

  it('routes links roughly like the core', () => {
    expect(routeLink(new URL('https://open.spotify.com/intl-de/track/abc'))).toMatchObject({ shape: 'track', service: 'Spotify' });
    expect(routeLink(new URL('https://music.youtube.com/playlist?list=x'))).toMatchObject({ shape: 'collection' });
    expect(routeLink(new URL('https://soundcloud.com/a/sets/b'))).toMatchObject({ shape: 'collection', service: 'SoundCloud' });
    expect(routeLink(new URL('https://example.com/radio-mix'))).toMatchObject({ shape: 'mix', service: null });
  });

  it('lists every directive it understands', () => {
    expect(MOCK_DIRECTIVES).toContain('offline');
  });
});

describe('recorded engine streams', () => {
  const lines = parseRecording(recording);

  it('parse into the engine types and keep the contract', () => {
    expect(lines.length).toBeGreaterThan(100);
    expect(checkEventContract(lines.map((l) => l.event))).toEqual([]);
    const pitch = lines.find((l) => typeof l.event === 'object' && 'Pitch' in l.event)!.event as Extract<SessionEvent, { Pitch: unknown }>;
    expect(pitch.Pitch.frames.some((f) => f.midi === null)).toBe(true);
    expect(pitch.Pitch.frames.some((f) => typeof f.midi === 'number')).toBe(true);
  });

  it('replay through the mock without their own Started', () => {
    const script = fromRecording(lines);
    expect(script.outcome.some((l) => typeof l.event === 'object' && 'Started' in l.event)).toBe(false);
    expect(script.outcome[0]!.wall_ms).toBe(0);
  });
});
