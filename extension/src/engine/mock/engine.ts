import { EngineError, type ActiveSession, type CaptureErrorCode, type CaptureStatus, type EngineClient, type EngineInfo, type EngineListener } from '../client';
import { isTerminal } from '../events';
import { extractUrl } from '../links';
import {
  type AnalyzeMode,
  type CaptureSource,
  type ExportFormat,
  type LibraryEntry,
  type LibraryQuery,
  type ListenMode,
  type ListenOptions,
  type Lyrics,
  type MelodyIndexStatus,
  type PageContext,
  type Segment,
  type Service,
  type SessionDetail,
  type SessionEvent,
  type SessionKind,
  type StorageUsage,
  type Track,
} from '../types';
import { searchLinks } from '../../providers/search';
import { DEMO_TRACKS, demoTrack } from './catalog';
import { exportDetail } from './export';
import { memoryPersistence, type MockPersistence, type MockSnapshot } from './persistence';
import {
  collectionScript,
  failedScript,
  humScript,
  lyricsScript,
  matchScript,
  mediaScript,
  melodyInstallScript,
  mixScript,
  noMatchScript,
  trackLinkScript,
  waitingScript,
  type MockScript,
  type TimedEvent,
} from './scripts';

/**
 * Outcomes the next live session can be forced to (UI work on states that are hard to
 * reach). Read from `MockEngineOptions.directive` when a session starts; analysis
 * sessions take `mf-mock=<directive>` in the link's query instead.
 */
export const MOCK_DIRECTIVES = [
  'match',
  'hold',
  'nomatch',
  'quiet',
  'silent',
  'ratelimited',
  'offline',
  'fail',
  'mix',
  'unsupported',
  // Capture that can't start (the host rejects startListening).
  'busy',
  'cantlisten',
  'nothing',
  'blocked',
  'micdenied',
] as const;

const CAPTURE_REJECTIONS: Partial<Record<MockDirective, [CaptureErrorCode, string]>> = {
  busy: ['CaptureBusy', 'Another capture is using this tab.'],
  cantlisten: ['CaptureNotAllowed', 'This page can’t be captured.'],
  nothing: ['NothingPlaying', 'Nothing is playing in this tab.'],
  blocked: ['CaptureBlocked', 'This tab blocks audio capture.'],
  micdenied: ['MicDenied', 'The microphone permission is off.'],
};
export type MockDirective = (typeof MOCK_DIRECTIVES)[number];

export interface MockEngineOptions {
  persistence?: MockPersistence;
  /** Multiplies every script delay: 1 = real time, 0 = as fast as timers allow. */
  timeScale?: number;
  /** The directive for the next live session, if any (e.g. read from localStorage). */
  directive?: () => string | null;
  /** Fill an empty library with a few finished sessions. */
  seed?: boolean;
  now?: () => number;
  /** Mark sessions stored as Running as Failed on open, like the core after a crash (default). */
  recoverInterrupted?: boolean;
}

export const MOCK_MELODY_INDEX = { version: '2026.10.1', songs: 58_006, bytes: 55_200_000 } as const;

/** What `hello()` reports as the core version: the release the mock stands in for. */
export const MOCK_CORE_VERSION = '1.1.0-mock';

interface Running {
  id: string;
  kind: SessionKind;
  source: CaptureSource | null;
  startedAt: number;
  script: MockScript;
  state: 'capture' | 'outcome' | 'done';
  timers: Set<ReturnType<typeof setTimeout>>;
  stored: boolean;
  events: SessionEvent[];
  onDone?: (event: SessionEvent) => void;
}

function emptyMelody(): MelodyIndexStatus {
  return { installed: false, version: null, song_count: 0, size_bytes: 0, available_version: null, download_bytes: null };
}

function asDirective(value: string | null | undefined): MockDirective | null {
  return value && (MOCK_DIRECTIVES as readonly string[]).includes(value) ? (value as MockDirective) : null;
}

interface LinkRoute {
  shape: 'track' | 'collection' | 'media' | 'mix';
  service: Service | null;
  site: string;
}

/** Roughly what the core's link routing would decide, by host and path. */
export function routeLink(url: URL): LinkRoute {
  const host = url.hostname.replace(/^(www|m)\./, '');
  const path = url.pathname;
  const has = (re: RegExp) => re.test(path);
  if (host === 'open.spotify.com') return { shape: has(/^\/(intl-[a-z]+\/)?track\//) ? 'track' : 'collection', service: 'Spotify', site: 'Spotify' };
  if (host === 'music.apple.com')
    return { shape: url.searchParams.has('i') || has(/\/song\//) ? 'track' : 'collection', service: 'AppleMusic', site: 'Apple Music' };
  if (host === 'deezer.com') return { shape: has(/\/track\//) ? 'track' : 'collection', service: 'Deezer', site: 'Deezer' };
  if (host === 'tidal.com' || host === 'listen.tidal.com') return { shape: has(/\/track\//) ? 'track' : 'collection', service: 'Tidal', site: 'Tidal' };
  if (host === 'music.youtube.com')
    return { shape: url.searchParams.has('list') && !url.searchParams.has('v') ? 'collection' : 'track', service: 'YouTubeMusic', site: 'YouTube Music' };
  if (host === 'youtube.com' || host === 'youtu.be') {
    if (url.searchParams.has('list') && !url.searchParams.has('v')) return { shape: 'collection', service: 'YouTube', site: 'YouTube' };
    return { shape: /mix|set/i.test(url.search + path) ? 'mix' : 'media', service: 'YouTube', site: 'YouTube' };
  }
  if (host === 'soundcloud.com') return { shape: has(/\/sets\//) ? 'collection' : 'media', service: 'SoundCloud', site: 'SoundCloud' };
  if (host === 'mixcloud.com') return { shape: 'mix', service: 'Mixcloud', site: 'Mixcloud' };
  return { shape: /mix|set/i.test(path) ? 'mix' : 'media', service: null, site: url.hostname };
}

/**
 * A stand-in for the wasm engine. It keeps the core's event guarantees (one `Started`,
 * exactly one terminal event, upserted segments, a final `Candidates` before
 * `Finished(Candidates)`), stores sessions in a library and survives reloads through
 * `persistence`, so UI work can proceed before the real engine exists.
 */
export class MockEngine implements EngineClient {
  private readonly listeners = new Set<EngineListener>();
  private readonly running = new Map<string, Running>();
  private readonly persistence: MockPersistence;
  private readonly now: () => number;
  private snapshot: MockSnapshot;
  private listenCount = 0;
  private installing: string | null = null;

  constructor(private readonly options: MockEngineOptions = {}) {
    this.persistence = options.persistence ?? memoryPersistence();
    this.now = options.now ?? (() => Date.now());
    const loaded = this.persistence.load();
    this.snapshot = loaded ?? { version: 1, sessions: [], melody: emptyMelody(), lowPower: false };
    // Like the core: sessions a crash left running are failed on open.
    let recovered = false;
    for (const detail of this.snapshot.sessions) {
      if (detail.entry.status === 'Running' && options.recoverInterrupted !== false) {
        detail.entry.status = 'Failed';
        recovered = true;
      }
    }
    if (!loaded && options.seed) this.seedLibrary();
    if (recovered || (!loaded && options.seed)) this.save();
    this.listenCount = this.snapshot.sessions.length;
  }

  // MARK: events

  subscribe(listener: EngineListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  protected notify(fn: (listener: EngineListener) => void) {
    for (const listener of this.listeners) {
      try {
        fn(listener);
      } catch (error) {
        console.error('engine listener failed', error);
      }
    }
  }

  private libraryChanged() {
    this.notify((l) => l.libraryChanged?.());
  }

  /** Every event a running or finished-in-this-instance session emitted (for tests). */
  eventsOf(sessionId: string): SessionEvent[] {
    return [...(this.running.get(sessionId)?.events ?? this.finishedEvents.get(sessionId) ?? [])];
  }
  private readonly finishedEvents = new Map<string, SessionEvent[]>();

  // MARK: sessions

  async hello(): Promise<EngineInfo> {
    return { kind: 'mock', coreVersion: MOCK_CORE_VERSION };
  }

  async activeSessions(): Promise<ActiveSession[]> {
    return [...this.running.values()]
      .filter((r) => r.stored)
      .sort((a, b) => b.startedAt - a.startedAt)
      .map((r) => ({ id: r.id, kind: r.kind, source: r.source, started_at_ms: r.startedAt }));
  }

  async startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions): Promise<string> {
    void options;
    const directive = asDirective(this.options.directive?.());
    const rejection = directive ? CAPTURE_REJECTIONS[directive] : undefined;
    if (rejection && (source === 'Tab' || rejection[0] === 'MicDenied')) throw new EngineError(rejection[0], rejection[1]);
    const hum = mode === 'Hum';
    const kind: SessionKind = hum ? 'Hum' : source === 'Microphone' ? 'Listen' : 'SystemAudio';
    const title = hum ? 'Hum' : source === 'Microphone' ? 'Microphone' : 'This tab';
    const n = this.listenCount++;
    let script: MockScript;
    if (hum) {
      script =
        directive === 'nomatch'
          ? humScript([], { indexInstalled: false })
          : humScript([demoTrack(n), demoTrack(n + 3), demoTrack(n + 5)], { indexInstalled: this.snapshot.melody.installed });
    } else {
      switch (directive) {
        case 'nomatch':
          script = noMatchScript('NotFound');
          break;
        case 'quiet':
          script = noMatchScript('TooQuiet');
          break;
        case 'silent':
          script = noMatchScript('CaptureSilent');
          break;
        case 'fail':
          script = failedScript('Network', 'No connection to the recognizer.');
          break;
        case 'ratelimited':
          script = matchScript(demoTrack(n), { rateLimited: true });
          break;
        case 'offline':
          script = waitingScript();
          break;
        case 'hold':
          // Keeps listening until stopped, then matches: deterministic for tests.
          script = { ...matchScript(demoTrack(n), { captureMs: 2000 }), loop: true };
          break;
        default:
          script = matchScript(demoTrack(n));
      }
    }
    return this.begin(kind, source, title, null, script);
  }

  async finishSession(sessionId: string): Promise<void> {
    const run = this.running.get(sessionId);
    if (run?.state === 'capture') this.playOutcome(run);
  }

  async cancelSession(sessionId: string): Promise<void> {
    const run = this.running.get(sessionId);
    if (!run || run.state === 'done') return;
    this.clearTimers(run);
    this.emit(run, 'Canceled');
  }

  async analyze(input: string, mode: AnalyzeMode, page: PageContext | null): Promise<string> {
    const text = input.trim();
    if (!text) throw new EngineError('InvalidInput', 'Paste a link to identify.');
    const href = extractUrl(text);
    if (!href) {
      if (/^[a-z][a-z0-9+.-]*:/i.test(text)) throw new EngineError('UnsupportedLink', 'Only web links can be identified.');
      throw new EngineError('InvalidInput', 'That doesn’t look like a link.');
    }
    const url = new URL(href);
    const directive = asDirective(url.searchParams.get('mf-mock'));
    if (directive === 'unsupported') throw new EngineError('UnsupportedLink', 'This link can’t be identified.');

    const route = routeLink(url);
    const shape = mode === 'Mix' || directive === 'mix' ? 'mix' : route.shape;
    const n = this.listenCount++;
    const pageTitle = page?.title ?? null;
    let script: MockScript;
    if (directive === 'fail') {
      script = failedScript('MediaUnavailable', 'This video is unavailable.', 700);
    } else if (shape === 'track') {
      script = trackLinkScript(demoTrack(n), url.href, route.service, route.site);
    } else if (shape === 'collection') {
      const tracks = [0, 1, 2, 3, 4].map((i) => demoTrack(n + i));
      script = collectionScript(tracks, url.href, route.service, route.site, pageTitle ?? `Mock playlist on ${route.site}`);
    } else if (shape === 'mix') {
      script = mixScript([...DEMO_TRACKS], url.href, route.service, route.site, pageTitle ?? 'Mock DJ set');
    } else {
      const track = directive === 'nomatch' ? null : demoTrack(n);
      const title = pageTitle ?? (track ? `${track.artist} - ${track.title} (Official Video)` : 'Untitled video');
      script = mediaScript(track, url.href, route.service, route.site, title);
    }
    const id = this.begin('Link', null, url.hostname.replace(/^www\./, ''), url.href, script);
    // A mix plays muted at 4× in its tab while it is analyzed (listen-through).
    if (shape === 'mix') this.captureStatus(id, { state: 'capturing', rate: 4, muted: true });
    return id;
  }

  /** Report a page capture's state, as the capture coordinator does next to the engine. */
  captureStatus(sessionId: string, status: CaptureStatus): void {
    this.notify((l) => l.captureStatus?.(sessionId, status));
  }

  async searchLyrics(text: string): Promise<string> {
    const query = text.trim();
    if (!query) throw new EngineError('InvalidInput', 'Type a few words of the lyrics.');
    const n = this.listenCount++;
    const found = /nomatch/i.test(query) ? [] : [demoTrack(n), demoTrack(n + 1)];
    return this.begin('Lyrics', null, `“${query.slice(0, 80)}”`, null, lyricsScript(query, found));
  }

  private begin(kind: SessionKind, source: CaptureSource | null, title: string, sourceUrl: string | null, script: MockScript, stored = true) {
    const id = crypto.randomUUID();
    const startedAt = this.now();
    const run: Running = { id, kind, source, startedAt, script, state: 'capture', timers: new Set(), stored, events: [] };
    this.running.set(id, run);
    if (stored) {
      this.snapshot.sessions.unshift({
        entry: {
          id,
          kind,
          status: 'Running',
          created_at_ms: startedAt,
          title,
          subtitle: null,
          artwork_url: null,
          track: null,
          track_count: 0,
          duration_ms: null,
          source_url: sourceUrl,
          pinned: false,
        },
        source: null,
        segments: [],
        candidates: [],
        transcript: null,
      });
      this.save();
    }
    this.emit(run, { Started: { session_id: id, kind } });
    if (stored) this.libraryChanged();
    this.playCapture(run);
    return id;
  }

  // MARK: replay

  private delay(ms: number): number {
    return Math.max(0, ms * (this.options.timeScale ?? 1));
  }

  private schedule(run: Running, ms: number, fn: () => void) {
    const timer = setTimeout(() => {
      run.timers.delete(timer);
      fn();
    }, this.delay(ms));
    run.timers.add(timer);
  }

  private clearTimers(run: Running) {
    for (const timer of run.timers) clearTimeout(timer);
    run.timers.clear();
  }

  private playEvents(run: Running, events: TimedEvent[], then?: () => void) {
    let last = 0;
    for (const line of events) {
      last = Math.max(last, line.wall_ms);
      this.schedule(run, line.wall_ms, () => this.emit(run, line.event));
    }
    if (then) this.schedule(run, last + 1, then);
  }

  private playCapture(run: Running) {
    if (run.script.capture.length === 0) {
      this.playOutcome(run);
      return;
    }
    this.playEvents(run, run.script.capture, () => (run.script.loop ? this.playCapture(run) : this.playOutcome(run)));
  }

  private playOutcome(run: Running) {
    if (run.state !== 'capture') return;
    run.state = 'outcome';
    this.clearTimers(run);
    this.playEvents(run, run.script.outcome);
  }

  private emit(run: Running, event: SessionEvent) {
    if (run.state === 'done') return;
    run.events.push(event);
    const terminal = isTerminal(event);
    if (terminal) {
      run.state = 'done';
      this.clearTimers(run);
      this.running.delete(run.id);
      this.finishedEvents.set(run.id, run.events);
    }
    if (run.stored) this.apply(run.id, event);
    this.notify((l) => l.sessionEvent?.(run.id, event));
    if (terminal) {
      run.onDone?.(event);
      if (run.stored) {
        this.save();
        this.libraryChanged();
      }
    }
  }

  /** Fold one event into the stored session, as the core's store does. */
  private apply(id: string, event: SessionEvent) {
    const detail = this.detail(id);
    if (!detail) return;
    const entry = detail.entry;
    if (event === 'Canceled') {
      entry.status = 'Canceled';
    } else if ('Source' in event) {
      const info = event.Source.info;
      detail.source = info;
      entry.title = info.title ?? entry.title;
      entry.subtitle = info.uploader ?? info.site ?? entry.subtitle;
      entry.duration_ms = info.duration_ms ?? entry.duration_ms;
      entry.source_url = info.url ?? entry.source_url;
      entry.artwork_url = info.thumbnail_url ?? entry.artwork_url;
    } else if ('Segment' in event) {
      const segment = event.Segment.segment;
      const i = detail.segments.findIndex((s) => s.id === segment.id);
      if (i >= 0) detail.segments[i] = segment;
      else detail.segments.push(segment);
      detail.segments.sort((a, b) => a.item_index - b.item_index || a.start_ms - b.start_ms);
    } else if ('SegmentRemoved' in event) {
      detail.segments = detail.segments.filter((s) => s.id !== event.SegmentRemoved.id);
    } else if ('Candidates' in event) {
      detail.candidates = event.Candidates.candidates;
    } else if ('Failed' in event) {
      entry.status = 'Failed';
    } else if ('Finished' in event) {
      const outcome = event.Finished.outcome;
      if (outcome === 'Waiting') {
        entry.status = 'Waiting';
      } else if ('Matched' in outcome) {
        entry.status = 'Matched';
        this.setEntryTrack(entry, outcome.Matched.track);
      } else if ('Candidates' in outcome) {
        entry.status = 'Candidates';
        const best = detail.candidates[0]?.track;
        if (best) {
          entry.subtitle = `${best.title} — ${best.artist}`;
          entry.artwork_url = best.artwork_url;
        }
      } else if ('Completed' in outcome) {
        entry.status = 'Completed';
        entry.track_count = outcome.Completed.identified;
        const first = detail.segments.find((s) => s.track)?.track;
        if (outcome.Completed.total === 1 && first) this.setEntryTrack(entry, first, false);
      } else {
        entry.status = 'NoMatch';
      }
    }
  }

  private setEntryTrack(entry: LibraryEntry, track: Track, retitle = true) {
    entry.track = track;
    entry.track_count = 1;
    entry.artwork_url = track.artwork_url ?? entry.artwork_url;
    if (retitle) {
      entry.title = track.title;
      entry.subtitle = track.artist;
    }
  }

  private detail(id: string): SessionDetail | undefined {
    return this.snapshot.sessions.find((d) => d.entry.id === id);
  }

  private save() {
    this.persistence.save(this.snapshot);
  }

  // MARK: library

  async library(query: LibraryQuery): Promise<LibraryEntry[]> {
    const needle = query.search?.trim().toLowerCase() ?? '';
    return this.snapshot.sessions
      .map((d) => d.entry)
      .filter((e) => query.kinds.length === 0 || query.kinds.includes(e.kind))
      .filter((e) => !query.pinned_only || e.pinned)
      .filter((e) => {
        if (!needle) return true;
        const hay = [e.title, e.subtitle, e.track?.title, e.track?.artist].filter(Boolean).join(' ').toLowerCase();
        return hay.includes(needle);
      })
      .sort((a, b) => b.created_at_ms - a.created_at_ms)
      .slice(query.offset, query.offset + (query.limit || Number.MAX_SAFE_INTEGER))
      .map((e) => structuredClone(e));
  }

  async session(id: string): Promise<SessionDetail | null> {
    const detail = this.detail(id);
    return detail ? structuredClone(detail) : null;
  }

  async deleteSessions(ids: string[]): Promise<void> {
    for (const id of ids) {
      const run = this.running.get(id);
      if (run) {
        this.clearTimers(run);
        this.emit(run, 'Canceled');
      }
    }
    const drop = new Set(ids);
    this.snapshot.sessions = this.snapshot.sessions.filter((d) => !drop.has(d.entry.id));
    this.save();
    this.libraryChanged();
  }

  async setPinned(id: string, pinned: boolean): Promise<void> {
    await this.setPinnedMany([id], pinned);
  }

  async setPinnedMany(ids: string[], pinned: boolean): Promise<void> {
    const set = new Set(ids);
    for (const detail of this.snapshot.sessions) if (set.has(detail.entry.id)) detail.entry.pinned = pinned;
    this.save();
    this.libraryChanged();
  }

  private segmentOf(sessionId: string, segmentId: string): { detail: SessionDetail; segment: Segment } {
    const detail = this.detail(sessionId);
    const segment = detail?.segments.find((s) => s.id === segmentId);
    if (!detail || !segment) throw new EngineError('InvalidInput', 'That part of the session no longer exists.');
    return { detail, segment };
  }

  async correctSegment(sessionId: string, segmentId: string, track: Track): Promise<Segment> {
    const { segment } = this.segmentOf(sessionId, segmentId);
    if (segment.track && segment.track.identity_key !== track.identity_key) segment.alternates.unshift(segment.track);
    Object.assign(segment, { kind: 'Track', track, tier: 'Confirmed', confidence: 1 });
    segment.evidence.push({ source: 'User', at_ms: segment.start_ms, score: 1, detail: null });
    this.save();
    this.libraryChanged();
    return structuredClone(segment);
  }

  async rejectSegment(sessionId: string, segmentId: string): Promise<Segment> {
    const { segment } = this.segmentOf(sessionId, segmentId);
    if (segment.track) segment.alternates.unshift(segment.track);
    Object.assign(segment, { kind: 'Unidentified', track: null, tier: 'Unknown', confidence: 0 });
    this.save();
    this.libraryChanged();
    return structuredClone(segment);
  }

  async confirmCandidate(sessionId: string, track: Track): Promise<void> {
    const detail = this.detail(sessionId);
    if (!detail) throw new EngineError('InvalidInput', 'That session no longer exists.');
    detail.entry.status = 'Matched';
    this.setEntryTrack(detail.entry, track);
    const i = detail.candidates.findIndex((c) => c.track.identity_key === track.identity_key);
    if (i > 0) detail.candidates.unshift(...detail.candidates.splice(i, 1));
    this.save();
    this.libraryChanged();
  }

  async exportSession(id: string, format: ExportFormat): Promise<string> {
    const detail = this.detail(id);
    if (!detail) throw new EngineError('InvalidInput', 'That session no longer exists.');
    return exportDetail(detail, format);
  }

  // MARK: storage, melody, misc

  async storageUsage(): Promise<StorageUsage> {
    return {
      database_bytes: JSON.stringify(this.snapshot.sessions).length,
      cache_bytes: 0,
      melody_index_bytes: this.snapshot.melody.size_bytes,
    };
  }

  async clearCache(): Promise<void> {}

  /** Like the core: waiting sessions get identified and arrive as library updates. */
  async retryWaiting(): Promise<number> {
    const waiting = this.snapshot.sessions.filter((d) => d.entry.status === 'Waiting');
    waiting.forEach((detail, i) => {
      const track = demoTrack(this.listenCount++ + i);
      detail.entry.status = 'Matched';
      this.setEntryTrack(detail.entry, track);
      detail.segments = [
        {
          id: 'seg-1',
          item_index: 0,
          start_ms: 0,
          end_ms: 8000,
          kind: 'Track',
          track,
          confidence: 0.95,
          tier: 'Confirmed',
          evidence: [{ source: 'Shazam', at_ms: 8000, score: 0.95, detail: null }],
          alternates: [],
          source_url: null,
        },
      ];
    });
    if (waiting.length > 0) {
      this.save();
      this.libraryChanged();
    }
    return waiting.length;
  }

  async melodyIndexStatus(): Promise<MelodyIndexStatus> {
    return structuredClone(this.snapshot.melody);
  }

  async checkMelodyIndex(): Promise<MelodyIndexStatus> {
    const melody = this.snapshot.melody;
    const current = melody.installed && melody.version === MOCK_MELODY_INDEX.version;
    melody.available_version = current ? null : MOCK_MELODY_INDEX.version;
    melody.download_bytes = current ? null : MOCK_MELODY_INDEX.bytes;
    this.save();
    return structuredClone(melody);
  }

  async installMelodyIndex(): Promise<string> {
    if (this.installing) throw new EngineError('InvalidInput', 'The melody index is already downloading.');
    const id = this.begin('Hum', null, 'Melody library', null, melodyInstallScript(), false);
    const run = this.running.get(id);
    this.installing = id;
    const finish = (event: SessionEvent) => {
      this.installing = null;
      if (typeof event !== 'string' && 'Finished' in event) {
        this.snapshot.melody = {
          installed: true,
          version: MOCK_MELODY_INDEX.version,
          song_count: MOCK_MELODY_INDEX.songs,
          size_bytes: MOCK_MELODY_INDEX.bytes,
          available_version: null,
          download_bytes: null,
        };
        this.save();
      }
    };
    if (run) run.onDone = finish;
    else this.installing = null;
    return id;
  }

  async removeMelodyIndex(): Promise<void> {
    if (this.installing) throw new EngineError('InvalidInput', 'The melody index is downloading; cancel the download first.');
    this.snapshot.melody = emptyMelody();
    this.save();
  }

  async enrichTrack(track: Track): Promise<Track> {
    const known = new Set(track.links.map((l) => l.service));
    const extra = searchLinks(track).filter((l) => !known.has(l.service));
    return { ...track, links: [...track.links, ...extra] };
  }

  async fetchLyrics(track: Track): Promise<Lyrics | null> {
    if (!track.title) return null;
    const lines = ['(Mock lyrics, line one)', '(Mock lyrics, line two)', '(Mock lyrics, line three)'];
    return { plain: lines.join('\n'), synced: lines.map((text, i) => ({ t_ms: 12_000 + i * 4000, text })) };
  }

  async setLowPower(enabled: boolean): Promise<void> {
    this.snapshot.lowPower = enabled;
    this.save();
  }

  async isLowPower(): Promise<boolean> {
    return this.snapshot.lowPower;
  }

  // MARK: seed

  private seedLibrary() {
    const hour = 3_600_000;
    const base = this.now();
    const sessions: SessionDetail[] = [];
    const add = (kind: SessionKind, title: string, ago: number, patch: (d: SessionDetail) => void) => {
      const detail: SessionDetail = {
        entry: {
          id: crypto.randomUUID(),
          kind,
          status: 'Completed',
          created_at_ms: base - ago,
          title,
          subtitle: null,
          artwork_url: null,
          track: null,
          track_count: 0,
          duration_ms: null,
          source_url: null,
          pinned: false,
        },
        source: null,
        segments: [],
        candidates: [],
        transcript: null,
      };
      patch(detail);
      sessions.push(detail);
    };
    add('Listen', 'Microphone', hour, (d) => {
      d.entry.status = 'Matched';
      this.setEntryTrack(d.entry, demoTrack(1));
      for (const { event } of matchScript(demoTrack(1)).outcome) {
        if (event !== 'Canceled' && 'Segment' in event) d.segments.push(event.Segment.segment);
      }
    });
    add('Hum', 'Hum', 26 * hour, (d) => {
      d.entry.status = 'NoMatch';
    });
    add('Link', 'open.spotify.com', 30 * hour, (d) => {
      d.entry.pinned = true;
      d.entry.status = 'Completed';
      this.setEntryTrack(d.entry, demoTrack(2));
      d.entry.source_url = 'https://open.spotify.com/track/7qiZfU4dY1lWllzX7mPBI3';
    });
    this.snapshot.sessions = sessions;
  }
}
