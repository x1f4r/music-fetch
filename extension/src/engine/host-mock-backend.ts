import { levelOf, type PcmChunk } from '../audio/types';
import { addCoverage, coverageGaps, coveredMs, type CaptureEvent, type CaptureHost, type Coverage } from '../capture/types';
import { EngineError, type ActiveSession, type EngineInfo, type EngineListener } from './client';
import { isTerminal } from './events';
import { engineFetch } from '../net/fetch';
import type { BackendEnv, EngineBackend, LiveInput, NetProbeResult } from './host-backend';
import type { Kv } from './host-idb';
import {
  matchTones,
  segmentTimeline,
  syntheticTrack,
  ToneTimeline,
  type ToneSegment,
} from './host-mock-audio';
import { extractUrl } from './links';
import { MockEngine, routeLink, type ExternalSession, type MockLiveOutcome } from './mock/engine';
import type { MockPersistence, MockSnapshot } from './mock/persistence';
import {
  type AnalyzeMode,
  type CaptureSource,
  type ExportFormat,
  type LibraryQuery,
  type ListenMode,
  type ListenOptions,
  type PageContext,
  type Segment,
  type SessionEvent,
  type Track,
} from './types';

/**
 * The mock engine as the engine Worker serves it (until the wasm engine replaces it in
 * `host-backend-factory.ts`). Everything is `MockEngine`, except that the audio the host
 * captures now matters, as it will for the real engine:
 *
 * - Live sessions get real `Level` events from the PCM, `CaptureSilent` after 3 s of
 *   digital silence, and recognize the synthetic test songs (`host-mock-audio.ts`) at the
 *   core's query times. Audio with no test song keeps the scripted outcome, so manual
 *   testing on real pages still shows a result; a silent capture ends in
 *   `NoMatch(CaptureSilent)` as the engine's would.
 * - "Analyze this page" on a media page drives the host's `CaptureHost`: listen-through
 *   (mixes, long media) or three survey windows (short media), then segments from the
 *   test songs it heard.
 * - A directive (`mf.mock.next`, or `mf-mock=` in a link) plays the scripted session.
 */

/** Live query times of the core (`pipeline/live.rs` QUERY_TIMES_MS). */
const QUERY_TIMES_MS = [4_000, 8_000, 13_000, 20_000, 28_000];
const MAX_LIVE_MS = 30_000;
const SILENT_NOTICE_MS = 3_000;
/** Media shorter than this without "Mix" gets survey windows, not a listen-through (plan §2.4). */
const LONG_MEDIA_MS = 15 * 60_000;

/** −60 dBFS → 0, 0 dBFS → 1, linear in dB (the core's `perceptual`). */
export function perceptual(linear: number): number {
  const db = 20 * Math.log10(Math.max(linear, 1e-9));
  return Math.min(1, Math.max(0, (db + 60) / 60));
}

/** The mock's library, kept in IndexedDB; written after every change, flushed on shutdown. */
export async function kvPersistence(kv: Kv, key = 'mock.engine'): Promise<MockPersistence & { flush(): Promise<void> }> {
  let current = (await kv.get<MockSnapshot>(key)) ?? null;
  if (current?.version !== 1) current = null;
  let queued = false;
  let writing: Promise<void> = Promise.resolve();
  return {
    load: () => (current ? structuredClone(current) : null),
    save(snapshot) {
      current = structuredClone(snapshot);
      if (queued) return;
      queued = true;
      writing = writing
        .then(() => {
          queued = false;
          return kv.put(key, current);
        })
        .catch((error: unknown) => console.warn('mock engine: could not save the library', error));
    },
    flush: () => writing,
  };
}

/** Recognizes the test songs in a live session's PCM and decides its outcome. */
class LiveProbe {
  readonly timeline = new ToneTimeline();
  heardMs = 0;
  hasAudio = false;
  private signal = false;
  private silentNoticed = false;
  private nextQuery = 0;
  private match: Track | null = null;
  private waiting: ((outcome: MockLiveOutcome | null) => void)[] = [];
  private waitTimer: ReturnType<typeof setTimeout> | null = null;
  private final = false;

  constructor(private readonly emit: (event: SessionEvent) => void) {}

  push(chunk: PcmChunk) {
    this.hasAudio = true;
    this.timeline.push(chunk);
    this.heardMs += (chunk.samples.length / (chunk.sampleRate / (chunk.speed || 1))) * 1000;
    const { rms, peak } = levelOf(chunk.samples);
    if (peak > 1e-4) this.signal = true;
    this.emit({ Level: { rms: perceptual(rms), peak: perceptual(peak) } });
    if (!this.signal && !this.silentNoticed && this.heardMs >= SILENT_NOTICE_MS) {
      this.silentNoticed = true;
      this.emit({ Notice: { notice: 'CaptureSilent' } });
    }
    while (this.nextQuery < QUERY_TIMES_MS.length && this.heardMs >= QUERY_TIMES_MS[this.nextQuery]!) {
      this.nextQuery++;
      this.query();
    }
    if (this.waiting.length) this.settleIfDecided();
  }

  private query() {
    if (this.match) return;
    const found = matchTones(this.timeline.tonesIn(Math.max(0, this.heardMs - 8000), this.heardMs));
    if (found) this.match = syntheticTrack(found.song);
  }

  /**
   * Tones were heard lately but not enough to decide: worth listening on. A test song
   * changes note every 500 ms; a steady tone (a fake microphone's beep) is not one.
   */
  private promising(): boolean {
    const tones = this.timeline.tonesIn(Math.max(0, this.heardMs - 4000), this.heardMs);
    return tones.length >= 10 && new Set(tones.map(([, tone]) => tone)).size >= 3;
  }

  private outcome(final: boolean): MockLiveOutcome | null | undefined {
    if (this.match) return { matched: this.match };
    if (this.hasAudio && !this.signal && this.heardMs >= SILENT_NOTICE_MS) return { noMatch: 'CaptureSilent' };
    if (final || this.heardMs >= MAX_LIVE_MS) return null;
    if (this.hasAudio && !this.promising() && this.heardMs >= 2000) return null;
    return undefined;
  }

  private settleIfDecided() {
    const outcome = this.outcome(this.final);
    if (outcome === undefined || this.waiting.length === 0) return;
    const waiting = this.waiting.splice(0);
    if (this.waitTimer) clearTimeout(this.waitTimer);
    this.waitTimer = null;
    for (const resolve of waiting) resolve(outcome);
  }

  /**
   * The scripted capture ended (`script`) or the capture stopped (`finish`: the user, or
   * the source ended). Audio sent before the stop has arrived by then: it travels in the
   * same queue as the call.
   */
  decide(reason: 'script' | 'finish'): Promise<MockLiveOutcome | null> {
    if (reason === 'finish') {
      this.final = true;
      this.query();
    }
    return new Promise((resolve) => {
      this.waiting.push(resolve);
      this.settleIfDecided();
      if (this.waiting.length && !this.waitTimer) {
        // No audio arriving at all (capture failed silently): don't hang the session.
        this.waitTimer = setTimeout(() => {
          this.final = true;
          this.settleIfDecided();
        }, this.hasAudio ? MAX_LIVE_MS : 3000);
      }
    });
  }
}

/** A capture-driven analysis of a media page: what the engine's E5 pipeline will do. */
class CaptureAnalysis {
  readonly timeline = new ToneTimeline();
  private coverage: Coverage = [];
  private totalMs = 0;
  private lastProgress = 0;
  private released = false;

  constructor(
    private readonly session: ExternalSession,
    private readonly capture: CaptureHost,
    private readonly url: string,
    private readonly tabId: number | null,
    private readonly mode: AnalyzeMode,
    private readonly lowPower: boolean,
  ) {}

  push(chunk: PcmChunk) {
    if (chunk.mediaMs === null) return;
    this.timeline.push(chunk);
    const endMs = chunk.mediaMs + (chunk.samples.length / chunk.sampleRate) * 1000 * chunk.speed;
    this.coverage = addCoverage(this.coverage, [chunk.mediaMs, endMs]);
    const now = Date.now();
    if (this.session.running && this.totalMs > 0 && now - this.lastProgress > 500) {
      this.lastProgress = now;
      const analyzed = Math.min(this.totalMs, Math.round(coveredMs(this.coverage)));
      const left = this.totalMs - analyzed;
      this.session.emit({ Progress: { analyzed_ms: analyzed, total_ms: this.totalMs, eta_ms: Math.round(left / Math.max(1, chunk.speed)) } });
    }
  }

  event(event: CaptureEvent) {
    if (!this.session.running) return;
    // The engine says so too: a capture that plays but hears nothing (CORS-tainted media).
    if (event.type === 'silent') this.session.emit({ Notice: { notice: 'CaptureSilent' } });
    if (event.type === 'closed' || event.type === 'navigated') {
      this.session.emit({ Failed: { code: 'MediaUnavailable', message: event.type === 'closed' ? 'The tab was closed.' : 'The page changed.' } });
    }
  }

  async release() {
    if (this.released) return;
    this.released = true;
    await this.capture.release(this.session.id).catch(() => undefined);
  }

  async run(): Promise<void> {
    const s = this.session;
    try {
      s.emit({ Phase: { phase: 'Resolving', fraction: null } });
      const target = await this.capture.attach(s.id, this.url, this.tabId);
      if (!s.running) return;
      s.emit({
        Source: {
          info: {
            url: target.url,
            title: target.title,
            uploader: null,
            thumbnail_url: null,
            duration_ms: target.durationMs,
            service: routeLink(new URL(target.url)).service,
            site: new URL(target.url).hostname.replace(/^www\./, ''),
            item_count: 1,
            hints: [],
          },
        },
      });
      if (target.live || !target.durationMs) {
        s.emit({ Failed: { code: 'MediaUnavailable', message: 'Live media can only be identified by listening.' } });
        return;
      }
      this.totalMs = target.durationMs;
      const rate = Math.min(target.maxRate, this.lowPower ? 2 : 4);
      s.emit({ Phase: { phase: 'Listening', fraction: null } });
      const through = this.mode === 'Mix' || target.durationMs >= LONG_MEDIA_MS;
      if (through) {
        await this.capture.listenThrough(s.id, 0, target.durationMs, rate);
        // Re-capture what an ad, a stall or a pause left out.
        const gaps = coverageGaps(this.coverage, 0, target.durationMs, 2000);
        if (s.running && gaps.length > 0 && target.canSeek) await this.capture.windows(s.id, gaps, rate);
      } else {
        const len = Math.min(6000, target.durationMs / 3);
        const windows = [0.5, 0.2, 0.8].map((f) => {
          const start = Math.max(0, Math.round(target.durationMs! * f - len / 2));
          return [start, Math.min(target.durationMs!, start + len)] as [number, number];
        });
        await this.capture.windows(s.id, windows, Math.min(2, target.maxRate));
      }
      if (!s.running) return;
      s.emit({ Phase: { phase: 'Recognizing', fraction: null } });
      await this.release();
      const segments = through ? segmentTimeline(this.timeline, target.durationMs) : this.survey(target.durationMs);
      let identified = 0;
      segments.forEach((segment, i) => {
        identified++;
        s.emit({ Segment: { segment: toSegment(segment, i, target.url) } });
      });
      if (identified === 0) s.emit({ Finished: { outcome: { NoMatch: { reason: 'NotFound' } } } });
      else s.emit({ Finished: { outcome: { Completed: { identified, total: segments.length } } } });
    } catch (error) {
      if (s.running) s.emit({ Failed: { code: 'MediaUnavailable', message: error instanceof Error ? error.message : String(error) } });
    } finally {
      await this.release();
    }
  }

  /** Short media: one song if the windows agree. */
  private survey(totalMs: number): ToneSegment[] {
    const frames = this.timeline.tonesIn(0, totalMs);
    const match = matchTones(frames, 20, 0.5);
    return match ? [{ song: match.song, startMs: 0, endMs: totalMs, score: match.score }] : [];
  }
}

function toSegment(segment: ToneSegment, index: number, url: string): Segment {
  return {
    id: `seg-${index + 1}`,
    item_index: 0,
    start_ms: segment.startMs,
    end_ms: segment.endMs,
    kind: 'Track',
    track: syntheticTrack(segment.song),
    confidence: segment.score,
    tier: segment.score >= 0.8 ? 'Confirmed' : 'Likely',
    evidence: [{ source: 'Shazam', at_ms: segment.startMs, score: segment.score, detail: 'mock: synthetic test song' }],
    alternates: [],
    source_url: url,
  };
}

export class MockBackend implements EngineBackend {
  private readonly engine: MockEngine;
  private readonly listeners = new Set<EngineListener>();
  private readonly probes = new Map<string, LiveProbe>();
  private readonly analyses = new Map<string, CaptureAnalysis>();
  private directive: string | null = null;

  constructor(
    private readonly env: BackendEnv,
    private readonly persistence: MockPersistence & { flush?(): Promise<void> },
    options: { timeScale?: number; seed?: boolean } = {},
  ) {
    this.engine = new MockEngine({
      persistence,
      seed: options.seed ?? true,
      timeScale: options.timeScale,
      directive: () => {
        const directive = this.directive;
        this.directive = null;
        return directive;
      },
      liveOutcome: (id, reason) => this.probes.get(id)?.decide(reason) ?? Promise.resolve(null),
    });
    if (env.lowPower) void this.engine.setLowPower(true);
    this.engine.subscribe({
      sessionEvent: (id, event) => this.forward(id, event),
      captureStatus: (id, status) => this.notify((l) => l.captureStatus?.(id, status)),
      libraryChanged: () => this.notify((l) => l.libraryChanged?.()),
    });
  }

  private notify(fn: (listener: EngineListener) => void) {
    for (const listener of this.listeners) {
      try {
        fn(listener);
      } catch (error) {
        console.error('backend listener failed', error);
      }
    }
  }

  private forward(id: string, event: SessionEvent) {
    // Real levels replace the script's once audio arrives.
    if (typeof event === 'object' && 'Level' in event && this.probes.get(id)?.hasAudio) return;
    if (isTerminal(event)) {
      this.probes.delete(id);
      const analysis = this.analyses.get(id);
      this.analyses.delete(id);
      void analysis?.release();
    }
    this.notify((l) => l.sessionEvent?.(id, event));
  }

  subscribe(listener: EngineListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async hello(): Promise<EngineInfo> {
    return this.engine.hello();
  }

  async activeSessions(): Promise<ActiveSession[]> {
    return this.engine.activeSessions();
  }

  async startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions): Promise<string> {
    return this.startLive(source, mode, options, { kind: 'none', sampleRate: 0, label: '', directive: null });
  }

  async startLive(source: CaptureSource, mode: ListenMode, options: ListenOptions, input: LiveInput): Promise<string> {
    this.directive = input.directive;
    const id = await this.engine.startListening(source, mode, options);
    this.directive = null;
    if (mode !== 'Hum' && !input.directive && input.kind !== 'none') {
      this.probes.set(id, new LiveProbe((event) => this.notify((l) => l.sessionEvent?.(id, event))));
    }
    return id;
  }

  pushPcm(sessionId: string, chunk: PcmChunk): void {
    this.probes.get(sessionId)?.push(chunk);
    this.analyses.get(sessionId)?.push(chunk);
  }

  async captureEnded(sessionId: string, reason: string): Promise<void> {
    void reason;
    await this.engine.finishSession(sessionId);
  }

  async captureEvent(sessionId: string, event: CaptureEvent): Promise<void> {
    this.analyses.get(sessionId)?.event(event);
  }

  async finishSession(sessionId: string): Promise<void> {
    await this.engine.finishSession(sessionId);
  }

  async cancelSession(sessionId: string): Promise<void> {
    await this.engine.cancelSession(sessionId);
  }

  async analyze(input: string, mode: AnalyzeMode, page: PageContext | null): Promise<string> {
    const href = extractUrl(input.trim());
    const hold = import.meta.env.MODE === 'e2e' && href ? Number(new URL(href).searchParams.get('mf-e2e-hold')) : 0;
    if (hold > 0) return this.holdSession(href!, hold);
    const directive = (page?.facts?.mockDirective as string | undefined) ?? null;
    if (href && page?.tab_id != null && !directive && !new URL(href).searchParams.has('mf-mock') && this.wantsCapture(new URL(href), mode)) {
      const url = href;
      const title = page.title ?? new URL(url).hostname.replace(/^www\./, '');
      let analysis: CaptureAnalysis | null = null;
      const session = this.engine.startExternal('Link', title, url, () => void analysis?.release());
      analysis = new CaptureAnalysis(session, this.env.capture, url, page.tab_id, mode, this.env.lowPower);
      this.analyses.set(session.id, analysis);
      void analysis.run();
      return session.id;
    }
    const id = await this.engine.analyze(directive && href ? withDirective(href, directive) : input, mode, page);
    return id;
  }

  /** Media pages and local test pages are captured; streaming and catalog links resolve as metadata. */
  private wantsCapture(url: URL, mode: AnalyzeMode): boolean {
    if (url.hostname === '127.0.0.1' || url.hostname === 'localhost') return true;
    const shape = routeLink(url).shape;
    return mode === 'Mix' || shape === 'media' || shape === 'mix';
  }

  analyzeFile(name: string, bytes: ArrayBuffer, mode: AnalyzeMode) {
    return this.engine.analyzeFile(name, bytes, mode);
  }
  searchLyrics(text: string) {
    return this.engine.searchLyrics(text);
  }
  library(query: LibraryQuery) {
    return this.engine.library(query);
  }
  session(id: string) {
    return this.engine.session(id);
  }
  deleteSessions(ids: string[]) {
    return this.engine.deleteSessions(ids);
  }
  setPinned(id: string, pinned: boolean) {
    return this.engine.setPinned(id, pinned);
  }
  setPinnedMany(ids: string[], pinned: boolean) {
    return this.engine.setPinnedMany(ids, pinned);
  }
  correctSegment(sessionId: string, segmentId: string, track: Track) {
    return this.engine.correctSegment(sessionId, segmentId, track);
  }
  rejectSegment(sessionId: string, segmentId: string) {
    return this.engine.rejectSegment(sessionId, segmentId);
  }
  confirmCandidate(sessionId: string, track: Track) {
    return this.engine.confirmCandidate(sessionId, track);
  }
  exportSession(id: string, format: ExportFormat) {
    return this.engine.exportSession(id, format);
  }
  storageUsage() {
    return this.engine.storageUsage();
  }
  clearCache() {
    return this.engine.clearCache();
  }
  retryWaiting() {
    return this.engine.retryWaiting();
  }
  melodyIndexStatus() {
    return this.engine.melodyIndexStatus();
  }
  checkMelodyIndex() {
    return this.engine.checkMelodyIndex();
  }
  installMelodyIndex() {
    return this.engine.installMelodyIndex();
  }
  removeMelodyIndex() {
    return this.engine.removeMelodyIndex();
  }
  enrichTrack(track: Track) {
    return this.engine.enrichTrack(track);
  }
  fetchLyrics(track: Track) {
    return this.engine.fetchLyrics(track);
  }
  setLowPower(enabled: boolean) {
    return this.engine.setLowPower(enabled);
  }
  isLowPower() {
    return this.engine.isLowPower();
  }

  async shutdown(): Promise<void> {
    await this.persistence.flush?.();
  }

  async netProbe(url: string, init: { method?: string; body?: string }): Promise<NetProbeResult> {
    if (import.meta.env.MODE !== 'e2e') throw new EngineError('InvalidInput', 'Only test builds probe the network.');
    const response = await engineFetch(url, { method: init.method ?? 'GET', ...(init.body ? { body: init.body, headers: { 'content-type': 'application/json' } } : {}) }, { allowHttp: true, allowPrivate: true });
    return { status: response.status, url: response.url, body: await response.text() };
  }

  /** e2e builds only: a link session that runs `ms` with no capture and no network (the keepalive test). */
  private holdSession(input: string, ms: number): string {
    const session = this.engine.startExternal('Link', 'Holding', input, () => undefined);
    session.emit({ Phase: { phase: 'Resolving', fraction: null } });
    setTimeout(() => {
      if (session.running) session.emit({ Finished: { outcome: { NoMatch: { reason: 'NotFound' } } } });
    }, ms);
    return session.id;
  }
}

function withDirective(href: string, directive: string): string {
  const url = new URL(href);
  url.searchParams.set('mf-mock', directive);
  return url.href;
}
