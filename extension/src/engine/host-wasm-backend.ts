import type { PcmChunk } from '../audio/types';
import { addCoverage, coverageGaps, type CaptureEvent, type CaptureHost, type Coverage } from '../capture/types';
import { engineFetch } from '../net/fetch';
import type { SiteFacts } from '../page/site';
import { EngineError, type ActiveSession, type EngineInfo, type EngineListener } from './client';
import { isTerminal } from './events';
import type { BackendEnv, EngineBackend, LiveInput, NetProbeResult } from './host-backend';
import { extractUrl } from './links';
import { routeLink } from './mock/engine';
import type {
  AnalyzeMode,
  CaptureSource,
  ExportFormat,
  LibraryEntry,
  LibraryQuery,
  ListenMode,
  ListenOptions,
  Lyrics,
  MelodyIndexStatus,
  PageContext,
  PageMedia,
  Segment,
  SessionDetail,
  SessionEvent,
  StorageUsage,
  Track,
} from './types';
import type { WebEngineConfig } from './types.gen';

/**
 * The wasm engine (`core/crates/mfcore-wasm`) as the engine Worker serves it: the core's
 * `Engine` one to one, plus what the host needs (extension plan §2.8, ext-runtime's
 * "For the other agents"):
 *
 * - live sessions take the host's PCM (`LiveSession.pushPcm`); "This tab" is a recording
 *   of what plays, `SystemAudio` to the engine;
 * - "Analyze this page" on a media page drives the host's `CaptureHost` (listen-through
 *   for mixes and long media, survey windows for a song) and feeds what it hears to
 *   `Engine.analyzeCaptured`, which runs the core's survey or mix pipeline on it;
 * - the melody index is read into memory before a session that may hum.
 */

/** The facade's classes, as `mfcore_wasm.js` exports them. */
export interface WasmModule {
  default(options?: { module_or_path: string | URL | BufferSource | WebAssembly.Module }): Promise<unknown>;
  Engine: WasmEngineStatic;
}

interface WasmEngineStatic {
  create(config: WebEngineConfig, host: { onEvent(sessionId: string, event: SessionEvent): void }): Promise<WasmEngine>;
  coreVersion(): string;
  memoryBytes(): number;
}

interface WasmLiveSession {
  readonly id: string;
  pushPcm(samples: Float32Array, sampleRate: number, channels: number): void;
  finish(): void;
  cancel(): void;
  isCapturing(): boolean;
}

interface WasmCaptureSession {
  readonly id: string;
  push(mediaMs: number, samples: Float32Array, sampleRate: number, speed: number, continuous: boolean): void;
  setDuration(ms: number): void;
  finish(): void;
  fail(code: string, message: string): void;
  cancel(): void;
}

interface WasmEngine {
  readonly interruptedSessions: string[];
  startListening(source: 'Microphone' | 'SystemAudio', mode: ListenMode, options: ListenOptions): WasmLiveSession;
  analyze(input: string, mode: AnalyzeMode): string;
  analyzeFile(name: string, bytes: Uint8Array, mode: AnalyzeMode): string;
  analyzeCaptured(page: PageMedia, mode: AnalyzeMode): WasmCaptureSession;
  searchLyrics(text: string): string;
  cancelSession(id: string): boolean;
  finishSession(id: string): boolean;
  library(query: LibraryQuery): LibraryEntry[];
  session(id: string): SessionDetail | null;
  deleteSessions(ids: string[]): Promise<void>;
  setPinned(id: string, pinned: boolean): void;
  setPinnedMany(ids: string[], pinned: boolean): void;
  correctSegment(sessionId: string, segmentId: string, track: Track): Segment;
  rejectSegment(sessionId: string, segmentId: string): Segment;
  confirmCandidate(sessionId: string, track: Track): void;
  exportSession(id: string, format: ExportFormat): string;
  storageUsage(): StorageUsage;
  clearCache(): void;
  retryWaiting(): number;
  melodyIndexStatus(): MelodyIndexStatus;
  checkMelodyIndex(): Promise<MelodyIndexStatus>;
  installMelodyIndex(): string;
  removeMelodyIndex(): void;
  loadMelodyIndex(): Promise<boolean>;
  unloadMelodyIndex(): void;
  melodyIndexLoaded(): boolean;
  enrichTrack(track: Track): Promise<Track>;
  fetchLyrics(track: Track): Promise<Lyrics | null>;
  setLowPower(enabled: boolean): void;
  isLowPower(): boolean;
  close(): Promise<void>;
}

/**
 * The published melody index manifests (docs/CORE_API.md "Melody index"): the phone tier
 * on Android, the large tier on desktops (as the Mac app).
 */
export function melodyIndexManifest(android: boolean): string {
  return `https://github.com/x1f4r/music-fetch/releases/download/melody-index-2/manifest-${android ? 'android' : 'mac'}.json`;
}
/** Media shorter than this without "Mix" gets survey windows, not a listen-through (plan §2.4). */
const LONG_MEDIA_MS = 15 * 60_000;
/** Survey windows: the core's first probes (50 %, 20 %, 80 %) of 12 s, with a second each side. */
const SURVEY_FRACTIONS = [0.5, 0.2, 0.8];
const SURVEY_WINDOW_MS = 14_000;

/** A rejected facade call (an `Error` with the core's `code`) as the UI's `EngineError`. */
export function engineError(error: unknown): EngineError {
  if (error instanceof EngineError) return error;
  const code = (error as { code?: unknown } | null)?.code;
  const message = error instanceof Error ? error.message : String(error);
  return new EngineError(typeof code === 'string' ? (code as EngineError['code']) : 'Internal', message);
}

function call<T>(fn: () => T): T {
  try {
    return fn();
  } catch (error) {
    throw engineError(error);
  }
}

async function callAsync<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    throw engineError(error);
  }
}

/** What the page's site adapter read, as the engine's hints. */
export function pageMedia(url: string, page: PageContext | null): PageMedia {
  const site = (page?.facts as { site?: SiteFacts | null } | null)?.site ?? null;
  return {
    url,
    title: site?.title ?? page?.title ?? null,
    uploader: site?.artist ?? null,
    duration_ms: site?.durationMs ?? null,
    description: site?.description ?? null,
    chapters: (site?.chapters ?? []).map((c) => ({ start_ms: Math.max(0, Math.round(c.startMs)), title: c.title })),
  };
}

/** Survey windows for a song of `durationMs`, in media ms. */
export function surveyWindows(durationMs: number): [number, number][] {
  const len = Math.min(SURVEY_WINDOW_MS, durationMs);
  return SURVEY_FRACTIONS.map((f) => {
    const start = Math.max(0, Math.min(durationMs - len, Math.round(durationMs * f - len / 2)));
    return [start, start + len] as [number, number];
  });
}

interface Active extends ActiveSession {
  live?: WasmLiveSession;
  capture?: CapturedAnalysis;
}

/** Drives the host's capture for one analysis and feeds the audio to the engine. */
class CapturedAnalysis {
  private coverage: Coverage = [];
  private released = false;

  constructor(
    readonly session: WasmCaptureSession,
    private readonly host: CaptureHost,
    private readonly url: string,
    private readonly tabId: number | null,
    private readonly mode: AnalyzeMode,
    private readonly lowPower: boolean,
  ) {}

  push(chunk: PcmChunk) {
    if (chunk.mediaMs === null || this.released) return;
    const speed = chunk.speed || 1;
    this.session.push(chunk.mediaMs, chunk.samples, chunk.sampleRate, speed, chunk.continuous);
    const endMs = chunk.mediaMs + (chunk.samples.length / chunk.sampleRate) * 1000 * speed;
    this.coverage = addCoverage(this.coverage, [chunk.mediaMs, endMs]);
  }

  event(event: CaptureEvent) {
    if (event.type === 'closed') this.session.fail('MediaUnavailable', 'The tab was closed.');
    if (event.type === 'navigated') this.session.fail('MediaUnavailable', 'The page changed.');
  }

  async release() {
    if (this.released) return;
    this.released = true;
    await this.host.release(this.session.id).catch(() => undefined);
  }

  async run(): Promise<void> {
    const id = this.session.id;
    try {
      const target = await this.host.attach(id, this.url, this.tabId);
      if (target.live || !target.durationMs) {
        this.session.fail('MediaUnavailable', 'Live media can only be identified by listening.');
        return;
      }
      const duration = target.durationMs;
      this.session.setDuration(duration);
      const rate = Math.max(1, Math.min(target.maxRate, this.lowPower ? 2 : 4));
      if (this.mode === 'Mix' || duration >= LONG_MEDIA_MS) {
        await this.host.listenThrough(id, 0, duration, rate);
        // Re-capture what an ad, a stall or a pause left out.
        const gaps = coverageGaps(this.coverage, 0, duration, 2000);
        if (!this.released && gaps.length > 0 && target.canSeek) await this.host.windows(id, gaps, rate);
      } else {
        await this.host.windows(id, surveyWindows(duration), Math.min(2, target.maxRate));
      }
      await this.release();
      this.session.finish();
    } catch (error) {
      this.session.fail('MediaUnavailable', error instanceof Error ? error.message : String(error));
    } finally {
      await this.release();
    }
  }
}

export class WasmBackend implements EngineBackend {
  private readonly listeners = new Set<EngineListener>();
  private readonly active = new Map<string, Active>();
  private engine: WasmEngine | null = null;

  private constructor(
    private readonly wasm: WasmModule,
    private readonly env: BackendEnv,
  ) {}

  /** Starts the engine over OPFS (`storage: 'memory'` in tests and Node). */
  static async create(wasm: WasmModule, env: BackendEnv, config: WebEngineConfig = {}): Promise<WasmBackend> {
    const backend = new WasmBackend(wasm, env);
    backend.engine = await wasm.Engine.create(
      {
        app_version: import.meta.env?.MODE === 'test' ? '1.1.0' : browserVersion(),
        melody_index_manifest_url: melodyIndexManifest(env.android),
        low_power: env.lowPower,
        ...config,
      },
      { onEvent: (sessionId, event) => backend.sessionEvent(sessionId, event) },
    );
    return backend;
  }

  private get core(): WasmEngine {
    if (!this.engine) throw new EngineError('Internal', 'The engine is not running.');
    return this.engine;
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

  private sessionEvent(sessionId: string, event: SessionEvent) {
    const started = typeof event === 'object' && 'Started' in event ? event.Started : null;
    if (started) {
      const known = this.active.get(sessionId);
      this.active.set(sessionId, { ...known, id: sessionId, kind: started.kind, source: known?.source ?? null, started_at_ms: known?.started_at_ms ?? Date.now() });
    }
    this.notify((l) => l.sessionEvent?.(sessionId, event));
    if (started) this.notify((l) => l.libraryChanged?.());
    if (isTerminal(event)) {
      const ended = this.active.get(sessionId);
      this.active.delete(sessionId);
      void ended?.capture?.release();
      this.notify((l) => l.libraryChanged?.());
    }
  }

  subscribe(listener: EngineListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async hello(): Promise<EngineInfo> {
    return { kind: 'wasm', coreVersion: this.wasm.Engine.coreVersion() };
  }

  async activeSessions(): Promise<ActiveSession[]> {
    return [...this.active.values()]
      .map(({ id, kind, source, started_at_ms }) => ({ id, kind, source, started_at_ms }))
      .sort((a, b) => b.started_at_ms - a.started_at_ms);
  }

  // MARK: live sessions

  async startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions): Promise<string> {
    return this.startLive(source, mode, options, { kind: 'none', sampleRate: 0, label: '', directive: null });
  }

  async startLive(source: CaptureSource, mode: ListenMode, options: ListenOptions, input: LiveInput): Promise<string> {
    void input;
    const engine = this.core;
    // Humming needs the melody index in memory: read it now (once per Worker).
    if (mode !== 'Recording' && source === 'Microphone' && !engine.melodyIndexLoaded() && engine.melodyIndexStatus().installed) {
      await callAsync(() => engine.loadMelodyIndex());
    }
    const live = call(() => engine.startListening(source === 'Tab' ? 'SystemAudio' : source, mode, options));
    const kind = mode === 'Hum' && source === 'Microphone' ? 'Hum' : source === 'Microphone' ? 'Listen' : 'SystemAudio';
    const known = this.active.get(live.id);
    this.active.set(live.id, { id: live.id, kind: known?.kind ?? kind, source, started_at_ms: known?.started_at_ms ?? Date.now(), live });
    return live.id;
  }

  pushPcm(sessionId: string, chunk: PcmChunk): void {
    const active = this.active.get(sessionId);
    if (active?.live) active.live.pushPcm(chunk.samples, chunk.sampleRate / (chunk.speed || 1), 1);
    else active?.capture?.push(chunk);
  }

  async captureEnded(sessionId: string, reason: string): Promise<void> {
    void reason;
    await this.finishSession(sessionId);
  }

  async captureEvent(sessionId: string, event: CaptureEvent): Promise<void> {
    const capture = this.active.get(sessionId)?.capture;
    if (!capture) return;
    capture.event(event);
    // The engine hears nothing either: say why (a CORS-tainted player).
    if (event.type === 'silent') this.notify((l) => l.sessionEvent?.(sessionId, { Notice: { notice: 'CaptureSilent' } }));
  }

  async finishSession(sessionId: string): Promise<void> {
    this.core.finishSession(sessionId);
  }

  async cancelSession(sessionId: string): Promise<void> {
    const capture = this.active.get(sessionId)?.capture;
    this.core.cancelSession(sessionId);
    await capture?.release();
  }

  // MARK: analyses

  async analyze(input: string, mode: AnalyzeMode, page: PageContext | null): Promise<string> {
    const engine = this.core;
    const href = extractUrl(input.trim());
    const hold = import.meta.env.MODE === 'e2e' && href ? Number(new URL(href).searchParams.get('mf-e2e-hold')) : 0;
    if (hold > 0) return this.holdSession(hold);
    if (href && page?.tab_id != null && this.wantsCapture(new URL(href), mode)) {
      const session = call(() => engine.analyzeCaptured(pageMedia(href, page), mode));
      const capture = new CapturedAnalysis(session, this.env.capture, href, page.tab_id, mode, this.env.lowPower);
      this.active.set(session.id, { id: session.id, kind: 'Link', source: null, started_at_ms: Date.now(), capture });
      void capture.run();
      return session.id;
    }
    return this.track(call(() => engine.analyze(input, mode)), 'Link');
  }

  /** Media pages are captured; streaming and catalog links resolve as metadata. */
  private wantsCapture(url: URL, mode: AnalyzeMode): boolean {
    const shape = routeLink(url).shape;
    return mode === 'Mix' || shape === 'media' || shape === 'mix';
  }

  private track(id: string, kind: ActiveSession['kind']): string {
    if (!this.active.has(id)) this.active.set(id, { id, kind, source: null, started_at_ms: Date.now() });
    return id;
  }

  async analyzeFile(name: string, bytes: ArrayBuffer, mode: AnalyzeMode): Promise<string> {
    return this.track(call(() => this.core.analyzeFile(name, new Uint8Array(bytes), mode)), 'File');
  }

  async searchLyrics(text: string): Promise<string> {
    return this.track(call(() => this.core.searchLyrics(text)), 'Lyrics');
  }

  // MARK: library

  async library(query: LibraryQuery): Promise<LibraryEntry[]> {
    return call(() => this.core.library(query));
  }

  async session(id: string): Promise<SessionDetail | null> {
    return call(() => this.core.session(id));
  }

  async deleteSessions(ids: string[]): Promise<void> {
    await callAsync(() => this.core.deleteSessions(ids));
    this.notify((l) => l.libraryChanged?.());
  }

  async setPinned(id: string, pinned: boolean): Promise<void> {
    call(() => this.core.setPinned(id, pinned));
    this.notify((l) => l.libraryChanged?.());
  }

  async setPinnedMany(ids: string[], pinned: boolean): Promise<void> {
    call(() => this.core.setPinnedMany(ids, pinned));
    this.notify((l) => l.libraryChanged?.());
  }

  async correctSegment(sessionId: string, segmentId: string, track: Track): Promise<Segment> {
    const segment = call(() => this.core.correctSegment(sessionId, segmentId, track));
    this.notify((l) => l.libraryChanged?.());
    return segment;
  }

  async rejectSegment(sessionId: string, segmentId: string): Promise<Segment> {
    const segment = call(() => this.core.rejectSegment(sessionId, segmentId));
    this.notify((l) => l.libraryChanged?.());
    return segment;
  }

  async confirmCandidate(sessionId: string, track: Track): Promise<void> {
    call(() => this.core.confirmCandidate(sessionId, track));
    this.notify((l) => l.libraryChanged?.());
  }

  async exportSession(id: string, format: ExportFormat): Promise<string> {
    return call(() => this.core.exportSession(id, format));
  }

  async storageUsage(): Promise<StorageUsage> {
    return call(() => this.core.storageUsage());
  }

  async clearCache(): Promise<void> {
    call(() => this.core.clearCache());
  }

  async retryWaiting(): Promise<number> {
    return call(() => this.core.retryWaiting());
  }

  // MARK: melody index

  async melodyIndexStatus(): Promise<MelodyIndexStatus> {
    return call(() => this.core.melodyIndexStatus());
  }

  async checkMelodyIndex(): Promise<MelodyIndexStatus> {
    return callAsync(() => this.core.checkMelodyIndex());
  }

  async installMelodyIndex(): Promise<string> {
    return this.track(call(() => this.core.installMelodyIndex()), 'Hum');
  }

  async removeMelodyIndex(): Promise<void> {
    call(() => this.core.removeMelodyIndex());
  }

  // MARK: tracks

  async enrichTrack(track: Track): Promise<Track> {
    return callAsync(() => this.core.enrichTrack(track));
  }

  async fetchLyrics(track: Track): Promise<Lyrics | null> {
    return callAsync(() => this.core.fetchLyrics(track));
  }

  async setLowPower(enabled: boolean): Promise<void> {
    this.core.setLowPower(enabled);
  }

  async isLowPower(): Promise<boolean> {
    return this.core.isLowPower();
  }

  // MARK: host

  async shutdown(): Promise<void> {
    const engine = this.engine;
    this.engine = null;
    for (const active of this.active.values()) await active.capture?.release();
    await engine?.close();
  }

  /**
   * e2e builds only: a session that runs `ms` with no audio and no network, then ends
   * `NoMatch` (the keepalive test): a live session nobody feeds, finished after `ms`.
   */
  private holdSession(ms: number): string {
    const live = call(() => this.core.startListening('SystemAudio', 'Recording', { auto_stop: false }));
    this.active.set(live.id, { id: live.id, kind: 'SystemAudio', source: null, started_at_ms: Date.now(), live });
    setTimeout(() => live.finish(), ms);
    return live.id;
  }

  async netProbe(url: string, init: { method?: string; body?: string }): Promise<NetProbeResult> {
    if (import.meta.env.MODE !== 'e2e') throw new EngineError('InvalidInput', 'Only test builds probe the network.');
    const response = await engineFetch(url, { method: init.method ?? 'GET', ...(init.body ? { body: init.body, headers: { 'content-type': 'application/json' } } : {}) }, { allowHttp: true, allowPrivate: true });
    return { status: response.status, url: response.url, body: await response.text() };
  }
}

/** The extension's version, for the engine's neutral user agent. */
function browserVersion(): string {
  try {
    const runtime = (globalThis as { chrome?: { runtime?: { getManifest?(): { version: string } } } }).chrome?.runtime;
    return runtime?.getManifest?.().version ?? '1.1.0';
  } catch {
    return '1.1.0';
  }
}
