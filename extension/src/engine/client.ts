import type {
  AnalyzeMode,
  CaptureSource,
  ErrorCode,
  ExportFormat,
  LibraryEntry,
  LibraryQuery,
  ListenMode,
  ListenOptions,
  Lyrics,
  MelodyIndexStatus,
  PageContext,
  Segment,
  SessionDetail,
  SessionEvent,
  SessionKind,
  StorageUsage,
  Track,
} from './types';

/**
 * What the UI can ask of the engine. It mirrors the core `Engine` (and the planned wasm
 * facade, extension plan §2.8) one to one, except that:
 *
 * - every call is async, because the engine runs in another context (the offscreen
 *   document on Chromium, the event page on Firefox);
 * - live sessions are driven by the engine host, which owns capture, so the UI only
 *   starts, finishes and cancels them by id;
 * - events arrive through `EngineClient.subscribe`, not a per-session observer.
 *
 * Validation errors reject with an `EngineError`; everything after a session starts
 * arrives as `SessionEvent`s.
 */
export interface EngineApi {
  /** Identifies the engine behind the client. Also used as the connection handshake. */
  hello(): Promise<EngineInfo>;
  /** Live sessions that have not ended yet, newest first. */
  activeSessions(): Promise<ActiveSession[]>;

  startListening(source: CaptureSource, mode: ListenMode, options: ListenOptions): Promise<string>;
  /** The user stopped capture: analyze what was heard and end the session. */
  finishSession(sessionId: string): Promise<void>;
  cancelSession(sessionId: string): Promise<void>;
  analyze(input: string, mode: AnalyzeMode, page: PageContext | null): Promise<string>;
  searchLyrics(text: string): Promise<string>;

  library(query: LibraryQuery): Promise<LibraryEntry[]>;
  session(id: string): Promise<SessionDetail | null>;
  deleteSessions(ids: string[]): Promise<void>;
  setPinned(id: string, pinned: boolean): Promise<void>;
  setPinnedMany(ids: string[], pinned: boolean): Promise<void>;
  correctSegment(sessionId: string, segmentId: string, track: Track): Promise<Segment>;
  rejectSegment(sessionId: string, segmentId: string): Promise<Segment>;
  confirmCandidate(sessionId: string, track: Track): Promise<void>;
  exportSession(id: string, format: ExportFormat): Promise<string>;

  storageUsage(): Promise<StorageUsage>;
  clearCache(): Promise<void>;
  retryWaiting(): Promise<number>;

  melodyIndexStatus(): Promise<MelodyIndexStatus>;
  checkMelodyIndex(): Promise<MelodyIndexStatus>;
  /** Starts the download; progress arrives as that session's `Phase(Downloading)` events. */
  installMelodyIndex(): Promise<string>;
  removeMelodyIndex(): Promise<void>;

  enrichTrack(track: Track): Promise<Track>;
  fetchLyrics(track: Track): Promise<Lyrics | null>;

  setLowPower(enabled: boolean): Promise<void>;
  isLowPower(): Promise<boolean>;
}

/** Every `EngineApi` method, for the host's allow-list and the remote client. */
export const ENGINE_METHODS = [
  'hello',
  'activeSessions',
  'startListening',
  'finishSession',
  'cancelSession',
  'analyze',
  'searchLyrics',
  'library',
  'session',
  'deleteSessions',
  'setPinned',
  'setPinnedMany',
  'correctSegment',
  'rejectSegment',
  'confirmCandidate',
  'exportSession',
  'storageUsage',
  'clearCache',
  'retryWaiting',
  'melodyIndexStatus',
  'checkMelodyIndex',
  'installMelodyIndex',
  'removeMelodyIndex',
  'enrichTrack',
  'fetchLyrics',
  'setLowPower',
  'isLowPower',
] as const satisfies readonly (keyof EngineApi)[];

export type EngineMethod = (typeof ENGINE_METHODS)[number];

// Compile-time check that ENGINE_METHODS lists every EngineApi method.
type MissingMethods = Exclude<keyof EngineApi, EngineMethod>;
const allMethodsListed: [MissingMethods] extends [never] ? true : MissingMethods = true;
void allMethodsListed;

export interface EngineListener {
  sessionEvent?(sessionId: string, event: SessionEvent): void;
  /** Sessions were added, changed or removed; reload library views. */
  libraryChanged?(): void;
  /** The connection to the engine host came up or went away. */
  connectionChanged?(connected: boolean): void;
}

export interface EngineClient extends EngineApi {
  /** Receive events from every session. Returns the unsubscribe function. */
  subscribe(listener: EngineListener): () => void;
}

export interface EngineInfo {
  /** "mock" until the wasm engine lands (extension plan E7). */
  kind: 'mock' | 'wasm';
  coreVersion: string;
}

export interface ActiveSession {
  id: string;
  kind: SessionKind;
  source: CaptureSource | null;
  started_at_ms: number;
}

/** A rejected engine call. `code` follows the core's `ErrorCode`. */
export class EngineError extends Error {
  constructor(
    readonly code: ErrorCode | 'Disconnected' | 'Timeout' | 'Unknown',
    message: string,
  ) {
    super(message);
    this.name = 'EngineError';
  }
}
