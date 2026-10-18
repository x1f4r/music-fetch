import { isCaptureErrorCode, type CaptureErrorCode } from '../engine/client';
import { eventName } from '../engine/events';
import type {
  Candidate,
  ErrorCode,
  NoMatchReason,
  PitchFrame,
  SessionEvent,
  SessionKind,
  Track,
} from '../engine/types';
import type { Message, MessageKey } from '../i18n';
import type { ListenChoice } from '../settings/settings';

/**
 * The listen stage of the popup, side panel and app tab, folded from one session's
 * events (docs/design/DESIGN.md §2.2, extension plan §5.3–5.5). Pure: the surfaces feed
 * it events and render the result, so every state is reachable in tests.
 */

export type StagePhase =
  /** Nothing running; the button starts a session. */
  | 'idle'
  /** Asked the engine to start; no events yet. */
  | 'starting'
  | 'listening'
  /** Capture ended; the engine is identifying (or resolving a link). */
  | 'working'
  | 'matched'
  | 'candidates'
  /** A link or file finished: `identified` of `total`. */
  | 'completed'
  | 'nomatch'
  | 'waiting'
  | 'failed';

export interface StageError {
  code: ErrorCode | CaptureErrorCode;
  message: string;
}

export interface StageState {
  phase: StagePhase;
  sessionId: string | null;
  kind: SessionKind | null;
  /** The source the session was started with (null for links and lyrics). */
  source: ListenChoice | null;
  /** The state line. */
  line: Message;
  /** The state line is the active (darker) style. */
  lineActive: boolean;
  /** Smoothed input level 0..1 for the live line; kept (frozen) while matching. */
  level: number;
  /** The input sounds like humming (explicit Hum, or detected). */
  humming: boolean;
  /** Voiced time so far, for the hum ring (`voicedMs / 12 s`). */
  voicedMs: number;
  /** The last 8 s of pitch, for the trace band. */
  pitch: PitchFrame[];
  /** Download fraction while `Phase(Downloading)` reports one. */
  fraction: number | null;
  track: Track | null;
  candidates: Candidate[];
  candidatesFinal: boolean;
  identified: number;
  total: number;
  reason: NoMatchReason | null;
  error: StageError | null;
  /** Hum without the melody library: the download prompt is up. */
  melodyMissing: boolean;
  /** "This tab" started on an audible tab (tells "nothing playing" from "blocked"). */
  tabAudible: boolean | null;
  /** Rate limited: when the engine tries again (epoch ms), for the countdown. */
  retryAt: number | null;
  /** The melody library download started from the stage. */
  download: { sessionId: string | null; fraction: number; totalBytes: number } | null;
  /** What "Try again" repeats for a link or a lyric search. */
  input: string | null;
}

export const TRACE_WINDOW_MS = 8000;

/** The resting line under the button: Hum asks for a hum, the rest for what's playing. */
export function idleLine(source: ListenChoice | null): Message {
  return { key: source === 'Hum' ? 'state.humStart' : 'state.idle' };
}

export function idleStage(source: ListenChoice | null = null): StageState {
  return {
    phase: 'idle',
    sessionId: null,
    kind: null,
    source: null,
    line: idleLine(source),
    lineActive: false,
    level: 0,
    humming: false,
    voicedMs: 0,
    pitch: [],
    fraction: null,
    track: null,
    candidates: [],
    candidatesFinal: false,
    identified: 0,
    total: 0,
    reason: null,
    error: null,
    melodyMissing: false,
    tabAudible: null,
    retryAt: null,
    download: null,
    input: null,
  };
}

/** The UI asked the engine for a session; the button reacts at once. */
export function startingStage(
  source: ListenChoice | null,
  kind: SessionKind,
  options: { tabAudible?: boolean | null; input?: string | null } = {},
): StageState {
  const hum = source === 'Hum';
  return {
    ...idleStage(source),
    phase: 'starting',
    kind,
    source,
    humming: hum,
    line: { key: kind === 'Link' ? 'state.resolving' : kind === 'Lyrics' ? 'state.searchingLyrics' : hum ? 'state.humStart' : 'state.listening' },
    lineActive: true,
    tabAudible: source === 'Tab' ? (options.tabAudible ?? null) : null,
    input: options.input ?? null,
  };
}

const NO_MATCH_LINES: Record<NoMatchReason, MessageKey> = {
  TooQuiet: 'state.tooQuiet',
  TooShort: 'state.tooShort',
  Noisy: 'state.noisy',
  MostlySpeech: 'state.mostlySpeech',
  CaptureSilent: 'state.tab.blocked',
  NotFound: 'state.noMatch',
};

export function errorLine(code: ErrorCode | CaptureErrorCode): MessageKey {
  switch (code) {
    case 'UnsupportedLink':
    case 'InvalidInput':
      return 'error.unsupportedLink';
    case 'MediaUnavailable':
      return 'error.mediaUnavailable';
    case 'Network':
      return 'error.network';
    case 'RateLimited':
      return 'error.rateLimited';
    case 'Decode':
      return 'error.decode';
    case 'IndexUnavailable':
      return 'error.indexUnavailable';
    case 'CaptureNotAllowed':
      return 'state.tab.cantListen';
    case 'CaptureBusy':
      return 'state.tab.busy';
    case 'NothingPlaying':
      return 'state.tab.nothingPlaying';
    case 'CaptureBlocked':
      return 'state.tab.blocked';
    case 'MicDenied':
      return 'state.micOff';
    default:
      return 'error.other';
  }
}

/** A capture that heard nothing: no media in the tab, DRM, or a silent microphone. */
function silentLine(state: StageState): MessageKey {
  if (state.source !== 'Tab') return 'state.micOff';
  return state.tabAudible === false ? 'state.tab.nothingPlaying' : 'state.tab.blocked';
}

/** Fold one event of the stage's session into the state. Events of other sessions are ignored. */
export function reduceStage(state: StageState, sessionId: string, event: SessionEvent, now: number = Date.now()): StageState {
  // The melody library download runs as its own (unstored) session.
  if (state.download && state.download.sessionId === sessionId) return reduceDownload(state, event);

  const name = eventName(event);
  if (name === 'Started') {
    if (state.phase !== 'starting' && state.sessionId !== sessionId) return state;
    const started = (event as Extract<SessionEvent, { Started: unknown }>).Started;
    const live = started.kind === 'Listen' || started.kind === 'SystemAudio' || started.kind === 'Hum';
    return { ...state, sessionId: started.session_id, kind: started.kind, phase: live ? 'listening' : 'working' };
  }
  if (state.sessionId !== sessionId) return state;
  if (event === 'Canceled') return idleStage(state.source);

  if ('Level' in event) {
    // Fast attack, slow release, like the apps' meter.
    const target = Math.max(0, Math.min(1, event.Level.rms));
    const level = target > state.level ? target : state.level * 0.85 + target * 0.15;
    return { ...state, level };
  }
  if ('Pitch' in event) {
    const frames = [...state.pitch, ...event.Pitch.frames];
    const newest = frames.at(-1)?.t_ms ?? 0;
    const voiced = event.Pitch.frames.filter((f) => f.midi !== null && f.confidence >= 0.5).length;
    const step = event.Pitch.frames.length > 1 ? event.Pitch.frames[1]!.t_ms - event.Pitch.frames[0]!.t_ms : 33;
    return {
      ...state,
      pitch: frames.filter((f) => f.t_ms > newest - TRACE_WINDOW_MS),
      voicedMs: state.voicedMs + voiced * step,
    };
  }
  if ('Phase' in event) {
    const { phase, fraction } = event.Phase;
    const settled = { ...state, retryAt: null };
    switch (phase) {
      case 'Listening':
        return { ...settled, phase: 'listening', line: { key: state.humming ? 'state.humStart' : 'state.listening' }, lineActive: true };
      case 'Resolving':
        return { ...settled, phase: 'working', line: { key: 'state.resolving' }, lineActive: true };
      case 'Downloading':
        return { ...settled, phase: 'working', fraction, line: { key: 'state.downloading' }, lineActive: true };
      case 'Decoding':
      case 'Segmenting':
        return { ...settled, phase: 'working', fraction: null, line: { key: 'state.preparing' }, lineActive: true };
      case 'SearchingLyrics':
        return { ...settled, phase: 'working', fraction: null, line: { key: state.kind === 'Lyrics' ? 'state.searchingLyrics' : 'state.matching' }, lineActive: true };
      default:
        // The live line freezes at its last frame (DESIGN §2.2 "Matching"): keep the level.
        return { ...settled, phase: 'working', fraction: null, line: { key: 'state.matching' }, lineActive: true };
    }
  }
  if ('Notice' in event) {
    const notice = event.Notice.notice;
    if (notice === 'MoveCloser') return { ...state, line: { key: 'state.moveCloser' } };
    if (notice === 'ImpressionDetected') return { ...state, humming: true, line: { key: 'state.humStart' } };
    if (notice === 'KeepGoing') return { ...state, line: { key: 'state.keepGoing' } };
    if (notice === 'CaptureSilent') return { ...state, line: { key: silentLine(state) } };
    if (notice === 'MelodyIndexMissing') return { ...state, melodyMissing: true };
    if (typeof notice === 'object' && 'RateLimited' in notice) {
      const ms = notice.RateLimited.retry_in_ms;
      return { ...state, retryAt: now + ms, line: { key: 'state.rateLimited', args: [Math.ceil(ms / 1000)] } };
    }
    // ProviderUnavailable: nothing to show (DESIGN §3.2).
    return state;
  }
  if ('Segment' in event) {
    return event.Segment.segment.track ? { ...state, track: state.track ?? event.Segment.segment.track } : state;
  }
  if ('Candidates' in event) {
    return { ...state, candidates: event.Candidates.candidates, candidatesFinal: event.Candidates.is_final };
  }
  if ('Failed' in event) {
    const { code, message } = event.Failed;
    return { ...state, phase: 'failed', level: 0, retryAt: null, error: { code, message }, line: { key: errorLine(code) }, lineActive: true };
  }
  if ('Finished' in event) {
    const outcome = event.Finished.outcome;
    const done = { ...state, level: 0, fraction: null, retryAt: null, lineActive: true };
    if (outcome === 'Waiting') return { ...done, phase: 'waiting', line: { key: 'state.offline' } };
    if ('Matched' in outcome) return { ...done, phase: 'matched', track: outcome.Matched.track, line: idleLine(state.source) };
    if ('Candidates' in outcome) return { ...done, phase: 'candidates', line: idleLine(state.source) };
    if ('Completed' in outcome) {
      const { identified, total } = outcome.Completed;
      if (total === 1 && identified === 1 && done.track) return { ...done, phase: 'matched', identified, total, line: idleLine(state.source) };
      if (identified === 0) return { ...done, phase: 'nomatch', reason: 'NotFound', identified, total, line: { key: 'state.noMatch' } };
      return { ...done, phase: 'completed', identified, total, line: { key: 'state.identified', args: [identified, total] } };
    }
    const reason = outcome.NoMatch.reason;
    return {
      ...done,
      phase: 'nomatch',
      reason,
      line: { key: reason === 'CaptureSilent' ? silentLine(state) : NO_MATCH_LINES[reason] },
    };
  }
  return state;
}

function reduceDownload(state: StageState, event: SessionEvent): StageState {
  const download = state.download!;
  if (event === 'Canceled' || (typeof event === 'object' && 'Failed' in event)) {
    const failed = event !== 'Canceled';
    return {
      ...idleStage('Hum'),
      phase: failed ? 'failed' : 'idle',
      error: failed ? { code: 'Network', message: 'The melody library could not be downloaded.' } : null,
      line: failed ? { key: 'melody.failed' } : idleLine('Hum'),
      lineActive: failed,
    };
  }
  if ('Phase' in event && event.Phase.phase === 'Downloading') {
    const fraction = event.Phase.fraction ?? download.fraction;
    return { ...state, download: { ...download, fraction }, line: downloadLine(fraction, download.totalBytes) };
  }
  if ('Finished' in event) return { ...idleStage('Hum'), line: { key: 'melody.ready' }, lineActive: true };
  return state;
}

export function megabytes(bytes: number): number {
  return Math.round(bytes / 1_000_000);
}

function downloadLine(fraction: number, totalBytes: number): Message {
  return { key: 'state.melodyDownloading', args: [megabytes(fraction * totalBytes), megabytes(totalBytes)] };
}

/** The melody library download began from the stage's prompt. */
export function downloadStage(totalBytes: number, sessionId: string | null = null): StageState {
  return {
    ...idleStage('Hum'),
    phase: 'working',
    source: 'Hum',
    download: { sessionId, fraction: 0, totalBytes },
    line: downloadLine(0, totalBytes),
    lineActive: true,
  };
}

/** The engine could not start the session (or a link failed before it began). */
export function failedStage(base: StageState, code: ErrorCode | CaptureErrorCode, message: string): StageState {
  return {
    ...idleStage(base.source),
    source: base.source,
    kind: base.kind,
    tabAudible: base.tabAudible,
    input: base.input,
    phase: 'failed',
    error: { code, message },
    line: { key: errorLine(code) },
    lineActive: true,
  };
}

/** The engine answered the start call with `sessionId`: follow that session from now on. */
export function adoptSession(state: StageState, sessionId: string): StageState {
  if (state.download) return state.download.sessionId === sessionId ? state : { ...state, download: { ...state.download, sessionId } };
  return state.sessionId === sessionId ? state : { ...state, sessionId };
}

/** The session is still running (the button stops it). */
export function isRunning(state: StageState): boolean {
  if (state.download) return false;
  return state.phase === 'starting' || state.phase === 'listening' || state.phase === 'working';
}

/** Capture is on: the live line, the halo and the dimmed toggles. */
export function isCapturing(state: StageState): boolean {
  return state.phase === 'listening' || (state.phase === 'starting' && state.kind !== 'Link' && state.kind !== 'Lyrics');
}

/** The stage shows a result or candidates instead of the button. */
export function showsResult(state: StageState): boolean {
  return state.phase === 'matched' || state.phase === 'candidates';
}

/** Hum ring 0–100: 12 voiced seconds fill it (design spec, "Listen button"). */
export function ringPercent(state: StageState): number {
  return Math.min(100, Math.round((state.voicedMs / 12_000) * 100));
}

/** Seconds left on a rate-limit countdown at `now`. */
export function retrySeconds(state: StageState, now: number = Date.now()): number | null {
  return state.retryAt === null ? null : Math.max(0, Math.ceil((state.retryAt - now) / 1000));
}

/** The state line at `now` (the rate-limit countdown ticks). */
export function stageLine(state: StageState, now: number = Date.now()): Message {
  const seconds = retrySeconds(state, now);
  return seconds === null ? state.line : { key: 'state.rateLimited', args: [seconds] };
}

/** Lines too long for one row of the popup (they break after the first sentence). */
export function isTwoLine(line: Message): boolean {
  return line.key === 'state.tab.blocked';
}

/**
 * The recovery row under the source toggles (design spec "Recovery row"): up to three
 * actions after a no-match, an error or a prompt; empty otherwise.
 */
export type RecoveryAction =
  | 'stop'
  | 'tryAgain'
  | 'humIt'
  | 'typeLyric'
  | 'useMicrophone'
  | 'allowMicrophone'
  | 'siteSettings'
  | 'ok'
  | 'open'
  | 'cancelDownload';

export interface Recovery {
  action: RecoveryAction;
  /** The one filled (primary) action, when the state has an obvious way out. */
  primary?: boolean;
}

export function recoveryActions(state: StageState): Recovery[] {
  if (state.download) return [{ action: 'cancelDownload' }];
  if (state.retryAt !== null && isRunning(state)) return [{ action: 'stop' }];
  switch (state.phase) {
    case 'nomatch': {
      const line = state.line.key;
      if (line === 'state.tab.blocked') return [{ action: 'useMicrophone', primary: true }];
      if (line === 'state.tab.nothingPlaying') return [{ action: 'tryAgain' }, { action: 'useMicrophone' }];
      if (line === 'state.micOff') return [{ action: 'allowMicrophone', primary: true }, { action: 'siteSettings' }];
      const actions: Recovery[] = [{ action: 'tryAgain' }];
      if (state.source !== 'Hum' && state.kind !== 'Link' && state.kind !== 'Lyrics') actions.push({ action: 'humIt' });
      actions.push({ action: 'typeLyric' });
      return actions;
    }
    case 'failed': {
      const code = state.error?.code;
      if (code && isCaptureErrorCode(code)) {
        switch (code) {
          case 'CaptureNotAllowed':
            return [{ action: 'useMicrophone' }];
          case 'CaptureBlocked':
            return [{ action: 'useMicrophone', primary: true }];
          case 'MicDenied':
            return [{ action: 'allowMicrophone', primary: true }, { action: 'siteSettings' }];
          default:
            return [{ action: 'tryAgain' }, { action: 'useMicrophone' }];
        }
      }
      return [{ action: 'tryAgain' }];
    }
    case 'waiting':
      return [{ action: 'ok' }];
    case 'completed':
      return [{ action: 'open' }];
    default:
      return [];
  }
}
