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
import type { Message } from '../i18n';
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
  /** Smoothed input level 0..1 for the live line. */
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
  error: { code: ErrorCode; message: string } | null;
  /** Hum without the melody library: offer the download. */
  melodyMissing: boolean;
}

export const TRACE_WINDOW_MS = 8000;

export function idleStage(): StageState {
  return {
    phase: 'idle',
    sessionId: null,
    kind: null,
    source: null,
    line: { key: 'state.idle' },
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
  };
}

/** The UI asked the engine for a session; the button reacts at once. */
export function startingStage(source: ListenChoice | null, kind: SessionKind): StageState {
  const hum = source === 'Hum';
  return {
    ...idleStage(),
    phase: 'starting',
    kind,
    source,
    humming: hum,
    line: { key: kind === 'Link' ? 'state.resolving' : hum ? 'state.humStart' : 'state.listening' },
    lineActive: true,
  };
}

const NO_MATCH_LINES: Record<NoMatchReason, Message['key']> = {
  TooQuiet: 'state.tooQuiet',
  TooShort: 'state.tooShort',
  Noisy: 'state.noisy',
  MostlySpeech: 'state.mostlySpeech',
  CaptureSilent: 'state.tab.blocked',
  NotFound: 'state.noMatch',
};

export function errorLine(code: ErrorCode): Message['key'] {
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
    default:
      return 'error.other';
  }
}

function silentLine(source: ListenChoice | null): Message['key'] {
  return source === 'Tab' ? 'state.tab.blocked' : 'state.micOff';
}

/** Fold one event of the stage's session into the state. Events of other sessions are ignored. */
export function reduceStage(state: StageState, sessionId: string, event: SessionEvent): StageState {
  const name = eventName(event);
  if (name === 'Started') {
    if (state.phase !== 'starting' && state.sessionId !== sessionId) return state;
    const started = (event as Extract<SessionEvent, { Started: unknown }>).Started;
    const live = started.kind === 'Listen' || started.kind === 'SystemAudio' || started.kind === 'Hum';
    return { ...state, sessionId: started.session_id, kind: started.kind, phase: live ? 'listening' : 'working' };
  }
  if (state.sessionId !== sessionId) return state;
  if (event === 'Canceled') return idleStage();

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
    switch (phase) {
      case 'Listening':
        return { ...state, phase: 'listening', line: { key: state.humming ? 'state.humStart' : 'state.listening' }, lineActive: true };
      case 'Resolving':
        return { ...state, phase: 'working', line: { key: 'state.resolving' }, lineActive: true };
      case 'Downloading':
        return { ...state, phase: 'working', fraction, line: { key: 'state.downloading' }, lineActive: true };
      case 'Decoding':
      case 'Segmenting':
        return { ...state, phase: 'working', fraction: null, line: { key: 'state.preparing' }, lineActive: true };
      default:
        return { ...state, phase: 'working', fraction: null, level: 0, line: { key: 'state.matching' }, lineActive: true };
    }
  }
  if ('Notice' in event) {
    const notice = event.Notice.notice;
    if (notice === 'MoveCloser') return { ...state, line: { key: 'state.moveCloser' } };
    if (notice === 'ImpressionDetected') return { ...state, humming: true, line: { key: 'state.humStart' } };
    if (notice === 'KeepGoing') return { ...state, line: { key: 'state.keepGoing' } };
    if (notice === 'CaptureSilent') return { ...state, line: { key: silentLine(state.source) } };
    if (notice === 'MelodyIndexMissing') return { ...state, melodyMissing: true };
    if (typeof notice === 'object' && 'RateLimited' in notice) {
      return { ...state, line: { key: 'state.rateLimited', args: [Math.ceil(notice.RateLimited.retry_in_ms / 1000)] } };
    }
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
    return { ...state, phase: 'failed', level: 0, error: { code, message }, line: { key: errorLine(code) }, lineActive: true };
  }
  if ('Finished' in event) {
    const outcome = event.Finished.outcome;
    const done = { ...state, level: 0, fraction: null, lineActive: true };
    if (outcome === 'Waiting') return { ...done, phase: 'waiting', line: { key: 'state.offline' } };
    if ('Matched' in outcome) return { ...done, phase: 'matched', track: outcome.Matched.track, line: { key: 'state.idle' } };
    if ('Candidates' in outcome) return { ...done, phase: 'candidates', line: { key: 'state.idle' } };
    if ('Completed' in outcome) {
      const { identified, total } = outcome.Completed;
      if (total === 1 && identified === 1 && done.track) return { ...done, phase: 'matched', identified, total, line: { key: 'state.idle' } };
      if (identified === 0) return { ...done, phase: 'nomatch', reason: 'NotFound', identified, total, line: { key: 'state.noMatch' } };
      return { ...done, phase: 'completed', identified, total, line: { key: 'state.identified', args: [identified, total] } };
    }
    const reason = outcome.NoMatch.reason;
    return {
      ...done,
      phase: 'nomatch',
      reason,
      line: { key: reason === 'CaptureSilent' ? silentLine(state.source) : NO_MATCH_LINES[reason] },
    };
  }
  return state;
}

/** The engine answered the start call with `sessionId`: follow that session from now on. */
export function adoptSession(state: StageState, sessionId: string): StageState {
  return state.sessionId === sessionId ? state : { ...state, sessionId };
}

/** The session is still running (the button stops it). */
export function isRunning(state: StageState): boolean {
  return state.phase === 'starting' || state.phase === 'listening' || state.phase === 'working';
}

/** The stage shows a result or candidates instead of the button. */
export function showsResult(state: StageState): boolean {
  return state.phase === 'matched' || state.phase === 'candidates';
}

/** Hum ring 0–100: 12 voiced seconds fill it (design spec, "Listen button"). */
export function ringPercent(state: StageState): number {
  return Math.min(100, Math.round((state.voicedMs / 12_000) * 100));
}
