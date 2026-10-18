import type { AdapterId, PageFacts } from '../page/facts';

/**
 * The capture side of an analysis (extension plan §2.4 `CaptureHost`): the engine asks
 * the extension to play parts of a media page and receives the audio, stamped with media
 * time, as `PcmChunk`s of the session (`EngineBackend.pushPcm`). The wasm facade (E7)
 * implements the Rust trait over this interface; the mock backend drives it directly.
 *
 * Calls are keyed by session id: one capture per session.
 */
export interface CaptureHost {
  /** Find (or wait for) the player for `url`, preferably in `tabId`, and take control of it. */
  attach(session: string, url: string, tabId: number | null): Promise<CaptureTarget>;
  /**
   * Play `[fromMs, toMs)` at `rate` (1–4, `preservesPitch` off, silent for the user) and
   * stream it into the session. Resolves with the media ranges actually captured once
   * `toMs` is reached, the session is released or the page goes away.
   */
  listenThrough(session: string, fromMs: number, toMs: number, rate: number): Promise<Coverage>;
  /** Capture short windows in order: seek, capture, next. */
  windows(session: string, windows: [number, number][], rate: number): Promise<Coverage>;
  /** Restore the user's position, rate, volume, mute and quality, and stop capturing. */
  release(session: string): Promise<void>;
}

export interface CaptureTarget {
  session: string;
  tabId: number;
  url: string;
  title: string | null;
  /** Null while unknown, and for live streams. */
  durationMs: number | null;
  /** The fastest listen-through this browser and source allow (S1, S2). */
  maxRate: number;
  canSeek: boolean;
  /** A live stream: no seeking, no listen-through, only live listening. */
  live: boolean;
  adapter: AdapterId;
  /** Chromium captures the tab; Firefox (and the Chromium fallback) the element. */
  via: 'tab' | 'element';
  /** What the page itself says (tracklist text, chapters, now playing): hints for the engine. */
  facts: PageFacts | null;
}

/** Captured media ranges in ms, sorted and merged. */
export type Coverage = [number, number][];

/** Things that happen to a capture while it runs; the engine turns them into notices. */
export type CaptureEvent =
  /** An ad started or ended (YouTube's `.ad-showing`): audio is dropped meanwhile. */
  | { type: 'ad'; showing: boolean }
  /** The user paused, played or seeked the player themselves. */
  | { type: 'user'; action: 'pause' | 'play' | 'seek' }
  /** Buffering stalled or resumed. */
  | { type: 'stall'; stalled: boolean }
  /** The page navigated to another video or site; the capture stopped. */
  | { type: 'navigated'; url: string | null }
  /** The tab was closed; the capture stopped. */
  | { type: 'closed' }
  /** The player's audio is digitally silent although it plays (CORS-tainted media): use the media URL instead. */
  | { type: 'silent'; mediaUrl: string | null }
  /** Where the capture is, for progress (media ms of the frontier). */
  | { type: 'progress'; mediaMs: number; rate: number };

/** Merge `range` into `coverage` (sorted, ranges closer than `joinMs` are joined). */
export function addCoverage(coverage: Coverage, range: [number, number], joinMs = 50): Coverage {
  if (range[1] <= range[0]) return coverage;
  const all = [...coverage, range].sort((a, b) => a[0] - b[0]);
  const out: Coverage = [];
  for (const [a, b] of all) {
    const last = out.at(-1);
    if (last && a <= last[1] + joinMs) last[1] = Math.max(last[1], b);
    else out.push([a, b]);
  }
  return out;
}

/** Total ms covered. */
export function coveredMs(coverage: Coverage): number {
  return coverage.reduce((sum, [a, b]) => sum + (b - a), 0);
}

/** The parts of `[from, to)` not in `coverage`, at least `minMs` long. */
export function coverageGaps(coverage: Coverage, from: number, to: number, minMs = 500): [number, number][] {
  const gaps: [number, number][] = [];
  let cursor = from;
  for (const [a, b] of coverage) {
    if (b <= cursor) continue;
    if (a >= to) break;
    if (a - cursor >= minMs) gaps.push([cursor, Math.min(a, to)]);
    cursor = Math.max(cursor, b);
  }
  if (to - cursor >= minMs) gaps.push([cursor, to]);
  return gaps;
}
