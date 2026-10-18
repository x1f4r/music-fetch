import type { MediaSample } from '../audio/media-clock';
import type { AudioSourceEvent, PcmChunk } from '../audio/types';
import type { AdapterId, PageFacts } from '../page/facts';
import type { CaptureEvent } from './types';

/**
 * The capture agent: the isolated-world content script the host injects into a tab
 * (`entrypoints/capture.content.ts`). It opens a `CAPTURE_PORT` to the host and serves
 * `AgentApi` on it; the host calls it, and it streams `AgentTopics` back. It controls the
 * player (through the site adapter in the MAIN world where the site has one), captures
 * the element on Firefox (and as Chromium's fallback), and samples the media clock.
 */
export interface AgentApi {
  hello(): Promise<AgentHello>;
  /** What the page says about its music (adapter facts merged with the generic reader). */
  facts(): Promise<PageFacts>;
  /** Pick the player (the adapter's, else the playing or longest element). Null: no media here. */
  attach(): Promise<PlayerInfo | null>;
  /**
   * Save the user's player state (once per capture) and set up a scan: rate with
   * `preservesPitch = false`, element mute (element capture only: tab capture follows the
   * element's volume, S1), lowest video quality (YouTube, S1: 1.1 cores → 0.26 at 4×).
   */
  prepare(options: PrepareOptions): Promise<void>;
  seek(ms: number): Promise<void>;
  play(): Promise<void>;
  pause(): Promise<void>;
  /** Capture the element; PCM arrives as `pcm` topics. `clocked`: stamp with media time. */
  startCapture(options: { clocked: boolean }): Promise<{ sampleRate: number; label: string }>;
  stopCapture(): Promise<void>;
  /** Start or stop `clock` samples (every 50 ms) and capture `event`s. */
  watch(on: boolean): Promise<void>;
  /** Put back what `prepare` saved, and stop everything. */
  restore(): Promise<void>;
  state(): Promise<PlayerInfo | null>;
}

export const AGENT_METHODS = [
  'hello',
  'facts',
  'attach',
  'prepare',
  'seek',
  'play',
  'pause',
  'startCapture',
  'stopCapture',
  'watch',
  'restore',
  'state',
] as const satisfies readonly (keyof AgentApi)[];

export interface AgentTopics {
  pcm: PcmChunk;
  audio: AudioSourceEvent;
  clock: MediaSample;
  event: CaptureEvent;
}

export interface AgentHello {
  url: string;
  title: string;
  adapter: AdapterId;
  /** The page script (MAIN world) answered: site adapters are available. */
  page: boolean;
}

export interface PrepareOptions {
  rate: number;
  /** Mute the element (element capture only). */
  mute: boolean;
  /** Drop the video quality during the scan (restored afterwards). */
  lowQuality: boolean;
}

export interface PlayerInfo {
  adapter: AdapterId;
  /** Null while unknown or for live streams. */
  durationMs: number | null;
  currentMs: number;
  paused: boolean;
  rate: number;
  muted: boolean;
  /** A live stream (infinite duration, or the adapter says so). */
  live: boolean;
  canSeek: boolean;
  title: string | null;
  /** The element's source when it is a plain media URL (not MSE/blob): the engine may fetch it instead. */
  mediaUrl: string | null;
  /** An ad is playing (YouTube). */
  ad: boolean;
  /** Protected by EME: only tab capture can hear it (Chromium). */
  protected: boolean;
  /** A Web Audio player with no media element: capture at 1× only (S1). */
  webAudioOnly: boolean;
}
