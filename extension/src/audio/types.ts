/**
 * Audio sources (extension plan §3.2): tab capture (Chromium), element capture (content
 * script) and the microphone all deliver the same `PcmChunk`s, so the engine host and
 * the engine never care where audio came from.
 */

/** One block of captured audio, mono. */
export interface PcmChunk {
  samples: Float32Array;
  /** The capture's sample rate (the AudioContext's), in Hz. */
  sampleRate: number;
  /**
   * Playback speed of the media while it was captured: 1 for live sources, up to 4 for a
   * listen-through with `preservesPitch = false`. The engine treats the block as audio
   * sampled at `sampleRate / speed`, which restores the original time base.
   */
  speed: number;
  /** Media time of the first sample in ms (element or tab with a player), else null. */
  mediaMs: number | null;
  /** Wall clock of the first sample (`performance.timeOrigin + now()`), in ms. */
  wallMs: number;
  /**
   * False when this block does not continue the previous one: a seek, stall, ad or user
   * pause came between them, so the engine starts a new run at `mediaMs`.
   */
  continuous: boolean;
  /** Per source, from 0. */
  seq: number;
}

export type AudioSourceKind = 'tab' | 'element' | 'mic';

export type AudioEndReason =
  /** `stop()` was called. */
  | 'stopped'
  /** The browser ended the track: the tab closed, the device went away, the user revoked access. */
  | 'ended'
  | 'error';

export type AudioSourceEvent =
  | { type: 'started'; sampleRate: number; label: string }
  /** Digital silence for a while although media time advanced: the source is tainted (CORS) or muted at the source. */
  | { type: 'silent'; forMs: number }
  /** Audio is back after `silent`. */
  | { type: 'sound' }
  /** The microphone switched to another device (the preferred one appeared or the current one went away). */
  | { type: 'device'; deviceId: string; label: string }
  | { type: 'ended'; reason: AudioEndReason; message?: string };

export interface AudioSource {
  readonly kind: AudioSourceKind;
  start(): Promise<void>;
  stop(): void;
  onPcm(callback: (chunk: PcmChunk) => void): () => void;
  onEvent(callback: (event: AudioSourceEvent) => void): () => void;
}

/** `performance.timeOrigin + performance.now()`: one high-resolution wall clock across extension contexts. */
export function wallNow(): number {
  return performance.timeOrigin + performance.now();
}

/** Root mean square and peak of a block. */
export function levelOf(samples: Float32Array): { rms: number; peak: number } {
  let sum = 0;
  let peak = 0;
  for (let i = 0; i < samples.length; i++) {
    const v = samples[i]!;
    sum += v * v;
    const a = v < 0 ? -v : v;
    if (a > peak) peak = a;
  }
  return { rms: samples.length ? Math.sqrt(sum / samples.length) : 0, peak };
}

/** A small typed listener set. */
export class Emitter<T> {
  private readonly listeners = new Set<(value: T) => void>();

  on(callback: (value: T) => void): () => void {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  }

  emit(value: T): void {
    for (const listener of this.listeners) {
      try {
        listener(value);
      } catch (error) {
        console.error('audio listener failed', error);
      }
    }
  }

  clear(): void {
    this.listeners.clear();
  }
}
