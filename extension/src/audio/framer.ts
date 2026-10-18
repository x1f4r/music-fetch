import type { MediaClock } from './media-clock';
import type { PcmChunk } from './types';

/**
 * Turns the tap's blocks into `PcmChunk`s: mono, numbered, stamped with wall clock and
 * (when a media clock is attached) media time. Blocks the clock rejects are dropped, so
 * the engine never gets audio it can't place.
 */
export class PcmFramer {
  private seq = 0;
  private started = false;
  /** Blocks dropped by the clock. */
  dropped = 0;
  frames = 0;

  constructor(
    private readonly emit: (chunk: PcmChunk) => void,
    private readonly options: {
      /** The playback speed for unclocked sources (1 for live). */
      speed?: () => number;
      clock?: MediaClock | null;
    } = {},
  ) {}

  /** A block of mono samples whose first sample was captured at `firstWallMs`. */
  push(samples: Float32Array, sampleRate: number, firstWallMs: number): void {
    if (samples.length === 0) return;
    const clock = this.options.clock;
    let mediaMs: number | null = null;
    let speed = this.options.speed?.() ?? 1;
    let continuous = this.started;
    if (clock) {
      const stamp = clock.stamp(samples.length, sampleRate, firstWallMs);
      if (!stamp) {
        this.dropped++;
        return;
      }
      mediaMs = stamp.mediaMs;
      speed = stamp.rate;
      continuous = stamp.continuous;
    }
    this.started = true;
    this.frames += samples.length;
    this.emit({ samples, sampleRate, speed, mediaMs, wallMs: firstWallMs, continuous, seq: this.seq++ });
  }
}

/** Average the channels of a planar block into one (a copy; inputs are reused by the audio thread). */
export function downmix(channels: readonly Float32Array[], frames = channels[0]?.length ?? 0): Float32Array {
  const out = new Float32Array(frames);
  const n = channels.length;
  if (n === 0) return out;
  if (n === 1) {
    out.set(channels[0]!.subarray(0, frames));
    return out;
  }
  for (const channel of channels) for (let i = 0; i < frames; i++) out[i]! += channel[i]!;
  const scale = 1 / n;
  for (let i = 0; i < frames; i++) out[i]! *= scale;
  return out;
}

/**
 * Collects blocks of any size (ScriptProcessor buffers, worklet quanta) into blocks of
 * exactly `size` frames.
 */
export class Rechunker {
  private buffer: Float32Array;
  private filled = 0;
  /** Index (frames since start) of the buffer's first sample. */
  private first = 0;
  private next = 0;

  constructor(
    private readonly size: number,
    /** `firstFrame`: index of the block's first sample since the first `push`. */
    private readonly out: (block: Float32Array, firstFrame: number) => void,
  ) {
    this.buffer = new Float32Array(size);
  }

  push(samples: Float32Array): void {
    let offset = 0;
    while (offset < samples.length) {
      if (this.filled === 0) this.first = this.next;
      const take = Math.min(this.size - this.filled, samples.length - offset);
      this.buffer.set(samples.subarray(offset, offset + take), this.filled);
      this.filled += take;
      offset += take;
      this.next += take;
      if (this.filled === this.size) {
        this.out(this.buffer, this.first);
        this.buffer = new Float32Array(this.size);
        this.filled = 0;
      }
    }
  }
}
