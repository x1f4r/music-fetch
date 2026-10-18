/**
 * Places captured blocks on the media's own time line (extension plan §3.2, spikes S1
 * and S2), so a listen-through at 4×, an ad, a stall or a seek still lands every second
 * of audio at the right position.
 *
 * The page reports `currentTime` samples (every 50 ms, plus one at every seek, stall,
 * ad or rate change). Blocks from the capture are stamped by advancing the previous
 * block's end by `frames / sampleRate × rate`; the sampled clock only decides when that
 * frame count has lost the media (a jump), and anchors new runs.
 *
 * - Chromium tab capture: captured audio trails `currentTime` by a steady 30–40 ms of wall
 *   time at every rate (S1), so `lagMs = 35`. Jitter within a run is 0.2–12 ms of media
 *   time up to 5×: anything up to about 20 ms is continuous.
 * - Element capture (S2): stamps taken from `currentTime` at arrival err by 0.1–0.5 s, so
 *   the clock anchors once and then trusts the frame count (`anchorOnly`), re-anchoring
 *   only on reported discontinuities or a gross error.
 */

export interface MediaSample {
  /** Wall clock (`performance.timeOrigin + now()`) when `currentTime` was read. */
  wallMs: number;
  /** `currentTime` in ms. */
  mediaMs: number;
  /** `playbackRate` at that moment. */
  rate: number;
  /**
   * The content is playing: not paused, stalled, seeking, or showing an ad. Blocks that
   * arrive while it is false are dropped (they are not the content at that position).
   */
  advancing: boolean;
}

export interface MediaClockOptions {
  /** Wall ms by which captured audio trails `currentTime` (35 for Chromium tab capture, 0 for element capture). */
  lagMs: number;
  /** Media ms of disagreement that still counts as the same run. Default 20 (S1). */
  toleranceMs?: number;
  /** Extra tolerance per unit of playback rate, in media ms. Default 5. */
  tolerancePerRateMs?: number;
  /** Trust the frame count and re-anchor only on `restart()` or an error above `driftLimitMs`. */
  anchorOnly?: boolean;
  /** Default 1500 ms of media. */
  driftLimitMs?: number;
}

export interface Stamp {
  mediaMs: number;
  /** The rate the block was played at. */
  rate: number;
  /** The block continues the previous one. */
  continuous: boolean;
}

export class MediaClock {
  private latest: MediaSample | null = null;
  /** Media time where the next block would start if it continues the run. */
  private nextMs: number | null = null;
  /** A discontinuity was reported; wait for a sample taken after `restartedAt`. */
  private restartedAt: number | null = null;
  private readonly lagMs: number;
  private readonly toleranceMs: number;
  private readonly tolerancePerRateMs: number;
  private readonly anchorOnly: boolean;
  private readonly driftLimitMs: number;
  /** Blocks dropped because the content was not advancing or no sample was known. */
  dropped = 0;
  /** Runs started (the first block and every discontinuity). */
  runs = 0;

  constructor(options: MediaClockOptions) {
    this.lagMs = options.lagMs;
    this.toleranceMs = options.toleranceMs ?? 20;
    this.tolerancePerRateMs = options.tolerancePerRateMs ?? 5;
    this.anchorOnly = options.anchorOnly ?? false;
    this.driftLimitMs = options.driftLimitMs ?? 1500;
  }

  /** A `currentTime` sample from the page. Samples older than the latest are ignored. */
  observe(sample: MediaSample): void {
    if (this.latest && sample.wallMs < this.latest.wallMs) return;
    if (!sample.advancing || (this.latest && sample.rate !== this.latest.rate)) this.nextMs = null;
    this.latest = sample;
  }

  /**
   * The page reported a discontinuity (seeked, ad over, playing again after a stall, rate
   * changed): the run ends, and blocks wait for a sample taken from now on.
   */
  restart(wallMs: number): void {
    this.nextMs = null;
    this.restartedAt = wallMs;
  }

  /** The latest sample, for progress and coverage. */
  get position(): MediaSample | null {
    return this.latest;
  }

  /**
   * Stamp a block of `frames` whose first sample was captured at `firstWallMs`. Returns
   * null when the block must be dropped: nothing known yet, the content isn't advancing,
   * or a restart is waiting for a fresh sample.
   */
  stamp(frames: number, sampleRate: number, firstWallMs: number): Stamp | null {
    const sample = this.latest;
    if (!sample || !sample.advancing || (this.restartedAt !== null && sample.wallMs < this.restartedAt)) {
      this.dropped++;
      return null;
    }
    this.restartedAt = null;
    const rate = sample.rate;
    const observed = sample.mediaMs + (firstWallMs - this.lagMs - sample.wallMs) * rate;
    let mediaMs = observed;
    let continuous = false;
    if (this.nextMs !== null) {
      const limit = this.anchorOnly ? this.driftLimitMs : this.toleranceMs + this.tolerancePerRateMs * rate;
      if (Math.abs(observed - this.nextMs) <= limit) {
        mediaMs = this.nextMs;
        continuous = true;
      }
    }
    if (!continuous) this.runs++;
    if (mediaMs < 0) mediaMs = 0;
    this.nextMs = mediaMs + (frames / sampleRate) * 1000 * rate;
    return { mediaMs, rate, continuous };
  }
}
