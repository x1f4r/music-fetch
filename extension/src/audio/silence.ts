import { levelOf } from './types';

/**
 * Notices digital silence: an element whose audio is cross-origin without CORS captures
 * as exact zeros while it plays (S2), and a tab with nothing playing captures zeros too.
 * `push` returns 'silent' once the zeros last `afterMs`, and 'sound' when audio is back.
 */
export class SilenceWatch {
  private zeroMs = 0;
  private silent = false;

  constructor(
    /** Default 1500 ms (S2). */
    private readonly afterMs = 1500,
    /** Peak below this counts as digital silence. */
    private readonly floor = 1e-6,
  ) {}

  get isSilent(): boolean {
    return this.silent;
  }

  /** Milliseconds of silence so far. */
  get silentForMs(): number {
    return this.zeroMs;
  }

  push(samples: Float32Array, sampleRate: number): 'silent' | 'sound' | null {
    const { peak } = levelOf(samples);
    if (peak < this.floor) {
      this.zeroMs += (samples.length / sampleRate) * 1000;
      if (!this.silent && this.zeroMs >= this.afterMs) {
        this.silent = true;
        return 'silent';
      }
      return null;
    }
    this.zeroMs = 0;
    if (this.silent) {
      this.silent = false;
      return 'sound';
    }
    return null;
  }
}
