/**
 * A hummed phrase for mock sessions and demo states: the design prototypes' melody
 * (ext-design proto.js, the same geometry as docs/design/mockups/mock.js), so mock traces
 * and overlays look like the designs. Semitones from the median; null = an unvoiced gap.
 */
export const MELODY: readonly [number | null, number][] = [
  [0, 0.5], [2, 0.5], [4, 0.6], [null, 0.25], [4, 0.45], [5, 0.45], [7, 0.9], [null, 0.35],
  [7, 0.4], [9, 0.4], [7, 0.4], [5, 0.5], [4, 0.8], [null, 0.3], [2, 0.5], [4, 0.5], [0, 0.9],
  [null, 0.4], [-3, 0.5], [0, 0.5], [2, 0.5], [4, 1.0], [null, 0.3], [2, 0.4], [0, 1.1],
];

export const MELODY_SECONDS = MELODY.reduce((sum, [, d]) => sum + d, 0);

function noteAt(t: number): { semi: number | null; start: number } {
  const local = ((t % MELODY_SECONDS) + MELODY_SECONDS) % MELODY_SECONDS;
  let at = 0;
  for (const [semi, duration] of MELODY) {
    if (local < at + duration) return { semi, start: at };
    at += duration;
  }
  return { semi: null, start: at };
}

/** A small deterministic generator (the prototypes' LCG). */
function prng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

export interface ContourOptions {
  /** Seconds. */
  from: number;
  to: number;
  stepMs: number;
  seed?: number;
  /** Pitch wander of a human voice (semitones); 0 for the song's line. */
  jitter?: number;
  /** Vibrato depth (semitones) at 5.6 Hz. */
  vibrato?: number;
  /** Glide between notes (seconds). */
  slide?: number;
}

/** The phrase sampled every `stepMs`: semitones from the median, NaN where unvoiced. */
export function contour(options: ContourOptions): number[] {
  const { from, to, stepMs, seed = 7, jitter = 0.22, vibrato = 0.1, slide = 0.06 } = options;
  const random = prng(seed);
  const out: number[] = [];
  let previous: number | null = null;
  let drift = 0;
  for (let t = from; t <= to + 1e-9; t += stepMs / 1000) {
    const note = noteAt(t);
    if (note.semi === null) {
      out.push(Number.NaN);
      continue;
    }
    const into = t - note.start;
    let semi = note.semi;
    if (previous !== null && previous !== note.semi && into < slide) semi = previous + (note.semi - previous) * (into / slide);
    drift = (drift + (random() - 0.5) * jitter * 0.25) * 0.9;
    out.push(semi + drift + vibrato * Math.sin(2 * Math.PI * 5.6 * t));
    if (into >= slide) previous = note.semi;
  }
  return out;
}
