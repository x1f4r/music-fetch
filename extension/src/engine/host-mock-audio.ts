import { emptyTrack, type Track } from './types';

/**
 * Test signals for the mock era. Until the wasm engine lands, the mock backend can't
 * recognize music, so the end-to-end tests play **synthetic songs** (sequences of pure
 * tones, nothing copyrighted) and this module recognizes them from the captured PCM.
 * That checks what the engine host is responsible for: the right audio reaches the
 * engine, at the right speed and the right media time.
 *
 * A song is 64 notes of 500 ms (a 32 s loop) drawn from 16 tones a whole tone apart
 * (330–1865 Hz). At 4× with `preservesPitch = false` the tab plays them at up to 7.5 kHz,
 * which the 48 kHz capture keeps; the detector undoes the speed the way the engine does,
 * by treating the block as sampled at `sampleRate / speed`.
 */

export const TONE_HZ: readonly number[] = Array.from({ length: 16 }, (_, k) => 330 * 2 ** ((2 * k) / 12));
export const NOTE_MS = 500;
export const NOTES_PER_SONG = 64;
export const SONG_LOOP_MS = NOTE_MS * NOTES_PER_SONG;
/** Analysis frames: 100 ms of the original time base. */
export const FRAME_MS = 100;
const FRAMES_PER_NOTE = NOTE_MS / FRAME_MS;
const FRAMES_PER_SONG = NOTES_PER_SONG * FRAMES_PER_NOTE;

export interface SyntheticSong {
  id: string;
  title: string;
  /** Tone index per note. */
  notes: number[];
}

function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function songNotes(seed: number): number[] {
  const next = lcg(seed);
  const notes: number[] = [];
  while (notes.length < NOTES_PER_SONG) {
    const note = Math.floor(next() * TONE_HZ.length);
    if (note !== notes.at(-1)) notes.push(note);
  }
  return notes;
}

const LETTERS = ['A', 'B', 'C', 'D', 'E', 'F'];

export const SYNTHETIC_SONGS: readonly SyntheticSong[] = LETTERS.map((letter, i) => ({
  id: `synth-${letter.toLowerCase()}`,
  title: `Test Signal ${letter}`,
  notes: songNotes(7919 * (i + 1)),
}));

export const SYNTHETIC_ARTIST = 'Music Fetch Test Signals';

/** The catalog entry the mock answers with when it hears `song`. */
export function syntheticTrack(song: SyntheticSong): Track {
  return emptyTrack({
    title: song.title,
    artist: SYNTHETIC_ARTIST,
    artists: [SYNTHETIC_ARTIST],
    album: 'Synthetic Songs',
    duration_ms: SONG_LOOP_MS,
    ids: { mock: song.id },
    identity_key: `mock:${song.id}`,
  });
}

/**
 * Render `durationMs` of `song` starting at `offsetMs` of its loop: mono float samples,
 * a 5 ms ramp at every note edge so the edges don't splatter.
 */
export function renderSong(song: SyntheticSong, sampleRate: number, durationMs: number, offsetMs = 0, amplitude = 0.3): Float32Array {
  const n = Math.round((durationMs / 1000) * sampleRate);
  const out = new Float32Array(n);
  const ramp = Math.round(0.005 * sampleRate);
  const noteLen = (NOTE_MS / 1000) * sampleRate;
  let phase = 0;
  for (let i = 0; i < n; i++) {
    const tMs = offsetMs + (i / sampleRate) * 1000;
    const index = Math.floor(tMs / NOTE_MS) % NOTES_PER_SONG;
    const hz = TONE_HZ[song.notes[index]!]!;
    const inNote = ((tMs % NOTE_MS) / 1000) * sampleRate;
    const edge = Math.min(inNote, noteLen - inNote);
    const gain = edge < ramp ? edge / ramp : 1;
    phase += (2 * Math.PI * hz) / sampleRate;
    if (phase > 2 * Math.PI) phase -= 2 * Math.PI;
    out[i] = amplitude * gain * Math.sin(phase);
  }
  return out;
}

/** A 16-bit PCM WAV file (mono) of `samples`. */
export function encodeWav(samples: Float32Array, sampleRate: number): Uint8Array {
  const bytes = new Uint8Array(44 + samples.length * 2);
  const view = new DataView(bytes.buffer);
  const text = (offset: number, s: string) => [...s].forEach((c, i) => view.setUint8(offset + i, c.charCodeAt(0)));
  text(0, 'RIFF');
  view.setUint32(4, 36 + samples.length * 2, true);
  text(8, 'WAVE');
  text(12, 'fmt ');
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  text(36, 'data');
  view.setUint32(40, samples.length * 2, true);
  for (let i = 0; i < samples.length; i++) view.setInt16(44 + i * 2, Math.max(-1, Math.min(1, samples[i]!)) * 0x7fff, true);
  return bytes;
}

/** Goertzel power of `hz` over `samples` at `sampleRate`. */
function goertzel(samples: Float32Array, start: number, length: number, sampleRate: number, hz: number): number {
  const w = (2 * Math.PI * hz) / sampleRate;
  const coeff = 2 * Math.cos(w);
  let s1 = 0;
  let s2 = 0;
  for (let i = start; i < start + length; i++) {
    const s0 = samples[i]! + coeff * s1 - s2;
    s2 = s1;
    s1 = s0;
  }
  return s1 * s1 + s2 * s2 - coeff * s1 * s2;
}

/** The tone a frame holds, or -1 for silence or no clear tone. */
export function frameTone(samples: Float32Array, start: number, length: number, sampleRate: number): number {
  let energy = 0;
  for (let i = start; i < start + length; i++) energy += samples[i]! * samples[i]!;
  if (energy / length < 1e-6) return -1;
  let best = -1;
  let bestPower = 0;
  let second = 0;
  for (let k = 0; k < TONE_HZ.length; k++) {
    if (TONE_HZ[k]! >= sampleRate / 2) continue;
    const power = goertzel(samples, start, length, sampleRate, TONE_HZ[k]!);
    if (power > bestPower) {
      second = bestPower;
      bestPower = power;
      best = k;
    } else if (power > second) second = power;
  }
  return bestPower > 4 * second ? best : -1;
}

/**
 * Tone labels on a time line of 100 ms frames. Chunks are placed by media time when they
 * have one, else after the previous chunk (live sources); speed is undone first.
 */
export class ToneTimeline {
  /** Frame index (time / 100 ms) → tone, -1 for none. */
  readonly labels = new Map<number, number>();
  private carry: Float32Array = new Float32Array(0);
  /** The frame the next full 100 ms of the run fills (null before the first block). */
  private nextFrame: number | null = null;
  private liveMs = 0;
  /** Captured ranges in ms of the original time base, merged. */
  readonly covered: [number, number][] = [];

  push(chunk: { samples: Float32Array; sampleRate: number; speed: number; mediaMs: number | null; continuous: boolean }): void {
    const rate = chunk.sampleRate / (chunk.speed || 1);
    const startMs = chunk.mediaMs ?? this.liveMs;
    const frameLen = Math.round((FRAME_MS / 1000) * rate);
    let samples = chunk.samples;
    // Where this block should start if it continues the run (the carry ends there).
    const expected = this.nextFrame === null ? null : this.nextFrame * FRAME_MS + (this.carry.length / rate) * 1000;
    const jumped = chunk.mediaMs !== null && expected !== null && Math.abs(chunk.mediaMs - expected) > 50;
    if (!chunk.continuous || this.nextFrame === null || jumped) {
      // A new run (first block, seek, stall, ad): drop the unfinished frame of the last one
      // and start on the next 100 ms mark of the time line.
      const first = Math.ceil(startMs / FRAME_MS - 1e-6);
      const skip = Math.round(((first * FRAME_MS - startMs) / 1000) * rate);
      samples = samples.subarray(Math.min(samples.length, Math.max(0, skip)));
      this.carry = new Float32Array(0);
      this.nextFrame = first;
    }
    // Within a run frames follow each other by sample count: no re-snapping, which would
    // skip a frame whenever rounding left the run a fraction of a sample past a mark.
    const joined = new Float32Array(this.carry.length + samples.length);
    joined.set(this.carry);
    joined.set(samples, this.carry.length);
    let offset = 0;
    while (offset + frameLen <= joined.length) {
      this.labels.set(this.nextFrame!, frameTone(joined, offset, frameLen, rate));
      offset += frameLen;
      this.nextFrame!++;
    }
    this.carry = joined.slice(offset);
    const endMs = startMs + (chunk.samples.length / rate) * 1000;
    this.liveMs = endMs;
    this.cover(startMs, endMs);
  }

  private cover(from: number, to: number) {
    const last = this.covered.at(-1);
    if (last && from <= last[1] + 25 && from >= last[0]) last[1] = Math.max(last[1], to);
    else {
      this.covered.push([from, to]);
      this.covered.sort((a, b) => a[0] - b[0]);
      for (let i = this.covered.length - 1; i > 0; i--) {
        const prev = this.covered[i - 1]!;
        const cur = this.covered[i]!;
        if (cur[0] <= prev[1] + 25) {
          prev[1] = Math.max(prev[1], cur[1]);
          this.covered.splice(i, 1);
        }
      }
    }
  }

  /** Ms of the original time base covered so far. */
  get coveredMs(): number {
    return this.covered.reduce((sum, [a, b]) => sum + (b - a), 0);
  }

  /** Labelled (non-silent) frames between `fromMs` and `toMs`. */
  tonesIn(fromMs: number, toMs: number): [number, number][] {
    const out: [number, number][] = [];
    for (let f = Math.floor(fromMs / FRAME_MS); f < Math.ceil(toMs / FRAME_MS); f++) {
      const tone = this.labels.get(f);
      if (tone !== undefined && tone >= 0) out.push([f, tone]);
    }
    return out;
  }
}

export interface ToneMatch {
  song: SyntheticSong;
  /** Agreement of the labelled frames with the song at the best offset (0–1). */
  score: number;
  /** Position in the song's loop of time line ms 0, in ms. */
  offsetMs: number;
  frames: number;
}

/** The synthetic song that best explains the labelled frames, if it explains them well. */
export function matchTones(frames: readonly [number, number][], minFrames = 25, minScore = 0.6): ToneMatch | null {
  if (frames.length < minFrames) return null;
  let best: ToneMatch | null = null;
  let runnerUp = 0;
  for (const song of SYNTHETIC_SONGS) {
    let songBest = 0;
    let songOffset = 0;
    for (let offset = 0; offset < FRAMES_PER_SONG; offset++) {
      let agree = 0;
      for (const [f, tone] of frames) {
        const index = Math.floor((((f + offset) % FRAMES_PER_SONG) + FRAMES_PER_SONG) % FRAMES_PER_SONG / FRAMES_PER_NOTE);
        if (song.notes[index] === tone) agree++;
      }
      if (agree > songBest) {
        songBest = agree;
        songOffset = offset;
      }
    }
    const score = songBest / frames.length;
    if (!best || score > best.score) {
      if (best) runnerUp = Math.max(runnerUp, best.score);
      best = { song, score, offsetMs: songOffset * FRAME_MS, frames: frames.length };
    } else runnerUp = Math.max(runnerUp, score);
  }
  if (!best || best.score < minScore || best.score - runnerUp < 0.2) return null;
  return best;
}

export interface ToneSegment {
  song: SyntheticSong;
  startMs: number;
  endMs: number;
  score: number;
}

/**
 * Split a listened-through time line into songs: 4 s windows every second, then runs of
 * the same song, with boundaries halfway between runs (like the mix pipeline's).
 */
export function segmentTimeline(timeline: ToneTimeline, totalMs: number, windowMs = 4000, stepMs = 1000): ToneSegment[] {
  const labels: (SyntheticSong | null)[] = [];
  const scores: number[] = [];
  for (let t = 0; t + windowMs <= totalMs + stepMs; t += stepMs) {
    const match = matchTones(timeline.tonesIn(t, Math.min(totalMs, t + windowMs)), 20);
    labels.push(match?.song ?? null);
    scores.push(match?.score ?? 0);
  }
  const runs: ToneSegment[] = [];
  labels.forEach((song, i) => {
    if (!song) return;
    const center = i * stepMs + windowMs / 2;
    const last = runs.at(-1);
    // The same song again after a few unsure windows (a stall, an ad, a rough edge) is one run.
    if (last && last.song === song && center - last.endMs <= 30_000) {
      last.endMs = center;
      last.score = Math.max(last.score, scores[i]!);
    } else runs.push({ song, startMs: center, endMs: center, score: scores[i]! });
  });
  // Runs end at window centres; stretch them to meet halfway and to cover the ends.
  return runs.map((run, i) => {
    const prev = runs[i - 1];
    const next = runs[i + 1];
    const startMs = prev ? (prev.endMs + run.startMs) / 2 : Math.max(0, run.startMs - windowMs / 2);
    const endMs = next ? (run.endMs + next.startMs) / 2 : Math.min(totalMs, run.endMs + windowMs / 2);
    return { ...run, startMs: Math.round(startMs), endMs: Math.round(endMs) };
  });
}
