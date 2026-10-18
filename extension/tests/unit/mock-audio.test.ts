import { describe, expect, it } from 'vitest';
import {
  matchTones,
  renderSong,
  segmentTimeline,
  SYNTHETIC_SONGS,
  ToneTimeline,
} from '@/src/engine/host-mock-audio';

/** Feed `samples` as chunks of `block` frames, like a capture would. */
function feed(timeline: ToneTimeline, samples: Float32Array, sampleRate: number, speed: number, mediaMs: number | null, block = 2048) {
  for (let i = 0, seq = 0; i < samples.length; i += block, seq++) {
    const part = samples.subarray(i, Math.min(samples.length, i + block));
    const ms = mediaMs === null ? null : mediaMs + ((i / sampleRate) * 1000 * speed);
    timeline.push({ samples: part, sampleRate, speed, mediaMs: ms, continuous: seq > 0 });
  }
}

describe('the synthetic test songs', () => {
  it('are told apart from 5 s of live audio, at the right loop position', () => {
    const song = SYNTHETIC_SONGS[2]!;
    const timeline = new ToneTimeline();
    feed(timeline, renderSong(song, 48_000, 5000, 7300), 48_000, 1, null);
    const match = matchTones(timeline.tonesIn(0, 5000));
    expect(match?.song.id).toBe(song.id);
    expect(Math.abs(match!.offsetMs - 7300)).toBeLessThanOrEqual(100);
  });

  it('survive a 4× listen-through with the speed undone', () => {
    const song = SYNTHETIC_SONGS[4]!;
    // At 4× with preservesPitch off, 6 s of the song arrive as 1.5 s of 48 kHz audio at 4×
    // the pitch: the same samples as the song rendered at 12 kHz.
    const original = renderSong(song, 12_000, 6000, 0);
    const timeline = new ToneTimeline();
    feed(timeline, original, 48_000, 4, 10_000);
    const match = matchTones(timeline.tonesIn(10_000, 16_000));
    expect(match?.song.id).toBe(song.id);
    expect(timeline.coveredMs).toBeGreaterThan(5900);
  });

  it('does not invent a song from noise or silence', () => {
    const timeline = new ToneTimeline();
    const noise = new Float32Array(48_000 * 4).map(() => (Math.random() - 0.5) * 0.4);
    feed(timeline, noise, 48_000, 1, null);
    feed(timeline, new Float32Array(48_000 * 2), 48_000, 1, null);
    expect(matchTones(timeline.tonesIn(0, 6000))).toBeNull();
  });

  it('segments a mix of three songs within a second of the truth', () => {
    const [a, b, c] = SYNTHETIC_SONGS as [typeof SYNTHETIC_SONGS[0], typeof SYNTHETIC_SONGS[0], typeof SYNTHETIC_SONGS[0]];
    const sr = 16_000;
    const mix = new Float32Array(sr * 60);
    mix.set(renderSong(a, sr, 20_000, 0), 0);
    mix.set(renderSong(b, sr, 20_000, 3000), sr * 20);
    mix.set(renderSong(c, sr, 20_000, 9000), sr * 40);
    const timeline = new ToneTimeline();
    feed(timeline, mix, sr, 1, 0);
    const segments = segmentTimeline(timeline, 60_000);
    expect(segments.map((s) => s.song.id)).toEqual([a.id, b.id, c.id]);
    expect(Math.abs(segments[0]!.endMs - 20_000)).toBeLessThanOrEqual(1000);
    expect(Math.abs(segments[1]!.endMs - 40_000)).toBeLessThanOrEqual(1000);
    expect(segments[2]!.endMs).toBeGreaterThan(58_000);
  });
});
