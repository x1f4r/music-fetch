import { describe, expect, it } from 'vitest';
import { downmix, PcmFramer, Rechunker } from '@/src/audio/framer';
import { MediaClock } from '@/src/audio/media-clock';
import { SilenceWatch } from '@/src/audio/silence';
import type { PcmChunk } from '@/src/audio/types';
import { chooseMic } from '@/src/audio/mic';

const SR = 48_000;
const BLOCK = 2048;
const blockMs = (BLOCK / SR) * 1000;

describe('the media clock', () => {
  it('stamps a 4× tab capture by frame count, 35 ms × rate behind currentTime', () => {
    const clock = new MediaClock({ lagMs: 35 });
    const stamps: number[] = [];
    // The page plays at 4× from media 60 s; currentTime is sampled every 50 ms with jitter.
    for (let i = 0; i < 40; i++) {
      const wall = 1000 + i * blockMs;
      clock.observe({ wallMs: wall - 3, mediaMs: 60_000 + (wall - 3 - 1000) * 4 + (i % 3) * 2, rate: 4, advancing: true });
      const s = clock.stamp(BLOCK, SR, wall - blockMs);
      if (s) stamps.push(s.mediaMs);
      if (i > 1) expect(s?.continuous).toBe(true);
    }
    // Every block advances by exactly frames × rate.
    for (let i = 1; i < stamps.length; i++) expect(stamps[i]! - stamps[i - 1]!).toBeCloseTo(blockMs * 4, 6);
    // The first block: (wall − 35 ms) on the page's clock.
    expect(stamps[0]).toBeCloseTo(60_000 + (1000 - blockMs - 35 - 1000) * 4, 0);
  });

  it('opens a new run at a seek and drops blocks while paused or after a restart', () => {
    const clock = new MediaClock({ lagMs: 35 });
    clock.observe({ wallMs: 0, mediaMs: 0, rate: 1, advancing: true });
    expect(clock.stamp(BLOCK, SR, 100)?.continuous).toBe(false);
    expect(clock.stamp(BLOCK, SR, 100 + blockMs)?.continuous).toBe(true);
    clock.observe({ wallMs: 200, mediaMs: 200, rate: 1, advancing: false });
    expect(clock.stamp(BLOCK, SR, 210)).toBeNull();
    clock.restart(300);
    clock.observe({ wallMs: 290, mediaMs: 90_000, rate: 1, advancing: true }); // older than the restart
    expect(clock.stamp(BLOCK, SR, 320)).toBeNull();
    clock.observe({ wallMs: 310, mediaMs: 90_000, rate: 1, advancing: true });
    const s = clock.stamp(BLOCK, SR, 345);
    expect(s?.continuous).toBe(false);
    expect(s?.mediaMs).toBeCloseTo(90_000, 0);
    expect(clock.dropped).toBe(2);
  });

  it('anchors once for element capture and ignores arrival jitter below the drift limit', () => {
    const clock = new MediaClock({ lagMs: 0, anchorOnly: true });
    clock.observe({ wallMs: 0, mediaMs: 5000, rate: 2, advancing: true });
    const first = clock.stamp(BLOCK, SR, 0)!;
    // Arrival-time samples err by ~0.3 s; the frame count keeps the run.
    clock.observe({ wallMs: blockMs, mediaMs: 5000 + blockMs * 2 + 300, rate: 2, advancing: true });
    const second = clock.stamp(BLOCK, SR, blockMs)!;
    expect(second.continuous).toBe(true);
    expect(second.mediaMs - first.mediaMs).toBeCloseTo(blockMs * 2, 6);
    // A rate change ends the run.
    clock.observe({ wallMs: 2 * blockMs, mediaMs: 5000 + blockMs * 4, rate: 4, advancing: true });
    expect(clock.stamp(BLOCK, SR, 2 * blockMs)?.continuous).toBe(false);
  });
});

describe('framing', () => {
  it('numbers chunks, marks the first as a new run and drops what the clock rejects', () => {
    const chunks: PcmChunk[] = [];
    const clock = new MediaClock({ lagMs: 0 });
    const framer = new PcmFramer((c) => chunks.push(c), { clock });
    framer.push(new Float32Array(BLOCK), SR, 1000); // no sample yet: dropped
    clock.observe({ wallMs: 1000, mediaMs: 0, rate: 1, advancing: true });
    framer.push(new Float32Array(BLOCK), SR, 1000 + blockMs);
    framer.push(new Float32Array(BLOCK), SR, 1000 + 2 * blockMs);
    expect(framer.dropped).toBe(1);
    expect(chunks.map((c) => [c.seq, c.continuous])).toEqual([
      [0, false],
      [1, true],
    ]);
    expect(chunks[0]!.wallMs).toBeCloseTo(1000 + blockMs, 6);
  });

  it('mixes channels down and re-chunks to a fixed size', () => {
    expect([...downmix([new Float32Array([1, 0.5]), new Float32Array([0, 0.5])])]).toEqual([0.5, 0.5]);
    const blocks: [number, number][] = [];
    const r = new Rechunker(4, (b, first) => blocks.push([b.length, first]));
    r.push(new Float32Array(3));
    r.push(new Float32Array(6));
    expect(blocks).toEqual([
      [4, 0],
      [4, 4],
    ]);
  });

  it('notices digital silence after 1.5 s and sound again', () => {
    const watch = new SilenceWatch();
    const zero = new Float32Array(4800);
    const changes = Array.from({ length: 16 }, () => watch.push(zero, SR)).filter(Boolean);
    expect(changes).toEqual(['silent']);
    expect(watch.push(new Float32Array([0.2, -0.1]), SR)).toBe('sound');
  });
});

describe('microphone priority', () => {
  const devices = [
    { deviceId: 'default', kind: 'audioinput' as const },
    { deviceId: 'usb', kind: 'audioinput' as const },
    { deviceId: 'built-in', kind: 'audioinput' as const },
    { deviceId: 'speaker', kind: 'audiooutput' as const },
  ];
  it('uses the first connected device of the list, else the system default', () => {
    expect(chooseMic(devices, ['gone', 'usb', 'built-in'])).toBe('usb');
    expect(chooseMic(devices, ['speaker', 'gone'])).toBeNull();
    expect(chooseMic(devices, ['default'])).toBeNull();
  });
});
