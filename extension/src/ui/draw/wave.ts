/**
 * The listen button's glyph: the contour mark at rest, the live line while listening
 * (three sines under a sin(πu) envelope, amplitude = level), the same geometry as the
 * design prototypes and docs/design/mockups/mock.js.
 */

export const MARK_VIEWBOX = '0 0 48 20';
export const MARK_PATH = 'M3 11 C7 11 8 5 12 5 C16 5 17 15 21 15 C26 15 26 3 31 3 C36 3 36 13 40 13 C43 13 44 9 45 9';
export const LIVE_VIEWBOX = '0 0 64 40';

const FREQUENCIES = [1.0, 2.3, 3.7];
const AMPLITUDES = [0.6, 0.3, 0.15];
const PHASES = [0.9, 3.1, 4.6];
/** The amplitudes' sum, so a full-scale line stays inside the box. */
const AMPLITUDE_SUM = AMPLITUDES.reduce((a, b) => a + b, 0);

/** The live line at `level` (0..1); `phase` (radians) moves the wave between frames. */
export function livePath(level: number, phase = 0, width = 64, height = 40, points = 64): string {
  const amp = Math.max(0, Math.min(1, level));
  const parts: string[] = [];
  for (let i = 0; i <= points; i++) {
    const u = i / points;
    const envelope = Math.sin(Math.PI * u);
    let y = 0;
    for (let k = 0; k < 3; k++) y += AMPLITUDES[k]! * Math.sin(2 * Math.PI * FREQUENCIES[k]! * u * 2 + PHASES[k]! + phase * (k + 1));
    const x = u * width;
    const py = height / 2 + (y / AMPLITUDE_SUM) * envelope * amp * (height / 2);
    parts.push(`${i ? 'L' : 'M'}${x.toFixed(1)} ${py.toFixed(1)}`);
  }
  return parts.join(' ');
}
