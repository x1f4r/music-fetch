/**
 * A contour (semitones per frame, NaN or null where unvoiced) as SVG path pieces in a
 * `width × height` box: ±`range` semitones around zero, gaps where the voice stops.
 */
export function contourPaths(values: readonly (number | null)[], options: { width: number; height: number; range?: number }): string[] {
  const { width, height } = options;
  const range = options.range ?? 12;
  const px = height / 2 / range;
  const n = Math.max(1, values.length - 1);
  const paths: string[] = [];
  let d = '';
  values.forEach((value, i) => {
    if (value === null || !Number.isFinite(value)) {
      if (d) paths.push(d);
      d = '';
      return;
    }
    const x = (i / n) * width;
    const y = height / 2 - Math.max(-range, Math.min(range, value)) * px;
    d += `${d ? ' L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`;
  });
  if (d) paths.push(d);
  return paths;
}

/** Seconds of voiced hum in an alignment. */
export function voicedSeconds(values: readonly (number | null)[], frameMs: number): number {
  return Math.round((values.filter((v) => v !== null && Number.isFinite(v)).length * frameMs) / 1000);
}
