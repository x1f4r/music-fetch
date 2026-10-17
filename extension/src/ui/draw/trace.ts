import type { PitchFrame } from '../../engine/types';

/**
 * The trace band (design spec "Trace band"): the last `windowMs` of pitch, newest at the
 * right, ±`range` semitones around the running median, gaps for unvoiced frames, opacity
 * from voicing confidence. Returns SVG path segments in a `width × height` box.
 */
export interface TraceSegment {
  d: string;
  opacity: number;
}

export interface TraceGeometry {
  segments: TraceSegment[];
  /** The newest voiced point, for the "now" dot. */
  now: { x: number; y: number } | null;
}

export function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
}

const voiced = (f: PitchFrame) => f.midi !== null && Number.isFinite(f.midi) && f.confidence >= 0.5;

export function traceGeometry(
  frames: readonly PitchFrame[],
  options: { width: number; height: number; windowMs?: number; range?: number; endMs?: number },
): TraceGeometry {
  const { width, height } = options;
  const windowMs = options.windowMs ?? 8000;
  const range = options.range ?? 12;
  const end = options.endMs ?? frames.at(-1)?.t_ms ?? 0;
  const start = end - windowMs;
  const visible = frames.filter((f) => f.t_ms >= start && f.t_ms <= end);
  const center = median(visible.filter(voiced).map((f) => f.midi as number));
  const px = height / 2 / range;
  const segments: TraceSegment[] = [];
  let d = '';
  let sum = 0;
  let count = 0;
  let now: TraceGeometry['now'] = null;
  const flush = () => {
    if (d) segments.push({ d, opacity: Math.min(1, 0.55 + 0.45 * (sum / count)) });
    d = '';
    sum = 0;
    count = 0;
  };
  for (const frame of visible) {
    if (!voiced(frame)) {
      flush();
      continue;
    }
    const semis = Math.max(-range, Math.min(range, (frame.midi as number) - center));
    const x = ((frame.t_ms - start) / windowMs) * width;
    const y = height / 2 - semis * px;
    d += `${d ? ' L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`;
    sum += frame.confidence;
    count++;
    now = { x, y };
  }
  flush();
  return { segments, now };
}
