import type { StorageUsage } from '../../engine/types';

/**
 * Settings › Storage: the engine's own numbers (library, cache, melody library) next to
 * what the browser counts for the extension (`navigator.storage.estimate()`), which also
 * holds the engine's databases and the browser's overhead (extension plan §3.5).
 */
export interface StorageView {
  /** What the extension takes in this browser. */
  total: number;
  library: number;
  cache: number;
  melody: number;
  /** What the browser counts beyond the engine's numbers (work files, the browser's own overhead); 0 when small. */
  other: number;
  /** Little room left: under 10 % of the browser's quota, or under 100 MB. */
  low: boolean;
}

export function storageView(usage: StorageUsage | null, estimate: { usage?: number; quota?: number } | null): StorageView | null {
  if (!usage && !estimate?.usage) return null;
  const engine = usage ? usage.database_bytes + usage.cache_bytes + usage.melody_index_bytes : 0;
  const total = Math.max(engine, estimate?.usage ?? 0);
  const quota = estimate?.quota ?? null;
  const left = quota !== null ? quota - total : null;
  return {
    total,
    library: usage?.database_bytes ?? 0,
    cache: usage?.cache_bytes ?? 0,
    melody: usage?.melody_index_bytes ?? 0,
    other: total - engine >= Math.max(1e6, engine * 0.1) ? total - engine : 0,
    low: left !== null && quota !== null && quota > 0 && (left < quota * 0.1 || left < 100e6),
  };
}

/** The browser's estimate for this extension's origin, or null where there is none. */
export async function browserEstimate(): Promise<{ usage?: number; quota?: number } | null> {
  try {
    return (await navigator.storage?.estimate?.()) ?? null;
  } catch {
    return null;
  }
}
