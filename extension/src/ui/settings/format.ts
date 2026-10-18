/** Sizes in Settings: "55 MB", "149 KB", "1.2 GB" (decimal units, as the apps show them). */
export function mb(bytes: number): string {
  if (bytes >= 1e9) return `${(bytes / 1e9).toLocaleString(undefined, { maximumFractionDigits: 1 })} GB`;
  if (bytes >= 1e6) return `${Math.round(bytes / 1e6)} MB`;
  return `${Math.max(1, Math.round(bytes / 1e3))} KB`;
}
