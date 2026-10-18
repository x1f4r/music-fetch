/**
 * The licenses Settings › About lists, from `licenses.json`, which the build writes next
 * to the manifest (`tools/licenses.ts`): THIRD_PARTY_NOTICES.md, the JavaScript packages
 * bundled into the extension, and the engine's Rust crates once the wasm engine ships.
 */
export interface LicenseEntry {
  title: string;
  /** One line: what it is, under which license. */
  summary: string;
  /** The full license text; may be empty. */
  text: string;
}

export interface Licenses {
  notices: LicenseEntry[];
  packages: { name: string; version: string; license: string; text: string }[];
  crates: { name: string; version: string; license: string | null }[];
}

export function parseLicenses(value: unknown): Licenses | null {
  const raw = value as Partial<Licenses> | null;
  if (!raw || !Array.isArray(raw.notices) || !Array.isArray(raw.packages)) return null;
  return { notices: raw.notices, packages: raw.packages, crates: Array.isArray(raw.crates) ? raw.crates : [] };
}

/** The license list of this build, or null when it is missing. */
export async function loadLicenses(url: string, fetchImpl: typeof fetch = fetch): Promise<Licenses | null> {
  try {
    const response = await fetchImpl(url);
    return response.ok ? parseLicenses(await response.json()) : null;
  } catch {
    return null;
  }
}
