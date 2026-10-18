/**
 * Writes `licenses.json` into the build output for Settings › About (build time, Node):
 *
 * - the sections of the repository's THIRD_PARTY_NOTICES.md ("## Title", prose, a fenced
 *   license), as the apps read them;
 * - every npm package whose code ends up in a bundle, found through the chunks' module
 *   ids, with its version, license and license text;
 * - the engine's Rust crates when the engine build left a `rust-crates.json`
 *   (`[{"name", "version", "license"}]`, the format scripts/build-macos-app.sh writes)
 *   next to this project (`.licenses/rust-crates.json`).
 *
 * Sorted and free of absolute paths, so the output is the same on every machine.
 */
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve, sep } from 'node:path';

export interface NoticeEntry {
  title: string;
  summary: string;
  text: string;
}

export interface PackageEntry {
  name: string;
  version: string;
  license: string;
  text: string;
}

export interface CrateEntry {
  name: string;
  version: string;
  license: string | null;
}

export interface LicensesFile {
  notices: NoticeEntry[];
  packages: PackageEntry[];
  crates: CrateEntry[];
}

/** Sections of a THIRD_PARTY_NOTICES.md (the same parser as the apps' `Licenses.parseNotices`). */
export function parseNotices(markdown: string): NoticeEntry[] {
  const entries: NoticeEntry[] = [];
  let title: string | null = null;
  let prose: string[] = [];
  let license: string[] = [];
  let fence = false;
  const flush = () => {
    if (title) entries.push({ title, summary: prose.join(' ').replaceAll('`', '').trim(), text: license.join('\n') });
  };
  for (const line of markdown.split(/\r?\n/)) {
    if (line.startsWith('```')) {
      fence = !fence;
      continue;
    }
    if (fence) license.push(line);
    else if (line.startsWith('## ')) {
      flush();
      title = line.slice(3).trim();
      prose = [];
      license = [];
    } else if (title && line.trim()) prose.push(line.trim());
  }
  flush();
  return entries;
}

/** The package directory of a module path inside node_modules (scoped names included), or null. */
export function packageDir(moduleId: string): string | null {
  const path = moduleId.replace(/\?.*$/, '').replace(/^\0/, '');
  const marker = `${sep}node_modules${sep}`;
  const at = path.lastIndexOf(marker);
  if (at < 0) return null;
  const rest = path.slice(at + marker.length).split(sep);
  const name = rest[0]?.startsWith('@') ? rest.slice(0, 2) : rest.slice(0, 1);
  if (!name.length || name.some((part) => !part)) return null;
  return path.slice(0, at + marker.length) + name.join(sep);
}

function licenseText(dir: string): string {
  const file = readdirSync(dir).find((f) => /^(licen[cs]e|copying)(\.(md|txt))?$/i.test(f));
  return file ? readFileSync(join(dir, file), 'utf8').trim() : '';
}

export function packageEntries(moduleIds: Iterable<string>): PackageEntry[] {
  const dirs = new Set<string>();
  for (const id of moduleIds) {
    const dir = packageDir(id);
    if (dir && existsSync(join(dir, 'package.json'))) dirs.add(dir);
  }
  const entries = new Map<string, PackageEntry>();
  for (const dir of dirs) {
    const pkg = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8')) as { name?: string; version?: string; license?: string | { type?: string } };
    if (!pkg.name) continue;
    const license = typeof pkg.license === 'string' ? pkg.license : (pkg.license?.type ?? 'see the package');
    entries.set(`${pkg.name}@${pkg.version}`, { name: pkg.name, version: pkg.version ?? '', license, text: licenseText(dir) });
  }
  return [...entries.values()].sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version));
}

export function readCrates(path: string): CrateEntry[] {
  if (!existsSync(path)) return [];
  const list = JSON.parse(readFileSync(path, 'utf8')) as CrateEntry[];
  return list.map((c) => ({ name: c.name, version: c.version, license: c.license ?? null })).sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version));
}

/** Everything the About pane lists, for the bundles made of `moduleIds`. */
export function collectLicenses(root: string, moduleIds: Iterable<string>): LicensesFile {
  const notices = resolve(root, '..', 'THIRD_PARTY_NOTICES.md');
  return {
    notices: existsSync(notices) ? parseNotices(readFileSync(notices, 'utf8')) : [],
    packages: packageEntries(moduleIds),
    crates: readCrates(resolve(root, '.licenses', 'rust-crates.json')),
  };
}

export function writeLicenses(outDir: string, licenses: LicensesFile): string {
  const path = join(outDir, 'licenses.json');
  writeFileSync(path, `${JSON.stringify(licenses, null, 1)}\n`);
  return path;
}

