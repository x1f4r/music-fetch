import type { LibraryEntry, SessionKind } from '../engine/types';
import type { Message } from '../i18n';

/**
 * Library sections like the apps (android-ux.md §2–3, DESIGN §6.8): Pinned on top, then
 * one group per day, newest first. Labels are messages (Today, Yesterday) or a date.
 */
export interface LibrarySection {
  id: string;
  label: Message | string;
  pinned: boolean;
  entries: LibraryEntry[];
}

function dayStart(ms: number): number {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

export function dayLabel(ms: number, now: number, locale?: string): Message | string {
  const day = dayStart(ms);
  const today = dayStart(now);
  if (day === today) return { key: 'library.today' };
  const yesterday = dayStart(today - 12 * 3_600_000);
  if (day === yesterday) return { key: 'library.yesterday' };
  const sameYear = new Date(ms).getFullYear() === new Date(now).getFullYear();
  return new Date(ms).toLocaleDateString(locale, { weekday: 'long', day: 'numeric', month: 'long', ...(sameYear ? {} : { year: 'numeric' }) });
}

export function librarySections(entries: readonly LibraryEntry[], now: number, options: { pinnedSection?: boolean } = {}): LibrarySection[] {
  const sorted = [...entries].sort((a, b) => b.created_at_ms - a.created_at_ms);
  const sections: LibrarySection[] = [];
  const pinnedSection = options.pinnedSection ?? true;
  if (pinnedSection) {
    const pinned = sorted.filter((e) => e.pinned);
    if (pinned.length > 0) sections.push({ id: 'pinned', label: { key: 'nav.pinned' }, pinned: true, entries: pinned });
  }
  for (const entry of sorted) {
    if (pinnedSection && entry.pinned) continue;
    const id = `day-${dayStart(entry.created_at_ms)}`;
    let section = sections.at(-1);
    if (!section || section.id !== id) {
      section = { id, label: dayLabel(entry.created_at_ms, now), pinned: false, entries: [] };
      sections.push(section);
    }
    section.entries.push(entry);
  }
  return sections;
}

/** Library sessions in display order (the order rows appear in, for ranges and arrows). */
export function displayOrder(sections: readonly LibrarySection[]): LibraryEntry[] {
  return sections.flatMap((s) => s.entries);
}

/** The library filters: All · Heard · Hummed · Links · Mixes · Pinned (DESIGN §6.8). */
export type LibraryFilter = 'all' | 'heard' | 'hummed' | 'links' | 'mixes' | 'pinned';

export const LIBRARY_FILTERS: readonly LibraryFilter[] = ['all', 'heard', 'hummed', 'links', 'mixes', 'pinned'];

export function filterKinds(filter: LibraryFilter): SessionKind[] {
  switch (filter) {
    case 'heard':
      return ['Listen', 'SystemAudio'];
    case 'hummed':
      return ['Hum'];
    case 'links':
      return ['Link', 'File', 'Lyrics'];
    case 'mixes':
      return ['Link', 'File'];
    default:
      return [];
  }
}

/** A link or file session with a tracklist: two or more tracks, or long media. */
export function isMixEntry(entry: LibraryEntry): boolean {
  if (entry.kind !== 'Link' && entry.kind !== 'File') return false;
  return entry.track_count > 1 || (entry.duration_ms ?? 0) >= 10 * 60_000;
}

export function matchesFilter(entry: LibraryEntry, filter: LibraryFilter): boolean {
  if (filter === 'pinned') return entry.pinned;
  const kinds = filterKinds(filter);
  if (kinds.length > 0 && !kinds.includes(entry.kind)) return false;
  if (filter === 'mixes') return isMixEntry(entry);
  if (filter === 'links') return !isMixEntry(entry);
  return true;
}

/** The kind symbol in front of a row's subtitle (DESIGN §4.6). */
export function kindIcon(entry: LibraryEntry): string {
  if (isMixEntry(entry)) return 'ic-mix';
  switch (entry.kind) {
    case 'Listen':
      return 'ic-mic';
    case 'SystemAudio':
      return 'ic-tab';
    case 'Hum':
      return 'ic-hum';
    case 'Link':
      return 'ic-link';
    case 'File':
      return 'ic-note';
    case 'Lyrics':
      return 'ic-text';
  }
}

function host(url: string | null): string | null {
  if (!url) return null;
  try {
    return new URL(url).hostname.replace(/^(www|m)\./, '');
  } catch {
    return null;
  }
}

/**
 * A row's subtitle, like the macOS library (LibraryView `subtitle`): the artist, the
 * candidate count for hums, "‹n› tracks so far" while a mix runs, the source's site.
 */
export function rowSubtitle(entry: LibraryEntry, candidateCount: number | null = null): Message | string {
  const artist = entry.track?.artist ?? entry.subtitle;
  switch (entry.status) {
    case 'Waiting':
      return { key: 'library.waiting' };
    case 'Failed':
      return { key: 'library.failed' };
    case 'NoMatch':
      return { key: 'state.noMatch' };
    case 'Canceled':
      if (entry.track_count === 0) return { key: 'library.stopped' };
      break;
    default:
      break;
  }
  switch (entry.kind) {
    case 'Hum':
    case 'Lyrics':
      if (!entry.track && candidateCount) return { key: 'library.candidates', args: [artist ?? '', candidateCount] };
      return artist ?? '';
    case 'Link':
    case 'File': {
      const site = entry.subtitle && entry.subtitle !== artist ? entry.subtitle : host(entry.source_url);
      if (entry.status === 'Running') return entry.track_count > 0 ? { key: 'mix.tracksSoFar', args: [entry.track_count] } : (site ?? { key: 'library.running' });
      if (isMixEntry(entry)) return [artist !== site ? artist : null, `${entry.track_count} tracks`, site].filter(Boolean).join(' · ');
      return [entry.track?.artist ?? null, site].filter(Boolean).join(' · ');
    }
    default:
      return artist ?? '';
  }
}

/** The trailing time: the clock today, "Yesterday" or a short date for older pinned rows. */
export function rowTime(entry: LibraryEntry, now: number, inDaySection: boolean, locale?: string): Message | string {
  const time = new Date(entry.created_at_ms).toLocaleTimeString(locale, { hour: 'numeric', minute: '2-digit' });
  if (inDaySection) return time;
  const label = dayLabel(entry.created_at_ms, now, locale);
  if (typeof label !== 'string') return label.key === 'library.today' ? time : label;
  return new Date(entry.created_at_ms).toLocaleDateString(locale, { day: 'numeric', month: 'short' });
}

// MARK: selection (DESIGN §6.8, extension plan §5.7)

/** Select or unselect one row (⌘/Ctrl-click, the checkbox, Space). */
export function toggleSelection(selection: ReadonlySet<string>, id: string): Set<string> {
  const next = new Set(selection);
  if (next.has(id)) next.delete(id);
  else next.add(id);
  return next;
}

/** Shift-click: everything between the anchor and `id` (in display order) joins the selection. */
export function rangeSelection(selection: ReadonlySet<string>, order: readonly string[], anchor: string | null, id: string): Set<string> {
  const next = new Set(selection);
  const from = anchor ? order.indexOf(anchor) : -1;
  const to = order.indexOf(id);
  if (from < 0 || to < 0) {
    next.add(id);
    return next;
  }
  const [a, b] = from < to ? [from, to] : [to, from];
  for (const rowId of order.slice(a, b + 1)) next.add(rowId);
  return next;
}

/** Pin unless every selected session is already pinned (macOS `togglePinned`). */
export function pinsSelection(entries: readonly LibraryEntry[]): boolean {
  return !entries.every((e) => e.pinned);
}

/** Deleting a mix or a pinned session asks first; everything else gets Undo. */
export function deleteNeedsConfirm(entries: readonly LibraryEntry[]): boolean {
  return entries.some((e) => e.pinned || isMixEntry(e));
}

/** "Artist – Title" lines for Copy (one per selected session with a track). */
export function copyText(entries: readonly LibraryEntry[]): string {
  return entries
    .map((e) => (e.track ? `${e.track.artist} – ${e.track.title}` : e.title))
    .filter(Boolean)
    .join('\n');
}
