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

/** The library filters: All · Heard · Hummed · Links · Mixes. */
export type LibraryFilter = 'all' | 'heard' | 'hummed' | 'links' | 'mixes';

export function filterKinds(filter: LibraryFilter): SessionKind[] {
  switch (filter) {
    case 'heard':
      return ['Listen', 'SystemAudio'];
    case 'hummed':
      return ['Hum', 'Lyrics'];
    case 'links':
    case 'mixes':
      return ['Link', 'File'];
    default:
      return [];
  }
}

/** Mixes are link or file sessions with more than one track. */
export function matchesFilter(entry: LibraryEntry, filter: LibraryFilter): boolean {
  const kinds = filterKinds(filter);
  if (kinds.length > 0 && !kinds.includes(entry.kind)) return false;
  if (filter === 'mixes') return entry.track_count > 1;
  if (filter === 'links') return entry.track_count <= 1;
  return true;
}
