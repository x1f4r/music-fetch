import { describe, expect, it } from 'vitest';
import type { LibraryEntry } from '@/src/engine/types';
import {
  copyText,
  deleteNeedsConfirm,
  displayOrder,
  isMixEntry,
  librarySections,
  matchesFilter,
  pinsSelection,
  rangeSelection,
  rowSubtitle,
  siteOf,
  toggleSelection,
} from '@/src/models/library';
import { demoSessions } from '@/src/ui/demo/library';
import { text } from '@/src/ui/format';

const entries = (): LibraryEntry[] => demoSessions().map((d) => d.entry);
const byId = (id: string) => entries().find((e) => e.id === id)!;

describe('library rows and filters', () => {
  it('puts pinned sessions on top, then days', () => {
    const sections = librarySections(entries(), Date.now());
    expect(sections[0]!.pinned).toBe(true);
    expect(sections[0]!.entries.every((e) => e.pinned)).toBe(true);
    expect(displayOrder(sections)).toHaveLength(entries().length);
  });

  it('filters like the apps: Hummed is hums, Links holds links, files and lyrics, Mixes the long ones', () => {
    const all = entries();
    expect(all.filter((e) => matchesFilter(e, 'hummed')).every((e) => e.kind === 'Hum')).toBe(true);
    expect(all.filter((e) => matchesFilter(e, 'mixes')).map((e) => e.id)).toEqual(['demo-mix', 'demo-playlist']);
    expect(all.filter((e) => matchesFilter(e, 'links'))).toHaveLength(0);
    expect(all.filter((e) => matchesFilter(e, 'pinned')).every((e) => e.pinned)).toBe(true);
    expect(isMixEntry(byId('demo-mix'))).toBe(true);
  });

  it('writes subtitles like the macOS library', () => {
    expect(text(rowSubtitle(byId('demo-hum'), 4))).toBe('a-ha · 4 candidates');
    expect(text(rowSubtitle(byId('demo-playlist')))).toBe('Sabrina Carpenter · 12 tracks · Spotify');
    expect(text(rowSubtitle(byId('demo-mix')))).toBe('13 tracks · YouTube');
    expect(text(rowSubtitle(byId('demo-waiting')))).toBe('Waiting for connection');
    expect(text(rowSubtitle(byId('demo-nomatch')))).toBe('No match found');
    expect(siteOf('https://music.youtube.com/watch?v=1')).toBe('YouTube Music');
  });

  it('selects with ⌘-click and Shift ranges', () => {
    const order = ['a', 'b', 'c', 'd'];
    let selection = toggleSelection(new Set(), 'b');
    selection = rangeSelection(selection, order, 'b', 'd');
    expect([...selection].sort()).toEqual(['b', 'c', 'd']);
    expect([...toggleSelection(selection, 'c')].sort()).toEqual(['b', 'd']);
  });

  it('pins unless all are pinned, and asks before deleting mixes or pinned sessions', () => {
    expect(pinsSelection([byId('demo-houdini'), byId('demo-nights')])).toBe(false);
    expect(pinsSelection([byId('demo-houdini'), byId('demo-blinding')])).toBe(true);
    expect(deleteNeedsConfirm([byId('demo-blinding')])).toBe(false);
    expect(deleteNeedsConfirm([byId('demo-blinding'), byId('demo-playlist')])).toBe(true);
    expect(copyText([byId('demo-houdini'), byId('demo-blinding')])).toBe('Dua Lipa – Houdini\nThe Weeknd – Blinding Lights');
  });
});
