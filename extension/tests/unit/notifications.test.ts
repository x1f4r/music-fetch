import { describe, expect, it } from 'vitest';
import { tm } from '@/src/i18n';
import { badgeFor, resultNotification } from '@/src/models/notifications';
import { demoSessions, TRACKS } from '@/src/ui/demo/library';

describe('notifications and the badge', () => {
  const entry = demoSessions().find((d) => d.entry.id === 'demo-blinding')!.entry;

  it('offers Open in ‹provider› and Pin for a match', () => {
    const note = resultNotification(entry, { Matched: { track: TRACKS.blinding } }, 'Spotify')!;
    expect(tm(note.title)).toBe('Blinding Lights — The Weeknd');
    expect(note.buttons.map(tm)).toEqual(['Open in Spotify', 'Pin']);
    expect(note.action).toBe('openInProvider');
  });

  it('says no match with Try again, and saves offline captures', () => {
    expect(tm(resultNotification(entry, { NoMatch: { reason: 'NotFound' } }, 'Spotify')!.title)).toBe('No match found');
    expect(resultNotification(entry, 'Waiting', 'Spotify')!.action).toBe('openSession');
    expect(tm(resultNotification(entry, { Completed: { identified: 12, total: 14 } }, 'Spotify')!.title)).toBe('12 of 14 identified');
  });

  it('shows a dot while listening and the mix progress', () => {
    expect(badgeFor({ capturing: true, working: false, mixFraction: null }).text).toBe('•');
    expect(badgeFor({ capturing: false, working: false, mixFraction: 0.256 }).text).toBe('25%');
    expect(badgeFor({ capturing: false, working: false, mixFraction: null }).text).toBe('');
  });
});
