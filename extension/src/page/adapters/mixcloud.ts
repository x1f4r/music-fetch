import { emptyFacts, pickElement, type SiteAdapter } from '../site';
import { genericFacts } from './generic';

const VERSION = '2026.10';

export const mixcloud: SiteAdapter = {
  id: 'mixcloud',
  version: VERSION,
  element: (_win, doc, played) => pickElement([...doc.querySelectorAll<HTMLMediaElement>('audio, video'), ...played]),
  facts(win, doc) {
    const facts = genericFacts(win, doc);
    // The tracklist section lists "artist – title" rows in order.
    const rows = [...doc.querySelectorAll('[data-testid="tracklist-row"], [class*="TrackList"] li')]
      .map((row) => {
        const title = row.querySelector('[data-testid="track-title"], [class*="Title"]')?.textContent?.trim() ?? '';
        const artist = row.querySelector('[data-testid="track-artist"], [class*="Artist"]')?.textContent?.trim() ?? null;
        return { title, artist, url: null, durationMs: null };
      })
      .filter((e) => e.title);
    return { ...emptyFacts('mixcloud', VERSION), ...facts, entries: rows };
  },
};
