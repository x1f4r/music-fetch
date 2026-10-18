import { emptyFacts, mediaSessionNowPlaying, text, type SiteAdapter } from '../site';
import type { NowPlaying } from '../facts';

const VERSION = '2026.10';

/** Spotify's now-playing widget: `aria-label="Now playing: Title by Artist"`. */
function spotifyNowPlaying(doc: Document): NowPlaying | null {
  const label = doc.querySelector('[data-testid="now-playing-widget"]')?.getAttribute('aria-label') ?? '';
  const m = /^Now playing:\s*(.+?)\s+by\s+(.+)$/i.exec(label.trim());
  if (m) return { title: m[1]!, artist: m[2]!, album: '' };
  const title = text(doc.querySelector('[data-testid="context-item-info-title"]')?.textContent);
  const artist = text(doc.querySelector('[data-testid="context-item-info-artist"]')?.textContent);
  return title ? { title, artist: artist ?? '', album: '' } : null;
}

/**
 * Streaming services (Spotify, Apple Music, Deezer, Tidal, Amazon Music): DRM keeps the
 * audio from element capture, but the page says exactly what plays (extension plan §3.2
 * "Now-playing first"). No player control.
 */
export const streaming: SiteAdapter = {
  id: 'streaming',
  version: VERSION,
  element: () => null,
  facts(win, doc) {
    const nowPlaying = mediaSessionNowPlaying(win) ?? (win.location.hostname === 'open.spotify.com' ? spotifyNowPlaying(doc) : null);
    return { ...emptyFacts('streaming', VERSION), nowPlaying };
  },
};
