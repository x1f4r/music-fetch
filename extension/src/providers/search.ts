import type { Service, ServiceLink, Track } from '../engine/types';

/**
 * Search links for the services the core links to, in its display order: a port of
 * `catalog::search_links` (what `provider_links` returns), so the UI can offer every
 * provider before the wasm engine exists. Byte-for-byte the same URLs; the unit tests
 * hold the core's own test vectors.
 */

/** `catalog::LINK_SERVICES`. */
export const LINK_SERVICES: readonly Service[] = [
  'Spotify',
  'AppleMusic',
  'YouTubeMusic',
  'YouTube',
  'Deezer',
  'Tidal',
  'AmazonMusic',
  'SoundCloud',
];

const HEX = '0123456789ABCDEF';

/**
 * The `url` crate's special-scheme path-segment set: controls, space, `"`, `<`, `>`,
 * `` ` ``, `#`, `?`, `{`, `}`, `/`, `%`, `\`, and every non-ASCII byte.
 */
function encodePathSegment(value: string): string {
  let out = '';
  for (const byte of new TextEncoder().encode(value)) {
    const keep = byte > 0x20 && byte < 0x7f && !'"<>`#?{}/%\\'.includes(String.fromCharCode(byte));
    out += keep ? String.fromCharCode(byte) : `%${HEX[byte >> 4]}${HEX[byte & 15]}`;
  }
  return out;
}

function pathSearch(base: string, query: string): string {
  return `${base}/${encodePathSegment(query)}`;
}

/** `application/x-www-form-urlencoded`, as `Url::parse_with_params` writes it. */
function querySearch(base: string, key: string, query: string): string {
  return `${base}?${new URLSearchParams([[key, query]]).toString()}`;
}

function primaryArtist(track: Pick<Track, 'artist' | 'artists'>): string {
  return (track.artists[0] ?? track.artist).trim();
}

export function searchLinks(track: Pick<Track, 'title' | 'artist' | 'artists'>): ServiceLink[] {
  if (!track.title.trim()) return [];
  const query = `${primaryArtist(track)} ${track.title.trim()}`.trim();
  const url: Record<string, string> = {
    Spotify: pathSearch('https://open.spotify.com/search', query),
    AppleMusic: querySearch('https://music.apple.com/search', 'term', query),
    YouTubeMusic: querySearch('https://music.youtube.com/search', 'q', query),
    YouTube: querySearch('https://www.youtube.com/results', 'search_query', query),
    Deezer: pathSearch('https://www.deezer.com/search', query),
    Tidal: querySearch('https://tidal.com/search', 'q', query),
    AmazonMusic: pathSearch('https://music.amazon.com/search', query),
    SoundCloud: querySearch('https://soundcloud.com/search', 'q', query),
  };
  return LINK_SERVICES.map((service) => ({ service, url: url[service]!, is_search: true }));
}
