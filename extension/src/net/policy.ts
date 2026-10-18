/**
 * The header needs of the engine's requests (extension plan §2.6 "Header policy",
 * validated against the real services from the Legion in spike S4). The native client
 * sets these in `PageRequest::apply` / the Shazam client; in the browser fetch() can't
 * set User-Agent (Chromium ignores it), Cookie, Origin or Referer, so the extension
 * installs them as declarativeNetRequest session rules scoped to its own requests
 * (`rules.ts`). Mirrors the core's planned `util::http::HeaderPolicy`; the wasm facade's
 * `Engine.headerRules()` replaces this table (E8).
 */

/** The eight Dalvik agents the native Shazam client picks from per request (`shazam/client.rs`). */
export const SHAZAM_USER_AGENTS = [
  'Dalvik/2.1.0 (Linux; U; Android 14; Pixel 8 Build/AP2A.240805.005)',
  'Dalvik/2.1.0 (Linux; U; Android 14; SM-S921B Build/UP1A.231005.007)',
  'Dalvik/2.1.0 (Linux; U; Android 13; SM-S911B Build/TP1A.220624.014)',
  'Dalvik/2.1.0 (Linux; U; Android 13; Pixel 7 Build/TQ3A.230901.001)',
  'Dalvik/2.1.0 (Linux; U; Android 12; SM-G991B Build/SP1A.210812.016)',
  'Dalvik/2.1.0 (Linux; U; Android 11; Mi 10T Pro Build/RKQ1.200826.002)',
  'Dalvik/2.1.0 (Linux; U; Android 10; SM-G973F Build/QP1A.190711.020)',
  'Dalvik/2.1.0 (Linux; U; Android 9; SM-G960F Build/PPR1.180610.011)',
] as const;

/** `util::http::CRAWLER_UA`: Spotify and Amazon serve their Open Graph tags to it. */
export const CRAWLER_USER_AGENT = 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)';

/** `util::http::user_agent`: the neutral agent, nothing personal (Genius refuses the browser's, S4). */
export function neutralUserAgent(version: string): string {
  return `MusicFetch/${version} (+https://github.com/x1f4r/music-fetch)`;
}

/** YouTube's consent wall, answered "accept necessary only" (`links/youtube.rs`). */
export const YOUTUBE_CONSENT_COOKIE = 'SOCS=CAI; CONSENT=YES+1';

export interface HeaderRule {
  id: 'shazam' | 'crawler' | 'youtube' | 'ytmusic' | 'neutral';
  /** URL prefixes the rule applies to (anchored at the start). */
  prefixes: string[];
  /** Headers to set, lower case. */
  set: Record<string, string>;
  /** Headers to remove, lower case. */
  remove: string[];
}

export function headerPolicy(version: string, shazamAgent = 0): HeaderRule[] {
  return [
    {
      id: 'shazam',
      prefixes: ['https://amp.shazam.com/discovery/'],
      set: { 'user-agent': SHAZAM_USER_AGENTS[shazamAgent % SHAZAM_USER_AGENTS.length]! },
      // Browsers add `Origin: chrome-extension://…` to POSTs; the Android client sends none.
      remove: ['origin'],
    },
    {
      id: 'crawler',
      prefixes: ['track', 'album', 'playlist', 'artist'].map((kind) => `https://open.spotify.com/${kind}/`).concat('https://music.amazon.com/'),
      set: { 'user-agent': CRAWLER_USER_AGENT },
      remove: [],
    },
    {
      id: 'youtube',
      prefixes: ['https://www.youtube.com/watch', 'https://www.youtube.com/shorts/', 'https://www.youtube.com/playlist', 'https://www.youtube.com/youtubei/v1/'],
      set: { cookie: YOUTUBE_CONSENT_COOKIE },
      remove: [],
    },
    {
      id: 'ytmusic',
      prefixes: ['https://music.youtube.com/youtubei/v1/search'],
      set: { origin: 'https://music.youtube.com', referer: 'https://music.youtube.com/' },
      remove: [],
    },
    {
      id: 'neutral',
      prefixes: ['https://genius.com/api/'],
      set: { 'user-agent': neutralUserAgent(version) },
      remove: [],
    },
  ];
}
