import type { Service } from '../engine/types';

/** Every provider "Open in" offers, in display order (docs/APPS.md "Opening links"). */
export const ALL_PROVIDERS: readonly Service[] = [
  'Spotify',
  'AppleMusic',
  'YouTubeMusic',
  'YouTube',
  'Deezer',
  'Tidal',
  'AmazonMusic',
  'SoundCloud',
  'Qobuz',
  'Pandora',
  'Audiomack',
  'Anghami',
  'JioSaavn',
  'Boomplay',
  'Kkbox',
  'YandexMusic',
  'NetEaseMusic',
  'Bandcamp',
  'Shazam',
  'LastFm',
  'Genius',
];

/** The first-launch panel's tiles; the rest are under "More…". */
export const COMMON_PROVIDERS: readonly Service[] = [
  'Spotify',
  'YouTubeMusic',
  'AppleMusic',
  'Tidal',
  'AmazonMusic',
  'Deezer',
  'SoundCloud',
];

export const MORE_PROVIDERS: readonly Service[] = ALL_PROVIDERS.filter((s) => !COMMON_PROVIDERS.includes(s));

const NAMES: Record<Service, string> = {
  Spotify: 'Spotify',
  AppleMusic: 'Apple Music',
  YouTubeMusic: 'YouTube Music',
  YouTube: 'YouTube',
  Deezer: 'Deezer',
  Tidal: 'Tidal',
  AmazonMusic: 'Amazon Music',
  SoundCloud: 'SoundCloud',
  Bandcamp: 'Bandcamp',
  Mixcloud: 'Mixcloud',
  Vimeo: 'Vimeo',
  TikTok: 'TikTok',
  Instagram: 'Instagram',
  Facebook: 'Facebook',
  X: 'X',
  Reddit: 'Reddit',
  Twitch: 'Twitch',
  Shazam: 'Shazam',
  Genius: 'Genius',
  Qobuz: 'Qobuz',
  Pandora: 'Pandora',
  Audiomack: 'Audiomack',
  Anghami: 'Anghami',
  JioSaavn: 'JioSaavn',
  Boomplay: 'Boomplay',
  Kkbox: 'KKBOX',
  YandexMusic: 'Yandex Music',
  NetEaseMusic: 'NetEase Cloud Music',
  LastFm: 'Last.fm',
  Web: 'Web',
};

export function serviceName(service: Service): string {
  return NAMES[service];
}

/**
 * Stable names for storage, shared with the apps' preferences ("spotify",
 * "appleMusic", …). Never shown.
 */
export function serviceKey(service: Service): string {
  return service.charAt(0).toLowerCase() + service.slice(1);
}

const BY_KEY = new Map((Object.keys(NAMES) as Service[]).map((s) => [serviceKey(s), s]));

export function serviceFromKey(key: string): Service | null {
  return BY_KEY.get(key) ?? null;
}

/** Streaming sites whose tab suggests the provider in the first-launch panel. */
const SITE_PROVIDERS: [RegExp, Service][] = [
  [/(^|\.)open\.spotify\.com$/, 'Spotify'],
  [/(^|\.)music\.youtube\.com$/, 'YouTubeMusic'],
  [/(^|\.)music\.apple\.com$/, 'AppleMusic'],
  [/(^|\.)(listen\.)?tidal\.com$/, 'Tidal'],
  [/(^|\.)music\.amazon\.[a-z.]+$/, 'AmazonMusic'],
  [/(^|\.)deezer\.com$/, 'Deezer'],
  [/(^|\.)soundcloud\.com$/, 'SoundCloud'],
  [/(^|\.)play\.qobuz\.com$/, 'Qobuz'],
  [/(^|\.)pandora\.com$/, 'Pandora'],
  [/(^|\.)audiomack\.com$/, 'Audiomack'],
  [/(^|\.)bandcamp\.com$/, 'Bandcamp'],
];

/**
 * Browsers can't detect installed music apps, so the panel pre-selects only the
 * streaming service the current tab is on (extension plan §4, provider choice).
 */
export function suggestProvider(tabUrl: string | undefined | null): Service | null {
  if (!tabUrl) return null;
  let host: string;
  try {
    host = new URL(tabUrl).hostname;
  } catch {
    return null;
  }
  return SITE_PROVIDERS.find(([re]) => re.test(host))?.[1] ?? null;
}
