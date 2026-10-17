import type { Service, Track } from '../engine/types';
import { searchLinks } from './search';
import { ALL_PROVIDERS, serviceFromKey, serviceKey } from './services';

/** Where "Open in …" sends a track: a service, or the user's custom provider. */
export type Provider = { kind: 'service'; service: Service } | { kind: 'custom' };

export const SPOTIFY: Provider = { kind: 'service', service: 'Spotify' };

export function providerKey(provider: Provider): string {
  return provider.kind === 'custom' ? 'custom' : serviceKey(provider.service);
}

export function providerFromKey(key: string | null | undefined): Provider | null {
  if (!key) return null;
  if (key === 'custom') return { kind: 'custom' };
  const service = serviceFromKey(key);
  return service ? { kind: 'service', service } : null;
}

export function sameProvider(a: Provider, b: Provider): boolean {
  return providerKey(a) === providerKey(b);
}

/** A name and a URL template with `{artist}`, `{title}`, `{query}` and `{isrc}`. */
export interface CustomProvider {
  name: string;
  template: string;
}

export const PLACEHOLDERS = ['{artist}', '{title}', '{query}', '{isrc}'] as const;

/** Percent-encode everything but ASCII letters and digits (Rust `NON_ALPHANUMERIC`). */
function encodeNonAlphanumeric(value: string): string {
  let out = '';
  for (const byte of new TextEncoder().encode(value.trim())) {
    const c = String.fromCharCode(byte);
    out += /[A-Za-z0-9]/.test(c) ? c : `%${byte.toString(16).toUpperCase().padStart(2, '0')}`;
  }
  return out;
}

/**
 * Fill a custom provider template, like the core's `custom_provider_link`: placeholders
 * are replaced with URL-encoded values; null unless the result is an http(s) URL.
 * Runs in the UI, so validation is instant while the user types.
 */
export function customProviderLink(track: Pick<Track, 'title' | 'artist' | 'artists' | 'isrc'>, template: string): string | null {
  const artist = track.artists[0] ?? track.artist;
  const query = `${artist.trim()} ${track.title.trim()}`.trim();
  const url = template
    .trim()
    .replaceAll('{artist}', encodeNonAlphanumeric(artist))
    .replaceAll('{title}', encodeNonAlphanumeric(track.title))
    .replaceAll('{query}', encodeNonAlphanumeric(query))
    .replaceAll('{isrc}', encodeNonAlphanumeric(track.isrc ?? ''));
  try {
    const parsed = new URL(url);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:' ? url : null;
  } catch {
    return null;
  }
}

const SAMPLE_TRACK = { title: 'Title', artist: 'Artist', artists: ['Artist'], isrc: 'USAAA0000001' };

/** Usable: at least one placeholder, and an http(s) URL once filled in. */
export function isValidTemplate(template: string): boolean {
  const trimmed = template.trim();
  if (!PLACEHOLDERS.some((p) => trimmed.includes(p))) return false;
  return customProviderLink(SAMPLE_TRACK, trimmed) !== null;
}

export function isValidCustomProvider(custom: CustomProvider | null | undefined): custom is CustomProvider {
  return Boolean(custom && custom.name.trim() && isValidTemplate(custom.template));
}

/** One provider's link for a track. */
export interface ProviderLink {
  provider: Provider;
  url: string;
  /** A search for "artist title" rather than the track's own page. */
  isSearch: boolean;
}

/**
 * One link per provider in display order (the track's exact link when known, else its
 * search link), then the custom provider when it is set up. Like the apps, it merges
 * the track's own links with `provider_links` (here its port, `searchLinks`).
 */
export function providerLinks(track: Track, custom: CustomProvider | null): ProviderLink[] {
  const all = [...track.links, ...searchLinks(track)];
  const result: ProviderLink[] = [];
  for (const service of ALL_PROVIDERS) {
    const candidates = all.filter((l) => l.service === service);
    const link = candidates.find((l) => !l.is_search) ?? candidates[0];
    if (link) result.push({ provider: { kind: 'service', service }, url: link.url, isSearch: link.is_search });
  }
  if (isValidCustomProvider(custom)) {
    const url = customProviderLink(track, custom.template);
    if (url) result.push({ provider: { kind: 'custom' }, url, isSearch: false });
  }
  return result;
}

/** What the big button opens: the preferred provider, else the first exact link, else any. */
export function primaryLink(links: ProviderLink[], preferred: Provider): ProviderLink | null {
  return (
    links.find((l) => sameProvider(l.provider, preferred)) ??
    links.find((l) => !l.isSearch && l.provider.kind !== 'custom') ??
    links[0] ??
    null
  );
}

/** The chevron menu: every other provider, exact links first, then searches, then custom. */
export function otherLinks(links: ProviderLink[], primary: ProviderLink | null): ProviderLink[] {
  const rest = links.filter((l) => l !== primary);
  const services = rest.filter((l) => l.provider.kind !== 'custom');
  return [
    ...services.filter((l) => !l.isSearch),
    ...services.filter((l) => l.isSearch),
    ...rest.filter((l) => l.provider.kind === 'custom'),
  ];
}
