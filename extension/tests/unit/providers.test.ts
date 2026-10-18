import { describe, expect, it } from 'vitest';
import { emptyTrack, type Service } from '@/src/engine/types';
import {
  customProviderLink,
  isValidCustomProvider,
  isValidTemplate,
  otherLinks,
  primaryLink,
  providerFromKey,
  providerKey,
  providerLinks,
  SPOTIFY,
} from '@/src/providers/links';
import { searchLinks } from '@/src/providers/search';
import { ALL_PROVIDERS, COMMON_PROVIDERS, MORE_PROVIDERS, serviceFromKey, serviceKey, suggestProvider } from '@/src/providers/services';

const daftPunk = emptyTrack({ title: 'Harder, Better, Faster, Stronger', artist: 'Daft Punk', artists: ['Daft Punk'] });

describe('search links (catalog::search_links)', () => {
  // The core's own test vectors (catalog/mod.rs, search_links_cover_every_service).
  it('match the core byte for byte', () => {
    const url = (s: Service) => searchLinks(daftPunk).find((l) => l.service === s)!.url;
    expect(searchLinks(daftPunk)).toHaveLength(8);
    expect(searchLinks(daftPunk).every((l) => l.is_search)).toBe(true);
    expect(url('Spotify')).toBe('https://open.spotify.com/search/Daft%20Punk%20Harder,%20Better,%20Faster,%20Stronger');
    expect(url('YouTubeMusic')).toBe('https://music.youtube.com/search?q=Daft+Punk+Harder%2C+Better%2C+Faster%2C+Stronger');
    expect(url('YouTube')).toBe('https://www.youtube.com/results?search_query=Daft+Punk+Harder%2C+Better%2C+Faster%2C+Stronger');
    expect(url('AppleMusic')).toBe('https://music.apple.com/search?term=Daft+Punk+Harder%2C+Better%2C+Faster%2C+Stronger');
    expect(url('Deezer').startsWith('https://www.deezer.com/search/Daft%20Punk')).toBe(true);
    expect(url('AmazonMusic').startsWith('https://music.amazon.com/search/Daft%20Punk')).toBe(true);
    expect(url('Tidal').startsWith('https://tidal.com/search?q=Daft+Punk')).toBe(true);
    expect(url('SoundCloud').startsWith('https://soundcloud.com/search?q=Daft+Punk')).toBe(true);
  });

  it('need a title and survive odd characters', () => {
    expect(searchLinks(emptyTrack({ title: '', artist: 'Daft Punk' }))).toEqual([]);
    for (const link of searchLinks(emptyTrack({ title: 'Song/With?Odd#Chars', artist: '' }))) {
      expect(() => new URL(link.url)).not.toThrow();
    }
    expect(searchLinks(emptyTrack({ title: 'Song/With?Odd#Chars', artist: '' }))[0]!.url).toBe(
      'https://open.spotify.com/search/Song%2FWith%3FOdd%23Chars',
    );
  });
});

describe('the custom provider (custom_provider_link)', () => {
  const aha = emptyTrack({ title: 'Take On Me', artist: 'a-ha' });

  // The core's test vectors (catalog/mod.rs, fills_placeholders_and_rejects_other_schemes).
  it('fills placeholders and rejects other schemes', () => {
    expect(customProviderLink(aha, 'https://example.com/search?q={query}')).toBe('https://example.com/search?q=a%2Dha%20Take%20On%20Me');
    expect(customProviderLink(aha, 'javascript:alert({title})')).toBeNull();
    expect(customProviderLink(aha, 'not a url {title}')).toBeNull();
  });

  it('validates what the settings and the panel accept', () => {
    expect(isValidTemplate('https://example.com/?q={query}')).toBe(true);
    expect(isValidTemplate('https://example.com/')).toBe(false);
    expect(isValidTemplate('ftp://example.com/{title}')).toBe(false);
    expect(isValidCustomProvider({ name: ' ', template: 'https://example.com/?q={query}' })).toBe(false);
    expect(isValidCustomProvider({ name: 'Plex', template: 'https://plex.example/search?q={query}' })).toBe(true);
  });
});

describe('Open in', () => {
  const track = emptyTrack({
    title: 'Blinding Lights',
    artist: 'The Weeknd',
    artists: ['The Weeknd'],
    links: [
      { service: 'Deezer', url: 'https://www.deezer.com/track/1', is_search: false },
      { service: 'Spotify', url: 'https://open.spotify.com/track/0VjIjW4GlUZAMYd2vXMi3b', is_search: false },
    ],
  });
  const custom = { name: 'Plex', template: 'https://plex.example/search?q={query}' };

  it('has one link per provider, exact links winning, custom last', () => {
    const links = providerLinks(track, custom);
    expect(links.map((l) => providerKey(l.provider))).toEqual([
      'spotify',
      'appleMusic',
      'youTubeMusic',
      'youTube',
      'deezer',
      'tidal',
      'amazonMusic',
      'soundCloud',
      'custom',
    ]);
    expect(links[0]).toMatchObject({ url: 'https://open.spotify.com/track/0VjIjW4GlUZAMYd2vXMi3b', isSearch: false });
  });

  it('opens the preferred provider and lists the rest, exact first', () => {
    const links = providerLinks(track, custom);
    const primary = primaryLink(links, { kind: 'service', service: 'AppleMusic' });
    expect(primary?.provider).toEqual({ kind: 'service', service: 'AppleMusic' });
    const others = otherLinks(links, primary);
    expect(others.map((l) => providerKey(l.provider)).slice(0, 2)).toEqual(['spotify', 'deezer']);
    expect(providerKey(others.at(-1)!.provider)).toBe('custom');
    expect(others).not.toContain(primary);
  });

  it('opens the custom provider when it is preferred', () => {
    const links = providerLinks(track, custom);
    expect(primaryLink(links, { kind: 'custom' })?.url).toBe('https://plex.example/search?q=The%20Weeknd%20Blinding%20Lights');
  });

  it('falls back to an exact link when the preferred provider has none', () => {
    const links = providerLinks(track, null);
    expect(primaryLink(links, { kind: 'custom' })?.provider).toEqual(SPOTIFY);
  });
});

describe('services', () => {
  it('use the apps’ storage keys', () => {
    expect(serviceKey('YouTubeMusic')).toBe('youTubeMusic');
    expect(serviceKey('NetEaseMusic')).toBe('netEaseMusic');
    expect(serviceKey('LastFm')).toBe('lastFm');
    for (const service of ALL_PROVIDERS) expect(serviceFromKey(serviceKey(service))).toBe(service);
    expect(providerFromKey('custom')).toEqual({ kind: 'custom' });
    expect(providerFromKey('nope')).toBeNull();
  });

  it('split the panel into common and more, like docs/APPS.md', () => {
    expect(COMMON_PROVIDERS).toEqual(['Spotify', 'YouTubeMusic', 'AppleMusic', 'Tidal', 'AmazonMusic', 'Deezer', 'SoundCloud']);
    expect(ALL_PROVIDERS).toHaveLength(21);
    expect(MORE_PROVIDERS).toHaveLength(14);
  });

  it('suggest the streaming site in the current tab only', () => {
    expect(suggestProvider('https://open.spotify.com/album/x')).toBe('Spotify');
    expect(suggestProvider('https://listen.tidal.com/')).toBe('Tidal');
    expect(suggestProvider('https://music.amazon.de/')).toBe('AmazonMusic');
    expect(suggestProvider('https://www.youtube.com/watch?v=x')).toBeNull();
    expect(suggestProvider('chrome://newtab')).toBeNull();
    expect(suggestProvider(undefined)).toBeNull();
  });
});
