import { searchLinks } from '../../providers/search';
import { emptyTrack, type Service, type ServiceLink, type Track } from '../types';

/**
 * Demo tracks for the mock engine. Metadata only (titles, artists, albums, years); the
 * artwork is a generated placeholder so mock sessions never load anything remotely.
 * Exact links are given only where the ids are the well-known public ones; everything
 * else gets search links, which also exercises the "(search)" entries in menus.
 */

function artwork(hue: number): string {
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 60 60">` +
    `<rect width="60" height="60" fill="hsl(${hue} 45% 42%)"/>` +
    `<path d="M8 40 Q 18 18 28 32 T 52 22" stroke="hsl(${hue} 70% 85%)" stroke-width="4" fill="none" stroke-linecap="round"/>` +
    `</svg>`;
  return `data:image/svg+xml,${encodeURIComponent(svg)}`;
}

interface DemoInput {
  title: string;
  artist: string;
  album: string;
  year: number;
  durationMs: number;
  genre: string;
  hue: number;
  isrc?: string;
  exact?: [Service, string][];
}

function demo(input: DemoInput): Track {
  const exact: ServiceLink[] = (input.exact ?? []).map(([service, url]) => ({ service, url, is_search: false }));
  const search = searchLinks({ title: input.title, artist: input.artist, artists: [input.artist] }).filter((l) => !exact.some((e) => e.service === l.service));
  return emptyTrack({
    title: input.title,
    artist: input.artist,
    artists: [input.artist],
    album: input.album,
    isrc: input.isrc ?? null,
    duration_ms: input.durationMs,
    release_year: input.year,
    genre: input.genre,
    artwork_url: artwork(input.hue),
    links: [...exact, ...search],
    ids: {},
    identity_key: `mock:${input.artist.toLowerCase()}|${input.title.toLowerCase()}`,
  });
}

export const DEMO_TRACKS: readonly Track[] = [
  demo({
    title: 'Never Gonna Give You Up',
    artist: 'Rick Astley',
    album: 'Whenever You Need Somebody',
    year: 1987,
    durationMs: 213_573,
    genre: 'Pop',
    hue: 200,
    isrc: 'GBARL9300135',
    exact: [
      ['Spotify', 'https://open.spotify.com/track/4PTG3Z6ehGkBFwjybzWkR8'],
      ['YouTube', 'https://www.youtube.com/watch?v=dQw4w9WgXcQ'],
    ],
  }),
  demo({
    title: 'Blinding Lights',
    artist: 'The Weeknd',
    album: 'After Hours',
    year: 2019,
    durationMs: 200_040,
    genre: 'R&B',
    hue: 350,
    exact: [['Spotify', 'https://open.spotify.com/track/0VjIjW4GlUZAMYd2vXMi3b']],
  }),
  demo({
    title: 'Shape of You',
    artist: 'Ed Sheeran',
    album: '÷ (Divide)',
    year: 2017,
    durationMs: 233_713,
    genre: 'Pop',
    hue: 190,
    exact: [['Spotify', 'https://open.spotify.com/track/7qiZfU4dY1lWllzX7mPBI3']],
  }),
  demo({ title: 'Billie Jean', artist: 'Michael Jackson', album: 'Thriller', year: 1982, durationMs: 294_000, genre: 'Pop', hue: 40 }),
  demo({ title: 'Shake It Off', artist: 'Taylor Swift', album: '1989', year: 2014, durationMs: 219_200, genre: 'Pop', hue: 300 }),
  demo({
    title: 'Viva La Vida',
    artist: 'Coldplay',
    album: 'Viva la Vida or Death and All His Friends',
    year: 2008,
    durationMs: 242_373,
    genre: 'Alternative',
    hue: 20,
  }),
  demo({ title: '99 Luftballons', artist: 'Nena', album: '99 Luftballons', year: 1983, durationMs: 232_000, genre: 'Pop', hue: 0 }),
  demo({ title: 'Wake Me Up', artist: 'Avicii', album: 'True', year: 2013, durationMs: 247_426, genre: 'Dance', hue: 30 }),
  demo({
    title: 'Megalovania',
    artist: 'Toby Fox',
    album: 'Undertale Soundtrack',
    year: 2015,
    durationMs: 156_000,
    genre: 'Soundtrack',
    hue: 260,
  }),
  demo({
    title: 'My Heart Will Go On',
    artist: 'Céline Dion',
    album: "Let's Talk About Love",
    year: 1997,
    durationMs: 280_000,
    genre: 'Pop',
    hue: 220,
  }),
];

/** The n-th demo track, cycling. */
export function demoTrack(n: number): Track {
  return structuredClone(DEMO_TRACKS[((n % DEMO_TRACKS.length) + DEMO_TRACKS.length) % DEMO_TRACKS.length]!);
}
