/**
 * Hosts the engine talks to, by purpose (extension plan §2.6). They become the required
 * `host_permissions`; everything else is requested per site through
 * `optional_host_permissions`.
 *
 * Kept by hand for now. The network task (X5, with E8) generates this table from the
 * core's Rust host lists so the two cannot drift.
 */
export const ENGINE_HOSTS = {
  recognition: ['amp.shazam.com'],
  catalog: [
    'api.deezer.com',
    'www.deezer.com',
    'cdnt-preview.dzcdn.net',
    'cdn-images.dzcdn.net',
    'itunes.apple.com',
    'music.apple.com',
    'audio-ssl.itunes.apple.com',
    '*.mzstatic.com',
  ],
  resolvers: [
    'open.spotify.com',
    'spotify.link',
    'spoti.fi',
    'spotify.app.link',
    'tidal.com',
    'listen.tidal.com',
    'embed.tidal.com',
    'music.amazon.com',
    'www.youtube.com',
    'm.youtube.com',
    'music.youtube.com',
    'youtu.be',
    'soundcloud.com',
    'on.soundcloud.com',
    'www.mixcloud.com',
    'api.mixcloud.com',
    '*.bandcamp.com',
    'vimeo.com',
    'www.tiktok.com',
    'vm.tiktok.com',
    'vt.tiktok.com',
    'link.deezer.com',
    'deezer.page.link',
    'dzr.page.link',
    'lis.tn',
    'apple.co',
  ],
  lyrics: ['lrclib.net', 'genius.com'],
  melodyIndex: ['github.com', 'release-assets.githubusercontent.com', 'objects.githubusercontent.com'],
} as const satisfies Record<string, readonly string[]>;

/** Match patterns for `host_permissions`, deduplicated, in table order. */
export function requiredHostPermissions(): string[] {
  const hosts = Object.values(ENGINE_HOSTS).flat();
  return [...new Set(hosts)].map((host) => `https://${host}/*`);
}

/** Requested at the moment of use, inside a user gesture. */
export const OPTIONAL_HOST_PERMISSIONS = ['https://*/*', 'http://*/*'];
