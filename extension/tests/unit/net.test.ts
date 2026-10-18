import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { checkUrl, engineFetch, FetchPolicyError, isPublicHost } from '@/src/net/fetch';
import { ENGINE_HOSTS, requiredHostPermissions } from '@/src/net/hosts';
import { isEngineHost, originPattern } from '@/src/net/permissions';
import { headerPolicy, SHAZAM_USER_AGENTS, YOUTUBE_CONSENT_COOKIE } from '@/src/net/policy';
import { compileRules } from '@/src/net/rules';

describe('header rules', () => {
  const rules = compileRules(headerPolicy('1.1.0'), 'abcdefghijklmnop');

  it('touch only the extension’s own requests (no tab, own initiator)', () => {
    expect(rules.length).toBeGreaterThan(8);
    for (const rule of rules) {
      expect(rule.condition.tabIds).toEqual([-1]);
      expect(rule.condition.initiatorDomains).toEqual(['abcdefghijklmnop']);
      expect(rule.condition.urlFilter.startsWith('|https://')).toBe(true);
      expect(rule.action.type).toBe('modifyHeaders');
    }
    expect(new Set(rules.map((r) => r.id)).size).toBe(rules.length);
  });

  it('set what each service needs (S4 table)', () => {
    const at = (prefix: string) => rules.find((r) => r.condition.urlFilter === `|${prefix}`)!.action.requestHeaders;
    expect(at('https://amp.shazam.com/discovery/')).toEqual([
      { header: 'user-agent', operation: 'set', value: SHAZAM_USER_AGENTS[0] },
      { header: 'origin', operation: 'remove' },
    ]);
    expect(at('https://open.spotify.com/track/')[0]?.value).toMatch(/^facebookexternalhit\/1\.1/);
    expect(at('https://www.youtube.com/watch')).toEqual([{ header: 'cookie', operation: 'set', value: YOUTUBE_CONSENT_COOKIE }]);
    expect(at('https://music.youtube.com/youtubei/v1/search')).toEqual([
      { header: 'origin', operation: 'set', value: 'https://music.youtube.com' },
      { header: 'referer', operation: 'set', value: 'https://music.youtube.com/' },
    ]);
    expect(at('https://genius.com/api/')[0]?.value).toBe('MusicFetch/1.1.0 (+https://github.com/x1f4r/music-fetch)');
    // Spotify's embed pages keep the browser's agent (the crawler rules are anchored).
    expect(rules.some((r) => 'https://open.spotify.com/embed/track/x'.startsWith(r.condition.urlFilter.slice(1)))).toBe(false);
  });

  it('rotate the Shazam agent when asked', () => {
    const third = compileRules(headerPolicy('1.1.0', 2), 'x').find((r) => r.condition.urlFilter.includes('shazam'))!;
    expect(third.action.requestHeaders[0]?.value).toBe(SHAZAM_USER_AGENTS[2]);
  });

  it('cover every rule’s host with a required host permission', () => {
    const granted = requiredHostPermissions();
    for (const rule of headerPolicy('1')) for (const prefix of rule.prefixes) expect(granted).toContain(`https://${new URL(prefix).hostname}/*`);
  });
});

describe('the engine’s fetch policy', () => {
  it('knows which hosts are public', () => {
    for (const host of ['amp.shazam.com', '8.8.8.8', '[2606:4700::1111]']) expect(isPublicHost(host)).toBe(true);
    for (const host of ['localhost', '127.0.0.1', '10.0.0.8', '192.168.178.1', '172.20.1.1', '169.254.1.1', '100.100.1.1', '[::1]', '[fd00::1]', 'printer.local', 'metadata.google.internal', 'nas.lan']) {
      expect(isPublicHost(host)).toBe(false);
    }
  });

  it('only fetches https on public hosts without credentials in the URL', () => {
    expect(checkUrl('https://lrclib.net/api/get').hostname).toBe('lrclib.net');
    expect(() => checkUrl('http://lrclib.net/')).toThrow(FetchPolicyError);
    expect(() => checkUrl('https://user:secret@example.org/')).toThrow(FetchPolicyError);
    expect(() => checkUrl('https://192.168.1.1/')).toThrow(FetchPolicyError);
    expect(checkUrl('http://127.0.0.1:8000/echo', { allowHttp: true, allowPrivate: true }).port).toBe('8000');
  });

  it('never sends cookies, follows redirects and checks where they ended', async () => {
    const calls: RequestInit[] = [];
    const fake = (finalUrl: string) =>
      (async (_url: RequestInfo | URL, init?: RequestInit) => {
        calls.push(init ?? {});
        return Object.defineProperty(new Response('ok'), 'url', { value: finalUrl });
      }) as typeof fetch;
    await engineFetch('https://spotify.link/abc', { method: 'GET' }, { fetchImpl: fake('https://open.spotify.com/track/1') });
    expect(calls[0]).toMatchObject({ credentials: 'omit', redirect: 'follow', method: 'GET' });
    await expect(engineFetch('https://t.co/x', {}, { fetchImpl: fake('http://192.168.0.1/admin') })).rejects.toBeInstanceOf(FetchPolicyError);
  });
});

describe('host permissions', () => {
  it('ask per site for anything that isn’t an engine host', () => {
    expect(originPattern('https://www.reddit.com/r/music/comments/1')).toBe('https://www.reddit.com/*');
    expect(originPattern('chrome://settings')).toBeNull();
    expect(isEngineHost('https://open.spotify.com/track/1')).toBe(true);
    expect(isEngineHost('https://artist.bandcamp.com/track/x')).toBe(true);
    expect(isEngineHost('https://www.reddit.com/')).toBe(false);
  });

  /**
   * Every host the core's Rust sources name is either a required host or explained here,
   * so a new resolver can't ship without its permission (extension plan §2.6).
   */
  it('match every host the core names', () => {
    const NOT_REQUIRED: Record<string, string> = {
      // Loaded by the UI as <img>/<audio> (no permission needed) or only built as links.
      'is1-ssl.mzstatic.com': 'artwork',
      'is2-ssl.mzstatic.com': 'artwork',
      'i.ytimg.com': 'artwork',
      'yt3.googleusercontent.com': 'artwork',
      'images.genius.com': 'artwork',
      'i1.sndcdn.com': 'artwork',
      'p.scdn.co': 'preview audio, played by the UI',
      'thumbnailer.mixcloud.com': 'artwork',
      'm.media-amazon.com': 'artwork',
      'f4.bcbits.com': 'artwork',
      'www.youtube-nocookie.com': 'embed links',
      'widget.deezer.com': 'embed links',
      'w.soundcloud.com': 'embed links',
      'embed.music.apple.com': 'embed links',
      'player.vimeo.com': 'embed links',
      'www.shazam.com': 'track links',
      'youtube.com': 'canonicalized to www',
      'deezer.com': 'canonicalized to www',
      'm.soundcloud.com': 'canonicalized',
      'm.mixcloud.com': 'canonicalized',
      'm.twitch.tv': 'canonicalized',
      'api.soundcloud.com': 'embed parameter only',
      // Generic pages: optional permission, asked at the moment of use.
      'www.reddit.com': 'optional',
      'x.com': 'optional',
      'twitter.com': 'optional',
      'www.instagram.com': 'optional',
      'l.instagram.com': 'optional',
      'www.facebook.com': 'optional',
      'www.twitch.tv': 'optional',
      'www.dailymotion.com': 'optional',
      'archive.org': 'optional',
      'www.jamendo.com': 'optional',
      'www.pornhub.com': 'optional (an unsupported-site test)',
      // Paid providers with the user's own keys (later, X11).
      'api.audd.io': 'keyed provider',
      'identify-eu-west-1.acrcloud.com': 'keyed provider',
      // Tests and docs.
      'schema.org': 'JSON-LD vocabulary',
    };
    const TEST_HOST = /(^|\.)(example|example\.com|test|local|internal)$/;
    const root = join(process.cwd(), '..', 'core', 'crates', 'mfcore', 'src');
    let exists = true;
    try {
      statSync(root);
    } catch {
      exists = false; // the AMO sources zip has no core/
    }
    if (!exists) return;
    const files: string[] = [];
    const walk = (dir: string) => {
      for (const name of readdirSync(dir)) {
        const path = join(dir, name);
        if (statSync(path).isDirectory()) walk(path);
        else if (name.endsWith('.rs')) files.push(path);
      }
    };
    walk(root);
    const hosts = new Set<string>();
    for (const file of files) for (const m of readFileSync(file, 'utf8').matchAll(/https?:\/\/([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/g)) hosts.add(m[1]!.toLowerCase());
    const required = Object.values(ENGINE_HOSTS).flat();
    const covered = (host: string) => required.some((h) => (h.startsWith('*.') ? host.endsWith(h.slice(1)) : h === host));
    const unexplained = [...hosts].filter((h) => !covered(h) && !(h in NOT_REQUIRED) && !TEST_HOST.test(h));
    expect(unexplained).toEqual([]);
  });
});
