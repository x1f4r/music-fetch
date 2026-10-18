import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { JSDOM } from 'jsdom';
import { describe, expect, it } from 'vitest';
import { ADAPTERS, siteAdapter } from '@/src/page/adapters';
import { twitchIsLive } from '@/src/page/adapters/twitch';
import { youtubeChapters, youtubeEntries } from '@/src/page/adapters/youtube';
import { clockToMs, isoDurationMs } from '@/src/page/site';

/** A fixture page at a site's URL, with its inline scripts run (no network: media never loads). */
function page(file: string, url: string) {
  const html = readFileSync(join(__dirname, '..', 'fixtures', 'pages', file), 'utf8');
  const dom = new JSDOM(html, { url, runScripts: 'dangerously', pretendToBeVisual: true });
  return { win: dom.window as unknown as Window & typeof globalThis, doc: dom.window.document };
}

describe('site adapters', () => {
  it('pick the adapter by host, and only e2e builds honour the test meta', () => {
    expect(siteAdapter('www.youtube.com').id).toBe('youtube');
    expect(siteAdapter('m.youtube.com').id).toBe('youtube');
    expect(siteAdapter('soundcloud.com').id).toBe('soundcloud');
    expect(siteAdapter('www.twitch.tv').id).toBe('twitch');
    expect(siteAdapter('artist.bandcamp.com').id).toBe('bandcamp');
    expect(siteAdapter('open.spotify.com').id).toBe('streaming');
    const { doc } = page('youtube.html', 'http://127.0.0.1:8000/youtube.html');
    expect(siteAdapter('127.0.0.1', doc).id).toBe('generic');
  });

  it('read a YouTube watch page: details, the description tracklist, chapters and the player', () => {
    const { win, doc } = page('youtube.html', 'https://www.youtube.com/watch?v=fakemix0001');
    const facts = ADAPTERS.youtube.facts(win, doc);
    expect(facts).toMatchObject({ title: 'Fake Mix', artist: 'Test Channel', durationMs: 60_000, live: false });
    expect(facts.description).toContain('0:20 Music Fetch Test Signals - Bravo Signal');
    expect(facts.chapters).toEqual([
      { title: 'Alpha Signal', startMs: 0 },
      { title: 'Bravo Signal', startMs: 20_000 },
      { title: 'Charlie Signal', startMs: 40_000 },
    ]);
    expect(ADAPTERS.youtube.element(win, doc, [])?.className).toBe('html5-main-video');
    // Player API: seek, quality (restored afterwards), ads.
    expect(ADAPTERS.youtube.seek!(win, 12_500)).toBe(true);
    expect(ADAPTERS.youtube.lowQuality!(win, true, null)).toBe('hd720');
    expect((win as unknown as { fakeQuality(): string }).fakeQuality()).toBe('tiny');
    ADAPTERS.youtube.lowQuality!(win, false, 'hd720');
    expect((win as unknown as { fakeQuality(): string }).fakeQuality()).toBe('hd720');
    expect(ADAPTERS.youtube.ad!(win, doc)).toBe(false);
    doc.getElementById('movie_player')!.classList.add('ad-showing');
    expect(ADAPTERS.youtube.ad!(win, doc)).toBe(true);
  });

  it('ignore ytInitialData that belongs to the previous video after an in-page navigation', () => {
    const { win, doc } = page('youtube.html', 'https://www.youtube.com/watch?v=fakemix0001');
    (win as unknown as { fakeNavigate(id: string, title: string): void }).fakeNavigate('other000001', 'Another video');
    const facts = ADAPTERS.youtube.facts(win, doc);
    expect(facts.title).toBe('Another video');
    expect(facts.chapters).toEqual([]);
  });

  it('read YouTube key moments and playlist entries', () => {
    const data = {
      a: { macroMarkersListItemRenderer: { title: { simpleText: 'Opening' }, timeDescription: { simpleText: '1:05' } } },
      b: [{ playlistVideoRenderer: { videoId: 'v1', title: { runs: [{ text: 'First' }] }, shortBylineText: { runs: [{ text: 'Chan' }] }, lengthSeconds: '61' } }],
    };
    expect(youtubeChapters(data)).toEqual([{ title: 'Opening', startMs: 65_000 }]);
    expect(youtubeEntries(data)).toEqual([{ title: 'First', artist: 'Chan', url: 'https://www.youtube.com/watch?v=v1', durationMs: 61_000 }]);
  });

  it('read SoundCloud tracks and sets from the hydration data, and the now-playing badge', () => {
    const track = page('soundcloud.html', 'https://soundcloud.com/test/b');
    expect(ADAPTERS.soundcloud.facts(track.win, track.doc)).toMatchObject({
      title: 'Bravo Signal',
      artist: 'Music Fetch Test Signals',
      durationMs: 32_000,
      nowPlaying: { title: 'Bravo Signal', artist: 'Music Fetch Test Signals', album: '' },
    });
    const set = page('soundcloud-set.html', 'https://soundcloud.com/test/sets/test-set');
    const facts = ADAPTERS.soundcloud.facts(set.win, set.doc);
    expect(facts.title).toBe('Test Set');
    // Entries without a title (not yet loaded) are left out.
    expect(facts.entries?.map((e) => e.title)).toEqual(['Alpha Signal', 'Bravo Signal']);
  });

  it('find SoundCloud’s detached player through the elements that played', () => {
    const { win, doc } = page('soundcloud.html', 'https://soundcloud.com/test/b');
    expect(doc.querySelector('audio')).toBeNull();
    const detached = new win.Audio('/media/song-b.wav');
    expect(ADAPTERS.soundcloud.element(win, doc, [detached])).toBe(detached);
  });

  it('read Mixcloud, Twitch, Bandcamp, streaming and generic pages', () => {
    const mix = page('mixcloud.html', 'https://www.mixcloud.com/test/test-mix/');
    expect(ADAPTERS.mixcloud.facts(mix.win, mix.doc)).toMatchObject({ title: 'Test Mix', artist: 'Music Fetch Test Signals', durationMs: 60_000 });
    expect(ADAPTERS.mixcloud.facts(mix.win, mix.doc).entries?.map((e) => e.title)).toEqual(['Alpha Signal', 'Bravo Signal', 'Charlie Signal']);

    const vod = page('twitch.html', 'https://www.twitch.tv/videos/123456');
    expect(ADAPTERS.twitch.facts(vod.win, vod.doc)).toMatchObject({ title: 'Test stream VOD', live: false });
    expect(twitchIsLive('/somechannel')).toBe(true);
    expect(twitchIsLive('/videos/42')).toBe(false);

    const bc = page('bandcamp.html', 'https://test.bandcamp.com/track/test-signal-e');
    expect(ADAPTERS.bandcamp.facts(bc.win, bc.doc)).toMatchObject({
      title: 'Echo Signal',
      artist: 'Music Fetch Test Signals',
      durationMs: 32_000,
      mediaUrl: 'https://t4.bcbits.example/stream/test-signal-e.mp3',
    });

    const sp = page('streaming.html', 'https://open.spotify.com/');
    expect(ADAPTERS.streaming.facts(sp.win, sp.doc).nowPlaying).toEqual({ title: 'Foxtrot Signal', artist: 'Music Fetch Test Signals', album: '' });
    expect(ADAPTERS.streaming.element(sp.win, sp.doc, [])).toBeNull();

    const gen = page('generic.html', 'https://example.org/song');
    expect(ADAPTERS.generic.facts(gen.win, gen.doc)).toMatchObject({
      title: 'Alpha Signal',
      artist: 'Music Fetch Test Signals',
      durationMs: 32_000,
      mediaUrl: 'https://example.org/test-signal-a.mp3',
    });
  });

  it('parse clock times and ISO durations', () => {
    expect(clockToMs('1:02:03')).toBe(3_723_000);
    expect(clockToMs('0:20')).toBe(20_000);
    expect(clockToMs('x')).toBeNull();
    expect(isoDurationMs('PT1H2M3.5S')).toBe(3_723_500);
    expect(isoDurationMs('P1D')).toBe(86_400_000);
    expect(isoDurationMs('soon')).toBeNull();
  });
});
