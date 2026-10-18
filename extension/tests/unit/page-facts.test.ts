import { describe, expect, it } from 'vitest';
import { adapterFor, isPageFactsMessage, PAGE_FACTS_MESSAGE, readPageFacts } from '@/src/page/facts';

describe('page facts', () => {
  it('pick the adapter by site', () => {
    expect(adapterFor('www.youtube.com')).toBe('youtube');
    expect(adapterFor('music.youtube.com')).toBe('youtube');
    expect(adapterFor('artist.bandcamp.com')).toBe('bandcamp');
    expect(adapterFor('open.spotify.com')).toBe('streaming');
    expect(adapterFor('example.org')).toBe('generic');
  });

  it('read titles, Open Graph, media elements and now playing', () => {
    document.head.innerHTML = '<meta property="og:title" content="A song"><meta property="og:type" content="music.song">';
    document.title = 'A page';
    document.body.innerHTML = '<audio src="a.mp3"></audio><video></video>';
    const session = { metadata: { title: 'Song', artist: 'Artist', album: 'Album' } } as unknown as MediaSession;
    const facts = readPageFacts(document, session);
    expect(facts).toMatchObject({ adapter: 'generic', title: 'A page', ogTitle: 'A song', ogType: 'music.song' });
    expect(facts.nowPlaying).toEqual({ title: 'Song', artist: 'Artist', album: 'Album' });
    expect(facts.media.map((m) => m.tag)).toEqual(['audio', 'video']);
    expect(readPageFacts(document, undefined).nowPlaying).toBeNull();
  });

  it('only trust messages with the nonce', () => {
    const facts = readPageFacts(document, undefined);
    expect(isPageFactsMessage({ type: PAGE_FACTS_MESSAGE, nonce: 'n', facts }, 'n')).toBe(true);
    expect(isPageFactsMessage({ type: PAGE_FACTS_MESSAGE, nonce: 'x', facts }, 'n')).toBe(false);
    expect(isPageFactsMessage('hello', 'n')).toBe(false);
  });
});
