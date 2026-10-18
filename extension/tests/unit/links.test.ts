import { describe, expect, it } from 'vitest';
import { extractUrl, isLink } from '@/src/engine/links';

describe('pasted links (links::extract_url)', () => {
  it('finds explicit URLs anywhere', () => {
    expect(extractUrl('https://open.spotify.com/track/abc')).toBe('https://open.spotify.com/track/abc');
    expect(extractUrl('Check this out https://youtu.be/abc via @app')).toBe('https://youtu.be/abc');
    expect(extractUrl('(https://example.com/a).')).toBe('https://example.com/a');
  });

  it('accepts bare domains with a path, or a lone host', () => {
    expect(extractUrl('youtu.be/abc')).toBe('https://youtu.be/abc');
    expect(extractUrl('listen to open.spotify.com/track/x now')).toBe('https://open.spotify.com/track/x');
    expect(extractUrl('soundcloud.com')).toBe('https://soundcloud.com/');
  });

  it('leaves lyrics alone', () => {
    expect(isLink('never gonna give you up')).toBe(false);
    expect(isLink('is this the real life. is this just fantasy')).toBe(false);
    expect(isLink('')).toBe(false);
    expect(isLink('javascript:alert(1)')).toBe(false);
  });
});
