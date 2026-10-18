import { describe, expect, it } from 'vitest';
import { checkEventContract } from '@/src/engine/events';
import type { LibraryEntry, PitchFrame } from '@/src/engine/types';
import { t } from '@/src/i18n';
import { librarySections, matchesFilter } from '@/src/models/library';
import { probeCapabilities } from '@/src/platform/capabilities';
import { isAnalyzablePage } from '@/src/platform/surfaces';
import { median, traceGeometry } from '@/src/ui/draw/trace';
import { livePath } from '@/src/ui/draw/wave';
import { parseRoute, routeHash, type Route } from '@/src/ui/router.svelte';

describe('capabilities', () => {
  it('know each browser’s surfaces', () => {
    const chromium = probeCapabilities({ sidePanel: {}, offscreen: {}, tabCapture: {}, contextMenus: {}, commands: {} }, 'Mozilla/5.0 (X11; Linux x86_64) Chrome/155', 'chromium');
    expect(chromium).toMatchObject({ panel: 'sidePanel', engineHost: 'offscreen', tabCapture: true, menus: true, shortcuts: true, android: false });
    const firefox = probeCapabilities({ sidebarAction: {}, menus: {}, commands: {} }, 'Mozilla/5.0 (X11; Linux x86_64; rv:158.0) Firefox/158.0', 'firefox');
    expect(firefox).toMatchObject({ panel: 'sidebar', engineHost: 'background', tabCapture: false, menus: true });
    const android = probeCapabilities({ sidebarAction: {}, menus: {}, commands: {} }, 'Mozilla/5.0 (Android 15; Mobile; rv:158.0) Firefox/158.0', 'firefox');
    expect(android).toMatchObject({ panel: null, menus: false, shortcuts: false, android: true });
  });

  it('never analyze browser and store pages', () => {
    expect(isAnalyzablePage('https://www.youtube.com/watch?v=x')).toBe(true);
    expect(isAnalyzablePage('chrome://extensions')).toBe(false);
    expect(isAnalyzablePage('about:addons')).toBe(false);
    expect(isAnalyzablePage('https://chromewebstore.google.com/detail/x')).toBe(false);
    expect(isAnalyzablePage('https://addons.mozilla.org/firefox/')).toBe(false);
    expect(isAnalyzablePage(undefined)).toBe(false);
  });
});

describe('the trace band and live line', () => {
  const frames: PitchFrame[] = [
    { t_ms: 0, midi: 60, confidence: 0.9 },
    { t_ms: 100, midi: 62, confidence: 0.9 },
    { t_ms: 200, midi: null, confidence: 0.1 },
    { t_ms: 300, midi: 64, confidence: 0.9 },
    { t_ms: 400, midi: 90, confidence: 0.9 },
  ];

  it('break at unvoiced frames, centre on the median and clamp to ±12', () => {
    expect(median([3, 1, 2])).toBe(2);
    const geometry = traceGeometry(frames, { width: 400, height: 96, windowMs: 400 });
    expect(geometry.segments).toHaveLength(2);
    expect(geometry.now).toEqual({ x: 400, y: 0 });
  });

  it('draws a flat line in silence and stays in its box', () => {
    expect(livePath(0)).toMatch(/^M0\.0 20\.0( L[\d.]+ 20\.0)+$/);
    const ys = [...livePath(1, 1).matchAll(/[ML][\d.]+ ([\d.-]+)/g)].map((m) => Number(m[1]));
    expect(Math.min(...ys)).toBeGreaterThanOrEqual(0);
    expect(Math.max(...ys)).toBeLessThanOrEqual(40);
  });
});

describe('app routes', () => {
  it('round-trip through the hash', () => {
    const routes: Route[] = [
      { name: 'listen' },
      { name: 'library', pinned: false },
      { name: 'library', pinned: true },
      { name: 'session', id: 'a b/c' },
      { name: 'settings', pane: 'melody' },
      { name: 'welcome' },
      { name: 'grant-mic', state: null },
      { name: 'open', url: 'https://youtu.be/x?t=1&list=y' },
    ];
    for (const route of routes) expect(parseRoute(routeHash(route))).toEqual(route);
    expect(parseRoute('')).toEqual({ name: 'listen' });
    expect(parseRoute('#/settings/nope')).toEqual({ name: 'settings', pane: 'general' });
  });
});

describe('library sections', () => {
  const now = new Date(2026, 9, 17, 14, 0).getTime();
  const entry = (id: string, at: number, extra: Partial<LibraryEntry> = {}): LibraryEntry => ({
    id,
    kind: 'Listen',
    status: 'Matched',
    created_at_ms: at,
    title: id,
    subtitle: null,
    artwork_url: null,
    track: null,
    track_count: 1,
    duration_ms: null,
    source_url: null,
    pinned: false,
    ...extra,
  });

  it('put Pinned first, then days newest first', () => {
    const sections = librarySections(
      [
        entry('old', now - 3 * 86_400_000),
        entry('today', now - 3_600_000),
        entry('pinned', now - 2 * 86_400_000, { pinned: true }),
        entry('yesterday', now - 86_400_000),
      ],
      now,
    );
    expect(sections.map((s) => s.entries.map((e) => e.id))).toEqual([['pinned'], ['today'], ['yesterday'], ['old']]);
    expect(sections[1]!.label).toEqual({ key: 'library.today' });
    expect(sections[2]!.label).toEqual({ key: 'library.yesterday' });
    expect(typeof sections[3]!.label).toBe('string');
  });

  it('filter mixes from single links', () => {
    const mix = entry('mix', now, { kind: 'Link', track_count: 12 });
    const link = entry('link', now, { kind: 'Link', track_count: 1 });
    expect(matchesFilter(mix, 'mixes')).toBe(true);
    expect(matchesFilter(link, 'mixes')).toBe(false);
    expect(matchesFilter(link, 'links')).toBe(true);
    expect(matchesFilter(entry('hum', now, { kind: 'Hum' }), 'hummed')).toBe(true);
  });
});

describe('copy', () => {
  it('fills placeholders in order or by position', () => {
    expect(t('result.openIn', 'Spotify')).toBe('Open in Spotify');
    expect(t('state.identified', 12, 14)).toBe('12 of 14 identified');
    expect(t('state.rateLimited', 30)).toBe('Too many requests. Trying again in 30 s.');
  });
});

describe('the event contract check', () => {
  it('finds broken streams', () => {
    expect(checkEventContract([])).toEqual(['no events']);
    expect(checkEventContract([{ Phase: { phase: 'Listening', fraction: null } }])).toContain('the first event is not Started');
    expect(
      checkEventContract([
        { Started: { session_id: 'a', kind: 'Hum' } },
        { Finished: { outcome: { Candidates: { count: 1 } } } },
      ]),
    ).toContain('Finished(Candidates) is not preceded by a final Candidates event');
    expect(
      checkEventContract([{ Started: { session_id: 'a', kind: 'Link' } }, 'Canceled', { Finished: { outcome: 'Waiting' } }]),
    ).toContain('expected exactly one terminal event, got 2');
  });
});
