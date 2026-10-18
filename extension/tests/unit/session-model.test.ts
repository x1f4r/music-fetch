import { describe, expect, it } from 'vitest';
import { mixScript } from '@/src/engine/mock/scripts';
import { DEMO_TRACKS } from '@/src/engine/mock/catalog';
import type { SessionDetail, SessionEvent } from '@/src/engine/types';
import { tm } from '@/src/i18n';
import {
  clock,
  cueLink,
  evidenceSummary,
  identifiedCounts,
  matchedBy,
  mixRows,
  playlistRows,
  reduceSession,
  sessionShape,
  sessionView,
  statusRow,
  timeline,
  tracklistText,
} from '@/src/models/session';
import { mixSession, playlistSession } from '@/src/ui/demo/library';

function empty(id = 's1'): SessionDetail {
  return {
    entry: { id, kind: 'Link', status: 'Running', created_at_ms: 0, title: 'x', subtitle: null, artwork_url: null, track: null, track_count: 0, duration_ms: null, source_url: null, pinned: false },
    source: null,
    segments: [],
    candidates: [],
    transcript: null,
  };
}

describe('the session view', () => {
  it('folds a live mix: segments upserted and stitched, progress, the finish', () => {
    const script = mixScript([...DEMO_TRACKS], 'https://www.youtube.com/watch?v=x&mix', 'YouTube', 'YouTube', 'A set');
    let view = sessionView(empty());
    const events: SessionEvent[] = [{ Started: { session_id: 's1', kind: 'Link' } }, ...script.outcome.map((l) => l.event)];
    let sawProgress = false;
    for (const event of events) {
      view = reduceSession(view, event);
      if (view.progress) sawProgress = true;
    }
    expect(sawProgress).toBe(true);
    expect(view.running).toBe(false);
    expect(view.detail.entry.status).toBe('Completed');
    expect(view.detail.segments.some((s) => s.id === 'seg-5b')).toBe(false);
    expect(sessionShape(view)).toBe('mix');
    expect(identifiedCounts(view.detail)).toEqual({ identified: 9, total: 10 });
    expect(tm(statusRow(view)!.message)).toBe('9 of 10 identified');
  });

  it('draws the timeline with tiers, the veil and the frontier while running', () => {
    const view = { ...sessionView(mixSession({ running: true })), progress: { analyzed_ms: 23 * 60_000, total_ms: 5_412_000, eta_ms: null }, capture: { state: 'capturing' as const, rate: 4, muted: true } };
    const geometry = timeline(view)!;
    expect(geometry.frontier).toBeCloseTo(23 * 60_000 / 5_412_000, 5);
    expect(geometry.segments.map((s) => s.cls)).toContain('unknown');
    expect(geometry.segments.map((s) => s.cls)).toContain('speech');
    expect(geometry.ticks.map((t) => t.label)).toEqual(['0:00', '22:33', '45:06', '1:07:39', '1:30:12']);
    expect(tm(statusRow(view)!.message)).toBe('Listening through · 23 of 90 min · 4×');
    // Rows beyond the frontier come from the tracklist (provisional), at 70 % with "from the tracklist".
    const rows = mixRows(view);
    expect(rows.filter((r) => r.provisional).length).toBeGreaterThan(3);
    expect(rows.find((r) => r.unknown)?.lengthLabel).toBe('2:20');
  });

  it('says what a paused, ad-waiting or closed capture is doing', () => {
    const base = { ...sessionView(mixSession({ running: true })), progress: { analyzed_ms: 60_000, total_ms: 5_412_000, eta_ms: null } };
    expect(tm(statusRow({ ...base, capture: { state: 'ad', rate: 4, muted: true } })!.message)).toBe('Waiting for the ad to end');
    expect(statusRow({ ...base, capture: { state: 'paused', rate: 4, muted: true } })!.action).toBe('resume');
    expect(tm(statusRow({ ...base, capture: { state: 'tabClosed', rate: null, muted: false } })!.message)).toBe('The tab was closed');
  });

  it('lists a playlist with its pending entries from the page', () => {
    const view = { ...sessionView(playlistSession({ resolved: 7 })) };
    expect(sessionShape(view)).toBe('playlist');
    const rows = playlistRows(view);
    expect(rows).toHaveLength(12);
    expect(rows.filter((r) => r.pending)).toHaveLength(5);
    expect(rows.find((r) => r.resolving)?.title).toBe('Dumb & Poetic');
    expect(tm(statusRow(view)!.message)).toBe('Resolving · 7 of 12');
    expect(statusRow(view)!.action).toBeNull();
  });

  it('copies a tracklist and links a cue to the source', () => {
    const view = sessionView(mixSession());
    const text = tracklistText(view).split('\n');
    expect(text[0]).toBe('0:00  Fred again.. – Marea (We’ve Lost Dancing)'.replace('’', "'"));
    const segment = view.detail.segments.find((s) => s.start_ms === 15 * 60_000 + 5_000)!;
    expect(cueLink(view.detail, segment)).toBe('https://www.youtube.com/watch?v=c0-hvBcPbdc&t=905s');
    expect(cueLink({ ...view.detail, source: { ...view.detail.source!, url: 'file:set.mp3' }, entry: { ...view.detail.entry, source_url: null } }, segment)).toBeNull();
  });

  it('names what matched', () => {
    expect(tm(matchedBy([{ source: 'Shazam', at_ms: 8000, score: 0.9, detail: null }])!)).toBe('Shazam, after 8 s');
    expect(tm(matchedBy([{ source: 'Melody', at_ms: 0, score: 0.8, detail: null }])!)).toBe('Melody');
    const view = sessionView(mixSession());
    expect(tm(evidenceSummary(view.detail.segments[0]!)!)).toBe('3 Shazam matches');
    expect(clock(3_725_000)).toBe('1:02:05');
  });
});
