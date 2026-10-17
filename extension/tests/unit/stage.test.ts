import { describe, expect, it } from 'vitest';
import { humScript, matchScript, mediaScript, noMatchScript, parseRecording, waitingScript } from '@/src/engine/mock/scripts';
import { demoTrack } from '@/src/engine/mock/catalog';
import type { SessionEvent, SessionKind } from '@/src/engine/types';
import { t, tm } from '@/src/i18n';
import { adoptSession, idleStage, isRunning, reduceStage, ringPercent, showsResult, startingStage, type StageState } from '@/src/models/stage';
import type { ListenChoice } from '@/src/settings/settings';
import recording from '../fixtures/hum-no-index.jsonl?raw';

const ID = 'session-1';

function play(source: ListenChoice | null, kind: SessionKind, events: SessionEvent[]): StageState[] {
  let state = startingStage(source, kind);
  const states = [state];
  for (const event of [{ Started: { session_id: ID, kind } } as SessionEvent, ...events]) {
    state = reduceStage(state, ID, event);
    states.push(state);
  }
  return states;
}

const flat = (script: { capture: { event: SessionEvent }[]; outcome: { event: SessionEvent }[] }) =>
  [...script.capture, ...script.outcome].map((l) => l.event);

describe('the listen stage', () => {
  it('goes listening → matching → matched', () => {
    const states = play('Tab', 'SystemAudio', flat(matchScript(demoTrack(0))));
    expect(states[1]!.phase).toBe('listening');
    expect(states.some((s) => s.level > 0.3)).toBe(true);
    expect(states.some((s) => s.phase === 'working' && tm(s.line) === t('state.matching'))).toBe(true);
    const last = states.at(-1)!;
    expect(last.phase).toBe('matched');
    expect(last.track?.title).toBe(demoTrack(0).title);
    expect(showsResult(last)).toBe(true);
    expect(isRunning(last)).toBe(false);
  });

  it('says why nothing matched', () => {
    expect(tm(play('Microphone', 'Listen', flat(noMatchScript('TooQuiet'))).at(-1)!.line)).toBe('Too quiet to hear');
    const silentTab = play('Tab', 'SystemAudio', flat(noMatchScript('CaptureSilent'))).at(-1)!;
    expect(silentTab.phase).toBe('nomatch');
    expect(tm(silentTab.line)).toBe(t('state.tab.blocked'));
    const silentMic = play('Microphone', 'Listen', flat(noMatchScript('CaptureSilent'))).at(-1)!;
    expect(tm(silentMic.line)).toBe(t('state.micOff'));
  });

  it('shows the rate limit with its countdown', () => {
    const states = play('Tab', 'SystemAudio', [{ Notice: { notice: { RateLimited: { retry_in_ms: 2400 } } } }]);
    expect(tm(states.at(-1)!.line)).toBe('Too many requests. Trying again in 3 s.');
  });

  it('fills the hum ring with voiced time and offers the melody download', () => {
    const states = play('Hum', 'Hum', flat(humScript([demoTrack(0)], { indexInstalled: false })));
    expect(states.some((s) => s.humming)).toBe(true);
    expect(Math.max(...states.map(ringPercent))).toBeGreaterThan(20);
    expect(states.at(-1)!.melodyMissing).toBe(true);
    expect(states.at(-1)!.phase).toBe('nomatch');
  });

  it('keeps candidates in tier order', () => {
    const tracks = [demoTrack(0), demoTrack(1), demoTrack(2)];
    const last = play('Hum', 'Hum', flat(humScript(tracks, { indexInstalled: true }))).at(-1)!;
    expect(last.phase).toBe('candidates');
    expect(last.candidatesFinal).toBe(true);
    expect(last.candidates.map((c) => c.tier)).toEqual(['Strong', 'Possible', 'LongShot']);
  });

  it('treats a single-track link as a result', () => {
    const track = demoTrack(3);
    const last = play(null, 'Link', flat(mediaScript(track, 'https://www.youtube.com/watch?v=x', 'YouTube', 'YouTube', 'Video'))).at(-1)!;
    expect(last.phase).toBe('matched');
    expect(last.track?.title).toBe(track.title);
  });

  it('saves offline captures for later', () => {
    const last = play('Microphone', 'Listen', flat(waitingScript())).at(-1)!;
    expect(last.phase).toBe('waiting');
    expect(tm(last.line)).toBe("You're offline. Saved to identify later.");
  });

  it('maps failures to the apps’ copy', () => {
    const last = play(null, 'Link', [{ Failed: { code: 'MediaUnavailable', message: 'gone' } }]).at(-1)!;
    expect(last.phase).toBe('failed');
    expect(tm(last.line)).toBe("This video isn't available.");
  });

  it('ignores other sessions and resets on cancel', () => {
    let state = adoptSession(startingStage('Tab', 'SystemAudio'), ID);
    state = reduceStage(state, 'other', { Finished: { outcome: { NoMatch: { reason: 'NotFound' } } } });
    expect(state.phase).toBe('starting');
    state = reduceStage(state, ID, 'Canceled');
    expect(state).toEqual(idleStage());
  });

  it('folds a stream recorded from the native engine', () => {
    const lines = parseRecording(recording);
    let state = startingStage('Hum', 'Hum');
    for (const { event } of lines) {
      const id = typeof event === 'object' && 'Started' in event ? event.Started.session_id : state.sessionId!;
      state = reduceStage(state, id, event);
    }
    expect(state.melodyMissing).toBe(true);
    expect(state.phase).toBe('nomatch');
    expect(state.voicedMs).toBeGreaterThan(3000);
    expect(state.pitch.length).toBeGreaterThan(50);
  });
});
