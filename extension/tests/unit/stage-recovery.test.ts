import { describe, expect, it } from 'vitest';
import type { SessionEvent } from '@/src/engine/types';
import { t, tm } from '@/src/i18n';
import {
  downloadStage,
  failedStage,
  idleStage,
  isTwoLine,
  recoveryActions,
  reduceStage,
  retrySeconds,
  stageLine,
  startingStage,
  type StageState,
} from '@/src/models/stage';

const ID = 's';
const actions = (state: ReturnType<typeof idleStage>) => recoveryActions(state).map((r) => `${r.action}${r.primary ? '!' : ''}`);

function noMatch(source: 'Tab' | 'Microphone' | 'Hum', reason: 'NotFound' | 'CaptureSilent', tabAudible: boolean | null = null) {
  let state = startingStage(source, source === 'Hum' ? 'Hum' : source === 'Tab' ? 'SystemAudio' : 'Listen', { tabAudible });
  state = reduceStage(state, ID, { Started: { session_id: ID, kind: state.kind! } });
  return reduceStage(state, ID, { Finished: { outcome: { NoMatch: { reason } } } });
}

describe('the recovery row', () => {
  it('offers Try again, Hum it and Type a lyric after no match (no Hum it after a hum)', () => {
    expect(actions(noMatch('Tab', 'NotFound'))).toEqual(['tryAgain', 'humIt', 'typeLyric']);
    expect(actions(noMatch('Hum', 'NotFound'))).toEqual(['tryAgain', 'typeLyric']);
  });

  it('tells a silent tab from a blocked one', () => {
    const silent = noMatch('Tab', 'CaptureSilent', false);
    expect(tm(silent.line)).toBe(t('state.tab.nothingPlaying'));
    expect(actions(silent)).toEqual(['tryAgain', 'useMicrophone']);
    const blocked = noMatch('Tab', 'CaptureSilent', true);
    expect(tm(blocked.line)).toBe(t('state.tab.blocked'));
    expect(isTwoLine(blocked.line)).toBe(true);
    expect(actions(blocked)).toEqual(['useMicrophone!']);
  });

  it('maps capture problems from the host to their lines and actions', () => {
    const base = startingStage('Tab', 'SystemAudio');
    const cases: [Parameters<typeof failedStage>[1], string, string[]][] = [
      ['CaptureNotAllowed', "Can't listen to this page", ['useMicrophone']],
      ['CaptureBusy', 'Another capture is using this tab', ['tryAgain', 'useMicrophone']],
      ['NothingPlaying', 'Nothing is playing in this tab', ['tryAgain', 'useMicrophone']],
      ['MicDenied', 'Microphone access is off.', ['allowMicrophone!', 'siteSettings']],
      ['Network', "Couldn't reach the server.", ['tryAgain']],
    ];
    for (const [code, line, expected] of cases) {
      const state = failedStage(base, code, 'x');
      expect(tm(state.line)).toBe(line);
      expect(actions(state)).toEqual(expected);
    }
  });

  it('counts down a rate limit and offers Stop', () => {
    let state = reduceStage(startingStage('Tab', 'SystemAudio'), ID, { Started: { session_id: ID, kind: 'SystemAudio' } }, 1000);
    state = reduceStage(state, ID, { Phase: { phase: 'Recognizing', fraction: null } }, 1000);
    state = reduceStage(state, ID, { Notice: { notice: { RateLimited: { retry_in_ms: 42_000 } } } }, 1000);
    expect(retrySeconds(state, 1000)).toBe(42);
    expect(tm(stageLine(state, 11_000))).toBe('Too many requests. Trying again in 32 s.');
    expect(actions(state)).toEqual(['stop']);
    state = reduceStage(state, ID, { Finished: { outcome: { Matched: { track: { title: 't', artist: 'a' } as never } } } }, 2000);
    expect(state.retryAt).toBeNull();
  });

  it('shows the melody library download, then Hum is ready', () => {
    let state: StageState = { ...downloadStage(55_000_000), download: { sessionId: 'dl', fraction: 0, totalBytes: 55_000_000 } };
    expect(actions(state)).toEqual(['cancelDownload']);
    const progress: SessionEvent = { Phase: { phase: 'Downloading', fraction: 0.42 } };
    state = reduceStage(state, 'dl', progress);
    expect(tm(state.line)).toBe('Downloading the melody library · 23 of 55 MB');
    state = reduceStage(state, 'dl', { Finished: { outcome: { Completed: { identified: 1, total: 1 } } } });
    expect(state.download).toBeNull();
    expect(state.phase).toBe('idle');
    expect(tm(state.line)).toBe(t('melody.ready'));
  });

  it('rests on a line that fits the selected method', () => {
    expect(tm(idleStage('Hum').line)).toBe('Hum, whistle or sing');
    expect(tm(idleStage('Tab').line)).toBe("Identify what's playing");
    expect(actions(idleStage())).toEqual([]);
  });
});
