import type { EngineClient } from '../engine/client';
import { isTerminal } from '../engine/events';
import type { Outcome, SessionEvent } from '../engine/types';
import { tm, type Message } from '../i18n';
import { badgeFor, resultNotification } from '../models/notifications';
import { errorLine } from '../models/stage';
import { primaryLink, providerLinks } from '../providers/links';
import { serviceName } from '../providers/services';
import { loadProviderSettings, settings } from '../settings/settings';

/**
 * The background's notifications and toolbar badge (extension plan §5.1, §5.3 step 4):
 * a dot while a session listens or works, the percentage while a mix is listened
 * through, and a notification when a session ends while no surface shows it.
 *
 * Wiring (background, task X2/X7): `startNotifier(engineClientForTheHost, browserNotifierApi(), {
 * hasUi: () => connectedUiPorts > 0, … })`. Pure apart from the `api` it is given.
 */
export interface NotifierApi {
  setBadge(text: string, color: string): void;
  notify(id: string, content: { title: string; message: string; buttons: string[] }): void;
  clear(id: string): void;
}

export interface NotifierHooks {
  /** A popup, panel or app tab is showing sessions: no notification needed. */
  hasUi(): boolean;
  openSession(sessionId: string): void;
  openUrl(url: string): void;
  tryAgain(sessionId: string): void;
  /** A problem notification was clicked: open Music Fetch. */
  openListen?(): void;
}

interface Running {
  capturing: boolean;
  mixFraction: number | null;
}

export interface Notifier {
  /** A click on the notification or one of its buttons (index), from the browser's events. */
  clicked(notificationId: string, button?: number): Promise<void>;
  /**
   * A session a shortcut or context menu started (X7): its answer is a notification even
   * when a surface is open somewhere, because the user asked without one. It may already
   * have ended (a link resolves in under a second), then it is shown now.
   */
  expect(sessionId: string): void;
  /** A shortcut or menu that couldn't start: say why. A click opens Music Fetch. */
  problem(message: string): void;
  stop(): void;
}

/** Sessions that ended unannounced, kept briefly in case a trigger claims them (`expect`). */
const RECENT_MS = 30_000;

export function startNotifier(engine: EngineClient, api: NotifierApi, hooks: NotifierHooks): Notifier {
  const running = new Map<string, Running>();
  const shown = new Map<string, { sessionId: string; outcome: Outcome }>();
  const expected = new Set<string>();
  const recent = new Map<string, { event: SessionEvent; at: number; shown: boolean }>();
  const problems = new Set<string>();
  let problemCount = 0;

  const badge = () => {
    const states = [...running.values()];
    const mix = states.find((s) => s.mixFraction !== null);
    const next = badgeFor({ capturing: states.some((s) => s.capturing), working: states.length > 0, mixFraction: mix?.mixFraction ?? null });
    api.setBadge(next.text, next.color);
  };

  async function finished(sessionId: string, event: SessionEvent) {
    if (expected.delete(sessionId)) {
      await announce(sessionId, event);
      return;
    }
    // A trigger may still claim it (its `expect` can arrive after the end).
    const now = Date.now();
    for (const [id, entry] of recent) if (now - entry.at > RECENT_MS) recent.delete(id);
    recent.set(sessionId, { event, at: now, shown: false });
    if (typeof event !== 'object' || !('Finished' in event)) return;
    if (hooks.hasUi() || !(await settings.notifyInBackground.getValue())) return;
    recent.get(sessionId)!.shown = true;
    await show(sessionId, event);
  }

  /** The answer to a trigger: the result, or why the session failed. */
  async function announce(sessionId: string, event: SessionEvent) {
    if (typeof event !== 'object') return;
    if ('Failed' in event) problem(tm({ key: errorLine(event.Failed.code) }));
    else if ('Finished' in event) await show(sessionId, event);
  }

  function problem(message: string) {
    const id = `mf-problem-${++problemCount}`;
    problems.add(id);
    api.notify(id, { title: tm({ key: 'app.name' }), message, buttons: [] });
  }

  async function show(sessionId: string, event: Extract<SessionEvent, { Finished: unknown }>) {
    const detail = await engine.session(sessionId).catch(() => null);
    if (!detail) return;
    const providers = await loadProviderSettings();
    const name = providers.provider.kind === 'custom' ? (providers.custom?.name ?? 'Custom') : serviceName(providers.provider.service);
    const content = resultNotification(detail.entry, event.Finished.outcome, name);
    if (!content) return;
    const id = `mf-${sessionId}`;
    shown.set(id, { sessionId, outcome: event.Finished.outcome });
    const text = (m: Message | string) => (typeof m === 'string' ? m : tm(m));
    api.notify(id, { title: text(content.title), message: text(content.message), buttons: content.buttons.map(text) });
  }

  const unsubscribe = engine.subscribe({
    sessionEvent: (sessionId, event) => {
      if (typeof event === 'object' && 'Started' in event) {
        const live = event.Started.kind === 'Listen' || event.Started.kind === 'SystemAudio' || event.Started.kind === 'Hum';
        running.set(sessionId, { capturing: live, mixFraction: null });
      } else if (typeof event === 'object' && 'Phase' in event && running.has(sessionId)) {
        running.get(sessionId)!.capturing = event.Phase.phase === 'Listening';
      } else if (typeof event === 'object' && 'Progress' in event && running.has(sessionId)) {
        const { analyzed_ms, total_ms } = event.Progress;
        running.get(sessionId)!.mixFraction = total_ms > 0 ? analyzed_ms / total_ms : null;
      }
      if (isTerminal(event)) {
        running.delete(sessionId);
        void finished(sessionId, event);
      }
      badge();
    },
  });

  return {
    expect(sessionId) {
      const ended = recent.get(sessionId);
      if (!ended) {
        expected.add(sessionId);
        return;
      }
      recent.delete(sessionId);
      if (!ended.shown) void announce(sessionId, ended.event);
    },
    problem,
    async clicked(notificationId, button) {
      if (problems.delete(notificationId)) {
        api.clear(notificationId);
        hooks.openListen?.();
        return;
      }
      const entry = shown.get(notificationId);
      if (!entry) return;
      api.clear(notificationId);
      shown.delete(notificationId);
      const outcome = entry.outcome;
      if (typeof outcome === 'object' && 'Matched' in outcome && button !== undefined) {
        if (button === 1) {
          await engine.setPinned(entry.sessionId, true);
          return;
        }
        const providers = await loadProviderSettings();
        const link = primaryLink(providerLinks(outcome.Matched.track, providers.custom), providers.provider);
        if (link) hooks.openUrl(link.url);
        return;
      }
      if (typeof outcome === 'object' && 'NoMatch' in outcome && button === 0) {
        hooks.tryAgain(entry.sessionId);
        return;
      }
      hooks.openSession(entry.sessionId);
    },
    stop() {
      unsubscribe();
      api.setBadge('', '#006b5a');
    },
  };
}
