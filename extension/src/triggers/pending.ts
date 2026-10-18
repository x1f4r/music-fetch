import { storage } from 'wxt/utils/storage';
import type { ListenChoice } from '../settings/settings';

/**
 * The Hum shortcut opens the side panel (Chromium) or sidebar (Firefox) and asks it to
 * start humming. The panel may be opening (it takes the request on start) or already open
 * (it watches for one). Kept in `storage.session`: gone when the browser closes, and a
 * request older than a few seconds is ignored.
 */
export interface PendingStart {
  source: ListenChoice;
  at: number;
}

const MAX_AGE_MS = 15_000;
const item = storage.defineItem<PendingStart | null>('session:pendingStart', { fallback: null });

export async function requestStart(source: ListenChoice, now = Date.now()): Promise<void> {
  await item.setValue({ source, at: now });
}

/** The waiting request, if fresh, removed so only one surface acts on it. */
export async function takePendingStart(now = Date.now()): Promise<PendingStart | null> {
  const pending = await item.getValue();
  if (!pending) return null;
  await item.removeValue();
  return now - pending.at <= MAX_AGE_MS ? pending : null;
}

/** Act on the request now if there is one, and on every later one while the surface lives. */
export function followPendingStart(start: (source: ListenChoice) => void): () => void {
  const take = () =>
    void takePendingStart()
      .then((pending) => pending && start(pending.source))
      .catch(() => undefined);
  take();
  return item.watch((value) => {
    if (value) take();
  });
}

/** Demo states (`?demo=`) never act on a real request. */
export function isDemo(): boolean {
  return typeof location !== 'undefined' && new URLSearchParams(location.search).has('demo');
}
