import { browser } from 'wxt/browser';

/**
 * e2e builds only: the notifications the background posted, in `storage.session`
 * (`mfE2eNotifications`), so the end-to-end tests of both browsers can read them from an
 * extension page. Release builds drop this code (the caller checks the build mode).
 */
export const E2E_NOTIFICATIONS_ITEM = 'mfE2eNotifications';

let queue: Promise<void> = Promise.resolve();

export function recordNotification(id: string, content: { title: string; message: string; buttons: string[] }): void {
  queue = queue
    .then(async () => {
      const area = (browser.storage as unknown as { session?: typeof browser.storage.local }).session;
      if (!area) return;
      const stored = ((await area.get(E2E_NOTIFICATIONS_ITEM))[E2E_NOTIFICATIONS_ITEM] as unknown[] | undefined) ?? [];
      await area.set({ [E2E_NOTIFICATIONS_ITEM]: [...stored, { id, ...content, at: Date.now() }].slice(-50) });
    })
    .catch(() => undefined);
}
