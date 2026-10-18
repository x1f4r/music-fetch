import { browser } from 'wxt/browser';

/**
 * Firefox suspends the event page 30 s after the last *incoming* event, even while the
 * engine Worker computes, a fetch streams or a port stays open (S2). While a session
 * runs the host makes sure an event arrives at least every 10 s:
 *
 * - capture traffic (element PCM from the tab's agent, microphone PCM from a page) is
 *   port messages, which count;
 * - for everything else (identifying after capture, links, downloads) the host writes
 *   `storage.session` and listens to its own `storage.onChanged`, so the event page
 *   receives an event on every beat;
 * - the engine's own requests reach the event page as `webRequest.onCompleted` events,
 *   the self-keepalive the spike verified for 13 minutes.
 */
export class EventPageKeepalive {
  private timer: ReturnType<typeof setInterval> | null = null;
  private beats = 0;
  /** Beats seen by the listener (diagnostics). */
  received = 0;

  constructor(
    private readonly intervalMs = 10_000,
    engineHosts: string[] = [],
  ) {
    browser.storage.onChanged.addListener((changes) => {
      if ('mfKeepalive' in changes) this.received++;
    });
    if (engineHosts.length > 0) {
      try {
        browser.webRequest?.onCompleted.addListener(() => undefined, { urls: engineHosts });
      } catch {
        // no webRequest permission (or Android without it): storage beats only
      }
    }
  }

  get active(): boolean {
    return this.timer !== null;
  }

  set(needed: boolean): void {
    if (needed && !this.timer) {
      this.beat();
      this.timer = setInterval(() => this.beat(), this.intervalMs);
    } else if (!needed && this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private beat() {
    const area = browser.storage.session ?? browser.storage.local;
    void area.set({ mfKeepalive: ++this.beats }).catch(() => undefined);
  }
}
