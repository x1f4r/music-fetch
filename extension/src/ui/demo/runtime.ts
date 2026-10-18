import type { ActiveSession } from '../../engine/client';
import { MockEngine } from '../../engine/mock/engine';
import { memoryPersistence } from '../../engine/mock/persistence';
import type { SessionDetail, SessionEvent } from '../../engine/types';
import { probeCapabilities, type Capabilities } from '../../platform/capabilities';
import type { ActiveTab } from '../../platform/surfaces';
import type { Platform } from '../context';
import { demoSnapshot } from './library';
import { demoUrl, demoState, type DemoAction, type DemoState } from './states';

/** The id of the session a demo stage follows (replayed, not stored). */
export const DEMO_LIVE_ID = 'demo-live';

/**
 * The mock engine with a demo library, a session the stage is already following
 * (`live`) and running library sessions that report progress (`running`).
 */
export class DemoEngine extends MockEngine {
  private liveDone = false;
  private replayed = new Set<string>();

  constructor(private readonly state: DemoState) {
    const sessions = typeof state.sessions === 'function' ? state.sessions() : (state.sessions ?? 'all');
    super({
      persistence: memoryPersistence(demoSnapshot({ sessions, melody: state.melody })),
      recoverInterrupted: false,
      timeScale: 1,
      directive: () => {
        const value = this.directive;
        this.directive = null;
        return value;
      },
    });
    this.directive = state.directive ?? null;
  }

  private directive: string | null;

  override async activeSessions(): Promise<ActiveSession[]> {
    const real = await super.activeSessions();
    const live = this.state.live;
    if (!live || this.liveDone) return real;
    // The stage adopts the session first; its events follow on the next turn.
    setTimeout(() => this.replay(DEMO_LIVE_ID, [{ Started: { session_id: DEMO_LIVE_ID, kind: live.kind } }, ...live.events]), 0);
    this.scheduleRunning();
    return [{ id: DEMO_LIVE_ID, kind: live.kind, source: live.source, started_at_ms: Date.now() - 5000 }, ...real];
  }

  override async library(...args: Parameters<MockEngine['library']>) {
    this.scheduleRunning();
    return super.library(...args);
  }

  override async session(id: string): Promise<SessionDetail | null> {
    const detail = await super.session(id);
    if (detail && this.state.running?.[id]) setTimeout(() => this.replayRunning(id), 0);
    return detail;
  }

  override async installMelodyIndex(): Promise<string> {
    if (this.state.melodyFails) {
      const id = 'demo-melody-download';
      setTimeout(
        () => this.replay(id, [{ Started: { session_id: id, kind: 'Hum' } }, { Phase: { phase: 'Downloading', fraction: 0.08 } }, { Failed: { code: 'Network', message: "Couldn't reach the server." } }]),
        0,
      );
      return id;
    }
    const hold = this.state.holdDownload;
    if (hold === undefined) return super.installMelodyIndex();
    // A still picture of the download: it stops part-way until cancelled.
    const id = 'demo-melody-download';
    setTimeout(() => this.replay(id, [{ Started: { session_id: id, kind: 'Hum' } }, { Phase: { phase: 'Downloading', fraction: hold } }]), 0);
    return id;
  }

  override async finishSession(sessionId: string): Promise<void> {
    if (sessionId !== DEMO_LIVE_ID) return super.finishSession(sessionId);
    this.liveDone = true;
    this.replay(DEMO_LIVE_ID, this.state.live?.onFinish ?? ['Canceled']);
  }

  override async cancelSession(sessionId: string): Promise<void> {
    if (sessionId === 'demo-melody-download') {
      this.replay(sessionId, ['Canceled']);
      return;
    }
    if (sessionId !== DEMO_LIVE_ID) return super.cancelSession(sessionId);
    this.liveDone = true;
    this.replay(DEMO_LIVE_ID, ['Canceled']);
  }

  private replay(sessionId: string, events: readonly SessionEvent[]) {
    for (const event of events) this.notify((l) => l.sessionEvent?.(sessionId, event));
  }

  /** Progress and capture state of running library sessions, once per page. */
  private scheduleRunning() {
    for (const id of Object.keys(this.state.running ?? {})) {
      if (this.replayed.has(id)) continue;
      this.replayed.add(id);
      setTimeout(() => this.replayRunning(id), 0);
    }
  }

  private replayRunning(id: string) {
    const running = this.state.running?.[id];
    if (!running) return;
    this.replay(id, running.events);
    if (running.capture) this.captureStatus(id, running.capture);
  }
}

/** A browser stand-in: the state's tab and capabilities; links open as demo pages or new tabs. */
export function demoPlatform(state: DemoState, base: Platform | null): Platform {
  const capabilities: Capabilities = {
    ...probeCapabilities(
      state.browser === 'firefox' ? { sidebarAction: {}, menus: {}, commands: {} } : { sidePanel: {}, offscreen: {}, tabCapture: {}, contextMenus: {}, commands: {} },
      state.android ? 'Mozilla/5.0 (Android 10; Mobile; rv:149.0) Gecko/149.0 Firefox/149.0' : 'Mozilla/5.0 (X11; Linux x86_64)',
      state.browser ?? 'chromium',
    ),
  };
  const tab: ActiveTab | null = state.tab
    ? { id: 7, windowId: 1, url: state.tab.url, title: state.tab.title, audible: state.tab.audible }
    : null;
  const toast = (text: string) => console.info(`[demo] ${text}`);
  const granted = new Set<string>();
  return {
    capabilities,
    activeTab: async () => tab,
    async hostAccess(url) {
      return state.needsHost?.includes(new URL(url).hostname) && !granted.has(new URL(url).hostname) ? 'needed' : 'granted';
    },
    async requestHost(url) {
      granted.add(new URL(url).hostname);
      return true;
    },
    pageFacts: async () => state.tab?.facts ?? null,
    async openApp(route = '/listen') {
      const app = demoState('app-library')!;
      window.open(`${demoUrl({ ...app, route })}`, '_blank');
    },
    async openPanel() {
      window.open(demoUrl(demoState('panel-listen')!), '_blank', 'width=360,height=800');
    },
    async openUrl(url) {
      if (base) await base.openUrl(url);
      else window.open(url, '_blank', 'noopener');
    },
    async copyText(text) {
      await navigator.clipboard?.writeText(text).catch(() => toast(`copied: ${text}`));
    },
    async share(data) {
      toast(`share: ${data.title}`);
      return Boolean(state.android);
    },
    async download(filename, _mime, text) {
      toast(`download ${filename}: ${text.length} bytes`);
    },
    async openSiteSettings() {
      toast('site settings');
    },
    async openShortcuts() {
      toast('shortcuts');
    },
    async shortcuts() {
      return [
        { name: '_execute_action', shortcut: 'Alt+Shift+M' },
        { name: 'identify-tab', shortcut: 'Alt+Shift+I' },
        { name: 'hum', shortcut: 'Alt+Shift+H' },
      ];
    },
    async microphones() {
      return [
        { id: 'usb', label: 'USB Microphone', inUse: false },
        { id: 'built-in', label: 'Built-in Microphone', inUse: true },
      ];
    },
    close() {
      toast('close');
    },
    async storageEstimate() {
      return state.storage ?? { usage: 57_400_000, quota: 12_000_000_000 };
    },
    async checkProvider(id) {
      return state.checks?.[id] ?? { state: 'ok' };
    },
  };
}

function find(selector: string, timeoutMs: number): Promise<HTMLElement | null> {
  return new Promise((resolve) => {
    const started = performance.now();
    const look = () => {
      const element = document.querySelector<HTMLElement>(selector);
      if (element) resolve(element);
      else if (performance.now() - started > timeoutMs) resolve(null);
      else setTimeout(look, 25);
    };
    look();
  });
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Play the state's clicks and keys once the page is up. */
export async function direct(actions: readonly DemoAction[]): Promise<string[]> {
  const problems: string[] = [];
  for (const action of actions) {
    if ('wait' in action) {
      await sleep(action.wait);
    } else if ('waitFor' in action) {
      if (!(await find(action.waitFor, 6000))) problems.push(`no ${action.waitFor}`);
      await sleep(30);
    } else if ('click' in action) {
      const element = await find(action.click, 4000);
      if (!element) problems.push(`no ${action.click}`);
      element?.click();
      await sleep(30);
    } else if ('type' in action) {
      const input = (await find(action.into, 4000)) as HTMLInputElement | null;
      if (!input) problems.push(`no ${action.into}`);
      else {
        input.focus();
        input.value = action.type;
        input.dispatchEvent(new Event('input', { bubbles: true }));
      }
    } else {
      const target = action.on ? await find(action.on, 4000) : (document.activeElement as HTMLElement | null) ?? document.body;
      target?.dispatchEvent(new KeyboardEvent('keydown', { key: action.key, shiftKey: action.shift, ctrlKey: action.mod, metaKey: action.mod, bubbles: true }));
      await sleep(30);
    }
  }
  return problems;
}
