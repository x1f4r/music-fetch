import { browser } from 'wxt/browser';
import { MicSource } from '../audio/mic';
import { TabCaptureSource } from '../audio/tab-capture';
import { TAP_WORKLET_URL } from '../audio/tap-url';
import { CaptureCoordinator, type TabsBridge } from '../capture/coordinator';
import { sendControl } from '../messaging/control';
import { CAPTURE_PORT } from '../messaging/protocol';
import { EngineHost, type HostPlatform, type HostRuntime, type IncomingPort } from './host-core';
import { openKv } from './host-idb';
import { takeMockDirective } from './host-mock-directive';
import { SessionRegistry } from './host-registry';

/** The service worker does what offscreen documents can't (they only have `runtime`). */
const swTabs: TabsBridge = {
  resolve: (tabId) => sendControl({ type: 'mf:tab.resolve', ...(tabId !== undefined ? { tabId } : {}) }),
  inject: async (tabId) => {
    await sendControl({ type: 'mf:tab.inject', tabId });
  },
  streamId: async (tabId) => (await sendControl({ type: 'mf:tab.stream', tabId })).streamId,
};

/**
 * Chromium's engine host, in the offscreen document (reasons USER_MEDIA and WORKERS).
 * Tab capture and the microphone run here, so sessions survive the popup closing; the
 * engine runs in a Worker started from here.
 */
export function startChromiumHost(): EngineHost {
  const runtime = browser.runtime as unknown as HostRuntime;
  let host: EngineHost | null = null;
  const coordinator = new CaptureCoordinator({
    target: 'chromium',
    android: false,
    tabs: swTabs,
    pushPcm: (sessionId, chunk) => host?.worker.pushPcm(sessionId, chunk),
    captureEvent: (sessionId, event) => void host?.worker.call('captureEvent', sessionId, event).catch(() => undefined),
    openTabCapture: async (streamId, clock, listenAlong) => {
      const source = new TabCaptureSource({ streamId, listenAlong, clock, tap: { workletUrl: TAP_WORKLET_URL } });
      await source.start();
      return source;
    },
  });
  const platform: HostPlatform = {
    target: 'chromium',
    android: false,
    async openTab() {
      // The stream id must be consumed within ~3 s: ask for it only now.
      const { streamId, tab } = await sendControl({ type: 'mf:tab.stream' });
      const source = new TabCaptureSource({ streamId, listenAlong: true, tap: { workletUrl: TAP_WORKLET_URL } });
      await source.start();
      return { source, via: 'tab', tab };
    },
    async openMic(_caller, priority) {
      const source = new MicSource({ priority, tap: { workletUrl: TAP_WORKLET_URL } });
      await source.start();
      return { source, via: 'mic', tab: null };
    },
    settings: () => sendControl({ type: 'mf:host.settings' }),
    activity: (activity) => void sendControl({ type: 'mf:host.activity', ...activity }).catch(() => undefined),
    idle: () => void sendControl({ type: 'mf:host.idle' }).catch(() => undefined),
    takeDirective: takeMockDirective,
    spawnWorker: () => new Worker(new URL('./host-worker.ts', import.meta.url), { type: 'module', name: 'Music Fetch engine' }),
    pageFacts: async (tabId) => (await coordinator.pageFacts(tabId)) as unknown as Record<string, unknown> | null,
  };
  host = new EngineHost({
    platform,
    runtime,
    capture: coordinator,
    registry: new SessionRegistry(openKv('mf-host').catch(() => null)),
    cpus: navigator.hardwareConcurrency,
    memoryGiB: (navigator as Navigator & { deviceMemory?: number }).deviceMemory ?? null,
  });
  runtime.onConnect.addListener((port: IncomingPort) => {
    if (port.name !== CAPTURE_PORT) return;
    if (port.sender?.id !== runtime.id || port.sender.tab?.id === undefined) {
      port.disconnect();
      return;
    }
    coordinator.acceptAgent(port);
  });
  // Header rules for the engine's requests (the service worker also installs them on start).
  void sendControl({ type: 'mf:net.ensure' }).catch(() => undefined);
  return host;
}
