import { browser } from 'wxt/browser';
import { RelaySource } from '../audio/relay-source';
import type { MicWire } from '../audio/page-mic';
import { CaptureCoordinator, CoordinatorError } from '../capture/coordinator';
import type { PortLike } from '../messaging/bus';
import { CAPTURE_PORT, MIC_PORT, type HostSettings } from '../messaging/protocol';
import { requiredHostPermissions } from '../net/hosts';
import { settings } from '../settings/settings';
import { showActivity } from './host-activity';
import { CaptureError, EngineHost, type HostPlatform, type HostRuntime, type IncomingPort, type OpenedSource, type UiConnection } from './host-core';
import { openKv } from './host-idb';
import { EventPageKeepalive } from './host-keepalive';
import { takeMockDirective } from './host-mock-directive';
import { SessionRegistry } from './host-registry';
import { injectAgent, resolveTab } from './host-tabs';

/**
 * Firefox's microphone: the event page can never use it (S2), so the page that asked
 * captures it and streams the PCM here over `MIC_PORT`.
 */
class PageMicRelay {
  private readonly waiting = new Map<string, (port: PortLike) => void>();

  accept(port: PortLike): void {
    const first = (message: unknown) => {
      const wire = message as MicWire;
      if (wire?.t !== 'hello') return;
      port.onMessage.removeListener(first);
      const resolve = this.waiting.get(wire.token);
      this.waiting.delete(wire.token);
      if (resolve) resolve(port);
      else port.disconnect();
    };
    port.onMessage.addListener(first);
  }

  async open(caller: UiConnection | null, priority: string[], android: boolean): Promise<OpenedSource> {
    if (android) throw new CaptureError('MicUnavailable', 'Firefox for Android has no microphone for extensions. Use the Music Fetch app.');
    if (!caller) throw new CaptureError('MicUnavailable', 'Open Music Fetch to use the microphone.');
    const token = crypto.randomUUID();
    const port = await new Promise<PortLike>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiting.delete(token);
        reject(new CaptureError('MicUnavailable', 'The page did not start the microphone.'));
      }, 10_000);
      this.waiting.set(token, (p) => {
        clearTimeout(timer);
        resolve(p);
      });
      caller.emit('capture:mic', { token, deviceIds: priority });
    });
    let started: ((info: { sampleRate: number; label: string }) => void) | null = null;
    let failed: ((error: Error) => void) | null = null;
    const source = new RelaySource(
      'mic',
      () =>
        new Promise((resolve, reject) => {
          started = resolve;
          failed = reject;
        }),
      () => port.disconnect(),
    );
    port.onMessage.addListener((message: unknown) => {
      const wire = message as MicWire;
      if (wire.t === 'started') started?.({ sampleRate: wire.sampleRate, label: wire.label });
      else if (wire.t === 'error') failed?.(new CaptureError(wire.reason, wire.message));
      else if (wire.t === 'pcm') source.feed(wire.chunk);
      else if (wire.t === 'event') source.signal(wire.event);
    });
    port.onDisconnect.addListener(() => {
      failed?.(new CaptureError('MicUnavailable', 'The page closed.'));
      source.signal({ type: 'ended', reason: 'ended', message: 'The page holding the microphone closed.' });
    });
    await source.start();
    return { source, via: 'mic', tab: null };
  }
}

async function readSettings(): Promise<HostSettings> {
  const [micPriority, lowPower] = await Promise.all([settings.micPriority.getValue(), settings.lowPower.getValue()]);
  return { micPriority, lowPower };
}

/**
 * Firefox's engine host, in the event page (Firefox has no offscreen documents; extension
 * plan §3.1). "This tab" is element capture in the tab's content script, the microphone
 * is captured by the page that asked, and the keepalive holds the event page while a
 * session runs.
 */
export function startFirefoxHost(): EngineHost {
  const runtime = browser.runtime as unknown as HostRuntime;
  const android = /\bAndroid\b/i.test(navigator.userAgent);
  const keepalive = new EventPageKeepalive(10_000, requiredHostPermissions());
  const mic = new PageMicRelay();
  let host: EngineHost | null = null;
  const coordinator = new CaptureCoordinator({
    target: 'firefox',
    android,
    tabs: { resolve: resolveTab, inject: injectAgent },
    pushPcm: (sessionId, chunk) => host?.worker.pushPcm(sessionId, chunk),
    captureEvent: (sessionId, event) => void host?.worker.call('captureEvent', sessionId, event).catch(() => undefined),
  });
  const platform: HostPlatform = {
    target: 'firefox',
    android,
    async openTab() {
      try {
        const { source, tab } = await coordinator.openLiveElement();
        return { source, via: 'element', tab };
      } catch (error) {
        if (error instanceof CoordinatorError) throw new CaptureError(error.reason, error.message);
        throw error;
      }
    },
    openMic: (caller, priority) => mic.open(caller, priority, android),
    settings: readSettings,
    activity: (activity) => void showActivity(activity),
    idle: () => undefined,
    takeDirective: takeMockDirective,
    spawnWorker: () => new Worker(new URL('./host-worker.ts', import.meta.url), { type: 'module', name: 'Music Fetch engine' }),
    keepalive: (needed) => keepalive.set(needed),
    pageFacts: async (tabId) => (await coordinator.pageFacts(tabId)) as unknown as Record<string, unknown> | null,
  };
  host = new EngineHost({
    platform,
    runtime,
    capture: coordinator,
    registry: new SessionRegistry(openKv('mf-host').catch(() => null)),
    cpus: navigator.hardwareConcurrency,
    memoryGiB: null,
  });
  runtime.onConnect.addListener((port: IncomingPort) => {
    const origin = runtime.getURL('/');
    if (port.name === CAPTURE_PORT) {
      if (port.sender?.id !== runtime.id || port.sender.tab?.id === undefined) port.disconnect();
      else coordinator.acceptAgent(port);
    } else if (port.name === MIC_PORT) {
      if (port.sender?.id !== runtime.id || !port.sender.url?.startsWith(origin)) port.disconnect();
      else mic.accept(port);
    }
  });
  return host;
}
