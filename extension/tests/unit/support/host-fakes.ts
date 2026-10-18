import { Emitter, type AudioSource, type AudioSourceEvent, type PcmChunk } from '@/src/audio/types';
import type { CaptureHost } from '@/src/capture/types';
import type { BackendFactory, EngineBackend } from '@/src/engine/host-backend';
import type { HostPlatform, HostRuntime, IncomingPort, OpenedSource } from '@/src/engine/host-core';
import { memoryKv, type Kv } from '@/src/engine/host-idb';
import { kvPersistence, MockBackend } from '@/src/engine/host-mock-backend';
import type { WorkerLike } from '@/src/engine/host-worker-client';
import { serveWorker, type WorkerInit } from '@/src/engine/host-worker-serve';
import type { EventLike, PortLike } from '@/src/messaging/bus';
import { createPortPair } from '@/src/messaging/memory-port';
import type { HostActivity } from '@/src/messaging/protocol';

export const ORIGIN = 'chrome-extension://abcdefghijklmnop/';

/** The engine Worker, run in this process over real MessageChannels. */
export class InProcessWorker implements WorkerLike {
  private readonly listeners = new Map<string, Set<(event: Event) => void>>();
  private ports: MessagePort[] = [];
  backend: EngineBackend | null = null;
  terminated = false;

  constructor(private readonly factory: BackendFactory) {}

  postMessage(message: unknown): void {
    const init = message as WorkerInit;
    if (init?.type !== 'mf:worker.init') return;
    this.ports = [init.engine, init.capture];
    void serveWorker(init, this.factory, (reply) => setTimeout(() => this.dispatch('message', { data: reply } as MessageEvent))).then((b) => {
      this.backend = b;
    });
  }

  terminate(): void {
    this.terminated = true;
    for (const port of this.ports) port.close();
  }

  addEventListener(type: string, listener: (event: Event) => void): void {
    let set = this.listeners.get(type);
    if (!set) this.listeners.set(type, (set = new Set()));
    set.add(listener);
  }

  /** What an uncaught error (a wasm panic) looks like to the host. */
  crash(message = 'RuntimeError: unreachable'): void {
    for (const port of this.ports) port.close();
    this.dispatch('error', { message } as ErrorEvent);
  }

  private dispatch(type: string, event: Partial<Event>) {
    for (const listener of this.listeners.get(type) ?? []) listener(event as Event);
  }
}

/** A mock backend whose library lives in `kv` (shared by every Worker the test starts). */
export function mockFactory(kv: Kv = memoryKv(), options: { timeScale?: number } = {}): BackendFactory {
  return async (env) => new MockBackend(env, await kvPersistence(kv), { timeScale: options.timeScale ?? 0, seed: false });
}

/** An audio source the test feeds by hand. */
export class FakeSource implements AudioSource {
  readonly pcm = new Emitter<PcmChunk>();
  readonly events = new Emitter<AudioSourceEvent>();
  stopped = false;
  sampleRate = 48_000;
  private seq = 0;

  constructor(readonly kind: 'tab' | 'element' | 'mic' = 'tab') {}

  async start(): Promise<void> {
    this.events.emit({ type: 'started', sampleRate: this.sampleRate, label: 'fake' });
  }

  stop(): void {
    this.stopped = true;
  }

  onPcm(callback: (chunk: PcmChunk) => void) {
    return this.pcm.on(callback);
  }

  onEvent(callback: (event: AudioSourceEvent) => void) {
    return this.events.on(callback);
  }

  /** Push `samples` as live chunks of 2048 frames. */
  play(samples: Float32Array, block = 2048): void {
    for (let i = 0; i < samples.length; i += block) {
      this.pcm.emit({
        samples: samples.slice(i, i + block),
        sampleRate: this.sampleRate,
        speed: 1,
        mediaMs: null,
        wallMs: Date.now(),
        continuous: this.seq > 0,
        seq: this.seq++,
      });
    }
  }

  end(): void {
    this.events.emit({ type: 'ended', reason: 'ended' });
  }
}

/** A fake `runtime` whose `connect` hands the other end of a memory port to `onConnect`. */
export function fakeRuntime() {
  const listeners = new Set<(port: IncomingPort) => void>();
  const onConnect: EventLike<(port: IncomingPort) => void> = {
    addListener: (f) => listeners.add(f),
    removeListener: (f) => listeners.delete(f),
  };
  const runtime: HostRuntime = { id: 'abcdefghijklmnop', getURL: (path) => new URL(path, ORIGIN).href, onConnect };
  const connect = (name: string, url = `${ORIGIN}popup.html`): PortLike => {
    const [ui, host] = createPortPair(name);
    const incoming = Object.assign(host, { sender: { id: runtime.id, url } });
    for (const listener of listeners) listener(incoming);
    return ui;
  };
  return { runtime, connect };
}

export interface FakePlatform extends HostPlatform {
  activities: HostActivity[];
  idles: number;
  sources: FakeSource[];
  directive: string | null;
  workers: InProcessWorker[];
}

export function fakePlatform(factory: BackendFactory, overrides: Partial<HostPlatform> = {}): FakePlatform {
  const platform: FakePlatform = {
    target: 'chromium',
    android: false,
    activities: [],
    idles: 0,
    sources: [],
    directive: null,
    workers: [],
    async openTab(): Promise<OpenedSource> {
      const source = new FakeSource('tab');
      platform.sources.push(source);
      await source.start();
      return { source, via: 'tab', tab: { id: 7, windowId: 1, url: 'https://example.org/', title: 'Example', audible: true, capture: 'ok' } };
    },
    async openMic(): Promise<OpenedSource> {
      const source = new FakeSource('mic');
      platform.sources.push(source);
      await source.start();
      return { source, via: 'mic', tab: null };
    },
    settings: async () => ({ micPriority: [], lowPower: 'off' }),
    activity: (a) => void platform.activities.push(a),
    idle: () => void platform.idles++,
    takeDirective: () => {
      const d = platform.directive;
      platform.directive = null;
      return d;
    },
    spawnWorker: () => {
      const worker = new InProcessWorker(factory);
      platform.workers.push(worker);
      return worker;
    },
    ...overrides,
  };
  return platform;
}

export const NO_CAPTURE: CaptureHost = {
  attach: async () => {
    throw new Error('no capture in this test');
  },
  listenThrough: async () => [],
  windows: async () => [],
  release: async () => undefined,
};
