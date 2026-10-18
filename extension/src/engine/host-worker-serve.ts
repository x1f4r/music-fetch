import type { PcmChunk } from '../audio/types';
import type { CaptureHost } from '../capture/types';
import { RpcClient, serveRpc } from '../messaging/bus';
import { applyStoredCredentials } from '../platform/secrets';
import type { EngineTopics } from '../messaging/protocol';
import { BACKEND_METHODS, type BackendConfig, type BackendFactory, type EngineBackend } from './host-backend';
import { channelPort } from './host-ports';

/** The message that starts the engine Worker; its ports are transferred with it. */
export interface WorkerInit {
  type: 'mf:worker.init';
  config: BackendConfig;
  /**
   * Host → engine calls, and audio (`PcmMessage`, transferred) in the same queue, so a
   * session's last audio always arrives before the call that finishes it; session and
   * library events back.
   */
  engine: MessagePort;
  /** Engine → host `CaptureHost` calls. */
  capture: MessagePort;
}

export type WorkerReply = { type: 'mf:worker.ready' } | { type: 'mf:worker.failed'; message: string };

export interface PcmMessage {
  t: 'mf:pcm';
  sessionId: string;
  chunk: PcmChunk;
}

function isPcm(message: unknown): message is PcmMessage {
  return (message as Partial<PcmMessage> | null)?.t === 'mf:pcm';
}

export const CAPTURE_METHODS = ['attach', 'listenThrough', 'windows', 'release'] as const satisfies readonly (keyof CaptureHost)[];

function captureProxy(rpc: RpcClient<CaptureHost>): CaptureHost {
  const call = rpc.call.bind(rpc) as (method: string, ...args: unknown[]) => Promise<unknown>;
  return Object.fromEntries(CAPTURE_METHODS.map((m) => [m, (...args: unknown[]) => call(m, ...args)])) as unknown as CaptureHost;
}

/**
 * The Worker side: build the backend and serve it on the ports of `init`. Shared by the
 * real Worker entry (`host-worker.ts`) and the in-process Worker of the unit tests.
 */
export async function serveWorker(init: WorkerInit, factory: BackendFactory, reply: (message: WorkerReply) => void): Promise<EngineBackend | null> {
  try {
    // Capture calls last as long as a listen-through: no timeout.
    const capture = captureProxy(new RpcClient<CaptureHost>(channelPort(init.capture, 'capture'), { timeoutMs: 0 }));
    const backend = await factory({ ...init.config, capture });
    // The paid providers' keys, straight from the encrypted store into the engine.
    if (backend.setCredentials) applyStoredCredentials((credentials) => backend.setCredentials!(credentials));
    const engine = channelPort(init.engine, 'engine');
    engine.onMessage.addListener((message) => {
      if (isPcm(message)) backend.pushPcm(message.sessionId, message.chunk);
    });
    const served = serveRpc<EngineBackend, EngineTopics>(engine, backend, BACKEND_METHODS);
    backend.subscribe({
      sessionEvent: (sessionId, event) => served.emit('session', { sessionId, event }),
      captureStatus: (sessionId, status) => served.emit('capture', { sessionId, status }),
      libraryChanged: () => served.emit('library:changed', null),
    });
    reply({ type: 'mf:worker.ready' });
    return backend;
  } catch (error) {
    reply({ type: 'mf:worker.failed', message: error instanceof Error ? error.message : String(error) });
    return null;
  }
}
