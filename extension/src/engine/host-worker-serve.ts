import type { PcmChunk } from '../audio/types';
import type { CaptureHost } from '../capture/types';
import { RpcClient, serveRpc } from '../messaging/bus';
import type { EngineTopics } from '../messaging/protocol';
import { BACKEND_METHODS, type BackendConfig, type BackendFactory, type EngineBackend } from './host-backend';
import { channelPort } from './host-ports';

/** The message that starts the engine Worker; its ports are transferred with it. */
export interface WorkerInit {
  type: 'mf:worker.init';
  config: BackendConfig;
  /** Host → engine calls; session and library events back. */
  engine: MessagePort;
  /** Engine → host `CaptureHost` calls. */
  capture: MessagePort;
  /** Host → engine audio, transferred, never answered. */
  pcm: MessagePort;
}

export type WorkerReply = { type: 'mf:worker.ready' } | { type: 'mf:worker.failed'; message: string };

export interface PcmMessage {
  sessionId: string;
  chunk: PcmChunk;
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
    const served = serveRpc<EngineBackend, EngineTopics>(channelPort(init.engine, 'engine'), backend, BACKEND_METHODS);
    backend.subscribe({
      sessionEvent: (sessionId, event) => served.emit('session', { sessionId, event }),
      libraryChanged: () => served.emit('library:changed', null),
    });
    init.pcm.onmessage = (message: MessageEvent<PcmMessage>) => backend.pushPcm(message.data.sessionId, message.data.chunk);
    reply({ type: 'mf:worker.ready' });
    return backend;
  } catch (error) {
    reply({ type: 'mf:worker.failed', message: error instanceof Error ? error.message : String(error) });
    return null;
  }
}
