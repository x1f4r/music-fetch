import { serveRpc, type EventLike, type PortLike, type ServedPort } from '../messaging/bus';
import { ENGINE_PORT, type EngineTopics } from '../messaging/protocol';
import { ENGINE_METHODS, type EngineClient } from './client';

/** A port as the host receives it; `sender` is set by the browser. */
export interface IncomingPort extends PortLike {
  sender?: { id?: string; url?: string };
}

export interface HostRuntime {
  readonly id: string;
  getURL(path: string): string;
  onConnect: EventLike<(port: IncomingPort) => void>;
}

export interface EngineHost {
  /** Number of connected UI ports. */
  readonly connections: number;
  close(): void;
}

/**
 * Serve `engine` in this context to UI pages: every Port named `ENGINE_PORT` can call
 * the engine's methods, and every session and library event is broadcast to all of them.
 * Only extension pages may connect.
 *
 * The extension itself runs `EngineHost` (`host-core.ts`), which adds the engine Worker
 * and capture; this in-process variant serves an engine object directly (tests, tools).
 */
export function serveEngineHost(engine: EngineClient, runtime: HostRuntime): EngineHost {
  const served = new Set<ServedPort<EngineTopics>>();
  const origin = runtime.getURL('/');

  const broadcast = <K extends keyof EngineTopics & string>(topic: K, payload: EngineTopics[K]) => {
    for (const port of served) port.emit(topic, payload);
  };

  const unsubscribe = engine.subscribe({
    sessionEvent: (sessionId, event) => broadcast('session', { sessionId, event }),
    libraryChanged: () => broadcast('library:changed', null),
  });

  const onConnect = (port: IncomingPort) => {
    if (port.name !== ENGINE_PORT) return;
    const sender = port.sender;
    if (sender && (sender.id !== runtime.id || !sender.url?.startsWith(origin))) {
      port.disconnect();
      return;
    }
    const entry: ServedPort<EngineTopics> = serveRpc<EngineClient, EngineTopics>(port, engine, ENGINE_METHODS, () =>
      served.delete(entry),
    );
    served.add(entry);
  };
  runtime.onConnect.addListener(onConnect);

  return {
    get connections() {
      return served.size;
    },
    close() {
      runtime.onConnect.removeListener(onConnect);
      unsubscribe();
      for (const port of [...served]) port.close();
    },
  };
}
