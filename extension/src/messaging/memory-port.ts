import type { EventLike, PortLike } from './bus';

class Listeners<F extends (...args: never[]) => void> implements EventLike<F> {
  readonly set = new Set<F>();
  addListener(callback: F) {
    this.set.add(callback);
  }
  removeListener(callback: F) {
    this.set.delete(callback);
  }
}

interface MemoryPort extends PortLike {
  onMessage: Listeners<(message: unknown) => void>;
  onDisconnect: Listeners<() => void>;
}

/**
 * Two connected in-memory ports that behave like `runtime.Port`: messages are cloned and
 * delivered asynchronously, and `disconnect()` fires `onDisconnect` on the other end only.
 */
export function createPortPair(name = 'test'): [PortLike, PortLike] {
  let connected = true;
  const make = (): MemoryPort => ({
    name,
    onMessage: new Listeners(),
    onDisconnect: new Listeners(),
    postMessage(message: unknown) {
      if (!connected) throw new Error('Attempting to use a disconnected port object');
      const copy = structuredClone(message);
      const target = this === a ? b : a;
      queueMicrotask(() => {
        if (connected) for (const callback of target.onMessage.set) callback(copy);
      });
    },
    disconnect() {
      if (!connected) return;
      connected = false;
      const target = this === a ? b : a;
      queueMicrotask(() => {
        for (const callback of target.onDisconnect.set) callback();
      });
    },
  });
  const a = make();
  const b = make();
  return [a, b];
}
