import type { EventLike, PortLike } from '../messaging/bus';

class Listeners<F extends (...args: never[]) => void> implements EventLike<F> {
  readonly set = new Set<F>();
  addListener(callback: F) {
    this.set.add(callback);
  }
  removeListener(callback: F) {
    this.set.delete(callback);
  }
}

export interface ChannelPort extends PortLike {
  /** Tell this side's listeners that the other side is gone (the Worker was terminated). */
  hangUp(): void;
}

/**
 * A `MessagePort` (host ↔ engine Worker) as the bus's `PortLike`. A MessagePort says
 * nothing when the other side goes away, so the owner calls `hangUp()` when it
 * terminates the Worker; pending calls then fail with `Disconnected`.
 */
export function channelPort(port: MessagePort, name: string): ChannelPort {
  const onMessage = new Listeners<(message: unknown) => void>();
  const onDisconnect = new Listeners<() => void>();
  let open = true;
  port.onmessage = (event: MessageEvent) => {
    for (const listener of onMessage.set) listener(event.data);
  };
  const hangUp = () => {
    if (!open) return;
    open = false;
    port.onmessage = null;
    for (const listener of [...onDisconnect.set]) listener();
  };
  return {
    name,
    postMessage(message) {
      if (!open) throw new Error('Attempting to use a disconnected port object');
      port.postMessage(message);
    },
    disconnect() {
      if (!open) return;
      open = false;
      port.onmessage = null;
      port.close();
    },
    onMessage,
    onDisconnect,
    hangUp,
  };
}
