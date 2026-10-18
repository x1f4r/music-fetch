/**
 * The engine Worker's entry (extension plan §2.8, §3.1): one dedicated Worker owns the
 * engine and its storage; the host starts it, talks to it over three MessagePorts and
 * recycles it when idle (wasm memory never shrinks). Workers have no extension APIs, so
 * nothing in here may use them.
 */
import { createBackend } from './host-backend-factory';
import { serveWorker, type WorkerInit, type WorkerReply } from './host-worker-serve';

let started = false;

self.addEventListener('message', (event: MessageEvent) => {
  const init = event.data as WorkerInit | null;
  if (init?.type !== 'mf:worker.init' || started) return;
  started = true;
  void serveWorker(init, createBackend, (reply: WorkerReply) => (self as unknown as Worker).postMessage(reply));
});
