import type { PortLike } from '../messaging/bus';
import { MIC_PORT, type MicCaptureRequest } from '../messaging/protocol';
import { MicError, MicSource } from './mic';
import { TAP_WORKLET_URL } from './tap-url';
import type { AudioSourceEvent, PcmChunk } from './types';

/** Messages from the page capturing the microphone to the host (`MIC_PORT`). */
export type MicWire =
  | { t: 'hello'; token: string }
  | { t: 'started'; sampleRate: number; label: string }
  | { t: 'pcm'; chunk: PcmChunk }
  | { t: 'event'; event: AudioSourceEvent }
  | { t: 'error'; reason: 'MicNotGranted' | 'MicUnavailable'; message: string };

/**
 * Firefox: the host asked this page (popup, sidebar, app tab) to capture the microphone,
 * because the background can't (S2). The capture lives as long as the page; when the
 * page closes the port drops and the host identifies what it heard.
 */
export async function servePageMic(request: MicCaptureRequest, connect: (name: string) => PortLike): Promise<void> {
  const port = connect(MIC_PORT);
  const send = (message: MicWire) => {
    try {
      port.postMessage(message);
    } catch {
      mic.stop();
    }
  };
  const mic = new MicSource({ priority: request.deviceIds, tap: { workletUrl: TAP_WORKLET_URL } });
  send({ t: 'hello', token: request.token });
  mic.onPcm((chunk) => send({ t: 'pcm', chunk }));
  mic.onEvent((event) => {
    if (event.type !== 'started') send({ t: 'event', event });
  });
  port.onDisconnect.addListener(() => mic.stop());
  try {
    await mic.start();
    const stats = mic.stats();
    send({ t: 'started', sampleRate: stats.sampleRate, label: mic.device ?? 'microphone' });
  } catch (error) {
    send({
      t: 'error',
      reason: error instanceof MicError ? error.reason : 'MicNotGranted',
      message: error instanceof Error ? error.message : String(error),
    });
    port.disconnect();
  }
}
