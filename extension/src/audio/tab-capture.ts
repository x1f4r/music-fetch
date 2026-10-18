import type { MediaClock } from './media-clock';
import { StreamSource } from './stream-source';
import type { TapOptions } from './tap';

export interface TabCaptureOptions {
  /** From `tabCapture.getMediaStreamId` in the service worker; valid for about 3 s (S1). */
  streamId: string;
  /**
   * Route the tab back to the speakers. A captured tab is silent otherwise: right for a
   * listen-through scan, wrong for "This tab" while the user listens (S1).
   */
  listenAlong: boolean;
  /** Media clock fed by the page's `currentTime` (listen-through); null for live listening. */
  clock?: MediaClock | null;
  tap?: TapOptions;
}

/** Wall ms by which tab-captured audio trails the page's `currentTime` (S1: 31.8–34.6). */
export const TAB_CAPTURE_LAG_MS = 35;

/**
 * Chromium "This tab" (extension plan §3.2): `tabCapture` consumed in the offscreen
 * document. Never mute the element or set its volume to 0: the capture follows the
 * element's volume and goes silent (S1). Tab mute is safe.
 */
export class TabCaptureSource extends StreamSource {
  readonly kind = 'tab' as const;

  constructor(private readonly options: TabCaptureOptions) {
    super({ ...options.tap, listenAlong: options.listenAlong }, options.clock ?? null);
  }

  async start(): Promise<void> {
    const constraints = {
      audio: { mandatory: { chromeMediaSource: 'tab', chromeMediaSourceId: this.options.streamId } },
      video: false,
    } as unknown as MediaStreamConstraints;
    const stream = await navigator.mediaDevices.getUserMedia(constraints);
    await this.attach(stream, stream.getAudioTracks()[0]?.label ?? 'tab');
  }
}
