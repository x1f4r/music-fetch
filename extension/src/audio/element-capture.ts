import { MediaClock } from './media-clock';
import { StreamSource } from './stream-source';
import type { TapOptions } from './tap';

/** The clock of an element capture: anchored once, then the frame count (S2). */
export function elementClock(): MediaClock {
  return new MediaClock({ lagMs: 0, anchorOnly: true });
}

/** Why an element can't be captured. */
export class ElementCaptureError extends Error {
  constructor(
    readonly reason: 'CaptureBlocked' | 'NothingPlaying',
    message: string,
  ) {
    super(message);
    this.name = 'ElementCaptureError';
  }
}

type CapturableElement = HTMLMediaElement & { captureStream?: () => MediaStream };

/**
 * A media element's own output, captured in a content script (Firefox's "This tab", and
 * Chromium's fallback): `captureStream()` → tap. Firefox keeps the element audible
 * (and keeps capturing when it is muted, which makes the silent listen-through);
 * `mozCaptureStream()` would silence it for the user, so it is never used (S2).
 *
 * Media time comes from the frame count after one anchor (S2: arrival stamps err by
 * 0.1–0.5 s); the capture coordinator restarts the clock at seeks, stalls and ads.
 */
export class ElementCaptureSource extends StreamSource {
  readonly kind = 'element' as const;

  constructor(
    private readonly element: CapturableElement,
    /** Null for live listening (no media time); `elementClock()` for a listen-through. */
    readonly clock: MediaClock | null,
    tap: TapOptions = {},
  ) {
    super(tap, clock);
  }

  async start(): Promise<void> {
    const element = this.element;
    if (element.mediaKeys) throw new ElementCaptureError('CaptureBlocked', 'This media is protected (EME) and cannot be captured.');
    if (typeof element.captureStream !== 'function') throw new ElementCaptureError('CaptureBlocked', 'This browser cannot capture media elements.');
    let stream: MediaStream;
    try {
      stream = element.captureStream();
    } catch (error) {
      // Chromium: "Stream capture not supported with EME".
      throw new ElementCaptureError('CaptureBlocked', error instanceof Error ? error.message : String(error));
    }
    const audio = await audioTracks(stream, 3000);
    if (audio.length === 0) throw new ElementCaptureError('NothingPlaying', 'The element has no audio.');
    // Only the audio: a video track would keep the decoder busy for nothing.
    await this.attach(new MediaStream(audio), element.currentSrc || element.tagName.toLowerCase(), false);
  }
}

/** The stream's audio tracks; a paused element adds them once it plays. */
async function audioTracks(stream: MediaStream, timeoutMs: number): Promise<MediaStreamTrack[]> {
  if (stream.getAudioTracks().length > 0) return stream.getAudioTracks();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      stream.removeEventListener('addtrack', onAdd);
      resolve(stream.getAudioTracks());
    };
    const onAdd = () => {
      if (stream.getAudioTracks().length > 0) done();
    };
    const timer = setTimeout(done, timeoutMs);
    stream.addEventListener('addtrack', onAdd);
  });
}
