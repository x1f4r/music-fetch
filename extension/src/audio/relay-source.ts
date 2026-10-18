import { Emitter, type AudioSource, type AudioSourceEvent, type AudioSourceKind, type PcmChunk } from './types';

/**
 * A source that captures in another context and relays its PCM here: Firefox's element
 * capture runs in the tab's content script, and its microphone in the UI page that asked
 * (the event page can never use it, S2). The host sees an ordinary `AudioSource`.
 */
export class RelaySource implements AudioSource {
  private readonly pcm = new Emitter<PcmChunk>();
  private readonly events = new Emitter<AudioSourceEvent>();
  private ended = false;
  sampleRate = 0;
  label = '';
  frames = 0;

  constructor(
    readonly kind: AudioSourceKind,
    /** Ask the remote side to start; resolves once it captures. */
    private readonly open: () => Promise<{ sampleRate: number; label: string }>,
    /** Ask the remote side to stop. */
    private readonly close: () => void,
  ) {}

  async start(): Promise<void> {
    const started = await this.open();
    this.sampleRate = started.sampleRate;
    this.label = started.label;
    this.events.emit({ type: 'started', ...started });
  }

  stop(): void {
    if (this.ended) return;
    this.close();
    this.end({ type: 'ended', reason: 'stopped' });
  }

  /** A chunk from the remote side. */
  feed(chunk: PcmChunk): void {
    if (this.ended) return;
    this.frames += chunk.samples.length;
    this.pcm.emit(chunk);
  }

  /** An event from the remote side; `ended` ends this source too. */
  signal(event: AudioSourceEvent): void {
    if (this.ended || event.type === 'started') return;
    if (event.type === 'ended') this.end(event);
    else this.events.emit(event);
  }

  get isEnded(): boolean {
    return this.ended;
  }

  private end(event: Extract<AudioSourceEvent, { type: 'ended' }>) {
    this.ended = true;
    this.events.emit(event);
    this.pcm.clear();
  }

  onPcm(callback: (chunk: PcmChunk) => void): () => void {
    return this.pcm.on(callback);
  }

  onEvent(callback: (event: AudioSourceEvent) => void): () => void {
    return this.events.on(callback);
  }
}
