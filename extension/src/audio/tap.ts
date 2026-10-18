import TAP_SOURCE from './tap-worklet.js?raw';
import { downmix, Rechunker } from './framer';
import { wallNow } from './types';

/**
 * How the tap is loaded:
 * - `url`: the packaged worklet file (extension pages, whose CSP allows only 'self');
 * - `blob`: the same source from a Blob URL (content scripts: it loads under strict page
 *   CSPs, S2, and needs no web_accessible_resources entry);
 * - `script`: a ScriptProcessorNode, the fallback where a worklet can't load.
 * MediaRecorder is never used: re-encoding loses peaks (S2: recall 0.47–0.74 vs 0.89).
 */
export type TapMode = 'url' | 'blob' | 'script';

export interface TapOptions {
  /**
   * Default 48 000: `sampleRate / rate` is then whole at 1–6× and 8× (S1). Where the
   * browser can't resample a stream into that rate the tap uses the default context rate.
   */
  sampleRate?: number;
  /** Frames per block. Default 2048 (43 ms at 48 kHz). */
  block?: number;
  /** In order of preference. Default `['url', 'blob', 'script']` with `workletUrl`, else `['blob', 'script']`. */
  modes?: TapMode[];
  /** The packaged worklet (from `tap-url.ts`, extension pages only). */
  workletUrl?: string;
  /**
   * Also play the stream. A captured tab is silent to the user unless its stream is
   * routed back ("This tab": listen along); a listen-through scan never is (S1).
   */
  listenAlong?: boolean;
}

export type BlockCallback = (samples: Float32Array, sampleRate: number, arrivalWallMs: number) => void;

let blobUrl: string | null = null;

function workletBlobUrl(): string {
  blobUrl ??= URL.createObjectURL(new Blob([TAP_SOURCE], { type: 'text/javascript' }));
  return blobUrl;
}

function newContext(sampleRate: number | undefined): AudioContext {
  return sampleRate ? new AudioContext({ sampleRate, latencyHint: 'playback' }) : new AudioContext({ latencyHint: 'playback' });
}

/** An AudioContext that turns a MediaStream into mono blocks. */
export class Tap {
  private closed = false;

  private constructor(
    private readonly ctx: AudioContext,
    private readonly nodes: AudioNode[],
    private readonly worklet: AudioWorkletNode | null,
    readonly mode: TapMode,
  ) {}

  get sampleRate(): number {
    return this.ctx.sampleRate;
  }

  get state(): AudioContextState {
    return this.ctx.state;
  }

  static async open(stream: MediaStream, options: TapOptions, onBlock: BlockCallback): Promise<Tap> {
    try {
      return await Tap.build(stream, options, options.sampleRate ?? 48_000, onBlock);
    } catch (error) {
      // Older engines refuse to connect a stream into a context of another rate.
      if (error instanceof DOMException && error.name === 'NotSupportedError') return Tap.build(stream, options, undefined, onBlock);
      throw error;
    }
  }

  private static async build(stream: MediaStream, options: TapOptions, sampleRate: number | undefined, onBlock: BlockCallback): Promise<Tap> {
    const ctx = newContext(sampleRate);
    try {
      const source = ctx.createMediaStreamSource(stream);
      // A silent path to the destination keeps the graph pulling audio through the tap.
      const sink = ctx.createGain();
      sink.gain.value = 0;
      sink.connect(ctx.destination);
      if (options.listenAlong) source.connect(ctx.destination);
      const block = options.block ?? 2048;
      const modes = options.modes ?? (options.workletUrl ? ['url', 'blob', 'script'] : ['blob', 'script']);
      let lastError: unknown = new Error('No tap mode to try.');
      for (const mode of modes) {
        try {
          if (mode === 'script') {
            const processor = ctx.createScriptProcessor(4096, 2, 1);
            const rechunk = new Rechunker(block, (b) => onBlock(b, ctx.sampleRate, wallNow()));
            processor.onaudioprocess = (event) => {
              const input = event.inputBuffer;
              const channels: Float32Array[] = [];
              for (let c = 0; c < input.numberOfChannels; c++) channels.push(input.getChannelData(c));
              rechunk.push(downmix(channels, input.length));
            };
            source.connect(processor);
            processor.connect(sink);
            if (ctx.state !== 'running') await ctx.resume().catch(() => undefined);
            return new Tap(ctx, [source, processor, sink], null, mode);
          }
          const url = mode === 'url' ? options.workletUrl : workletBlobUrl();
          if (!url) continue;
          await ctx.audioWorklet.addModule(url);
          const node = new AudioWorkletNode(ctx, 'mf-tap', {
            numberOfInputs: 1,
            numberOfOutputs: 1,
            outputChannelCount: [1],
            processorOptions: { block },
          });
          node.port.onmessage = (event: MessageEvent<{ samples: Float32Array }>) => onBlock(event.data.samples, ctx.sampleRate, wallNow());
          source.connect(node);
          node.connect(sink);
          if (ctx.state !== 'running') await ctx.resume().catch(() => undefined);
          return new Tap(ctx, [source, node, sink], node, mode);
        } catch (error) {
          lastError = error;
        }
      }
      throw lastError;
    } catch (error) {
      void ctx.close().catch(() => undefined);
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    try {
      this.worklet?.port.postMessage('stop');
    } catch {
      // gone
    }
    for (const node of this.nodes) {
      try {
        node.disconnect();
      } catch {
        // already disconnected
      }
    }
    await this.ctx.close().catch(() => undefined);
  }
}
