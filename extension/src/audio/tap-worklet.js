// The capture tap (an AudioWorklet processor). It mixes its input to mono and posts
// blocks of `block` frames (processorOptions, default 2048) to the main thread, with the
// index of the block's first frame. Loaded from the extension (extension pages) or from
// a Blob URL (content scripts: no web_accessible_resources entry pages could probe).
class MfTap extends AudioWorkletProcessor {
  constructor(options) {
    super();
    const opts = (options && options.processorOptions) || {};
    this.size = opts.block || 2048;
    this.buf = new Float32Array(this.size);
    this.n = 0;
    this.first = 0;
    this.running = true;
    this.port.onmessage = (event) => {
      if (event.data === 'stop') this.running = false;
    };
  }

  process(inputs) {
    const input = inputs[0];
    const frames = input && input.length ? input[0].length : 0;
    if (!frames) return this.running;
    const channels = input.length;
    for (let i = 0; i < frames; i++) {
      let v = 0;
      for (let c = 0; c < channels; c++) v += input[c][i];
      if (this.n === 0) this.first = currentFrame + i;
      this.buf[this.n++] = v / channels;
      if (this.n === this.size) {
        this.port.postMessage({ samples: this.buf, frame: this.first }, [this.buf.buffer]);
        this.buf = new Float32Array(this.size);
        this.n = 0;
      }
    }
    return this.running;
  }
}

registerProcessor('mf-tap', MfTap);
