//! SwiftF0 0.3.0 (MIT, Lars Nieradzik): the embedded ONNX model and a Rust port of
//! the reference package's pre- and post-processing.
//!
//! The graph is a three-resolution STFT front end followed by a small CNN. tract
//! cannot type the front end for a symbolic length (its pad widths depend on the
//! input length), so the front end runs here and the graph is cut at its
//! log-spectrogram features. The CNN then accepts any number of frames, and since
//! its output for a frame does not depend on where the frame sits in the input,
//! chunked, parallel and streaming runs all produce the same frames: bit-identical
//! on arm64 and AVX2, within float rounding with AVX-512, where tract picks its
//! matmul kernels by tensor shape and so by the number of frames in a run.
//!
//! Frame `f` is centered on sample `256 f`. Its features need samples up to
//! `256 f + 1024`; the CNN looks 7 frames to either side, so a frame is final once
//! the audio reaches `256 (f + 11)` (the reference package's 10-frame lookahead).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use realfft::{RealFftPlanner, RealToComplex};
use tract_onnx::pb;
use tract_onnx::prelude::*;
use tract_onnx::tract_core::internal::{TractErrorContext, ensure};

use crate::error::{MfError, Result};

/// Model sample rate.
pub const SAMPLE_RATE: u32 = 16_000;
/// Samples per frame.
pub const HOP: usize = 256;
/// Lowest and highest pitch the model reports.
pub const FMIN_HZ: f32 = 46.875;
pub const FMAX_HZ: f32 = 2093.75;
/// Frames a frame waits for before it is final.
pub const LOOKAHEAD_FRAMES: usize = 10;
/// The reference voicing threshold.
pub const VOICED_CONFIDENCE: f32 = 0.5;

/// Frames whose hop peaks below this get confidence 0 (digital silence makes the
/// model answer with random voiced frames).
const SILENCE_PEAK: f32 = 1e-3;
/// CNN receptive field in frames on each side.
const CNN_CONTEXT: usize = 7;
/// Half of the largest STFT window, in hops.
const STFT_REACH: usize = 4;
const BANDS: usize = 128;
const CHANNELS: usize = 3;
/// Features per frame: three resolutions × 128 log bands.
pub(crate) const FEATURES: usize = CHANNELS * BANDS;
/// CNN input length used for batch chunks; outputs `CHUNK_FRAMES - 2 * CNN_CONTEXT`
/// final frames per run.
const CHUNK_FRAMES: usize = 142;
/// Concrete plans worth caching beyond the batch chunk (streaming block sizes).
const CACHED_PLAN_LIMIT: usize = 6;

static MODEL_ONNX: &[u8] = include_bytes!("models/swiftf0.onnx");

/// Feature vector of one frame, laid out `[channel][band]`.
pub(crate) type Features = [f32; FEATURES];

/// One model frame before any gating.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawFrame {
    pub hz: f32,
    /// Voicing confidence; 0 for digital silence.
    pub confidence: f32,
    /// Level of the 32 ms ending with this frame's hop, in dBFS.
    pub loudness_db: f32,
}

/// The loaded model: front-end constants and CNN plans.
pub(crate) struct Model {
    resolutions: Vec<Resolution>,
    eps: f32,
    proto: pb::ModelProto,
    symbolic: Arc<TypedSimplePlan>,
    concrete: Mutex<HashMap<usize, Arc<TypedSimplePlan>>>,
}

struct Resolution {
    size: usize,
    window: Vec<f32>,
    fft: Arc<dyn RealToComplex<f32>>,
    /// Sparse filterbank: (fft bin, band, weight).
    filters: Vec<(u16, u8, f32)>,
}

/// Per-thread FFT buffers.
pub(crate) struct Scratch {
    input: Vec<f32>,
    spectrum: Vec<realfft::num_complex::Complex<f32>>,
    fft: Vec<realfft::num_complex::Complex<f32>>,
    mags: Vec<f32>,
}

impl Model {
    pub fn load() -> Result<Self> {
        Self::load_inner().map_err(|e| {
            tracing::error!("pitch model: {e:#}");
            MfError::internal("the pitch model could not be loaded")
        })
    }

    fn load_inner() -> TractResult<Self> {
        let onnx = tract_onnx::onnx();
        let mut proto = onnx.proto_model_for_read(&mut &MODEL_ONNX[..])?;
        let graph = proto.graph.as_mut().context("model has no graph")?;
        fix_constant_of_shape(graph);
        let initializers: HashMap<&str, &pb::TensorProto> =
            graph.initializer.iter().map(|t| (t.name.as_str(), t)).collect();
        let tensor = |name: &str| -> TractResult<Tensor> {
            let proto = initializers.get(name).with_context(|| format!("missing initializer {name}"))?;
            tract_onnx::tensor::load_tensor(&*tract_onnx::data_resolver::default(), proto, None)
        };
        // (window, scatter indices, scatter values) of the 1024-, 2048- and 512-point
        // resolutions, in the graph's channel order.
        let mut planner = RealFftPlanner::<f32>::new();
        let mut resolutions = Vec::with_capacity(CHANNELS);
        for (size, window, index, value) in
            [(1024, "v41", "v123", "v124"), (2048, "v161", "v241", "v242"), (512, "v262", "v342", "v343")]
        {
            let window: Vec<f32> =
                tensor(window)?.cast_to::<f32>()?.to_plain_array_view::<f32>()?.iter().copied().collect();
            let index: Vec<i64> =
                tensor(index)?.cast_to::<i64>()?.to_plain_array_view::<i64>()?.iter().copied().collect();
            let value: Vec<f32> =
                tensor(value)?.cast_to::<f32>()?.to_plain_array_view::<f32>()?.iter().copied().collect();
            ensure!(window.len() == size && index.len() == value.len(), "unexpected front-end constants");
            let bins = size / 2 + 1;
            let mut filters = Vec::with_capacity(index.len());
            for (&i, &w) in index.iter().zip(&value) {
                let (bin, band) = ((i / BANDS as i64) as usize, (i % BANDS as i64) as usize);
                ensure!(i >= 0 && bin < bins, "filterbank index out of range");
                filters.push((bin as u16, band as u8, w));
            }
            filters.sort_by_key(|&(bin, band, _)| (bin, band));
            resolutions.push(Resolution { size, window, fft: planner.plan_fft_forward(size), filters });
        }
        let eps =
            tensor("v129")?.cast_to::<f32>()?.to_plain_array_view::<f32>()?.iter().copied().next().unwrap_or(1e-8);
        cut_at_features(graph, "v352")?;
        let symbolic = build_plan(&proto, None)?;
        Ok(Self { resolutions, eps, proto, symbolic, concrete: Mutex::new(HashMap::new()) })
    }

    pub fn scratch(&self) -> Scratch {
        let largest = self.resolutions.iter().map(|r| r.size).max().unwrap_or(2048);
        let fft_scratch = self.resolutions.iter().map(|r| r.fft.get_scratch_len()).max().unwrap_or(0);
        Scratch {
            input: vec![0.0; largest],
            spectrum: vec![Default::default(); largest / 2 + 1],
            fft: vec![Default::default(); fft_scratch],
            mags: vec![0.0; largest / 2 + 1],
        }
    }

    /// Features of frame `frame` from `samples` (zero outside the slice). `offset` is
    /// the absolute sample index of `samples[0]`.
    pub fn features(&self, samples: &[f32], offset: usize, frame: usize, scratch: &mut Scratch, out: &mut Features) {
        let center = (frame * HOP) as isize - offset as isize;
        for (c, res) in self.resolutions.iter().enumerate() {
            let half = (res.size / 2) as isize;
            let input = &mut scratch.input[..res.size];
            for (m, (x, w)) in input.iter_mut().zip(&res.window).enumerate() {
                let s = center - half + m as isize;
                *x = if s >= 0 && (s as usize) < samples.len() { samples[s as usize] * w } else { 0.0 };
            }
            let bins = res.size / 2 + 1;
            let spectrum = &mut scratch.spectrum[..bins];
            let fft_scratch = &mut scratch.fft[..res.fft.get_scratch_len()];
            // Lengths always match the plan, so this cannot fail.
            let _ = res.fft.process_with_scratch(input, spectrum, fft_scratch);
            let mags = &mut scratch.mags[..bins];
            for (m, z) in mags.iter_mut().zip(spectrum.iter()) {
                *m = (z.re * z.re + z.im * z.im).sqrt();
            }
            let bands = &mut out[c * BANDS..(c + 1) * BANDS];
            bands.fill(0.0);
            for &(bin, band, w) in &res.filters {
                bands[band as usize] += mags[bin as usize] * w;
            }
            for b in bands.iter_mut() {
                *b = (*b + self.eps).ln();
            }
        }
    }

    /// Pitch (Hz) and confidence for every frame of `feats`. Frames within 7 of either
    /// end are computed as if the signal ended there.
    pub fn cnn(&self, feats: &[Features]) -> Result<(Vec<f32>, Vec<f32>)> {
        let t = feats.len();
        if t == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut x = vec![0f32; FEATURES * t];
        for (j, f) in feats.iter().enumerate() {
            for (k, &v) in f.iter().enumerate() {
                x[k * t + j] = v;
            }
        }
        let plan = self.plan(t);
        let run = || -> TractResult<(Vec<f32>, Vec<f32>)> {
            let input = Tensor::from_shape(&[1, CHANNELS, BANDS, t], &x)?;
            let out = plan.run(tvec!(input.into(), tensor0(FMIN_HZ).into(), tensor0(FMAX_HZ).into()))?;
            let pitch = out[0].cast_to::<f32>()?.to_plain_array_view::<f32>()?.iter().copied().collect::<Vec<_>>();
            let conf = out[1].cast_to::<f32>()?.to_plain_array_view::<f32>()?.iter().copied().collect::<Vec<_>>();
            ensure!(pitch.len() == t && conf.len() == t, "unexpected model output");
            Ok((pitch, conf))
        };
        run().map_err(|e| {
            tracing::error!("pitch model run: {e:#}");
            MfError::internal("pitch tracking failed")
        })
    }

    /// A plan for `t` frames: cached concrete plans for the sizes the tracker uses
    /// repeatedly (about twice as fast), the symbolic plan otherwise.
    fn plan(&self, t: usize) -> Arc<TypedSimplePlan> {
        if let Some(plan) = self.concrete.lock().get(&t) {
            return plan.clone();
        }
        let wanted = t == CHUNK_FRAMES || self.concrete.lock().len() < CACHED_PLAN_LIMIT && t <= 32;
        if wanted && let Ok(plan) = build_plan(&self.proto, Some(t)) {
            self.concrete.lock().insert(t, plan.clone());
            return plan;
        }
        self.symbolic.clone()
    }

    /// Frames for a whole signal, replicating `SwiftF0.detect` at 16 kHz.
    ///
    /// The signal is processed in fixed-size overlapping chunks (features and CNN
    /// together, so memory stays bounded), in parallel on up to `threads` threads.
    /// Each chunk keeps the frames that have the full context or touch a signal
    /// edge; the last chunk is aligned to the end.
    pub fn analyze(&self, samples: &[f32], threads: usize) -> Result<Vec<RawFrame>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }
        let n = frame_count(samples.len());
        let t = n.min(CHUNK_FRAMES);
        let step = CHUNK_FRAMES - 2 * CNN_CONTEXT;
        let mut starts: Vec<usize> = (0..).map(|k| k * step).take_while(|&a| a + CHUNK_FRAMES < n).collect();
        starts.push(n - t);
        let run = |a: usize, scratch: &mut Scratch| -> CnnOutput {
            let mut feats = vec![[0f32; FEATURES]; t];
            for (j, out) in feats.iter_mut().enumerate() {
                self.features(samples, 0, a + j, scratch, out);
            }
            self.cnn(&feats)
        };
        let next = std::sync::atomic::AtomicUsize::new(0);
        let slots: Vec<Mutex<Option<CnnOutput>>> = starts.iter().map(|_| Mutex::new(None)).collect();
        let work = || {
            let mut scratch = self.scratch();
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(&a) = starts.get(i) else { break };
                *slots[i].lock() = Some(run(a, &mut scratch));
            }
        };
        // wasm32 without the atomics target feature has no threads (spawning panics).
        let threads = if cfg!(all(target_family = "wasm", not(target_feature = "atomics"))) {
            1
        } else {
            threads.clamp(1, 16).min(starts.len())
        };
        if threads <= 1 {
            work();
        } else {
            std::thread::scope(|s| {
                for _ in 0..threads {
                    s.spawn(work);
                }
            });
        }
        let mut hz = vec![0f32; n];
        let mut conf = vec![0f32; n];
        for (&a, slot) in starts.iter().zip(slots) {
            let (h, c) = slot.into_inner().unwrap_or_else(|| Err(MfError::internal("pitch tracking failed")))?;
            let lo = if a == 0 { 0 } else { a + CNN_CONTEXT };
            let hi = if a + t == n { n } else { a + t - CNN_CONTEXT };
            hz[lo..hi].copy_from_slice(&h[lo - a..hi - a]);
            conf[lo..hi].copy_from_slice(&c[lo - a..hi - a]);
        }
        Ok(postprocess(samples, 0, 0, &hz, &conf))
    }
}

/// Pitch and confidence per frame.
type CnnOutput = Result<(Vec<f32>, Vec<f32>)>;

/// Number of frames the reference produces for `len` samples.
pub fn frame_count(len: usize) -> usize {
    (len / HOP).max(1)
}

/// Silence gate and loudness for frames `first..first + hz.len()`, as the reference
/// package computes them from the model input. `samples[0]` is absolute sample `offset`.
pub(crate) fn postprocess(samples: &[f32], offset: usize, first: usize, hz: &[f32], conf: &[f32]) -> Vec<RawFrame> {
    hz.iter()
        .zip(conf)
        .enumerate()
        .map(|(j, (&hz, &conf))| {
            let f = first + j;
            let (peak, power) = hop_stats(samples, offset, f);
            let prev = if f == 0 { 0.0 } else { hop_stats(samples, offset, f - 1).1 };
            RawFrame {
                hz,
                confidence: if peak < SILENCE_PEAK { 0.0 } else { conf },
                loudness_db: loudness_db(prev, power),
            }
        })
        .collect()
}

/// Peak and power of hop `f` (a short signal's only hop is the whole signal).
pub(crate) fn hop_stats(samples: &[f32], offset: usize, f: usize) -> (f32, f64) {
    let start = (f * HOP).saturating_sub(offset).min(samples.len());
    let end = (f * HOP + HOP).saturating_sub(offset).min(samples.len());
    let hop = &samples[start..end];
    let peak = hop.iter().fold(0f32, |m, x| m.max(x.abs()));
    let power = hop.iter().map(|&x| (x as f64) * (x as f64)).sum();
    (peak, power)
}

/// The reference loudness: RMS of the previous and the current hop, in dB.
pub(crate) fn loudness_db(prev_power: f64, power: f64) -> f32 {
    (20.0 * ((prev_power + power) / (2 * HOP) as f64).sqrt().max(1e-7).log10()) as f32
}

/// Incremental runner: feed model-rate samples, get final frames in order.
pub(crate) struct RawStream {
    model: Arc<Model>,
    scratch: Scratch,
    /// Samples from absolute index `base` on.
    buf: Vec<f32>,
    base: usize,
    total: usize,
    /// Features of frames from `feat_base` on.
    feats: Vec<Features>,
    feat_base: usize,
    emitted: usize,
    block: usize,
}

impl RawStream {
    pub fn new(model: Arc<Model>, block: usize) -> Self {
        let scratch = model.scratch();
        Self {
            model,
            scratch,
            buf: Vec::new(),
            base: 0,
            total: 0,
            feats: Vec::new(),
            feat_base: 0,
            emitted: 0,
            block: block.max(1),
        }
    }

    pub fn push(&mut self, samples: &[f32]) -> Result<Vec<RawFrame>> {
        self.buf.extend_from_slice(samples);
        self.total += samples.len();
        // Frame g needs samples up to 256 g + 1024 for its features.
        let ready = (self.total / HOP).saturating_sub(STFT_REACH - 1);
        self.compute_features(ready);
        let finalizable = ready.saturating_sub(CNN_CONTEXT);
        if finalizable < self.emitted + self.block {
            return Ok(Vec::new());
        }
        let frames = self.run(finalizable, ready)?;
        self.trim();
        Ok(frames)
    }

    /// Final frames of the signal (the stream cannot be pushed to afterwards).
    pub fn finish(&mut self) -> Result<Vec<RawFrame>> {
        if self.total == 0 {
            return Ok(Vec::new());
        }
        let n = frame_count(self.total);
        self.compute_features(n);
        let frames = self.run(n, n)?;
        self.buf.clear();
        self.feats.clear();
        Ok(frames)
    }

    fn compute_features(&mut self, upto: usize) {
        let have = self.feat_base + self.feats.len();
        for f in have..upto {
            let mut out = [0f32; FEATURES];
            self.model.features(&self.buf, self.base, f, &mut self.scratch, &mut out);
            self.feats.push(out);
        }
    }

    /// Emit frames `emitted..upto` using features up to `available`.
    fn run(&mut self, upto: usize, available: usize) -> Result<Vec<RawFrame>> {
        if upto <= self.emitted {
            return Ok(Vec::new());
        }
        let lo = self.emitted.saturating_sub(CNN_CONTEXT).max(self.feat_base);
        let hi = (upto + CNN_CONTEXT).min(available);
        let (hz, conf) = self.model.cnn(&self.feats[lo - self.feat_base..hi - self.feat_base])?;
        let skip = self.emitted - lo;
        let take = upto - self.emitted;
        let frames = postprocess(&self.buf, self.base, self.emitted, &hz[skip..skip + take], &conf[skip..skip + take]);
        self.emitted = upto;
        Ok(frames)
    }

    /// Drop samples and features no future frame needs.
    fn trim(&mut self) {
        let keep_frame = self.emitted.saturating_sub(CNN_CONTEXT);
        if keep_frame > self.feat_base {
            self.feats.drain(..keep_frame - self.feat_base);
            self.feat_base = keep_frame;
        }
        // Features of the next frames need samples from (f - STFT_REACH) hops; loudness
        // needs the previous hop of the next emitted frame.
        let next_feature = self.feat_base + self.feats.len();
        let keep_sample = (next_feature.saturating_sub(STFT_REACH) * HOP).min(self.emitted.saturating_sub(1) * HOP);
        if keep_sample > self.base {
            self.buf.drain(..keep_sample - self.base);
            self.base = keep_sample;
        }
    }
}

/// ONNX says ConstantOfShape defaults to a float32 zero; tract assumes f64.
fn fix_constant_of_shape(graph: &mut pb::GraphProto) {
    for node in graph.node.iter_mut() {
        if node.op_type == "ConstantOfShape" && !node.attribute.iter().any(|a| a.name == "value") {
            node.attribute.push(pb::AttributeProto {
                name: "value".into(),
                r#type: pb::attribute_proto::AttributeType::Tensor as i32,
                t: Some(pb::TensorProto {
                    dims: vec![1],
                    data_type: pb::tensor_proto::DataType::Float as i32,
                    float_data: vec![0.0],
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }
}

/// Make `cut` the first graph input (replacing `audio`) and drop everything only the
/// front end needed.
fn cut_at_features(graph: &mut pb::GraphProto, cut: &str) -> TractResult<()> {
    ensure!(graph.node.iter().any(|n| n.output.iter().any(|o| o == cut)), "feature tensor not found");
    let mut needed: HashSet<String> = graph.output.iter().map(|o| o.name.clone()).collect();
    let mut keep = vec![false; graph.node.len()];
    for (i, node) in graph.node.iter().enumerate().rev() {
        if node.output.iter().any(|o| needed.contains(o)) && !node.output.iter().any(|o| o == cut) {
            keep[i] = true;
            needed.extend(node.input.iter().cloned());
        }
    }
    let nodes = std::mem::take(&mut graph.node);
    graph.node = nodes.into_iter().zip(keep).filter_map(|(n, k)| k.then_some(n)).collect();
    graph.initializer.retain(|t| needed.contains(&t.name));
    let mut input = graph.input.iter().find(|i| i.name == "audio").cloned().context("audio input not found")?;
    input.name = cut.into();
    graph.input.retain(|i| i.name != "audio");
    graph.input.insert(0, input);
    Ok(())
}

fn build_plan(proto: &pb::ModelProto, frames: Option<usize>) -> TractResult<Arc<TypedSimplePlan>> {
    let mut model = tract_onnx::onnx().model_for_proto_model(proto)?;
    let fact = match frames {
        Some(t) => f32::fact([1, CHANNELS, BANDS, t]),
        None => {
            let t = model.symbols.sym("T");
            f32::fact(&[1.to_dim(), CHANNELS.to_dim(), BANDS.to_dim(), t.to_dim()])
        }
    };
    model.set_input_fact(0, fact.into())?;
    model.set_input_fact(1, f32::scalar_fact().into())?;
    model.set_input_fact(2, f32::scalar_fact().into())?;
    model.into_optimized()?.into_runnable()
}
