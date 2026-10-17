//! Monophonic pitch tracking (SwiftF0 ONNX via tract), with a whistle path.
//!
//! On top of the reference model this adds three things, all causal so that
//! [`StreamingPitch`] and [`PitchTracker::analyze`] produce identical frames:
//!
//! - **Level normalization.** SwiftF0 loses voicing quickly below about −20 dBFS
//!   peak, so each 256-sample hop is scaled toward a −6 dBFS reference peak (the
//!   third-largest hop peak so far, which ignores clicks), by at most +40 dB.
//! - **Loudness gate.** The model reports any pitched sound; frames quieter than
//!   −60 dBFS, or 40 dB below the loudest frame of the last 2 s (plus the 160 ms
//!   lookahead), are unvoiced.
//! - **Whistle path.** Whistles sit above the model's 2093.75 Hz ceiling. When
//!   voiced frames sit above 1.2 kHz, pin at the ceiling, or the loud frames are
//!   pure tones above 1.2 kHz, the 2× upsampled audio is fed to the model as if it
//!   were 16 kHz (half speed); pitches are doubled and times halved. Whistles whose
//!   tones sit above 1.5 kHz go through 4× upsampling instead (quarter speed), so
//!   their upper notes stay well below the 2× path's 4.19 kHz ceiling.

use std::collections::VecDeque;
use std::sync::Arc;

use realfft::{RealFftPlanner, RealToComplex};

use crate::error::Result;
use crate::melody::swiftf0::{self, HOP, LOOKAHEAD_FRAMES, Model, RawFrame, RawStream, VOICED_CONFIDENCE};
use crate::model::PitchFrame;

pub use swiftf0::{FMAX_HZ, FMIN_HZ, SAMPLE_RATE};

/// Spacing of pitch frames.
pub const FRAME_MS: u64 = 16;

/// Hops are scaled toward this reference peak (−6 dBFS)...
const TARGET_PEAK: f32 = 0.5;
/// ...by at most +40 dB.
const MAX_GAIN: f32 = 100.0;
/// Frames quieter than this (original level) are unvoiced.
const ABS_GATE_DB: f32 = -60.0;
/// Frames this far below the loudest recent frame are unvoiced.
const REL_GATE_DB: f32 = 40.0;
/// Frames of history for the relative gate (2 s).
const GATE_HISTORY: usize = 125;
/// Loudness of frames with no audio.
const FLOOR_DB: f32 = -140.0;

/// Whistle evidence: voiced pitch above this...
const WHISTLE_HZ: f32 = 1200.0;
/// ...or pinned near the model ceiling...
const CEILING_HZ: f32 = 1950.0;
/// ...or a tone this pure (share of energy within ±2 bins of the peak).
const TONAL_SHARE: f32 = 0.5;
/// Whistles whose tonal peaks have a median above this use the 4× path.
const FOUR_X_HZ: f32 = 1500.0;
/// Minimum frames before deciding on a whistle.
const WHISTLE_MIN_FRAMES: usize = 8;
/// Frames the streaming tracker looks back over for whistle evidence (1 s).
const WHISTLE_WINDOW: usize = 62;
/// Frames the whistle path starts before the switch, so its edge effects never
/// reach emitted frames.
const WHISTLE_LEAD: usize = 12;
/// Streaming blocks: frames per model run. Worst-case latency is
/// (LOOKAHEAD_FRAMES + STREAM_BLOCK) × 16 ms = 224 ms after a frame's center.
const STREAM_BLOCK: usize = 4;
/// Half-band upsampler half-length (taps per side).
const UPSAMPLE_TAPS: usize = 16;
const TONAL_FFT: usize = 1024;

/// One analysis frame with the detail melody code needs beyond [`PitchFrame`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameDetail {
    pub t_ms: u64,
    /// Model pitch in Hz, also reported for unvoiced frames.
    pub hz: f32,
    /// Model confidence; 0 where the silence or loudness gate closed.
    pub confidence: f32,
    /// Level of the original input over the 32 ms ending with this frame's hop, dBFS.
    pub loudness_db: f32,
    pub voiced: bool,
}

impl FrameDetail {
    /// Pitch as a fractional MIDI note; NaN when unvoiced.
    pub fn midi(&self) -> f32 {
        if self.voiced { hz_to_midi(self.hz) } else { f32::NAN }
    }

    pub fn to_pitch_frame(&self) -> PitchFrame {
        PitchFrame { t_ms: self.t_ms, midi: self.midi(), confidence: self.confidence }
    }
}

/// Result of a batch analysis.
#[derive(Debug, Clone, Default)]
pub struct PitchAnalysis {
    pub frames: Vec<FrameDetail>,
    /// The whistle path was used.
    pub whistle: bool,
}

pub fn hz_to_midi(hz: f32) -> f32 {
    69.0 + 12.0 * (hz / 440.0).log2()
}

pub fn midi_to_hz(midi: f32) -> f32 {
    440.0 * 2f32.powf((midi - 69.0) / 12.0)
}

/// Batch pitch tracker. Cheap to share: wrap in an `Arc` and reuse.
pub struct PitchTracker {
    model: Arc<Model>,
    threads: usize,
    tonal_fft: Arc<dyn RealToComplex<f32>>,
}

impl PitchTracker {
    /// Load the embedded model (about 25 ms).
    pub fn new() -> Result<Self> {
        let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).min(4);
        Self::with_threads(threads)
    }

    /// Like [`new`](Self::new) with a fixed number of worker threads for batch analysis.
    pub fn with_threads(threads: usize) -> Result<Self> {
        let tonal_fft = RealFftPlanner::<f32>::new().plan_fft_forward(TONAL_FFT);
        Ok(Self { model: Arc::new(Model::load()?), threads: threads.max(1), tonal_fft })
    }

    /// Pitch frames for 16 kHz mono audio; `t_ms` is relative to the first sample.
    /// Returns no frames if the model fails (which it should not).
    pub fn analyze(&self, samples: &[f32]) -> Vec<PitchFrame> {
        match self.analyze_detailed(samples) {
            Ok(a) => a.frames.iter().map(FrameDetail::to_pitch_frame).collect(),
            Err(e) => {
                tracing::warn!("pitch analysis failed: {e}");
                Vec::new()
            }
        }
    }

    /// Frames with loudness and voicing detail, and whether the whistle path ran.
    pub fn analyze_detailed(&self, samples: &[f32]) -> Result<PitchAnalysis> {
        if samples.is_empty() {
            return Ok(PitchAnalysis::default());
        }
        let original = sanitize(samples);
        let scaled = normalize_level(&original);
        let raw = self.model.analyze(&scaled, self.threads)?;
        let loud = frame_loudness(&original, raw.len());
        let frames = gate(&raw, &loud);
        let mut vote = WhistleVote::default();
        for (f, frame) in frames.iter().enumerate() {
            let tonal = frame_loud_enough(&loud, f).then(|| tonal_peak(&*self.tonal_fft, &original, 0, f));
            vote.add(frame, tonal);
        }
        if !vote.is_whistle() {
            return Ok(PitchAnalysis { frames, whistle: false });
        }
        let factor = vote.factor();
        let mut up = scaled;
        for _ in 0..factor.trailing_zeros() {
            up = upsample_2x(&up);
        }
        let slow = self.model.analyze(&up, self.threads)?;
        let raw: Vec<RawFrame> = (0..raw.len()).map(|f| slowed_frame(slow.get(factor * f).copied(), factor)).collect();
        Ok(PitchAnalysis { frames: gate(&raw, &loud), whistle: true })
    }

    /// The reference package's frames (`SwiftF0.detect` at 16 kHz): no level
    /// normalization, loudness gate or whistle path.
    pub fn reference_frames(&self, samples: &[f32]) -> Result<Vec<RawFrame>> {
        self.model.analyze(&sanitize(samples), self.threads)
    }
}

/// Incremental tracker for live input. Emits each frame at most 224 ms after its
/// center. Frames are identical to [`PitchTracker::analyze`] on the same audio
/// unless the whistle decision differs: the stream switches to the whistle path
/// once the last second sounds like whistling and cannot revise earlier frames.
pub struct StreamingPitch {
    tracker: Arc<PitchTracker>,
    gain: LevelGain,
    /// Original samples of the hop being filled.
    pending: Vec<f32>,
    /// Original-level power of complete hops.
    hop_power: Vec<f64>,
    /// Original-level loudness per frame, filled as hops complete.
    loud: Vec<f32>,
    /// Scaled samples from absolute index `scaled_base` on (for a whistle start).
    scaled: Vec<f32>,
    scaled_base: usize,
    /// Original samples from absolute index `original_base` on (for the tonal check).
    original: Vec<f32>,
    original_base: usize,
    samples: usize,
    normal: RawStream,
    whistle: Option<WhistleStream>,
    /// Model frames waiting for the gate's lookahead.
    held: VecDeque<RawFrame>,
    /// Whistle evidence of recent frames.
    recent: VecDeque<(FrameDetail, Option<(f32, f32)>)>,
    emitted: usize,
    finished: bool,
}

impl StreamingPitch {
    pub fn new(tracker: Arc<PitchTracker>) -> Self {
        let normal = RawStream::new(tracker.model.clone(), STREAM_BLOCK);
        Self {
            tracker,
            gain: LevelGain::default(),
            pending: Vec::with_capacity(HOP),
            hop_power: Vec::new(),
            loud: Vec::new(),
            scaled: Vec::new(),
            scaled_base: 0,
            original: Vec::new(),
            original_base: 0,
            samples: 0,
            normal,
            whistle: None,
            held: VecDeque::new(),
            recent: VecDeque::new(),
            emitted: 0,
            finished: false,
        }
    }

    /// Push 16 kHz mono samples; returns frames that became final. `t_ms` counts
    /// from the first pushed sample.
    pub fn push(&mut self, samples: &[f32]) -> Vec<PitchFrame> {
        self.push_detailed(samples).iter().map(FrameDetail::to_pitch_frame).collect()
    }

    /// Like [`push`](Self::push), with loudness and voicing detail.
    pub fn push_detailed(&mut self, samples: &[f32]) -> Vec<FrameDetail> {
        if self.finished || samples.is_empty() {
            return Vec::new();
        }
        let samples = sanitize(samples);
        self.samples += samples.len();
        self.original.extend_from_slice(&samples);
        let mut scaled = Vec::with_capacity(samples.len() + HOP);
        let mut rest: &[f32] = &samples;
        while !rest.is_empty() {
            let take = rest.len().min(HOP - self.pending.len());
            self.pending.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.pending.len() == HOP {
                self.close_hop(&mut scaled);
            }
        }
        self.advance(scaled, false)
    }

    /// The input ended: returns the remaining frames.
    pub fn finish(&mut self) -> Vec<PitchFrame> {
        self.finish_detailed().iter().map(FrameDetail::to_pitch_frame).collect()
    }

    /// Like [`finish`](Self::finish), with loudness and voicing detail.
    pub fn finish_detailed(&mut self) -> Vec<FrameDetail> {
        if self.finished || self.samples == 0 {
            self.finished = true;
            return Vec::new();
        }
        let mut scaled = Vec::new();
        if !self.pending.is_empty() {
            // The partial hop is scaled like any hop; it only gets a frame of its own
            // when the whole signal is shorter than one hop.
            let g = self.gain.push(peak(&self.pending));
            scaled.extend(self.pending.iter().map(|x| x * g));
            if self.hop_power.is_empty() {
                let p = power(&self.pending);
                self.hop_power.push(p);
                self.loud.push(swiftf0::loudness_db(0.0, p));
            }
            self.pending.clear();
        }
        let frames = self.advance(scaled, true);
        self.finished = true;
        frames
    }

    /// True once the stream switched to the whistle path.
    pub fn is_whistle(&self) -> bool {
        self.whistle.is_some()
    }

    fn close_hop(&mut self, scaled: &mut Vec<f32>) {
        let g = self.gain.push(peak(&self.pending));
        scaled.extend(self.pending.iter().map(|x| x * g));
        let p = power(&self.pending);
        let prev = self.hop_power.last().copied().unwrap_or(0.0);
        self.hop_power.push(p);
        self.loud.push(swiftf0::loudness_db(prev, p));
        self.pending.clear();
    }

    fn advance(&mut self, scaled: Vec<f32>, last: bool) -> Vec<FrameDetail> {
        self.scaled.extend_from_slice(&scaled);
        let raw = match &mut self.whistle {
            Some(w) => w.push(&scaled, last),
            None if last => self.normal.push(&scaled).and_then(|mut r| {
                r.extend(self.normal.finish()?);
                Ok(r)
            }),
            None => self.normal.push(&scaled),
        };
        self.take_raw(raw);
        let mut out = self.release(last);
        if self.whistle.is_none() && !last && !out.is_empty() {
            for frame in &out {
                let f = (frame.t_ms / FRAME_MS) as usize;
                let tonal = frame_loud_enough(&self.loud, f)
                    .then(|| tonal_peak(&*self.tracker.tonal_fft, &self.original, self.original_base, f));
                self.recent.push_back((*frame, tonal));
                if self.recent.len() > WHISTLE_WINDOW {
                    self.recent.pop_front();
                }
            }
            let mut vote = WhistleVote::default();
            for (frame, tonal) in &self.recent {
                vote.add(frame, *tonal);
            }
            if vote.is_whistle() {
                self.start_whistle(vote.factor());
                out.extend(self.release(false));
            }
        }
        self.trim();
        out
    }

    fn take_raw(&mut self, raw: Result<Vec<RawFrame>>) {
        match raw {
            Ok(frames) => self.held.extend(frames),
            Err(e) => tracing::warn!("streaming pitch failed: {e}"),
        }
    }

    /// Gate and emit held frames whose lookahead loudness is known.
    fn release(&mut self, last: bool) -> Vec<FrameDetail> {
        let n = swiftf0::frame_count(self.samples);
        let mut out = Vec::new();
        while let Some(&r) = self.held.front() {
            let f = self.emitted;
            if f >= n && last {
                self.held.clear();
                break;
            }
            if !last && self.loud.len() <= f + LOOKAHEAD_FRAMES {
                break;
            }
            self.held.pop_front();
            out.push(gate_frame(&self.loud, f, r));
            self.emitted += 1;
        }
        out
    }

    /// Switch to the slowed-down path (`factor` 2 or 4) for frames from `self.emitted` on.
    fn start_whistle(&mut self, factor: usize) {
        let next = self.emitted + self.held.len();
        let start_frame = next.saturating_sub(WHISTLE_LEAD).max(self.scaled_base.div_ceil(HOP));
        let start = start_frame * HOP;
        let mut stream = WhistleStream {
            ups: (0..factor.trailing_zeros()).map(|_| Upsampler::default()).collect(),
            factor,
            raw: RawStream::new(self.tracker.model.clone(), factor * STREAM_BLOCK),
            first_frame: start_frame,
            seen: 0,
            from_frame: next,
        };
        let history = self.scaled.get(start - self.scaled_base..).unwrap_or(&[]).to_vec();
        let raw = stream.push(&history, false);
        self.take_raw(raw);
        self.whistle = Some(stream);
        self.recent.clear();
    }

    fn trim(&mut self) {
        // Scaled audio: enough to start the whistle path WHISTLE_LEAD frames back.
        let next = self.emitted + self.held.len();
        let keep = next.saturating_sub(WHISTLE_LEAD + 1) * HOP;
        if self.whistle.is_none() && keep > self.scaled_base + SAMPLE_RATE as usize {
            self.scaled.drain(..keep - self.scaled_base);
            self.scaled_base = keep;
        } else if self.whistle.is_some() {
            self.scaled.clear();
            self.scaled_base = self.samples;
        }
        // Original audio: the tonal window of the next frame to release.
        let keep = (self.emitted * HOP).saturating_sub(TONAL_FFT);
        if keep > self.original_base + SAMPLE_RATE as usize {
            self.original.drain(..keep - self.original_base);
            self.original_base = keep;
        }
    }
}

/// The slowed-down model fed with 2× or 4× upsampled scaled audio.
struct WhistleStream {
    /// One half-band stage per doubling.
    ups: Vec<Upsampler>,
    factor: usize,
    raw: RawStream,
    /// Original frame of slowed frame 0.
    first_frame: usize,
    /// Slowed frames received so far.
    seen: usize,
    /// First original frame to return.
    from_frame: usize,
}

impl WhistleStream {
    /// Feed scaled original-rate samples; returns frames from `from_frame` on, in order.
    fn push(&mut self, scaled: &[f32], last: bool) -> Result<Vec<RawFrame>> {
        let mut up = scaled.to_vec();
        for stage in &mut self.ups {
            let mut next = stage.push(&up);
            if last {
                next.extend(stage.finish());
            }
            up = next;
        }
        let raw = if last {
            let mut r = self.raw.push(&up)?;
            r.extend(self.raw.finish()?);
            r
        } else {
            self.raw.push(&up)?
        };
        let mut out = Vec::with_capacity(raw.len() / self.factor + 1);
        for r in raw {
            let g = self.seen;
            self.seen += 1;
            if g.is_multiple_of(self.factor) && self.first_frame + g / self.factor >= self.from_frame {
                out.push(slowed_frame(Some(r), self.factor));
            }
        }
        Ok(out)
    }
}

/// Per-hop gain toward the reference peak; causal and identical in both paths.
#[derive(Debug, Clone, Default)]
struct LevelGain {
    top: [f32; 3],
    hops: usize,
}

impl LevelGain {
    fn push(&mut self, peak: f32) -> f32 {
        if peak > self.top[2] {
            self.top[2] = peak;
            self.top.sort_by(|a, b| b.total_cmp(a));
        }
        self.hops += 1;
        let reference = self.top[self.hops.min(3) - 1];
        if reference <= 0.0 { MAX_GAIN } else { (TARGET_PEAK / reference).clamp(1.0, MAX_GAIN) }
    }
}

fn normalize_level(samples: &[f32]) -> Vec<f32> {
    let mut gain = LevelGain::default();
    let mut out = Vec::with_capacity(samples.len());
    for hop in samples.chunks(HOP) {
        let g = gain.push(peak(hop));
        out.extend(hop.iter().map(|x| x * g));
    }
    out
}

/// Non-finite samples become silence; absurd values are clipped.
fn sanitize(samples: &[f32]) -> Vec<f32> {
    samples.iter().map(|&x| if x.is_finite() { x.clamp(-4.0, 4.0) } else { 0.0 }).collect()
}

fn peak(hop: &[f32]) -> f32 {
    hop.iter().fold(0f32, |m, x| m.max(x.abs()))
}

fn power(hop: &[f32]) -> f64 {
    hop.iter().map(|&x| (x as f64) * (x as f64)).sum()
}

/// Original-level loudness of `n` frames (the reference formula).
fn frame_loudness(original: &[f32], n: usize) -> Vec<f32> {
    let mut prev = 0.0;
    (0..n)
        .map(|f| {
            let (_, p) = swiftf0::hop_stats(original, 0, f);
            let l = swiftf0::loudness_db(prev, p);
            prev = p;
            l
        })
        .collect()
}

/// Whether frame `f` passes the loudness gate, given loudness up to its lookahead.
fn frame_loud_enough(loud: &[f32], f: usize) -> bool {
    let Some(&level) = loud.get(f) else {
        return false;
    };
    let lo = f.saturating_sub(GATE_HISTORY);
    let hi = (f + LOOKAHEAD_FRAMES + 1).min(loud.len());
    let max = loud[lo..hi].iter().copied().fold(FLOOR_DB, f32::max);
    level >= ABS_GATE_DB && level >= max - REL_GATE_DB
}

fn gate_frame(loud: &[f32], f: usize, r: RawFrame) -> FrameDetail {
    let pass = frame_loud_enough(loud, f);
    FrameDetail {
        t_ms: f as u64 * FRAME_MS,
        hz: r.hz,
        confidence: if pass { r.confidence } else { 0.0 },
        loudness_db: loud.get(f).copied().unwrap_or(FLOOR_DB),
        voiced: pass && r.confidence >= VOICED_CONFIDENCE && r.hz.is_finite() && r.hz > 0.0,
    }
}

fn gate(raw: &[RawFrame], loud: &[f32]) -> Vec<FrameDetail> {
    raw.iter().enumerate().map(|(f, &r)| gate_frame(loud, f, r)).collect()
}

/// A frame of the model run on audio slowed down `factor` times, at the original pitch.
fn slowed_frame(r: Option<RawFrame>, factor: usize) -> RawFrame {
    match r {
        Some(r) => RawFrame { hz: r.hz * factor as f32, confidence: r.confidence, loudness_db: r.loudness_db },
        None => RawFrame { hz: 0.0, confidence: 0.0, loudness_db: FLOOR_DB },
    }
}

/// Counts whistle evidence over frames.
#[derive(Debug, Default)]
struct WhistleVote {
    voiced: usize,
    high: usize,
    pinned: usize,
    loud: usize,
    tonal_high: usize,
    /// Peak frequencies of the pure tones above `WHISTLE_HZ`.
    tonal_hz: Vec<f32>,
}

impl WhistleVote {
    /// `tonal` is the spectral peak and its energy share, for frames passing the gate.
    fn add(&mut self, frame: &FrameDetail, tonal: Option<(f32, f32)>) {
        if frame.voiced {
            self.voiced += 1;
            if frame.hz > WHISTLE_HZ {
                self.high += 1;
            }
            if frame.hz > CEILING_HZ {
                self.pinned += 1;
            }
        }
        if let Some((hz, share)) = tonal {
            self.loud += 1;
            if hz > WHISTLE_HZ && share >= TONAL_SHARE {
                self.tonal_high += 1;
                self.tonal_hz.push(hz);
            }
        }
    }

    fn is_whistle(&self) -> bool {
        let by_pitch =
            self.voiced >= WHISTLE_MIN_FRAMES && (2 * self.high >= self.voiced || 4 * self.pinned >= self.voiced);
        let by_tone = self.loud >= WHISTLE_MIN_FRAMES && 2 * self.tonal_high >= self.loud;
        by_pitch || by_tone
    }

    /// Upsampling factor for the whistle path: 4 when the tones sit high.
    fn factor(&self) -> usize {
        let mut hz = self.tonal_hz.clone();
        hz.sort_by(f32::total_cmp);
        match hz.get(hz.len() / 2) {
            Some(&median) if median > FOUR_X_HZ => 4,
            _ => 2,
        }
    }
}

/// Peak frequency in 100 Hz–7 kHz and the share of energy within ±2 bins of it, for
/// the 1024-sample Hann window centered on frame `f`. `samples[0]` is absolute
/// sample `offset`.
fn tonal_peak(fft: &dyn RealToComplex<f32>, samples: &[f32], offset: usize, f: usize) -> (f32, f32) {
    let mut input = fft.make_input_vec();
    let mut spectrum = fft.make_output_vec();
    let start = (f * HOP) as isize - (TONAL_FFT / 2) as isize - offset as isize;
    for (m, x) in input.iter_mut().enumerate() {
        let s = start + m as isize;
        let w = 0.5 - 0.5 * (2.0 * std::f32::consts::PI * m as f32 / TONAL_FFT as f32).cos();
        *x = if s >= 0 && (s as usize) < samples.len() { samples[s as usize] * w } else { 0.0 };
    }
    if fft.process(&mut input, &mut spectrum).is_err() {
        return (0.0, 0.0);
    }
    let bin_hz = SAMPLE_RATE as f32 / TONAL_FFT as f32;
    let lo = (100.0 / bin_hz) as usize;
    let hi = ((7000.0 / bin_hz) as usize).min(spectrum.len() - 1);
    let energy: Vec<f32> = spectrum.iter().map(|z| z.norm_sqr()).collect();
    let total: f32 = energy[lo..=hi].iter().sum();
    let Some(top) = (lo..=hi).max_by(|&a, &b| energy[a].total_cmp(&energy[b])) else {
        return (0.0, 0.0);
    };
    let around: f32 = energy[top.saturating_sub(2)..=(top + 2).min(energy.len() - 1)].iter().sum();
    (top as f32 * bin_hz, if total > 0.0 { around / total } else { 0.0 })
}

/// Half-band 2× interpolation filter: odd outputs from 2 × 16 Kaiser-windowed taps.
fn half_band_taps() -> [f32; 2 * UPSAMPLE_TAPS] {
    let beta = 8.0f64;
    let mut taps = [0f32; 2 * UPSAMPLE_TAPS];
    for (j, tap) in taps.iter_mut().enumerate() {
        // Tap j weighs input i + j - 15, which sits t = j - 15.5 samples from output i + ½.
        let t = j as f64 - (UPSAMPLE_TAPS as f64 - 0.5);
        let sinc = (std::f64::consts::PI * t).sin() / (std::f64::consts::PI * t);
        let r = t / UPSAMPLE_TAPS as f64;
        let w = bessel_i0(beta * (1.0 - r * r).max(0.0).sqrt()) / bessel_i0(beta);
        *tap = (sinc * w) as f32;
    }
    taps
}

fn bessel_i0(x: f64) -> f64 {
    let mut sum = 1.0;
    let mut term = 1.0;
    for k in 1..60 {
        term *= (x / (2.0 * k as f64)).powi(2);
        sum += term;
        if term < 1e-14 * sum {
            break;
        }
    }
    sum
}

/// 2× upsampling (zero outside the signal), as used by the whistle path.
pub fn upsample_2x(x: &[f32]) -> Vec<f32> {
    let mut up = Upsampler::default();
    let mut out = up.push(x);
    out.extend(up.finish());
    out
}

/// Streaming half-band upsampler; output lags input by 16 samples.
struct Upsampler {
    taps: [f32; 2 * UPSAMPLE_TAPS],
    /// Input from absolute index `base` on.
    buf: Vec<f32>,
    base: usize,
    /// Next input index whose output pair is due.
    next: usize,
}

impl Default for Upsampler {
    fn default() -> Self {
        Self { taps: half_band_taps(), buf: Vec::new(), base: 0, next: 0 }
    }
}

impl Upsampler {
    fn push(&mut self, x: &[f32]) -> Vec<f32> {
        self.buf.extend_from_slice(x);
        let total = self.base + self.buf.len();
        self.emit(total.saturating_sub(UPSAMPLE_TAPS), total)
    }

    fn finish(&mut self) -> Vec<f32> {
        let total = self.base + self.buf.len();
        let out = self.emit(total, total);
        self.buf.clear();
        self.base = total;
        out
    }

    /// Output pairs for inputs `next..upto`; samples at or after `total` are zero.
    fn emit(&mut self, upto: usize, total: usize) -> Vec<f32> {
        let mut out = Vec::with_capacity(2 * upto.saturating_sub(self.next));
        let at = |i: isize| -> f32 {
            if i < self.base as isize || i >= total as isize { 0.0 } else { self.buf[i as usize - self.base] }
        };
        for i in self.next..upto {
            let i = i as isize;
            out.push(at(i));
            let mut acc = 0f32;
            for (j, &h) in self.taps.iter().enumerate() {
                acc += h * at(i + j as isize - (UPSAMPLE_TAPS as isize - 1));
            }
            out.push(acc);
        }
        self.next = self.next.max(upto);
        // Keep what the taps of the next output reach back to.
        let keep = self.next.saturating_sub(UPSAMPLE_TAPS);
        if keep > self.base {
            self.buf.drain(..keep - self.base);
            self.base = keep;
        }
        out
    }
}

#[cfg(test)]
mod tests;
