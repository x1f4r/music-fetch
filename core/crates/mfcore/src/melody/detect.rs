//! Decides whether live input is a person humming/whistling/singing rather than
//! recorded music.
//!
//! Features, accumulated over non-silent audio only:
//!
//! - from pitch frames: voiced share, mean confidence, pitch jumps and median
//!   frame-to-frame pitch change (one voice is continuous; a mix's dominant pitch
//!   hops between instruments);
//! - harmonicity: the share of spectral energy on the harmonics of the tracked
//!   pitch, beyond what a comb of that density catches by chance (a single voice
//!   puts nearly everything there; a mix does not);
//! - from 1024-point spectra every 16 ms, limited to 100 Hz–4 kHz so band-limited
//!   recordings look like full-band ones: spectral flatness, centroid, low-band
//!   share, onset rate from spectral flux (drums) and level spread (recorded
//!   music is compressed; hums breathe).
//!
//! A logistic model over these gives P(impression). The verdict is given early
//! when the model is sure, and always once 4 s of non-silent audio have been
//! seen; it then stays fixed.

use std::collections::VecDeque;
use std::sync::Arc;

use realfft::{RealFftPlanner, RealToComplex};

use crate::model::PitchFrame;

const SAMPLE_RATE: f32 = 16_000.0;
const FFT: usize = 1024;
const HOP: usize = 256;
const FRAME_MS: u64 = 16;
const BAND_LO_HZ: f32 = 100.0;
const BAND_HI_HZ: f32 = 4000.0;
/// Spectral frames quieter than this are silence (dBFS).
const SILENT_DB: f32 = -55.0;
/// Spectra kept for matching late pitch frames (about 1 s).
const SPECTRA_KEPT: usize = 64;
/// Non-silent audio needed before an early verdict, and for the final one (ms).
const EARLY_MS: u64 = 2_000;
const DECIDE_MS: u64 = 4_000;

/// Logistic model fitted by tools/eval/fit_detector.py on 419 impression clips
/// (MIR-QBSH, IOACAS, a cappella singing, humming, whistling) and 171 music clips
/// (31 Deezer previews across styles, a vocal-with-guitar recording). Feature
/// order as in [`ImpressionDetector::features`].
const BIAS: f32 = 0.6007;
const WEIGHTS: [f32; FEATURES] = [
    0.3115, 0.6635, -1.4740, 0.4474, -0.9339, 0.5461, 0.3128, -0.7360, 2.7320, 2.5383,
];
const MEANS: [f32; FEATURES] = [
    0.6764, 0.6588, 0.0152, 0.0842, -22.0864, 0.5510, 0.5047, 1.8003, 8.6728, 0.7686,
];
const SCALES: [f32; FEATURES] = [
    0.1743, 0.1468, 0.0261, 0.0413, 6.8719, 0.2406, 0.1932, 1.1306, 3.9445, 0.2617,
];
/// P(impression) needed for an impression verdict at the deadline; set for < 2%
/// music → impression errors in cross-validation.
const DECIDE_THRESHOLD: f32 = 0.7384;
/// Early verdicts need this much certainty.
const EARLY_CONFIDENCE: f32 = 0.93;

/// Number of model features.
pub const FEATURES: usize = 10;

pub struct ImpressionDetector {
    fft: Arc<dyn RealToComplex<f32>>,
    window: Vec<f32>,
    /// Audio not yet framed.
    pending: Vec<f32>,
    /// Spectral frames seen (including silent ones).
    spectra: u64,
    /// Recent band power spectra: (frame index, power).
    recent: VecDeque<(u64, Vec<f32>)>,
    prev_log_spectrum: Vec<f32>,
    flux: VecDeque<f32>,
    // Spectral sums over non-silent frames.
    loud_frames: u64,
    flatness: f64,
    centroid: f64,
    low_share: f64,
    level: f64,
    level_sq: f64,
    onsets: u64,
    // Pitch sums.
    sounding: u64,
    voiced: u64,
    confidence: f64,
    jumps: u64,
    steps: Vec<f32>,
    harmonic: f64,
    harmonic_frames: u64,
    last_midi: Option<f32>,
    last_t: Option<u64>,
    last_voiced_t: Option<u64>,
    verdict: Option<bool>,
}

impl Default for ImpressionDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl ImpressionDetector {
    pub fn new() -> Self {
        let window = (0..FFT)
            .map(|i| 0.5 - 0.5 * (2.0 * std::f32::consts::PI * i as f32 / FFT as f32).cos())
            .collect();
        Self {
            fft: RealFftPlanner::<f32>::new().plan_fft_forward(FFT),
            window,
            pending: Vec::with_capacity(FFT * 2),
            spectra: 0,
            recent: VecDeque::with_capacity(SPECTRA_KEPT + 1),
            prev_log_spectrum: Vec::new(),
            flux: VecDeque::with_capacity(33),
            loud_frames: 0,
            flatness: 0.0,
            centroid: 0.0,
            low_share: 0.0,
            level: 0.0,
            level_sq: 0.0,
            onsets: 0,
            sounding: 0,
            voiced: 0,
            confidence: 0.0,
            jumps: 0,
            steps: Vec::new(),
            harmonic: 0.0,
            harmonic_frames: 0,
            last_midi: None,
            last_t: None,
            last_voiced_t: None,
            verdict: None,
        }
    }

    /// Feed pitch frames and the matching 16 kHz audio chunk. Both timelines start
    /// at the session's first sample; pitch frames may lag the audio by up to a second.
    pub fn push(&mut self, frames: &[PitchFrame], samples: &[f32]) {
        for f in frames {
            self.push_frame(f);
        }
        self.pending
            .extend(samples.iter().map(|x| if x.is_finite() { *x } else { 0.0 }));
        let mut start = 0;
        while self.pending.len() - start >= FFT {
            let block: Vec<f32> = self.pending[start..start + FFT].to_vec();
            self.push_spectrum(&block);
            start += HOP;
        }
        self.pending.drain(..start);
        if self.verdict.is_none() {
            self.decide();
        }
    }

    /// Some(true) once the input is very likely an impression, Some(false) once it
    /// is very likely recorded music, None while undecided.
    pub fn verdict(&self) -> Option<bool> {
        self.verdict
    }

    /// Current P(impression), once there is anything to judge.
    pub fn probability(&self) -> Option<f32> {
        (self.loud_frames > 0 && self.sounding > 0).then(|| probability(&self.features()))
    }

    /// Total voiced duration seen so far.
    pub fn voiced_ms(&self) -> u64 {
        self.voiced * FRAME_MS
    }

    /// Milliseconds since the last voiced frame.
    pub fn silence_ms(&self) -> u64 {
        match (self.last_t, self.last_voiced_t) {
            (Some(t), Some(v)) => t.saturating_sub(v),
            (Some(t), None) => t + FRAME_MS,
            _ => 0,
        }
    }

    /// Non-silent audio seen so far.
    pub fn sounding_ms(&self) -> u64 {
        self.loud_frames * FRAME_MS
    }

    /// The model's input features (for calibration).
    pub fn features(&self) -> [f32; FEATURES] {
        let loud = self.loud_frames.max(1) as f64;
        let sounding = self.sounding.max(1) as f64;
        let mean_level = self.level / loud;
        let level_sd = (self.level_sq / loud - mean_level * mean_level)
            .max(0.0)
            .sqrt();
        let mut steps = self.steps.clone();
        let step = crate::melody::notes::median(&mut steps).unwrap_or(3.0);
        let pairs = self.steps.len().max(1) as f64;
        [
            (self.voiced as f64 / sounding) as f32,
            (self.confidence / sounding) as f32,
            (self.jumps as f64 / pairs) as f32,
            step.min(6.0),
            (self.flatness / loud) as f32,
            (self.centroid / loud / 1000.0) as f32,
            (self.low_share / loud) as f32,
            (self.onsets as f64 / (loud * FRAME_MS as f64 / 1000.0)) as f32,
            level_sd as f32,
            if self.harmonic_frames > 0 {
                (self.harmonic / self.harmonic_frames as f64) as f32
            } else {
                0.0
            },
        ]
    }

    fn push_frame(&mut self, f: &PitchFrame) {
        self.last_t = Some(self.last_t.map_or(f.t_ms, |t| t.max(f.t_ms)));
        if f.confidence > 0.0 {
            self.sounding += 1;
            self.confidence += f.confidence.clamp(0.0, 1.0) as f64;
        }
        if !f.midi.is_finite() {
            self.last_midi = None;
            return;
        }
        self.voiced += 1;
        self.last_voiced_t = Some(f.t_ms);
        if let Some(prev) = self.last_midi {
            let d = (f.midi - prev).abs();
            self.steps.push(d);
            if d > 1.5 {
                self.jumps += 1;
            }
        }
        self.last_midi = Some(f.midi);
        // The spectrum whose window is centered on this frame (window k spans
        // samples 256k..256k + 1024, centered on pitch frame k + 2).
        let k = (f.t_ms / FRAME_MS).checked_sub(2);
        if let Some(power) = k
            .and_then(|k| self.recent.iter().find(|(i, _)| *i == k))
            .map(|(_, p)| p)
        {
            let hz = 440.0 * 2f32.powf((f.midi - 69.0) / 12.0);
            if let Some(h) = harmonicity(power, hz) {
                self.harmonic += h as f64;
                self.harmonic_frames += 1;
            }
        }
    }

    fn push_spectrum(&mut self, block: &[f32]) {
        let index = self.spectra;
        self.spectra += 1;
        let mut input: Vec<f32> = block.iter().zip(&self.window).map(|(x, w)| x * w).collect();
        let mut spectrum = self.fft.make_output_vec();
        if self.fft.process(&mut input, &mut spectrum).is_err() {
            return;
        }
        let (lo, hi) = band();
        let power: Vec<f32> = spectrum[lo..=hi]
            .iter()
            .map(|z| z.norm_sqr() + 1e-12)
            .collect();
        let total: f32 = power.iter().sum();
        // Level of the analysed band relative to a full-scale sine.
        let level = 10.0 * (total / (FFT as f32 * FFT as f32 / 8.0)).log10();
        let log_spec: Vec<f32> = power.iter().map(|p| p.ln()).collect();
        let flux = if self.prev_log_spectrum.len() == log_spec.len() {
            log_spec
                .iter()
                .zip(&self.prev_log_spectrum)
                .map(|(a, b)| (a - b).max(0.0))
                .sum::<f32>()
                / log_spec.len() as f32
        } else {
            0.0
        };
        self.prev_log_spectrum = log_spec;
        if level < SILENT_DB {
            self.flux.clear();
            return;
        }
        self.loud_frames += 1;
        let n = power.len() as f32;
        let geo = (power.iter().map(|p| p.ln()).sum::<f32>() / n).exp();
        self.flatness += (10.0 * (geo / (total / n)).log10()) as f64;
        let bin_hz = SAMPLE_RATE / FFT as f32;
        let centroid: f32 = power
            .iter()
            .enumerate()
            .map(|(i, p)| (lo + i) as f32 * bin_hz * p)
            .sum::<f32>()
            / total;
        self.centroid += centroid as f64;
        let low_edge = ((300.0 / bin_hz) as usize)
            .saturating_sub(lo)
            .min(power.len());
        self.low_share += (power[..low_edge].iter().sum::<f32>() / total) as f64;
        self.level += level as f64;
        self.level_sq += (level * level) as f64;
        // Onset: a flux peak well above the recent median.
        self.flux.push_back(flux);
        if self.flux.len() > 32 {
            self.flux.pop_front();
        }
        let m = self.flux.len();
        if m >= 3 {
            let mut recent: Vec<f32> = self.flux.iter().copied().collect();
            let med = crate::melody::notes::median(&mut recent).unwrap_or(0.0);
            let (a, b, c) = (self.flux[m - 3], self.flux[m - 2], self.flux[m - 1]);
            if b > a && b >= c && b > med + 0.6 {
                self.onsets += 1;
            }
        }
        self.recent.push_back((index, power));
        if self.recent.len() > SPECTRA_KEPT {
            self.recent.pop_front();
        }
    }

    fn decide(&mut self) {
        let ms = self.sounding_ms();
        if ms < EARLY_MS || self.sounding == 0 {
            return;
        }
        let p = probability(&self.features());
        if p >= EARLY_CONFIDENCE {
            self.verdict = Some(true);
        } else if p <= 1.0 - EARLY_CONFIDENCE {
            self.verdict = Some(false);
        } else if ms >= DECIDE_MS {
            self.verdict = Some(p >= DECIDE_THRESHOLD);
        }
    }
}

/// FFT bins of the analysed band.
fn band() -> (usize, usize) {
    let bin_hz = SAMPLE_RATE / FFT as f32;
    (
        (BAND_LO_HZ / bin_hz) as usize,
        (BAND_HI_HZ / bin_hz) as usize,
    )
}

/// Energy share on the harmonics of `hz` in a band power spectrum, scaled so a
/// comb catching energy only by chance scores 0 and all energy on it scores 1.
fn harmonicity(power: &[f32], hz: f32) -> Option<f32> {
    if hz.is_nan() || hz <= 50.0 {
        return None;
    }
    let (lo, _) = band();
    let bin_hz = SAMPLE_RATE / FFT as f32;
    let total: f32 = power.iter().sum();
    let mut on = vec![false; power.len()];
    let mut k = 1.0f32;
    while k * hz < BAND_HI_HZ {
        let center = k * hz / bin_hz;
        let half = (center * 0.015).max(1.0);
        let a = (center - half).round().max(lo as f32) as usize;
        let b = (center + half).round() as usize;
        for i in a..=b {
            if let Some(slot) = i.checked_sub(lo).and_then(|j| on.get_mut(j)) {
                *slot = true;
            }
        }
        k += 1.0;
    }
    let covered = on.iter().filter(|&&x| x).count() as f32 / on.len().max(1) as f32;
    if total <= 0.0 || covered >= 0.95 {
        return None;
    }
    let share = power
        .iter()
        .zip(&on)
        .filter(|&(_, &x)| x)
        .map(|(p, _)| p)
        .sum::<f32>()
        / total;
    Some(((share - covered) / (1.0 - covered)).clamp(-1.0, 1.0))
}

/// P(impression) from features.
pub fn probability(x: &[f32; FEATURES]) -> f32 {
    let z = BIAS
        + (0..FEATURES)
            .map(|i| WEIGHTS[i] * (x[i] - MEANS[i]) / SCALES[i])
            .sum::<f32>();
    1.0 / (1.0 + (-z).exp())
}

#[cfg(test)]
mod tests;
