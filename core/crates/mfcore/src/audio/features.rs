//! Frame features for segmentation and voice/music classification.
//!
//! Each frame summarizes `[t_ms, t_ms + hop_ms)` of 16 kHz mono audio.
//! Time-domain features (RMS, zero-crossing rate) use every sample of the hop.
//! Spectral features come from a 2048-point Hann STFT (realfft) and are
//! averaged over sub-frames spaced at most 1024 samples apart, so a 1 s hop
//! describes the whole second rather than one 128 ms window.

use std::f32::consts::PI;
use std::sync::Arc;

use realfft::num_complex::Complex32;
use realfft::{RealFftPlanner, RealToComplex};

use crate::audio::{PcmFile, SAMPLE_RATE};
use crate::error::{MfError, Result};
use crate::util::cancel::CancelToken;

/// STFT window length in samples (128 ms).
pub const WINDOW: usize = 2048;
/// Largest spacing between STFT sub-frames inside one output frame.
const MAX_SUB_HOP: usize = 1024;
const BINS: usize = WINDOW / 2 + 1;
const MEL_BANDS: usize = 40;
const MFCC_COEFFS: usize = 13;
/// Chroma uses bins between C2 and C8; lower notes are narrower than an FFT bin.
const CHROMA_MIN_HZ: f32 = 65.0;
const CHROMA_MAX_HZ: f32 = 4200.0;
/// Log compression for spectral flux.
const FLUX_GAMMA: f32 = 1000.0;
/// Mean bin power below which a sub-frame counts as digital silence.
const SILENCE_POWER: f32 = 1e-12;
/// Audio read per batch when streaming from a file.
const BATCH_SAMPLES: usize = 30 * SAMPLE_RATE as usize;

#[derive(Debug, Clone)]
pub struct FrameFeatures {
    /// Start of the frame.
    pub t_ms: u64,
    /// Root mean square level, linear (full scale sine ≈ 0.707).
    pub rms: f32,
    /// Spectral flatness 0..=1: ~0 for tones, ~0.5 for white noise, 0 for silence.
    pub flatness: f32,
    /// Zero crossings per sample, 0..=1.
    pub zcr: f32,
    /// Spectral centroid in Hz (0 for silence).
    pub centroid_hz: f32,
    /// Onset strength: mean positive log-spectral flux per bin.
    pub onset: f32,
    /// Pitch-class energy, index 0 = C, normalized to a maximum of 1 (all 0 for silence).
    pub chroma: [f32; 12],
    /// MFCCs from 40 mel bands (natural-log energies, orthonormal DCT-II).
    pub mfcc: [f32; 13],
}

/// Features every `hop_ms` over the whole file.
pub fn compute(pcm: &PcmFile, hop_ms: u32, cancel: &CancelToken) -> Result<Vec<FrameFeatures>> {
    let mut source = FileSource { pcm, block_start: 0, block: Vec::new(), scratch: Vec::new() };
    analyze(pcm.len_samples(), hop_ms, cancel, |start, out| source.fill(start, out))
}

/// Features every `hop_ms` over in-memory 16 kHz mono samples (-1..=1).
pub fn compute_samples(samples: &[f32], hop_ms: u32) -> Result<Vec<FrameFeatures>> {
    let cancel = CancelToken::new();
    analyze(samples.len() as u64, hop_ms, &cancel, |start, out| {
        fill_from(samples, start, out);
        Ok(())
    })
}

/// Copy `samples[start..start + out.len()]` into `out`, zero outside the slice.
fn fill_from(samples: &[f32], start: i64, out: &mut [f32]) {
    for (i, o) in out.iter_mut().enumerate() {
        let at = start + i as i64;
        *o = if at >= 0 { samples.get(at as usize).copied().unwrap_or(0.0) } else { 0.0 };
    }
}

/// Reads a PCM file in large blocks.
struct FileSource<'a> {
    pcm: &'a PcmFile,
    block_start: i64,
    block: Vec<f32>,
    scratch: Vec<i16>,
}

impl FileSource<'_> {
    fn fill(&mut self, start: i64, out: &mut [f32]) -> Result<()> {
        let end = start + out.len() as i64;
        let cached_end = self.block_start + self.block.len() as i64;
        if start < self.block_start || end > cached_end {
            self.block_start = start.max(0);
            let len = out.len().max(BATCH_SAMPLES);
            self.pcm.read_samples_into(self.block_start as u64, len, &mut self.scratch)?;
            self.block.clear();
            self.block.extend(self.scratch.iter().map(|&s| s as f32 / 32768.0));
            // Past the end of the file: keep the block covering the request.
            self.block.resize(len, 0.0);
        }
        for (i, o) in out.iter_mut().enumerate() {
            let at = start + i as i64 - self.block_start;
            *o = if at >= 0 { self.block.get(at as usize).copied().unwrap_or(0.0) } else { 0.0 };
        }
        Ok(())
    }
}

fn analyze(
    len_samples: u64,
    hop_ms: u32,
    cancel: &CancelToken,
    mut fill: impl FnMut(i64, &mut [f32]) -> Result<()>,
) -> Result<Vec<FrameFeatures>> {
    if hop_ms == 0 || hop_ms > 600_000 {
        return Err(MfError::invalid(format!("feature hop must be 1..=600000 ms, got {hop_ms}")));
    }
    let hop = (hop_ms as usize * SAMPLE_RATE as usize / 1000).max(1);
    let frames = len_samples.div_ceil(hop as u64) as usize;
    let mut analyzer = Analyzer::new();
    let mut out = Vec::with_capacity(frames);
    let mut span = vec![0f32; hop + WINDOW];
    for index in 0..frames {
        if index % 64 == 0 {
            cancel.check()?;
        }
        let start = index as u64 * hop as u64;
        let len = (len_samples - start).min(hop as u64) as usize;
        // The hop plus half a window on each side.
        fill(start as i64 - (WINDOW / 2) as i64, &mut span)?;
        out.push(analyzer.frame(&span, len, hop, start * 1000 / SAMPLE_RATE as u64));
    }
    Ok(out)
}

struct MelFilter {
    first_bin: usize,
    weights: Vec<f32>,
}

/// Precomputed STFT, filterbanks and scratch buffers.
struct Analyzer {
    fft: Arc<dyn RealToComplex<f32>>,
    window: Vec<f32>,
    input: Vec<f32>,
    spectrum: Vec<Complex32>,
    scratch: Vec<Complex32>,
    magnitude: Vec<f32>,
    power: Vec<f32>,
    log_mag: Vec<f32>,
    prev_log_mag: Vec<f32>,
    mel: Vec<MelFilter>,
    /// (bin, pitch class, weight) pairs.
    chroma_map: Vec<(usize, usize, f32)>,
    dct: Vec<[f32; MEL_BANDS]>,
    bin_hz: Vec<f32>,
}

impl Analyzer {
    fn new() -> Self {
        let fft = RealFftPlanner::<f32>::new().plan_fft_forward(WINDOW);
        let input = fft.make_input_vec();
        let spectrum = fft.make_output_vec();
        let scratch = fft.make_scratch_vec();
        // Periodic Hann.
        let window = (0..WINDOW).map(|i| 0.5 - 0.5 * (2.0 * PI * i as f32 / WINDOW as f32).cos()).collect();
        let bin_hz = (0..BINS).map(|k| k as f32 * SAMPLE_RATE as f32 / WINDOW as f32).collect();
        Self {
            fft,
            window,
            input,
            spectrum,
            scratch,
            magnitude: vec![0.0; BINS],
            power: vec![0.0; BINS],
            log_mag: vec![0.0; BINS],
            prev_log_mag: vec![0.0; BINS],
            mel: mel_filters(),
            chroma_map: chroma_map(),
            dct: dct_matrix(),
            bin_hz,
        }
    }

    /// Features of one frame. `span` holds the hop with `WINDOW / 2` samples of
    /// context on each side; `len` samples of the hop are inside the audio.
    fn frame(&mut self, span: &[f32], len: usize, hop: usize, t_ms: u64) -> FrameFeatures {
        let samples = &span[WINDOW / 2..WINDOW / 2 + len];
        let rms = if samples.is_empty() {
            0.0
        } else {
            (samples.iter().map(|s| s * s).sum::<f32>() / samples.len() as f32).sqrt()
        };
        let zcr = if samples.len() < 2 {
            0.0
        } else {
            let crossings = samples.windows(2).filter(|w| (w[0] >= 0.0) != (w[1] >= 0.0)).count();
            crossings as f32 / (samples.len() - 1) as f32
        };

        // Windows lie inside the hop when it is long enough, so an onset at a
        // frame boundary belongs to the frame it starts; shorter hops use one
        // window centered on the hop. Offsets are window starts within `span`
        // (which begins half a window before the hop).
        let mut sums = Sums::default();
        if hop >= WINDOW {
            let room = hop - WINDOW;
            let count = room.div_ceil(MAX_SUB_HOP) + 1;
            for j in 0..count {
                let start = if count == 1 { 0 } else { j * room / (count - 1) };
                // A short last frame still gets one window.
                if j > 0 && start >= len {
                    break;
                }
                let offset = start + WINDOW / 2;
                self.spectral(&span[offset..offset + WINDOW], &mut sums);
            }
        } else {
            let offset = hop / 2;
            self.spectral(&span[offset..offset + WINDOW], &mut sums);
        }
        sums.finish(rms, zcr, t_ms)
    }

    fn spectral(&mut self, frame: &[f32], sums: &mut Sums) {
        for ((dst, &x), &w) in self.input.iter_mut().zip(frame).zip(&self.window) {
            *dst = x * w;
        }
        if self.fft.process_with_scratch(&mut self.input, &mut self.spectrum, &mut self.scratch).is_err() {
            return;
        }
        // Full-scale sine → magnitude ~1.
        let norm = 2.0 / (WINDOW as f32 / 2.0);
        let mut total_power = 0.0f32;
        let mut total_mag = 0.0f32;
        let mut weighted_hz = 0.0f32;
        let mut log_power_sum = 0.0f32;
        let mut flux = 0.0f32;
        for k in 0..BINS {
            let m = self.spectrum[k].norm() * norm;
            let p = m * m;
            self.magnitude[k] = m;
            self.power[k] = p;
            let l = (FLUX_GAMMA * m).ln_1p();
            flux += (l - self.prev_log_mag[k]).max(0.0);
            self.log_mag[k] = l;
            if k > 0 {
                total_power += p;
                total_mag += m;
                weighted_hz += m * self.bin_hz[k];
                log_power_sum += (p + 1e-20).ln();
            }
        }
        std::mem::swap(&mut self.log_mag, &mut self.prev_log_mag);
        sums.count += 1;
        sums.onset += flux / BINS as f32;

        let bins = (BINS - 1) as f32;
        let mean_power = total_power / bins;
        if mean_power < SILENCE_POWER {
            // Silence: flatness and centroid stay 0; MFCCs see the floor.
            sums.add_mfcc(&self.mfcc_of_power());
            return;
        }
        sums.flatness += ((log_power_sum / bins).exp() / mean_power).clamp(0.0, 1.0);
        sums.centroid += weighted_hz / total_mag.max(1e-12);
        sums.spectral_frames += 1;
        for &(k, class, w) in &self.chroma_map {
            sums.chroma[class] += w * self.power[k];
        }
        sums.add_mfcc(&self.mfcc_of_power());
    }

    fn mfcc_of_power(&self) -> [f32; MFCC_COEFFS] {
        let mut log_energy = [0f32; MEL_BANDS];
        for (e, filter) in log_energy.iter_mut().zip(&self.mel) {
            let energy: f32 =
                filter.weights.iter().zip(&self.power[filter.first_bin..]).map(|(w, p)| w * p).sum::<f32>();
            *e = (energy + 1e-10).ln();
        }
        let mut out = [0f32; MFCC_COEFFS];
        for (c, row) in out.iter_mut().zip(&self.dct) {
            *c = row.iter().zip(&log_energy).map(|(a, b)| a * b).sum();
        }
        out
    }
}

#[derive(Default)]
struct Sums {
    count: usize,
    spectral_frames: usize,
    flatness: f32,
    centroid: f32,
    onset: f32,
    chroma: [f32; 12],
    mfcc: [f32; MFCC_COEFFS],
    mfcc_frames: f32,
}

impl Sums {
    fn add_mfcc(&mut self, mfcc: &[f32; MFCC_COEFFS]) {
        for (a, b) in self.mfcc.iter_mut().zip(mfcc) {
            *a += b;
        }
        self.mfcc_frames += 1.0;
    }

    fn finish(self, rms: f32, zcr: f32, t_ms: u64) -> FrameFeatures {
        let spectral = self.spectral_frames.max(1) as f32;
        let mut chroma = self.chroma;
        let max = chroma.iter().copied().fold(0.0f32, f32::max);
        if max > 0.0 {
            chroma.iter_mut().for_each(|c| *c /= max);
        }
        let mut mfcc = self.mfcc;
        if self.mfcc_frames > 0.0 {
            mfcc.iter_mut().for_each(|c| *c /= self.mfcc_frames);
        }
        FrameFeatures {
            t_ms,
            rms,
            flatness: self.flatness / spectral,
            zcr,
            centroid_hz: self.centroid / spectral,
            onset: self.onset / self.count.max(1) as f32,
            chroma,
            mfcc,
        }
    }
}

fn hz_to_mel(hz: f32) -> f32 {
    2595.0 * (1.0 + hz / 700.0).log10()
}

fn mel_to_hz(mel: f32) -> f32 {
    700.0 * (10f32.powf(mel / 2595.0) - 1.0)
}

/// Triangular HTK-mel filters from 0 Hz to Nyquist, peak 1.
fn mel_filters() -> Vec<MelFilter> {
    let nyquist = SAMPLE_RATE as f32 / 2.0;
    let top = hz_to_mel(nyquist);
    let edges: Vec<f32> = (0..MEL_BANDS + 2).map(|i| mel_to_hz(top * i as f32 / (MEL_BANDS + 1) as f32)).collect();
    let bin_hz = |k: usize| k as f32 * SAMPLE_RATE as f32 / WINDOW as f32;
    (0..MEL_BANDS)
        .map(|b| {
            let (lo, mid, hi) = (edges[b], edges[b + 1], edges[b + 2]);
            let first_bin = (0..BINS).find(|&k| bin_hz(k) > lo).unwrap_or(BINS - 1);
            let weights: Vec<f32> = (first_bin..BINS)
                .take_while(|&k| bin_hz(k) < hi)
                .map(|k| {
                    let f = bin_hz(k);
                    if f <= mid { (f - lo) / (mid - lo) } else { (hi - f) / (hi - mid) }
                })
                .collect();
            MelFilter { first_bin, weights }
        })
        .collect()
}

/// Each bin in the chroma range spreads its power over the two nearest pitch
/// classes (triangular, one semitone wide). Index 0 is C.
fn chroma_map() -> Vec<(usize, usize, f32)> {
    let mut map = Vec::new();
    for k in 1..BINS {
        let hz = k as f32 * SAMPLE_RATE as f32 / WINDOW as f32;
        if !(CHROMA_MIN_HZ..=CHROMA_MAX_HZ).contains(&hz) {
            continue;
        }
        let midi = 69.0 + 12.0 * (hz / 440.0).log2();
        let lower = midi.floor();
        let frac = midi - lower;
        let class = |m: f32| (m as i64).rem_euclid(12) as usize;
        map.push((k, class(lower), 1.0 - frac));
        if frac > 0.0 {
            map.push((k, class(lower + 1.0), frac));
        }
    }
    map
}

/// Orthonormal DCT-II rows for the first 13 coefficients.
fn dct_matrix() -> Vec<[f32; MEL_BANDS]> {
    let m = MEL_BANDS as f32;
    (0..MFCC_COEFFS)
        .map(|n| {
            let scale = if n == 0 { (1.0 / m).sqrt() } else { (2.0 / m).sqrt() };
            let mut row = [0f32; MEL_BANDS];
            for (i, v) in row.iter_mut().enumerate() {
                *v = scale * (PI * n as f32 * (i as f32 + 0.5) / m).cos();
            }
            row
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::PcmWriter;
    use crate::audio::testutil::{TempDir, noise, sine};

    fn argmax(v: &[f32]) -> usize {
        v.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1)).map(|(i, _)| i).unwrap()
    }

    #[test]
    fn frame_grid() {
        let x = sine(SAMPLE_RATE, 440.0, 10.5, 0.5);
        let f = compute_samples(&x, 1000).unwrap();
        assert_eq!(f.len(), 11);
        assert_eq!(f.iter().map(|f| f.t_ms).collect::<Vec<_>>(), (0..11).map(|i| i * 1000).collect::<Vec<_>>());
        assert_eq!(compute_samples(&x, 10).unwrap().len(), 1050);
        assert_eq!(compute_samples(&x[..100], 1000).unwrap().len(), 1);
        assert!(compute_samples(&[], 1000).unwrap().is_empty());
        assert!(compute_samples(&x, 0).is_err());
    }

    #[test]
    fn sine_features() {
        let x = sine(SAMPLE_RATE, 1000.0, 5.0, 0.5);
        for f in &compute_samples(&x, 1000).unwrap()[1..4] {
            assert!((f.rms - 0.5 / 2f32.sqrt()).abs() < 0.005, "rms {}", f.rms);
            assert!((f.zcr - 2.0 * 1000.0 / 16_000.0).abs() < 0.002, "zcr {}", f.zcr);
            assert!((f.centroid_hz - 1000.0).abs() < 60.0, "centroid {}", f.centroid_hz);
            assert!(f.flatness < 0.05, "flatness {}", f.flatness);
            assert!(f.mfcc.iter().all(|c| c.is_finite()));
        }
    }

    #[test]
    fn chroma_peaks_at_the_pitch_class() {
        // (frequency, pitch class): A4, C4, C5, E4, G3, F#5.
        for (hz, class) in [(440.0, 9), (261.63, 0), (523.25, 0), (329.63, 4), (196.0, 7), (739.99, 6)] {
            let x = sine(SAMPLE_RATE, hz, 3.0, 0.5);
            for f in &compute_samples(&x, 500).unwrap()[1..5] {
                assert_eq!(argmax(&f.chroma), class, "{hz} Hz: {:?}", f.chroma);
                assert_eq!(f.chroma[class], 1.0);
            }
        }
        // A C major triad lights up C, E and G.
        let triad: Vec<f32> = sine(SAMPLE_RATE, 261.63, 3.0, 0.3)
            .iter()
            .zip(sine(SAMPLE_RATE, 329.63, 3.0, 0.3))
            .zip(sine(SAMPLE_RATE, 392.0, 3.0, 0.3))
            .map(|((a, b), c)| a + b + c)
            .collect();
        let f = &compute_samples(&triad, 1000).unwrap()[1];
        let mut top: Vec<usize> = (0..12).collect();
        top.sort_by(|a, b| f.chroma[*b].total_cmp(&f.chroma[*a]));
        let mut top3 = top[..3].to_vec();
        top3.sort();
        assert_eq!(top3, [0, 4, 7], "{:?}", f.chroma);
    }

    #[test]
    fn noise_is_flat_and_bright() {
        let x = noise(SAMPLE_RATE as usize * 4, 0.5, 3);
        for f in &compute_samples(&x, 1000).unwrap()[..4] {
            assert!(f.flatness > 0.4, "flatness {}", f.flatness);
            assert!((3000.0..5000.0).contains(&f.centroid_hz), "centroid {}", f.centroid_hz);
            assert!(f.zcr > 0.4, "zcr {}", f.zcr);
        }
    }

    #[test]
    fn silence_is_quiet_everywhere() {
        let f = &compute_samples(&vec![0.0; 32_000], 1000).unwrap()[1];
        assert_eq!((f.rms, f.flatness, f.zcr, f.centroid_hz, f.onset), (0.0, 0.0, 0.0, 0.0, 0.0));
        assert_eq!(f.chroma, [0.0; 12]);
        assert!(f.mfcc.iter().all(|c| c.is_finite()));
    }

    #[test]
    fn onsets_mark_new_sounds() {
        // Silence, then a tone starting at 3 s, then a different tone at 6 s.
        let mut x = vec![0.0; 3 * SAMPLE_RATE as usize];
        x.extend(sine(SAMPLE_RATE, 440.0, 3.0, 0.5));
        x.extend(sine(SAMPLE_RATE, 1500.0, 3.0, 0.5));
        let f = compute_samples(&x, 500).unwrap();
        let onset: Vec<f32> = f.iter().map(|f| f.onset).collect();
        // The frames where the sounds start stand out from steady ones and silence.
        assert_eq!(argmax(&onset), 6, "{onset:?}");
        assert!(onset[6] > 10.0 * onset[8], "{onset:?}");
        assert!(onset[12] > 4.0 * onset[14], "{onset:?}");
        assert_eq!(onset[2], 0.0);
    }

    #[test]
    fn mfcc_follow_loudness_and_timbre() {
        let loud = &compute_samples(&noise(32_000, 0.5, 1), 1000).unwrap()[1];
        let quiet = &compute_samples(&noise(32_000, 0.05, 1), 1000).unwrap()[1];
        let tone = &compute_samples(&sine(SAMPLE_RATE, 300.0, 2.0, 0.5), 1000).unwrap()[1];
        // 20 dB less in every band: c0 drops by sqrt(40)·ln(100) ≈ 29; the shape stays.
        assert!((loud.mfcc[0] - quiet.mfcc[0] - 29.1).abs() < 1.0, "{} vs {}", loud.mfcc[0], quiet.mfcc[0]);
        assert!(loud.mfcc[1..].iter().zip(&quiet.mfcc[1..]).all(|(a, b)| (a - b).abs() < 0.5));
        // A tone and noise have different spectral shapes.
        let shape_distance: f32 =
            tone.mfcc[1..].iter().zip(&loud.mfcc[1..]).map(|(a, b)| (a - b) * (a - b)).sum::<f32>().sqrt();
        assert!(shape_distance > 10.0, "{shape_distance}");
    }

    #[test]
    fn file_and_memory_agree() {
        let dir = TempDir::new("feat");
        let mut x = sine(SAMPLE_RATE, 330.0, 70.0, 0.4);
        for (i, v) in noise(x.len(), 0.05, 9).iter().enumerate() {
            x[i] += v;
        }
        let mut w = PcmWriter::create(&dir.path().join("f.pcm")).unwrap();
        w.push_f32(&x).unwrap();
        let pcm = w.finish().unwrap();
        let quantized: Vec<f32> = pcm.read_samples(0, x.len()).unwrap().iter().map(|&s| s as f32 / 32768.0).collect();
        let from_file = compute(&pcm, 250, &CancelToken::new()).unwrap();
        let from_memory = compute_samples(&quantized, 250).unwrap();
        assert_eq!(from_file.len(), 280);
        for (a, b) in from_file.iter().zip(&from_memory) {
            assert_eq!(a.t_ms, b.t_ms);
            assert!((a.rms - b.rms).abs() < 1e-6 && (a.onset - b.onset).abs() < 1e-4);
            assert!(a.chroma.iter().zip(&b.chroma).all(|(x, y)| (x - y).abs() < 1e-4));
        }
    }

    #[test]
    fn cancellation() {
        let dir = TempDir::new("featc");
        let mut w = PcmWriter::create(&dir.path().join("c.pcm")).unwrap();
        w.push(&vec![0; 16_000 * 10]).unwrap();
        let pcm = w.finish().unwrap();
        let cancel = CancelToken::new();
        cancel.cancel();
        assert_eq!(compute(&pcm, 100, &cancel).unwrap_err(), MfError::Canceled);
    }

    /// `cargo test -p mfcore --release features_one_hour -- --ignored --nocapture`
    #[test]
    #[ignore = "performance: writes 115 MB, run in release"]
    fn features_one_hour_at_one_second_hop() {
        let dir = TempDir::new("feathour");
        let mut w = PcmWriter::create(&dir.path().join("hour.pcm")).unwrap();
        let minute: Vec<f32> = sine(SAMPLE_RATE, 440.0, 60.0, 0.3)
            .iter()
            .zip(noise(SAMPLE_RATE as usize * 60, 0.1, 5))
            .map(|(a, b)| a + b)
            .collect();
        for _ in 0..60 {
            w.push_f32(&minute).unwrap();
        }
        let pcm = w.finish().unwrap();
        let started = std::time::Instant::now();
        let features = compute(&pcm, 1000, &CancelToken::new()).unwrap();
        let elapsed = started.elapsed();
        eprintln!("{} frames for 60 min in {:.2} s", features.len(), elapsed.as_secs_f64());
        assert_eq!(features.len(), 3600);
        if !cfg!(debug_assertions) {
            assert!(elapsed.as_secs_f64() < 3.0, "took {elapsed:?}");
        }
    }
}
