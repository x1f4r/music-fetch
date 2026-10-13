//! Streaming conversion from interleaved f32 (any rate, any channel count) to
//! 16 kHz mono f32.
//!
//! Channels are averaged first, so only one channel is resampled. Rates whose
//! ratio to 16 kHz has a short period (44.1k, 48k, 22.05k, 96k, 8k, ...) use
//! rubato's synchronous FFT resampler; awkward rates fall back to the
//! asynchronous sinc resampler. The resampler's startup delay is trimmed and
//! `flush` pads the tail, so the output length is exactly
//! `round(input_frames * 16000 / input_rate)` and stays time-aligned.

use rubato::audioadapter_buffers::direct::SequentialSlice;
use rubato::{
    Async, Fft, FixedAsync, FixedSync, Resampler, SincInterpolationParameters, SincInterpolationType, WindowFunction,
};

use crate::error::{MfError, Result};

use super::SAMPLE_RATE;

/// Lowest and highest accepted input rates.
pub const MIN_INPUT_RATE: u32 = 4_000;
pub const MAX_INPUT_RATE: u32 = 384_000;
/// Most channels accepted in one interleaved frame.
pub const MAX_CHANNELS: u32 = 64;

/// Chunking preset: small chunks for live input, large ones for file decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Latency {
    /// ~10 ms input chunks.
    Live,
    /// ~100 ms input chunks with larger FFT blocks (throughput over delay).
    Offline,
}

enum Engine {
    Passthrough,
    Fft(Box<Fft<f32>>),
    Sinc(Box<Async<f32>>),
}

impl Engine {
    fn resampler(&mut self) -> Option<&mut dyn Resampler<f32>> {
        match self {
            Engine::Passthrough => None,
            Engine::Fft(r) => Some(r.as_mut()),
            Engine::Sinc(r) => Some(r.as_mut()),
        }
    }
}

/// Converts a stream of interleaved f32 frames to 16 kHz mono.
pub struct StreamResampler {
    input_rate: u32,
    channels: u32,
    engine: Engine,
    /// Mono input waiting for a full chunk.
    pending: Vec<f32>,
    /// Samples of an incomplete interleaved frame carried to the next call.
    carry: Vec<f32>,
    mono: Vec<f32>,
    chunk: Vec<f32>,
    out_chunk: Vec<f32>,
    /// Output frames still to drop (resampler delay).
    skip: usize,
    delay: usize,
    frames_in: u64,
    frames_out: u64,
}

impl StreamResampler {
    /// Resampler tuned for live input (chunks of 10–100 ms; ~10 ms internal blocks).
    pub fn new(input_rate: u32, channels: u32) -> Result<Self> {
        Self::build(input_rate, channels, Latency::Live)
    }

    /// Resampler tuned for throughput (file decoding).
    pub fn offline(input_rate: u32, channels: u32) -> Result<Self> {
        Self::build(input_rate, channels, Latency::Offline)
    }

    fn build(input_rate: u32, channels: u32, latency: Latency) -> Result<Self> {
        if !(MIN_INPUT_RATE..=MAX_INPUT_RATE).contains(&input_rate) {
            return Err(MfError::invalid(format!("unsupported sample rate: {input_rate} Hz")));
        }
        if channels == 0 || channels > MAX_CHANNELS {
            return Err(MfError::invalid(format!("unsupported channel count: {channels}")));
        }
        let engine = make_engine(input_rate, latency)?;
        let mut this = Self {
            input_rate,
            channels,
            engine,
            pending: Vec::new(),
            carry: Vec::new(),
            mono: Vec::new(),
            chunk: Vec::new(),
            out_chunk: Vec::new(),
            skip: 0,
            delay: 0,
            frames_in: 0,
            frames_out: 0,
        };
        if let Some(r) = this.engine.resampler() {
            let delay = r.output_delay();
            let max_out = r.output_frames_max();
            let max_in = r.input_frames_max();
            this.delay = delay;
            this.skip = delay;
            this.out_chunk = vec![0.0; max_out];
            this.chunk = vec![0.0; max_in];
        }
        Ok(this)
    }

    pub fn input_rate(&self) -> u32 {
        self.input_rate
    }

    pub fn channels(&self) -> u32 {
        self.channels
    }

    /// Output frames the converter holds back (startup delay of the resampler).
    pub fn latency_frames(&self) -> usize {
        self.delay
    }

    /// Convert one chunk; output length varies with internal buffering.
    pub fn process(&mut self, interleaved: &[f32]) -> Vec<f32> {
        let mut out = Vec::new();
        self.process_into(interleaved, &mut out);
        out
    }

    /// Like [`process`](Self::process), appending to `out`.
    pub fn process_into(&mut self, interleaved: &[f32], out: &mut Vec<f32>) {
        let mut mono = std::mem::take(&mut self.mono);
        mono.clear();
        let ch = self.channels as usize;
        if ch == 1 {
            mono.extend_from_slice(interleaved);
        } else {
            let mut data = interleaved;
            // Complete a frame left over from the previous call.
            if !self.carry.is_empty() {
                let need = ch - self.carry.len();
                let take = need.min(data.len());
                self.carry.extend_from_slice(&data[..take]);
                data = &data[take..];
                if self.carry.len() == ch {
                    mono.push(average(&self.carry));
                    self.carry.clear();
                }
            }
            let whole = data.len() / ch * ch;
            downmix_into(&data[..whole], ch, &mut mono);
            self.carry.extend_from_slice(&data[whole..]);
        }
        self.push_mono(&mono, out);
        self.mono = mono;
    }

    /// Feed samples that are already mono at the input rate.
    pub fn process_mono_into(&mut self, mono: &[f32], out: &mut Vec<f32>) {
        self.push_mono(mono, out);
    }

    fn push_mono(&mut self, mono: &[f32], out: &mut Vec<f32>) {
        self.frames_in += mono.len() as u64;
        if matches!(self.engine, Engine::Passthrough) {
            out.extend(mono.iter().map(|s| sanitize(*s)));
            self.frames_out += mono.len() as u64;
            return;
        }
        self.pending.extend(mono.iter().map(|s| sanitize(*s)));
        let mut pos = 0;
        while let Some(need) = self.engine.resampler().map(|r| r.input_frames_next()) {
            if need == 0 || self.pending.len() - pos < need {
                break;
            }
            let consumed = self.run_chunk(pos, need, out);
            if consumed == 0 {
                break;
            }
            pos += consumed;
        }
        self.pending.drain(..pos);
    }

    /// Resample `pending[pos..pos + len]` (exactly one chunk) and emit the output.
    fn run_chunk(&mut self, pos: usize, len: usize, out: &mut Vec<f32>) -> usize {
        let Some(r) = self.engine.resampler() else { return 0 };
        let out_len = self.out_chunk.len();
        let input = &self.pending[pos..pos + len];
        let (Ok(adapter_in), Ok(mut adapter_out)) =
            (SequentialSlice::new(input, 1, len), SequentialSlice::new_mut(&mut self.out_chunk, 1, out_len))
        else {
            return 0;
        };
        match r.process_into_buffer(&adapter_in, &mut adapter_out, None) {
            Ok((consumed, produced)) => {
                self.emit_out_chunk(produced, out);
                consumed
            }
            Err(e) => {
                tracing::warn!("resampler chunk failed: {e}");
                0
            }
        }
    }

    fn emit_out_chunk(&mut self, produced: usize, out: &mut Vec<f32>) {
        let produced = produced.min(self.out_chunk.len());
        let drop = self.skip.min(produced);
        self.skip -= drop;
        out.extend_from_slice(&self.out_chunk[drop..produced]);
        self.frames_out += (produced - drop) as u64;
    }

    /// Emit everything still buffered and reset for a new stream.
    pub fn flush(&mut self) -> Vec<f32> {
        let mut out = Vec::new();
        self.flush_into(&mut out);
        out
    }

    /// Like [`flush`](Self::flush), appending to `out`.
    pub fn flush_into(&mut self, out: &mut Vec<f32>) {
        let expected = expected_output_frames(self.frames_in, self.input_rate);
        if !matches!(self.engine, Engine::Passthrough) {
            let start = out.len();
            // Zero-padded chunks push the remaining input and the delay line out.
            let mut guard = 0;
            while self.frames_out < expected && guard < 64 {
                guard += 1;
                let Some(need) = self.engine.resampler().map(|r| r.input_frames_next()) else { break };
                if need == 0 {
                    break;
                }
                let take = need.min(self.pending.len());
                let mut chunk = std::mem::take(&mut self.chunk);
                chunk.clear();
                chunk.extend_from_slice(&self.pending[..take]);
                chunk.resize(need, 0.0);
                self.pending.drain(..take);
                let before = self.frames_out;
                self.run_padded_chunk(&chunk, out);
                self.chunk = chunk;
                if self.frames_out == before && self.skip == 0 {
                    break;
                }
            }
            // Trim padding output beyond the exact length.
            let excess = self.frames_out.saturating_sub(expected) as usize;
            let new_len = out.len().saturating_sub(excess).max(start);
            out.truncate(new_len);
        }
        self.reset();
    }

    fn run_padded_chunk(&mut self, chunk: &[f32], out: &mut Vec<f32>) {
        let Some(r) = self.engine.resampler() else { return };
        let out_len = self.out_chunk.len();
        let (Ok(adapter_in), Ok(mut adapter_out)) =
            (SequentialSlice::new(chunk, 1, chunk.len()), SequentialSlice::new_mut(&mut self.out_chunk, 1, out_len))
        else {
            return;
        };
        match r.process_into_buffer(&adapter_in, &mut adapter_out, None) {
            Ok((_, produced)) => self.emit_out_chunk(produced, out),
            Err(e) => tracing::warn!("resampler flush failed: {e}"),
        }
    }

    /// Drop all buffered audio and start a new stream with the same format.
    pub fn reset(&mut self) {
        if let Some(r) = self.engine.resampler() {
            r.reset();
        }
        self.pending.clear();
        self.carry.clear();
        self.skip = self.delay;
        self.frames_in = 0;
        self.frames_out = 0;
    }
}

fn make_engine(input_rate: u32, latency: Latency) -> Result<Engine> {
    if input_rate == SAMPLE_RATE {
        return Ok(Engine::Passthrough);
    }
    let rate_in = input_rate as usize;
    let rate_out = SAMPLE_RATE as usize;
    let min_block = rate_in / gcd(rate_in, rate_out);
    let target = match latency {
        Latency::Live => rate_in / 100,
        Latency::Offline => rate_in / 10,
    };
    let map_err = |e: rubato::ResamplerConstructionError| MfError::internal(format!("resampler: {e}"));

    // The FFT resampler works on whole periods of the rate ratio; use it when a
    // period is at most 100 ms of input.
    if min_block <= rate_in / 10 {
        let chunk = target.max(min_block).div_ceil(min_block) * min_block;
        let sub_chunks = match latency {
            Latency::Live => 1,
            Latency::Offline => (chunk / 1024).max(1),
        };
        let fft = Fft::<f32>::new_custom(
            rate_in,
            rate_out,
            chunk,
            sub_chunks,
            1,
            WindowFunction::BlackmanHarris2,
            FixedSync::Input,
        )
        .map_err(map_err)?;
        return Ok(Engine::Fft(Box::new(fft)));
    }

    let params = SincInterpolationParameters {
        sinc_len: 128,
        f_cutoff: None,
        oversampling_factor: 256,
        interpolation: SincInterpolationType::Cubic,
        window: WindowFunction::BlackmanHarris2,
    };
    let ratio = rate_out as f64 / rate_in as f64;
    let sinc = Async::<f32>::new_sinc(ratio, 1.0, &params, target.max(64), 1, FixedAsync::Input).map_err(map_err)?;
    Ok(Engine::Sinc(Box::new(sinc)))
}

/// Exact output length for `frames_in` input frames.
fn expected_output_frames(frames_in: u64, input_rate: u32) -> u64 {
    let rate_in = input_rate as u128;
    ((frames_in as u128 * SAMPLE_RATE as u128 + rate_in / 2) / rate_in) as u64
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a.max(1)
}

#[inline]
fn sanitize(s: f32) -> f32 {
    if s.is_finite() { s } else { 0.0 }
}

#[inline]
fn average(frame: &[f32]) -> f32 {
    frame.iter().sum::<f32>() / frame.len() as f32
}

/// Average interleaved channels into `out` (appends `data.len() / channels` samples).
pub fn downmix_into(data: &[f32], channels: usize, out: &mut Vec<f32>) {
    match channels {
        0 => {}
        1 => out.extend_from_slice(data),
        2 => out.extend(data.as_chunks::<2>().0.iter().map(|[l, r]| (l + r) * 0.5)),
        n => out.extend(data.chunks_exact(n).map(average)),
    }
}

/// Convert a whole in-memory clip to 16 kHz mono.
pub fn to_mono_16k(interleaved: &[f32], input_rate: u32, channels: u32) -> Result<Vec<f32>> {
    let mut r = StreamResampler::offline(input_rate, channels)?;
    let mut out = Vec::with_capacity((expected_output_frames(interleaved.len() as u64, input_rate) + 1) as usize);
    r.process_into(interleaved, &mut out);
    r.flush_into(&mut out);
    Ok(out)
}

/// One f32 sample in -1..=1 to i16 (clamped; NaN becomes 0).
#[inline]
pub fn f32_sample_to_i16(s: f32) -> i16 {
    if s.is_nan() { 0 } else { (s.clamp(-1.0, 1.0) * 32767.0).round() as i16 }
}

pub fn f32_to_i16(samples: &[f32]) -> Vec<i16> {
    samples.iter().map(|&s| f32_sample_to_i16(s)).collect()
}

pub fn i16_to_f32(samples: &[i16]) -> Vec<f32> {
    samples.iter().map(|&s| s as f32 / 32768.0).collect()
}

#[cfg(test)]
mod tests {
    use std::f32::consts::TAU;

    use super::*;

    /// Interleaved sine at `freq` with `channels` copies (later channels attenuated).
    fn sine(rate: u32, channels: u32, freq: f32, secs: f32, amp: f32) -> Vec<f32> {
        let n = (rate as f32 * secs) as usize;
        let mut out = Vec::with_capacity(n * channels as usize);
        for i in 0..n {
            let v = amp * (TAU * freq * i as f32 / rate as f32).sin();
            for c in 0..channels {
                out.push(if c == 0 { v } else { v * 0.5 });
            }
        }
        out
    }

    /// Linear sweep from `f0` to `f1` Hz, mono.
    fn sweep(rate: u32, f0: f32, f1: f32, secs: f32) -> Vec<f32> {
        let n = (rate as f32 * secs) as usize;
        let k = (f1 - f0) / secs;
        (0..n)
            .map(|i| {
                let t = i as f32 / rate as f32;
                0.9 * (TAU * (f0 * t + 0.5 * k * t * t)).sin()
            })
            .collect()
    }

    /// Dominant frequency of `x` at 16 kHz by counting zero crossings.
    fn zero_cross_freq(x: &[f32]) -> f32 {
        let crossings = x.windows(2).filter(|w| (w[0] < 0.0) != (w[1] < 0.0)).count();
        crossings as f32 / 2.0 / (x.len() as f32 / SAMPLE_RATE as f32)
    }

    fn feed_in_chunks(r: &mut StreamResampler, data: &[f32], chunk_frames: usize) -> Vec<f32> {
        let step = chunk_frames * r.channels() as usize;
        let mut out = Vec::new();
        for c in data.chunks(step) {
            r.process_into(c, &mut out);
        }
        r.flush_into(&mut out);
        out
    }

    #[test]
    fn rejects_bad_formats() {
        assert!(StreamResampler::new(0, 1).is_err());
        assert!(StreamResampler::new(1_000, 1).is_err());
        assert!(StreamResampler::new(1_000_000, 1).is_err());
        assert!(StreamResampler::new(48_000, 0).is_err());
        assert!(StreamResampler::new(48_000, 65).is_err());
    }

    #[test]
    fn sine_frequency_and_length_preserved_across_rates() {
        let rates = [8_000, 11_025, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000, 88_200, 96_000, 176_400, 192_000];
        for &rate in &rates {
            for &channels in &[1u32, 2, 6] {
                let input = sine(rate, channels, 440.0, 2.0, 0.8);
                let frames = input.len() / channels as usize;
                // Live-style chunks: 20 ms.
                let mut r = StreamResampler::new(rate, channels).unwrap();
                let out = feed_in_chunks(&mut r, &input, rate as usize / 50);
                let expected = (frames as u64 * 16_000 / rate as u64) as usize;
                assert!(out.len().abs_diff(expected) <= 1, "rate {rate} ch {channels}: {} vs {expected}", out.len());
                // Skip edges; check frequency of the steady part.
                let mid = &out[1600..out.len() - 1600];
                let f = zero_cross_freq(mid);
                assert!((f - 440.0).abs() < 4.0, "rate {rate} ch {channels}: freq {f}");
                let peak = out.iter().fold(0f32, |m, v| m.max(v.abs()));
                // Mixed channel levels: 0.8 for mono, (0.8 + 0.4·(n-1))/n otherwise.
                let level = if channels == 1 { 0.8 } else { (0.8 + 0.4 * (channels - 1) as f32) / channels as f32 };
                assert!((peak - level).abs() < 0.05, "rate {rate} ch {channels}: peak {peak} level {level}");
            }
        }
    }

    #[test]
    fn sweep_tracks_frequency_without_clipping() {
        for &rate in &[44_100u32, 48_000, 22_050, 96_000] {
            // 100 Hz → 7 kHz in 6 s.
            let input = sweep(rate, 100.0, 7_000.0, 6.0);
            let mut r = StreamResampler::new(rate, 1).unwrap();
            let out = feed_in_chunks(&mut r, &input, rate as usize / 10);
            let expected = input.len() as f64 * 16_000.0 / rate as f64;
            assert!((out.len() as f64 - expected).abs() / expected < 0.01, "rate {rate}: len {}", out.len());
            assert!(out.iter().all(|v| v.abs() <= 1.0), "rate {rate}: clipped");
            // Instantaneous frequency in 250 ms windows follows the sweep.
            for sec in [0.5f32, 2.0, 3.5, 5.0] {
                let start = (sec * 16_000.0) as usize;
                let f = zero_cross_freq(&out[start..start + 4000]);
                let want = 100.0 + (7_000.0 - 100.0) * (sec + 0.125) / 6.0;
                assert!((f - want).abs() / want < 0.03, "rate {rate} at {sec}s: {f} vs {want}");
            }
        }
    }

    #[test]
    fn content_above_nyquist_is_removed() {
        // 12 kHz tone at 48 kHz must not alias into the 16 kHz output.
        let input = sine(48_000, 1, 12_000.0, 1.0, 0.8);
        let out = to_mono_16k(&input, 48_000, 1).unwrap();
        let rms = (out[1600..14_400].iter().map(|v| v * v).sum::<f32>() / 12_800.0).sqrt();
        assert!(rms < 0.01, "alias rms {rms}");
    }

    #[test]
    fn output_is_time_aligned() {
        // An impulse at 0.5 s stays at 0.5 s.
        for &rate in &[44_100u32, 48_000, 8_000, 37_000] {
            let mut input = vec![0f32; rate as usize];
            input[rate as usize / 2] = 1.0;
            let out = to_mono_16k(&input, rate, 1).unwrap();
            let peak = out.iter().enumerate().max_by(|a, b| a.1.abs().total_cmp(&b.1.abs())).map(|(i, _)| i).unwrap();
            assert!(peak.abs_diff(8_000) <= 2, "rate {rate}: peak at {peak}");
        }
    }

    #[test]
    fn awkward_rates_use_sinc_and_keep_length() {
        // 37 kHz and 44.1k±1 have no short ratio period with 16 kHz.
        for &rate in &[37_000u32, 44_101, 7_350] {
            let input = sine(rate, 2, 1_000.0, 1.5, 0.5);
            let out = to_mono_16k(&input, rate, 2).unwrap();
            let expected = (input.len() / 2) as f64 * 16_000.0 / rate as f64;
            assert!((out.len() as f64 - expected).abs() <= 1.0, "rate {rate}: {}", out.len());
            let f = zero_cross_freq(&out[2000..out.len() - 2000]);
            assert!((f - 1000.0).abs() < 8.0, "rate {rate}: freq {f}");
        }
    }

    #[test]
    fn partial_frames_are_carried() {
        let input = sine(48_000, 2, 440.0, 0.5, 0.5);
        let mut whole = StreamResampler::new(48_000, 2).unwrap();
        let a = feed_in_chunks(&mut whole, &input, 480);
        // Odd sample counts split frames across calls.
        let mut split = StreamResampler::new(48_000, 2).unwrap();
        let mut b = Vec::new();
        for c in input.chunks(333) {
            split.process_into(c, &mut b);
        }
        split.flush_into(&mut b);
        assert_eq!(a.len(), b.len());
        assert!(a.iter().zip(&b).all(|(x, y)| (x - y).abs() < 1e-4));
    }

    #[test]
    fn passthrough_and_reuse_after_flush() {
        let input = sine(16_000, 1, 300.0, 0.25, 0.5);
        let mut r = StreamResampler::new(16_000, 1).unwrap();
        assert_eq!(r.process(&input), input);
        assert!(r.flush().is_empty());
        let mut r = StreamResampler::new(48_000, 1).unwrap();
        let first = feed_in_chunks(&mut r, &sine(48_000, 1, 300.0, 0.25, 0.5), 480);
        let second = feed_in_chunks(&mut r, &sine(48_000, 1, 300.0, 0.25, 0.5), 480);
        assert_eq!(first.len(), 4000);
        assert_eq!(first, second);
    }

    #[test]
    fn live_latency_is_low() {
        let r = StreamResampler::new(48_000, 2).unwrap();
        // Under 20 ms of delay at 16 kHz.
        assert!(r.latency_frames() < 320, "latency {}", r.latency_frames());
        let r = StreamResampler::new(44_100, 1).unwrap();
        assert!(r.latency_frames() < 320, "latency {}", r.latency_frames());
    }

    #[test]
    fn non_finite_input_is_zeroed() {
        let mut input = sine(48_000, 1, 440.0, 0.2, 0.5);
        input[100] = f32::NAN;
        input[200] = f32::INFINITY;
        let out = to_mono_16k(&input, 48_000, 1).unwrap();
        assert!(out.iter().all(|v| v.is_finite()));
    }

    #[test]
    fn i16_conversions() {
        assert_eq!(f32_to_i16(&[0.0, 1.0, -1.0, 1.5, f32::NAN]), vec![0, 32767, -32767, 32767, 0]);
        assert_eq!(i16_to_f32(&[0, -32768, 16384]), vec![0.0, -1.0, 0.5]);
    }
}
