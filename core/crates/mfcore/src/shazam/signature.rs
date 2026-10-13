//! Shazam-compatible signature generation (16 kHz mono i16 input).
//!
//! Clean-room implementation of the spectral-peak algorithm and binary format
//! written up in the project's research notes. The signal is cut into 128-sample
//! hops; every hop yields a 2048-point power spectrum. A bin becomes a peak when
//! it dominates its neighbourhood in frequency and time; peaks are quantised to
//! 1/64-bin resolution, grouped into four frequency bands and serialised as a
//! CRC-protected TLV blob that the Shazam endpoint and Apple's `SHSignature`
//! both accept.

use std::sync::Arc;

use base64::Engine as _;
use realfft::num_complex::Complex;
use realfft::{RealFftPlanner, RealToComplex};

use crate::error::{MfError, Result};

/// Sample rate the algorithm and the format assume.
pub const SAMPLE_RATE: u32 = 16_000;
/// Longest clip the service matches (12 s). Longer input is cut to its middle 12 s.
pub const MAX_SAMPLES: usize = 12 * SAMPLE_RATE as usize;
/// Shortest clip worth sending (3 s).
pub const MIN_SAMPLES: usize = 3 * SAMPLE_RATE as usize;
/// Samples per frame (8 ms).
pub const HOP: usize = 128;

const FFT_SIZE: usize = 2048;
const BINS: usize = FFT_SIZE / 2 + 1;
/// Frames kept in the power and spread rings.
const RING: usize = 256;
/// Frames between computing a spectrum and testing it for peaks.
const DETECT_DELAY: usize = 46;
/// Ring offset of the spread frame compared with the candidate (candidate − 3).
const SPREAD_DELAY: usize = 49;
const MIN_POWER: f32 = 1.0 / 64.0;
/// Frequency neighbours (in bins) of the spread frame a peak must exceed.
const NEIGHBOUR_BINS: [isize; 8] = [-10, -7, -4, -3, 1, 2, 5, 8];
/// Time neighbours as ring offsets from the newest slot: frames c−7, c+1, c−45,
/// c−38, c−31, c−24, c−17, c−10, c+4, c+11, c+18, c+25, c+32, c+39.
const NEIGHBOUR_FRAMES: [usize; 14] = [
    203, 211, 165, 172, 179, 186, 193, 200, 214, 221, 228, 235, 242, 249,
];
/// Frames of zeros fed after the input when `Options::pad_tail` is set.
const TAIL_FRAMES: usize = DETECT_DELAY;

const MAGIC1: u32 = 0xCAFE_2580;
const MAGIC2: u32 = 0x9411_9C00;
/// `magic2` written by Apple's ShazamKit generator; accepted when decoding.
const MAGIC2_APPLE: u32 = 0x42F0_2010;
const APPLE_WRAPPER_MAGIC: u32 = 0x2580_2580;
const HEADER_LEN: usize = 48;
const FIXED_FIELD: u32 = (15 << 19) + 0x40000;
const TLV_SIGNATURE: u32 = 0x4000_0000;
const TLV_BAND_BASE: u32 = 0x6003_0040;
const SAMPLE_RATE_ID_16K: u32 = 3;

/// One of the four frequency bands peaks are grouped into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Band {
    /// 250–520 Hz.
    Low,
    /// 520–1450 Hz.
    LowMid,
    /// 1450–3500 Hz.
    HighMid,
    /// 3500–5500 Hz.
    High,
}

impl Band {
    pub const ALL: [Band; 4] = [Band::Low, Band::LowMid, Band::HighMid, Band::High];

    /// Band for an integer frequency, `None` outside 250–5500 Hz.
    pub fn from_hz(hz: u32) -> Option<Band> {
        match hz {
            250..=519 => Some(Band::Low),
            520..=1449 => Some(Band::LowMid),
            1450..=3499 => Some(Band::HighMid),
            3500..=5500 => Some(Band::High),
            _ => None,
        }
    }

    pub fn index(self) -> usize {
        self as usize
    }

    /// Display label, e.g. "250–520 Hz".
    pub fn label(self) -> &'static str {
        match self {
            Band::Low => "250–520 Hz",
            Band::LowMid => "520–1450 Hz",
            Band::HighMid => "1450–3500 Hz",
            Band::High => "3500–5500 Hz",
        }
    }
}

/// A spectral peak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Peak {
    /// Frame index (8 ms per frame).
    pub frame: u32,
    /// Log-power magnitude.
    pub magnitude: u16,
    /// Frequency in 1/64-bin units (`fbin × 0.1220703125` Hz).
    pub fbin: u16,
}

impl Peak {
    pub fn hz(&self) -> f32 {
        self.fbin as f32 * (SAMPLE_RATE as f32 / 2.0 / 1024.0 / 64.0)
    }
}

/// Peaks per band, indexed by `Band::index`, sorted by frame.
pub type BandPeaks = [Vec<Peak>; 4];

/// Generator variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// Feed 46 frames of silence after the input so the last ~0.37 s also yields
    /// peaks, as Apple's generator does. The declared sample count stays the real
    /// one. On by default; off is the plain reference algorithm.
    pub pad_tail: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self { pad_tail: true }
    }
}

/// Encoded signature ready to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub bytes: Vec<u8>,
    /// Number of input samples the signature covers.
    pub sample_count: u32,
}

impl Signature {
    /// `data:audio/vnd.shazam.sig;base64,...`
    pub fn data_uri(&self) -> String {
        let mut uri = String::from("data:audio/vnd.shazam.sig;base64,");
        base64::engine::general_purpose::STANDARD.encode_string(&self.bytes, &mut uri);
        uri
    }

    pub fn sample_ms(&self) -> u64 {
        self.sample_count as u64 * 1000 / SAMPLE_RATE as u64
    }

    /// Parse the encoded bytes back into peaks.
    pub fn decode(&self) -> Result<DecodedSignature> {
        decode(&self.bytes)
    }
}

/// Generate a signature from 16 kHz mono audio.
///
/// Input longer than 12 s is cut to its middle 12 s (live callers pass exactly
/// the window they want). A trailing partial hop is ignored.
pub fn generate(samples: &[i16]) -> Signature {
    generate_with(samples, &Options::default())
}

/// [`generate`] with explicit [`Options`].
pub fn generate_with(samples: &[i16], options: &Options) -> Signature {
    let clip = middle(samples, MAX_SAMPLES);
    let bands = peaks(clip, options.pad_tail);
    let sample_count = u32::try_from(clip.len()).unwrap_or(u32::MAX);
    Signature {
        bytes: encode(&bands, sample_count),
        sample_count,
    }
}

/// The middle `max` samples of `samples`.
fn middle(samples: &[i16], max: usize) -> &[i16] {
    if samples.len() <= max {
        return samples;
    }
    let start = (samples.len() - max) / 2;
    &samples[start..start + max]
}

/// Detect peaks in `samples` (no length limit).
pub fn peaks(samples: &[i16], pad_tail: bool) -> BandPeaks {
    let mut fp = Fingerprinter::new();
    for hop in samples.as_chunks::<HOP>().0 {
        fp.push_hop(hop);
    }
    if pad_tail {
        let silence = [0i16; HOP];
        for _ in 0..TAIL_FRAMES {
            fp.push_hop(&silence);
        }
    }
    fp.peaks
}

/// Streaming peak detector over 128-sample hops.
struct Fingerprinter {
    fft: Arc<dyn RealToComplex<f32>>,
    window: Vec<f32>,
    ring: Vec<i16>,
    ring_pos: usize,
    frame_in: Vec<f32>,
    spectrum: Vec<Complex<f32>>,
    scratch: Vec<Complex<f32>>,
    current: Vec<f32>,
    /// `RING` power spectra, flat.
    power: Vec<f32>,
    /// `RING` spectra spread over neighbouring bins and frames, flat.
    spread: Vec<f32>,
    frames: usize,
    peaks: BandPeaks,
}

impl Fingerprinter {
    fn new() -> Self {
        let fft = RealFftPlanner::<f32>::new().plan_fft_forward(FFT_SIZE);
        // numpy.hanning(2050)[1:-1]
        let window = (0..FFT_SIZE)
            .map(|i| {
                let x = 2.0 * std::f64::consts::PI * (i + 1) as f64 / (FFT_SIZE + 1) as f64;
                (0.5 - 0.5 * x.cos()) as f32
            })
            .collect();
        Self {
            frame_in: fft.make_input_vec(),
            spectrum: fft.make_output_vec(),
            scratch: fft.make_scratch_vec(),
            fft,
            window,
            ring: vec![0; FFT_SIZE],
            ring_pos: 0,
            current: vec![0.0; BINS],
            power: vec![0.0; RING * BINS],
            spread: vec![0.0; RING * BINS],
            frames: 0,
            peaks: Default::default(),
        }
    }

    fn push_hop(&mut self, hop: &[i16; HOP]) {
        self.ring[self.ring_pos..self.ring_pos + HOP].copy_from_slice(hop);
        self.ring_pos = (self.ring_pos + HOP) % FFT_SIZE;
        for (i, x) in self.frame_in.iter_mut().enumerate() {
            *x = self.ring[(self.ring_pos + i) % FFT_SIZE] as f32 * self.window[i];
        }
        if self
            .fft
            .process_with_scratch(&mut self.frame_in, &mut self.spectrum, &mut self.scratch)
            .is_err()
        {
            // Buffer sizes come from the plan itself, so this cannot happen.
            self.spectrum.fill(Complex::new(0.0, 0.0));
        }

        let slot = self.frames % RING;
        let power = &mut self.power[slot * BINS..(slot + 1) * BINS];
        for (p, c) in power.iter_mut().zip(&self.spectrum) {
            *p = ((c.re * c.re + c.im * c.im) / 131_072.0).max(1e-10);
        }

        // Spread over the next two bins, then back over frames −1, −3 and −6.
        self.current.copy_from_slice(power);
        for k in 0..BINS - 2 {
            self.current[k] = self.current[k]
                .max(self.current[k + 1])
                .max(self.current[k + 2]);
        }
        self.spread[slot * BINS..(slot + 1) * BINS].copy_from_slice(&self.current);
        for back in [1, 3, 6] {
            let other = (slot + RING - back) % RING;
            for (s, &c) in self.spread[other * BINS..(other + 1) * BINS]
                .iter_mut()
                .zip(&self.current)
            {
                *s = s.max(c);
            }
        }

        self.frames += 1;
        if self.frames >= DETECT_DELAY {
            self.detect();
        }
    }

    /// Test frame `frames − 46` for peaks.
    fn detect(&mut self) {
        let candidate = self.frames - DETECT_DELAY;
        let a_slot = candidate % RING;
        let b_slot = (self.frames + RING - SPREAD_DELAY) % RING;
        let power = &self.power[a_slot * BINS..(a_slot + 1) * BINS];
        let spread_b = &self.spread[b_slot * BINS..(b_slot + 1) * BINS];

        for k in 10..=1014 {
            let v = power[k];
            if v < MIN_POWER || v < spread_b[k - 1] {
                continue;
            }
            let mut m = NEIGHBOUR_BINS
                .iter()
                .map(|&o| spread_b[k.wrapping_add_signed(o)])
                .fold(f32::MIN, f32::max);
            if v <= m {
                continue;
            }
            for &o in &NEIGHBOUR_FRAMES {
                let slot = (self.frames + o) % RING;
                m = m.max(self.spread[slot * BINS + k - 1]);
            }
            if v <= m {
                continue;
            }

            let mag = log_magnitude(power[k]);
            let before = log_magnitude(power[k - 1]);
            let after = log_magnitude(power[k + 1]);
            let curvature = 2.0 * mag - before - after;
            let shift = if curvature > 0.0 {
                (after - before) * 32.0 / curvature
            } else {
                0.0
            };
            let fbin = (k as i32 * 64 + shift.trunc() as i32).clamp(0, u16::MAX as i32) as u32;
            // fbin × 16000 / 2 / 1024 / 64 == fbin × 125 / 1024 (exact).
            let Some(band) = Band::from_hz(fbin * 125 / 1024) else {
                continue;
            };
            self.peaks[band.index()].push(Peak {
                frame: candidate as u32,
                magnitude: mag as u16,
                fbin: fbin as u16,
            });
        }
    }
}

fn log_magnitude(power: f32) -> f32 {
    power.ln().max(1.0 / 64.0) * 1477.3 + 6144.0
}

/// Serialise peaks into the binary signature format.
pub fn encode(bands: &BandPeaks, sample_count: u32) -> Vec<u8> {
    let mut out = vec![0u8; HEADER_LEN + 8];
    for (index, peaks) in bands.iter().enumerate() {
        if peaks.is_empty() {
            continue;
        }
        let mut sorted = peaks.clone();
        sorted.sort_by_key(|p| p.frame);
        let payload = encode_peaks(&sorted);
        put_u32(&mut out, TLV_BAND_BASE + index as u32);
        put_u32(&mut out, payload.len() as u32);
        out.extend_from_slice(&payload);
        out.resize(out.len().next_multiple_of(4), 0);
    }
    let body_len = (out.len() - HEADER_LEN) as u32;
    let sample_rate_samples = SAMPLE_RATE * 24 / 100;
    set_u32(&mut out, 0, MAGIC1);
    set_u32(&mut out, 8, body_len);
    set_u32(&mut out, 12, MAGIC2);
    set_u32(&mut out, 28, SAMPLE_RATE_ID_16K << 27);
    set_u32(
        &mut out,
        40,
        sample_count.saturating_add(sample_rate_samples),
    );
    set_u32(&mut out, 44, FIXED_FIELD);
    set_u32(&mut out, 48, TLV_SIGNATURE);
    set_u32(&mut out, 52, body_len);
    let crc = crc32fast::hash(&out[8..]);
    set_u32(&mut out, 4, crc);
    out
}

fn encode_peaks(peaks: &[Peak]) -> Vec<u8> {
    let mut out = Vec::with_capacity(peaks.len() * 5 + 16);
    let mut last = 0u32;
    for peak in peaks {
        if peak.frame - last >= 255 {
            out.push(0xFF);
            put_u32(&mut out, peak.frame);
            last = peak.frame;
        }
        out.push((peak.frame - last) as u8);
        out.extend_from_slice(&peak.magnitude.to_le_bytes());
        out.extend_from_slice(&peak.fbin.to_le_bytes());
        last = peak.frame;
    }
    out
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn set_u32(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

/// A parsed signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSignature {
    pub sample_rate: u32,
    /// Input samples covered (the header value minus the 0.24 s allowance).
    pub sample_count: u32,
    /// True when the blob came from Apple's generator (`magic2 = 0x42F02010`).
    pub apple: bool,
    pub bands: BandPeaks,
}

impl DecodedSignature {
    pub fn peak_count(&self) -> usize {
        self.bands.iter().map(Vec::len).sum()
    }
}

/// Parse a signature, with or without Apple's 12-byte `dataRepresentation` wrapper.
pub fn decode(bytes: &[u8]) -> Result<DecodedSignature> {
    let mut reader = Reader::new(bytes);
    let bytes = if reader.u32_at(0) == Some(APPLE_WRAPPER_MAGIC) {
        let skip = reader
            .u32_at(8)
            .ok_or_else(|| invalid("truncated wrapper"))? as usize;
        bytes
            .get(skip..)
            .ok_or_else(|| invalid("truncated wrapper"))?
    } else {
        bytes
    };
    reader = Reader::new(bytes);
    if bytes.len() < HEADER_LEN + 8 {
        return Err(invalid("too short"));
    }
    if reader.u32_at(0) != Some(MAGIC1) {
        return Err(invalid("bad magic"));
    }
    if reader.u32_at(4) != Some(crc32fast::hash(&bytes[8..])) {
        return Err(invalid("checksum mismatch"));
    }
    if reader.u32_at(8) != Some((bytes.len() - HEADER_LEN) as u32) {
        return Err(invalid("length mismatch"));
    }
    let apple = match reader.u32_at(12) {
        Some(MAGIC2) => false,
        Some(MAGIC2_APPLE) => true,
        _ => return Err(invalid("bad magic")),
    };
    let sample_rate = match reader.u32_at(28).map(|v| v >> 27) {
        Some(1) => 8000,
        Some(2) => 11025,
        Some(3) => 16000,
        Some(4) => 32000,
        Some(5) => 44100,
        Some(6) => 48000,
        _ => return Err(invalid("unknown sample rate")),
    };
    let declared = reader.u32_at(40).unwrap_or(0);
    let sample_count = declared.saturating_sub(sample_rate * 24 / 100);
    if reader.u32_at(48) != Some(TLV_SIGNATURE) {
        return Err(invalid("missing signature block"));
    }

    let mut bands: BandPeaks = Default::default();
    let mut pos = HEADER_LEN + 8;
    while pos < bytes.len() {
        let tag = reader
            .u32_at(pos)
            .ok_or_else(|| invalid("truncated block"))?;
        let len = reader
            .u32_at(pos + 4)
            .ok_or_else(|| invalid("truncated block"))? as usize;
        let start = pos + 8;
        let payload = start
            .checked_add(len)
            .and_then(|end| bytes.get(start..end))
            .ok_or_else(|| invalid("truncated block"))?;
        if let Some(index) = tag.checked_sub(TLV_BAND_BASE).filter(|i| *i < 4) {
            bands[index as usize] = decode_peaks(payload)?;
        }
        pos = (start + len).next_multiple_of(4);
    }
    Ok(DecodedSignature {
        sample_rate,
        sample_count,
        apple,
        bands,
    })
}

fn decode_peaks(payload: &[u8]) -> Result<Vec<Peak>> {
    let reader = Reader::new(payload);
    let mut peaks = Vec::with_capacity(payload.len() / 5);
    let mut pos = 0;
    let mut last = 0u32;
    while pos < payload.len() {
        let delta = payload[pos];
        if delta == 0xFF {
            last = reader
                .u32_at(pos + 1)
                .ok_or_else(|| invalid("truncated peak"))?;
            pos += 5;
            continue;
        }
        let magnitude = reader
            .u16_at(pos + 1)
            .ok_or_else(|| invalid("truncated peak"))?;
        let fbin = reader
            .u16_at(pos + 3)
            .ok_or_else(|| invalid("truncated peak"))?;
        let frame = last
            .checked_add(delta as u32)
            .ok_or_else(|| invalid("frame overflow"))?;
        peaks.push(Peak {
            frame,
            magnitude,
            fbin,
        });
        last = frame;
        pos += 5;
    }
    Ok(peaks)
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn u32_at(&self, pos: usize) -> Option<u32> {
        let b = self.bytes.get(pos..pos.checked_add(4)?)?;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u16_at(&self, pos: usize) -> Option<u16> {
        let b = self.bytes.get(pos..pos.checked_add(2)?)?;
        Some(u16::from_le_bytes([b[0], b[1]]))
    }
}

fn invalid(reason: &str) -> MfError {
    MfError::invalid(format!("Not a valid Shazam signature ({reason})."))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Deterministic test signal: a few harmonic tones with vibrato, note changes
    /// and noise bursts, loud enough to produce peaks in every band.
    pub(crate) fn test_signal(seconds: f32, seed: u64) -> Vec<i16> {
        let mut rng = fastrand::Rng::with_seed(seed);
        let n = (seconds * SAMPLE_RATE as f32) as usize;
        let note_len = SAMPLE_RATE as usize / 4;
        let mut out = Vec::with_capacity(n);
        let mut freqs = [0f32; 3];
        let mut phases = [0f32; 3];
        for i in 0..n {
            if i % note_len == 0 {
                for f in freqs.iter_mut() {
                    *f = 110.0 * 2f32.powf(rng.u32(0..48) as f32 / 12.0);
                }
            }
            let t = i as f32 / SAMPLE_RATE as f32;
            let mut s = 0.0;
            for (f, phase) in freqs.iter().zip(phases.iter_mut()) {
                let inst = f * (1.0 + 0.003 * (2.0 * std::f32::consts::PI * 5.0 * t).sin());
                *phase += 2.0 * std::f32::consts::PI * inst / SAMPLE_RATE as f32;
                for h in 1..=8 {
                    s += (*phase * h as f32).sin() / h as f32;
                }
            }
            if i % (note_len / 2) < 400 {
                s += (rng.f32() - 0.5) * 2.0;
            }
            out.push((s * 2500.0).clamp(-32767.0, 32767.0) as i16);
        }
        out
    }

    fn u32_at(bytes: &[u8], pos: usize) -> u32 {
        u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap())
    }

    #[test]
    fn window_matches_numpy_hanning_2050_trimmed() {
        let fp = Fingerprinter::new();
        // numpy.hanning(2050)[1:-1] at i = 0, 1023, 2047
        assert!((fp.window[0] - 2.350_443_6e-6).abs() < 1e-9);
        assert!((fp.window[1023] - 0.999_999_4).abs() < 1e-6);
        assert!((fp.window[2047] - fp.window[0]).abs() < 1e-9);
    }

    #[test]
    fn header_fields() {
        let samples = test_signal(12.0, 1);
        let sig = generate(&samples);
        let b = &sig.bytes;
        assert_eq!(u32_at(b, 0), 0xCAFE_2580);
        assert_eq!(u32_at(b, 8) as usize, b.len() - 48);
        assert_eq!(u32_at(b, 12), 0x9411_9C00);
        assert_eq!(&b[16..28], &[0; 12]);
        assert_eq!(u32_at(b, 28), 0x1800_0000);
        assert_eq!(&b[32..40], &[0; 8]);
        assert_eq!(u32_at(b, 40), 192_000 + 3840);
        assert_eq!(u32_at(b, 44), 0x007C_0000);
        assert_eq!(u32_at(b, 48), 0x4000_0000);
        assert_eq!(u32_at(b, 52) as usize, b.len() - 48);
        assert_eq!(u32_at(b, 56), 0x6003_0040);
        assert_eq!(b.len() % 4, 0);
        assert_eq!(sig.sample_count, 192_000);
        assert_eq!(sig.sample_ms(), 12_000);
    }

    #[test]
    fn crc_covers_everything_after_offset_8() {
        let sig = generate(&test_signal(6.0, 2));
        assert_eq!(u32_at(&sig.bytes, 4), crc32fast::hash(&sig.bytes[8..]));
        let mut tampered = sig.bytes.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(decode(&tampered).is_err());
    }

    #[test]
    fn roundtrip_decodes_own_encoding() {
        let samples = test_signal(12.0, 3);
        let bands = peaks(&samples, true);
        assert!(
            bands.iter().all(|b| !b.is_empty()),
            "every band should have peaks"
        );
        let sig = generate(&samples);
        let decoded = sig.decode().unwrap();
        assert_eq!(decoded.sample_rate, 16_000);
        assert_eq!(decoded.sample_count, 192_000);
        assert!(!decoded.apple);
        assert_eq!(decoded.bands, bands);
        for band in &decoded.bands {
            assert!(band.windows(2).all(|w| w[0].frame <= w[1].frame));
            assert!(band.iter().all(|p| p.frame <= 1500));
        }
    }

    #[test]
    fn roundtrip_handles_long_gaps_and_empty_bands() {
        let mut bands: BandPeaks = Default::default();
        bands[1] = vec![
            Peak {
                frame: 0,
                magnitude: 7000,
                fbin: 5000,
            },
            Peak {
                frame: 254,
                magnitude: 7001,
                fbin: 5001,
            },
            Peak {
                frame: 509,
                magnitude: 7002,
                fbin: 5002,
            },
            Peak {
                frame: 509,
                magnitude: 7003,
                fbin: 5003,
            },
            Peak {
                frame: 1400,
                magnitude: 7004,
                fbin: 5004,
            },
        ];
        bands[3] = vec![Peak {
            frame: 300,
            magnitude: 9000,
            fbin: 40000,
        }];
        let bytes = encode(&bands, 64_000);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.bands, bands);
        assert_eq!(decoded.sample_count, 64_000);
        // Empty bands are omitted: the first block is band 1.
        assert_eq!(u32_at(&bytes, 56), 0x6003_0041);
    }

    #[test]
    fn deterministic() {
        let samples = test_signal(8.0, 4);
        assert_eq!(generate(&samples), generate(&samples));
    }

    #[test]
    fn data_uri_is_base64_of_bytes() {
        let sig = generate(&test_signal(4.0, 5));
        let uri = sig.data_uri();
        let payload = uri
            .strip_prefix("data:audio/vnd.shazam.sig;base64,")
            .unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .unwrap(),
            sig.bytes
        );
    }

    #[test]
    fn long_input_uses_middle_twelve_seconds() {
        let samples = test_signal(20.0, 6);
        let start = (samples.len() - MAX_SAMPLES) / 2;
        let sig = generate(&samples);
        assert_eq!(sig.sample_count as usize, MAX_SAMPLES);
        assert_eq!(sig, generate(&samples[start..start + MAX_SAMPLES]));
    }

    #[test]
    fn silence_and_tiny_input_yield_empty_signatures() {
        for samples in [vec![0i16; 48_000], vec![1000i16; 100], Vec::new()] {
            let sig = generate(&samples);
            assert_eq!(sig.bytes.len(), 56);
            let decoded = sig.decode().unwrap();
            assert_eq!(decoded.peak_count(), 0);
            assert_eq!(decoded.sample_count as usize, samples.len());
        }
    }

    #[test]
    fn tail_padding_adds_late_peaks_only() {
        let samples = test_signal(12.0, 7);
        let plain = peaks(&samples, false);
        let padded = peaks(&samples, true);
        for (a, b) in plain.iter().zip(&padded) {
            assert_eq!(&b[..a.len()], &a[..]);
            assert!(b[a.len()..].iter().all(|p| p.frame > 1500 - 47));
        }
        let sig = generate_with(&samples, &Options { pad_tail: true });
        assert_eq!(sig.sample_count, 192_000);
        assert_eq!(sig, generate(&samples));
        let decoded = generate_with(&samples, &Options { pad_tail: false })
            .decode()
            .unwrap();
        assert_eq!(decoded.bands, plain);
    }

    #[test]
    fn decode_rejects_garbage_without_panicking() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0u8; 100]).is_err());
        assert!(decode(&[0x80, 0x25, 0x80, 0x25, 2, 0, 0, 0, 0xff, 0xff, 0xff, 0xff]).is_err());
        let sig = generate(&test_signal(4.0, 9));
        for cut in [10, 55, 57, sig.bytes.len() - 3] {
            assert!(decode(&sig.bytes[..cut]).is_err());
        }
    }

    #[test]
    fn decode_strips_apple_wrapper() {
        let sig = generate(&test_signal(4.0, 10));
        let mut wrapped = Vec::new();
        for v in [0x2580_2580u32, 2, 12] {
            wrapped.extend_from_slice(&v.to_le_bytes());
        }
        wrapped.extend_from_slice(&sig.bytes);
        assert_eq!(decode(&wrapped).unwrap(), sig.decode().unwrap());
    }

    #[test]
    fn bands_follow_documented_ranges() {
        assert_eq!(Band::from_hz(249), None);
        assert_eq!(Band::from_hz(250), Some(Band::Low));
        assert_eq!(Band::from_hz(519), Some(Band::Low));
        assert_eq!(Band::from_hz(520), Some(Band::LowMid));
        assert_eq!(Band::from_hz(1450), Some(Band::HighMid));
        assert_eq!(Band::from_hz(3500), Some(Band::High));
        assert_eq!(Band::from_hz(5500), Some(Band::High));
        assert_eq!(Band::from_hz(5501), None);
    }
}
