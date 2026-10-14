//! Peaks and landmark hashes.

use crate::audio::{PcmFile, SAMPLE_RATE};
use crate::error::Result;
use crate::shazam::signature::{self, HOP};
use crate::util::cancel::CancelToken;

/// Duration of one peak frame in milliseconds (128 samples at 16 kHz).
pub const FRAME_MS: f64 = HOP as f64 * 1000.0 / SAMPLE_RATE as f64;

/// Smallest anchor–target distance in frames (0.08 s).
pub(super) const DT_MIN: f32 = 10.0;
/// Largest anchor–target distance in frames (1.5 s).
pub(super) const DT_MAX: f32 = 187.0;
/// Targets paired with each anchor.
pub(super) const FANOUT: usize = 6;
/// Log-frequency bucket: 1.25 % (about a fifth of a semitone).
const LOG_STEP: f32 = 0.012_422;
/// ln(200 Hz): bucket 0.
const LOG_BASE: f32 = 5.298_317;
/// Largest anchor–target frequency difference in buckets (about ±2.2×).
pub(super) const DF_MAX: i32 = 63;
/// Time-difference bucket in frames (16 ms).
const DT_STEP: f32 = 2.0;

/// A spectral peak.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Peak {
    /// Frame index (8 ms) from the start of the analysed audio.
    pub frame: u32,
    /// Natural log of the frequency in Hz.
    pub log_hz: f32,
    pub magnitude: u16,
}

/// Peaks of 16 kHz mono audio, in time order.
pub fn peaks(samples: &[i16]) -> Vec<Peak> {
    let bands = signature::peaks(samples, true);
    let mut out: Vec<Peak> = bands
        .iter()
        .flatten()
        .filter(|p| p.fbin > 0)
        .map(|p| Peak { frame: p.frame, log_hz: p.hz().ln(), magnitude: p.magnitude })
        .collect();
    out.sort_by(|a, b| a.frame.cmp(&b.frame).then(a.log_hz.total_cmp(&b.log_hz)));
    out
}

/// Peaks of `[start_ms, start_ms + len_ms)` of a PCM file, read in chunks so
/// memory stays bounded. Frames count from `start_ms`.
pub fn peaks_in(pcm: &PcmFile, start_ms: u64, len_ms: u64, cancel: &CancelToken) -> Result<Vec<Peak>> {
    // Chunks and pre-roll are whole frames so every chunk shares one hop grid.
    const CHUNK_FRAMES: u64 = 7_500;
    // The detector needs ~2.5 s of history before its peaks are stable.
    const PREROLL_FRAMES: u64 = 320;
    // Peaks are reported 46 frames late; read a little past the chunk.
    const TAIL_FRAMES: u64 = 64;
    let hop = HOP as u64;
    let start = crate::audio::pcm::ms_to_samples(start_ms);
    let end = crate::audio::pcm::ms_to_samples(start_ms.saturating_add(len_ms)).min(pcm.len_samples());
    let mut out = Vec::new();
    let mut chunk = 0u64;
    loop {
        let chunk_start = start + chunk * CHUNK_FRAMES * hop;
        if chunk_start >= end {
            break;
        }
        cancel.check()?;
        let chunk_end = (chunk_start + CHUNK_FRAMES * hop).min(end);
        let preroll = PREROLL_FRAMES.min(chunk_start / hop);
        let read_start = chunk_start - preroll * hop;
        let count = (chunk_end + TAIL_FRAMES * hop).min(pcm.len_samples()) - read_start;
        let samples = pcm.read_samples(read_start, usize::try_from(count).unwrap_or(usize::MAX))?;
        let first = chunk * CHUNK_FRAMES;
        let last = first + (chunk_end - chunk_start).div_ceil(hop);
        for p in peaks(&samples) {
            let frame = (p.frame as u64 + first).checked_sub(preroll);
            if let Some(frame) = frame.filter(|f| (first..last).contains(f)) {
                out.push(Peak { frame: frame as u32, ..p });
            }
        }
        chunk += 1;
    }
    Ok(out)
}

/// A hash with the anchor's time in reference frames.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Landmark {
    pub hash: u32,
    pub t: f32,
}

/// Landmarks of `peaks` mapped into reference coordinates: times are
/// multiplied by `time_factor`, frequencies divided by `freq_factor`.
pub(super) fn landmarks(peaks: &[Peak], time_factor: f32, freq_factor: f32, out: &mut Vec<Landmark>) {
    out.clear();
    let log_shift = freq_factor.ln();
    let bucket = |log_hz: f32| ((log_hz - log_shift - LOG_BASE) / LOG_STEP).round() as i32;
    let buckets: Vec<i32> = peaks.iter().map(|p| bucket(p.log_hz)).collect();
    for (i, anchor) in peaks.iter().enumerate() {
        let f1 = buckets[i];
        if !(0..512).contains(&f1) {
            continue;
        }
        let t1 = anchor.frame as f32 * time_factor;
        let mut taken = 0;
        for (j, target) in peaks.iter().enumerate().skip(i + 1) {
            let dt = (target.frame as f32 - anchor.frame as f32) * time_factor;
            if dt < DT_MIN {
                continue;
            }
            if dt > DT_MAX {
                break;
            }
            let df = buckets[j] - f1;
            if df.abs() > DF_MAX {
                continue;
            }
            let dtq = (dt / DT_STEP).round() as u32;
            let hash = (f1 as u32) << 14 | ((df + 64) as u32) << 7 | (dtq & 0x7F);
            out.push(Landmark { hash, t: t1 });
            taken += 1;
            if taken == FANOUT {
                break;
            }
        }
    }
}
