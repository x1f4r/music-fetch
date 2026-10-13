//! From pitch frames to a matchable query: cleaned contour and notes.
//!
//! Frames are first placed on the 16 ms grid, then cleaned: voiced islands
//! shorter than 48 ms are dropped, a 5-frame median filter smooths jitter, short
//! excursions an octave away from their surroundings are folded back, gaps under
//! 200 ms hold the previous pitch and gaps of 300 ms or more stay as phrase
//! breaks. The contour is resampled to 20 fps. Notes come from the same dynamic
//! programme SwiftF0's `segment_notes` uses (80 ms pitch hold), followed by a
//! 60 ms minimum length and merging of same-pitch fragments that no voicing or
//! energy dip separates.

use crate::melody::pitch::FrameDetail;
use crate::model::PitchFrame;

/// Contour frame rate used for matching.
pub const CONTOUR_FPS: u32 = 20;
/// Contour frame length.
pub const CONTOUR_MS: u32 = 1000 / CONTOUR_FPS;
/// Minimum voiced material for a query.
pub const MIN_VOICED_MS: u32 = 2500;

const FRAME_MS: f64 = 16.0;
/// Voiced runs shorter than this many frames are dropped (48 ms).
const MIN_ISLAND: usize = 3;
const MEDIAN_WIDTH: usize = 5;
/// Octave folding: runs up to this long (frames, ~250 ms)...
const OCTAVE_RUN_MAX: usize = 16;
/// ...compared with this much context on each side (frames, ~500 ms).
const OCTAVE_CONTEXT: usize = 31;
/// Gaps shorter than this hold the previous pitch (frames, < 200 ms).
const HOLD_GAP: usize = 12;
/// Penalty per note in the segmentation, as a duration.
const PITCH_HOLD_MS: f64 = 80.0;
const MIN_NOTE_MS: u32 = 60;
/// Longest query, in frames (30 s).
const MAX_QUERY_FRAMES: usize = 1875;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Note {
    /// MIDI note number (fractional).
    pub pitch: f32,
    pub onset_ms: u32,
    pub dur_ms: u32,
}

#[derive(Debug, Clone)]
pub struct MelodyQuery {
    /// 20 fps contour in MIDI semitones; NaN = unvoiced.
    pub contour: Vec<f32>,
    pub notes: Vec<Note>,
    pub voiced_ms: u32,
}

/// None when there is too little voiced material to search with.
pub fn query_from_frames(frames: &[PitchFrame]) -> Option<MelodyQuery> {
    let grid = Grid::from_pitch_frames(frames)?;
    build(grid)
}

/// Like [`query_from_frames`], using loudness to split repeated notes.
pub fn query_from_details(frames: &[FrameDetail]) -> Option<MelodyQuery> {
    let grid = Grid::from_details(frames)?;
    build(grid)
}

/// Frames on the 16 ms grid.
struct Grid {
    midi: Vec<f32>,
    conf: Vec<f32>,
    level: Option<Vec<f32>>,
}

impl Grid {
    fn from_pitch_frames(frames: &[PitchFrame]) -> Option<Self> {
        Self::place(frames.iter().map(|f| (f.t_ms, f.midi, f.confidence, None)))
    }

    fn from_details(frames: &[FrameDetail]) -> Option<Self> {
        Self::place(
            frames
                .iter()
                .map(|f| (f.t_ms, f.midi(), f.confidence, Some(f.loudness_db))),
        )
    }

    fn place(frames: impl Iterator<Item = (u64, f32, f32, Option<f32>)> + Clone) -> Option<Self> {
        let first = frames.clone().map(|f| f.0).min()?;
        let last = frames.clone().map(|f| f.0).max()?;
        let n = ((last - first) as f64 / FRAME_MS).round() as usize + 1;
        // A day of frames is far beyond any query; refuse rather than allocate.
        if n > 5_400_000 {
            return None;
        }
        let mut midi = vec![f32::NAN; n];
        let mut conf = vec![0f32; n];
        let mut level: Option<Vec<f32>> = None;
        for (t, m, c, l) in frames {
            let i = (((t - first) as f64) / FRAME_MS).round() as usize;
            if i >= n {
                continue;
            }
            midi[i] = if m.is_finite() && (0.0..140.0).contains(&m) {
                m
            } else {
                f32::NAN
            };
            conf[i] = if c.is_finite() {
                c.clamp(0.0, 1.0)
            } else {
                0.0
            };
            if let Some(l) = l.filter(|l| l.is_finite()) {
                level.get_or_insert_with(|| vec![-140.0; n])[i] = l;
            }
        }
        Some(Self { midi, conf, level })
    }
}

fn build(grid: Grid) -> Option<MelodyQuery> {
    let mut midi = grid.midi;
    drop_islands(&mut midi);
    // Queries are at most 30 s from the first voiced frame.
    let start = midi.iter().position(|m| m.is_finite())?;
    midi.truncate(start + MAX_QUERY_FRAMES);
    let voiced = midi.iter().filter(|m| m.is_finite()).count();
    let voiced_ms = (voiced as f64 * FRAME_MS) as u32;
    if voiced_ms < MIN_VOICED_MS {
        return None;
    }
    let mut midi = median_filter(&midi);
    fold_octaves(&mut midi);
    let first = midi.iter().position(|m| m.is_finite())?;
    let last = midi.iter().rposition(|m| m.is_finite())?;
    let midi = midi[first..=last].to_vec();
    let conf = &grid.conf[first..=last];
    let level = grid.level.as_ref().map(|l| &l[first..=last]);
    let notes = segment(&midi, conf, level);
    let filled = hold_gaps(&midi);
    Some(MelodyQuery {
        contour: resample_contour(&filled),
        notes,
        voiced_ms,
    })
}

/// Unvoice voiced runs shorter than `MIN_ISLAND` frames.
fn drop_islands(midi: &mut [f32]) {
    for (a, b) in runs(midi) {
        if b - a < MIN_ISLAND {
            midi[a..b].fill(f32::NAN);
        }
    }
}

/// Voiced runs as half-open ranges.
fn runs(midi: &[f32]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, m) in midi.iter().enumerate() {
        match (m.is_finite(), start) {
            (true, None) => start = Some(i),
            (false, Some(a)) => {
                out.push((a, i));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(a) = start {
        out.push((a, midi.len()));
    }
    out
}

/// Median of the voiced frames in a centered window; unvoiced frames stay unvoiced.
fn median_filter(midi: &[f32]) -> Vec<f32> {
    let half = MEDIAN_WIDTH / 2;
    let mut buf = Vec::with_capacity(MEDIAN_WIDTH);
    midi.iter()
        .enumerate()
        .map(|(i, &m)| {
            if !m.is_finite() {
                return m;
            }
            buf.clear();
            let lo = i.saturating_sub(half);
            let hi = (i + half + 1).min(midi.len());
            buf.extend(midi[lo..hi].iter().copied().filter(|v| v.is_finite()));
            median(&mut buf).unwrap_or(m)
        })
        .collect()
}

pub(crate) fn median(v: &mut [f32]) -> Option<f32> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f32::total_cmp);
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        0.5 * (v[n / 2 - 1] + v[n / 2])
    })
}

/// Fold short stable stretches that sit an octave from their surroundings.
fn fold_octaves(midi: &mut [f32]) {
    // Stretches of voiced frames that stay within a semitone of their start.
    let mut segments = Vec::new();
    for (a, b) in runs(midi) {
        let mut s = a;
        for i in a + 1..=b {
            if i == b || (midi[i] - midi[i - 1]).abs() > 1.5 {
                segments.push((s, i));
                s = i;
            }
        }
    }
    for &(a, b) in &segments {
        if b - a > OCTAVE_RUN_MAX {
            continue;
        }
        let mut context: Vec<f32> = midi[a.saturating_sub(OCTAVE_CONTEXT)..a]
            .iter()
            .chain(&midi[b..(b + OCTAVE_CONTEXT).min(midi.len())])
            .copied()
            .filter(|v| v.is_finite())
            .collect();
        if context.len() < 2 * MIN_ISLAND {
            continue;
        }
        let Some(around) = median(&mut context) else {
            continue;
        };
        let mut own: Vec<f32> = midi[a..b].to_vec();
        let Some(here) = median(&mut own) else {
            continue;
        };
        let d = here - around;
        let shift = if (d - 12.0).abs() < 2.5 {
            -12.0
        } else if (d + 12.0).abs() < 2.5 {
            12.0
        } else {
            continue;
        };
        for m in &mut midi[a..b] {
            *m += shift;
        }
    }
}

/// Hold the previous pitch through gaps shorter than `HOLD_GAP` frames.
fn hold_gaps(midi: &[f32]) -> Vec<f32> {
    let mut out = midi.to_vec();
    let mut i = 0;
    while i < out.len() {
        if out[i].is_finite() {
            i += 1;
            continue;
        }
        let start = i;
        while i < out.len() && !out[i].is_finite() {
            i += 1;
        }
        if start > 0 && i < out.len() && i - start < HOLD_GAP {
            let held = out[start - 1];
            out[start..i].fill(held);
        }
    }
    out
}

/// Median of the 16 ms frames centered in each 50 ms bin; NaN unless at least half
/// of them are voiced.
fn resample_contour(midi: &[f32]) -> Vec<f32> {
    let total_ms = midi.len() as f64 * FRAME_MS;
    let bins = (total_ms / CONTOUR_MS as f64).ceil() as usize;
    let mut buf = Vec::with_capacity(4);
    (0..bins)
        .map(|k| {
            let center = k as f64 * CONTOUR_MS as f64;
            let lo = ((center - CONTOUR_MS as f64 / 2.0) / FRAME_MS)
                .ceil()
                .max(0.0) as usize;
            let hi =
                (((center + CONTOUR_MS as f64 / 2.0) / FRAME_MS).ceil() as usize).min(midi.len());
            buf.clear();
            buf.extend(
                midi[lo.min(hi)..hi]
                    .iter()
                    .copied()
                    .filter(|v| v.is_finite()),
            );
            if hi > lo && 2 * buf.len() >= hi - lo {
                median(&mut buf).unwrap_or(f32::NAN)
            } else {
                f32::NAN
            }
        })
        .collect()
}

/// Notes by the minimum-cost split of frames into notes and gaps (SwiftF0's
/// `segment_notes`), then the minimum length and same-pitch merging.
fn segment(midi: &[f32], conf: &[f32], level: Option<&[f32]>) -> Vec<Note> {
    let n = midi.len();
    let w: Vec<f64> = (0..n)
        .map(|t| {
            if midi[t].is_finite() {
                conf[t] as f64
            } else {
                0.0
            }
        })
        .collect();
    let quiet = quiet_cost(level, n);
    let q: Vec<f64> = (0..n)
        .map(|t| {
            let c = w[t].clamp(0.01, 0.99);
            -(c / (1.0 - c)).ln() + quiet[t]
        })
        .collect();
    let mut mu: Vec<f32> = midi
        .iter()
        .filter(|m| m.is_finite())
        .map(|m| (m * 100.0).round() / 100.0)
        .collect();
    mu.sort_by(f32::total_cmp);
    mu.dedup();
    if mu.is_empty() {
        return Vec::new();
    }
    let beta = PITCH_HOLD_MS / FRAME_MS;
    let mut vn = vec![f64::INFINITY; mu.len()];
    let mut note_start = vec![0usize; mu.len()];
    let mut back = vec![0usize; n + 1];
    let mut kind = vec![usize::MAX; n + 1];
    let mut best = 0.0f64;
    for t in 0..n {
        let start = best + beta;
        let m = midi[t];
        for (j, &u) in mu.iter().enumerate() {
            if start < vn[j] {
                vn[j] = start;
                note_start[j] = t;
            }
            let off = if m.is_finite() {
                ((u - m).abs() as f64).min(2.0)
            } else {
                0.0
            };
            vn[j] += w[t] * off + q[t];
        }
        let (j, &v) = vn
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.total_cmp(b.1))
            .unwrap_or((0, &f64::INFINITY));
        if v < best {
            best = v;
            back[t + 1] = note_start[j];
            kind[t + 1] = j;
        } else {
            back[t + 1] = t;
        }
    }
    let mut spans = Vec::new();
    let mut b = n;
    while b > 0 {
        let a = back[b];
        if kind[b] != usize::MAX {
            spans.push((a, b, mu[kind[b]]));
        }
        b = a;
    }
    spans.reverse();
    let min_frames = (MIN_NOTE_MS as f64 / FRAME_MS).ceil() as usize;
    spans.retain(|&(a, b, _)| b - a >= min_frames);
    let spans = merge_repeats(spans, &w, level);
    spans
        .into_iter()
        .map(|(a, b, p)| Note {
            pitch: p,
            onset_ms: (a as f64 * FRAME_MS) as u32,
            dur_ms: ((b - a) as f64 * FRAME_MS) as u32,
        })
        .collect()
}

/// SwiftF0's "quiet" term: dB by which a frame lies more than 3 dB below the
/// loudest frames within 64 ms on both sides.
fn quiet_cost(level: Option<&[f32]>, n: usize) -> Vec<f64> {
    let Some(level) = level else {
        return vec![0.0; n];
    };
    let at = |i: isize| -> f64 {
        // Symmetric padding as numpy's "symmetric" mode.
        let len = n as isize;
        let j = if i < 0 {
            -i - 1
        } else if i >= len {
            2 * len - i - 1
        } else {
            i
        };
        level[j.clamp(0, len - 1) as usize] as f64
    };
    (0..n as isize)
        .map(|t| {
            let left = (t - 4..=t).map(at).fold(f64::NEG_INFINITY, f64::max);
            let right = (t..=t + 4).map(at).fold(f64::NEG_INFINITY, f64::max);
            (left.min(right) - at(t) - 10.0 * 2f64.log10()).max(0.0)
        })
        .collect()
}

/// Merge adjacent same-pitch fragments unless a voicing or energy dip separates them.
fn merge_repeats(
    spans: Vec<(usize, usize, f32)>,
    w: &[f64],
    level: Option<&[f32]>,
) -> Vec<(usize, usize, f32)> {
    let mut out: Vec<(usize, usize, f32)> = Vec::with_capacity(spans.len());
    for s in spans {
        if let Some(prev) = out.last_mut()
            && s.0 == prev.1
            && (s.2 - prev.2).abs() < 0.5
            && !dip_between(prev.0, prev.1, s.1, w, level)
        {
            let (la, lb) = ((prev.1 - prev.0) as f32, (s.1 - s.0) as f32);
            prev.2 = (prev.2 * la + s.2 * lb) / (la + lb);
            prev.1 = s.1;
            continue;
        }
        out.push(s);
    }
    out
}

/// A confidence or loudness dip around the boundary `mid` of `[a, mid)` and `[mid, b)`.
fn dip_between(a: usize, mid: usize, b: usize, w: &[f64], level: Option<&[f32]>) -> bool {
    let lo = mid.saturating_sub(2).max(a);
    let hi = (mid + 2).min(b);
    let typical = |s: &[f64]| {
        let mut v: Vec<f32> = s.iter().map(|&x| x as f32).collect();
        median(&mut v).unwrap_or(0.0) as f64
    };
    let floor_w = w[lo..hi].iter().copied().fold(f64::INFINITY, f64::min);
    if floor_w < 0.5 * typical(&w[a..mid]).min(typical(&w[mid..b])) {
        return true;
    }
    if let Some(level) = level {
        let lv: Vec<f64> = level.iter().map(|&x| x as f64).collect();
        let floor_l = lv[lo..hi].iter().copied().fold(f64::INFINITY, f64::min);
        if floor_l < typical(&lv[a..mid]).min(typical(&lv[mid..b])) - 6.0 {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(spec: &[(f32, u32)]) -> Vec<PitchFrame> {
        // (midi or NaN, duration in ms)
        let mut out = Vec::new();
        let mut t = 0u64;
        for &(m, ms) in spec {
            for _ in 0..ms / 16 {
                out.push(PitchFrame {
                    t_ms: t,
                    midi: m,
                    confidence: if m.is_finite() { 0.9 } else { 0.1 },
                });
                t += 16;
            }
        }
        out
    }

    #[test]
    fn too_short_is_none() {
        assert!(
            query_from_frames(&frames(&[(60.0, 1000), (f32::NAN, 500), (62.0, 1000)])).is_none()
        );
        assert!(query_from_frames(&[]).is_none());
    }

    #[test]
    fn notes_and_contour_follow_the_melody() {
        let q = query_from_frames(&frames(&[
            (f32::NAN, 400),
            (60.0, 496),
            (62.0, 496),
            (64.0, 496),
            (f32::NAN, 400),
            (65.0, 496),
            (67.0, 992),
        ]))
        .expect("query");
        let pitches: Vec<f32> = q.notes.iter().map(|n| n.pitch).collect();
        assert_eq!(pitches, vec![60.0, 62.0, 64.0, 65.0, 67.0]);
        assert_eq!(q.notes[0].onset_ms, 0);
        // The phrase break stays unvoiced in the 20 fps contour; leading silence is trimmed.
        assert!((q.contour[0] - 60.0).abs() < 1e-3);
        let breaks = q.contour.iter().filter(|m| m.is_nan()).count();
        assert!((6..=9).contains(&breaks), "{breaks}");
        assert!(q.voiced_ms >= 2900);
    }

    #[test]
    fn short_gaps_hold_and_octave_slips_fold() {
        let mut f = frames(&[
            (60.0, 1008),
            (f32::NAN, 96),
            (60.0, 800),
            (72.0, 128),
            (60.0, 800),
            (62.0, 800),
        ]);
        // A single wild frame is smoothed away by the median filter.
        f[10].midi = 90.0;
        let q = query_from_frames(&f).expect("query");
        // 2832 ms of 60 (gap held, octave slip folded), then 62.
        assert!(
            q.contour.iter().take(56).all(|m| (m - 60.0).abs() < 0.01),
            "{:?}",
            &q.contour[..56]
        );
        assert!(q.contour.iter().skip(58).all(|m| (m - 62.0).abs() < 0.01));
        let pitches: Vec<f32> = q.notes.iter().map(|n| n.pitch.round()).collect();
        assert_eq!(pitches.last(), Some(&62.0));
        assert!(!pitches.contains(&72.0));
    }

    #[test]
    fn repeated_notes_split_on_voicing_dips_only() {
        // Same pitch twice with a 32 ms unvoiced dip: two notes.
        let q = query_from_frames(&frames(&[
            (64.0, 800),
            (f32::NAN, 32),
            (64.0, 800),
            (65.0, 1200),
        ]))
        .expect("query");
        assert_eq!(
            q.notes
                .iter()
                .filter(|n| (n.pitch - 64.0).abs() < 0.1)
                .count(),
            2
        );
        // Continuous: one note.
        let q = query_from_frames(&frames(&[(64.0, 1600), (65.0, 1200)])).expect("query");
        assert_eq!(q.notes.len(), 2);
    }

    #[test]
    fn irregular_and_hostile_input_does_not_panic() {
        let mut f = frames(&[(60.0, 3000), (61.0, 1000)]);
        f[3].midi = f32::INFINITY;
        f[4].confidence = f32::NAN;
        f.swap(10, 40);
        let _ = query_from_frames(&f);
        let _ = query_from_frames(&[
            PitchFrame {
                t_ms: u64::MAX,
                midi: 60.0,
                confidence: 1.0,
            },
            PitchFrame {
                t_ms: 0,
                midi: 60.0,
                confidence: 1.0,
            },
        ]);
    }
}
