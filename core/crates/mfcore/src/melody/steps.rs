//! Rhythm-free note matching: the query's pitch changes aligned with a
//! reference's, ignoring how long each note lasts.
//!
//! Contours compare well when the hum keeps the song's rhythm; people often
//! shorten long notes and skip rests, which no tempo or warping path within
//! sensible slopes absorbs. The sequence of pitch changes survives that, so
//! the matcher scores candidates on both.

use crate::melody::notes::{CONTOUR_MS, Note};

/// Query notes shorter than this are dropped from the pitch changes (ms).
const MIN_STEP_MS: u32 = 100;
/// Query pitch drifts below this collapse into one pitch change (semitones).
const QUERY_COLLAPSE: f32 = 0.7;
/// Reference frames this far from the current step start a new one (semitones).
const REF_SPLIT: f32 = 0.5;
/// Reference steps shorter than this between two others are ornaments, glides or
/// transcription noise that hums leave out (ms).
const REF_MIN_STEP_MS: u32 = 180;
/// Alignment costs: insertion/deletion, merge (plus its pitch cost) and the
/// weight of rhythm against pitch in a substitution.
const ALIGN_INDEL: f32 = 0.7;
const ALIGN_MERGE: f32 = 0.35;
const ALIGN_RHYTHM: f32 = 0.1;

/// A note of the collapsed sequence: consecutive notes of about the same pitch merged.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Step {
    pub pitch: f32,
    pub onset_ms: u32,
    pub end_ms: u32,
}

/// The query's pitch changes: blips shorter than 100 ms dropped and drifts of
/// less than 0.7 semitone collapsed (sung pitch wanders more than notated pitch).
pub fn query_steps(notes: &[Note]) -> Vec<Step> {
    let mut out: Vec<Step> = Vec::with_capacity(notes.len());
    for n in notes.iter().filter(|n| n.dur_ms >= MIN_STEP_MS && n.pitch.is_finite()) {
        let end = n.onset_ms.saturating_add(n.dur_ms);
        match out.last_mut() {
            Some(last) if (n.pitch - last.pitch).abs() < QUERY_COLLAPSE => last.end_ms = last.end_ms.max(end),
            _ => out.push(Step { pitch: n.pitch, onset_ms: n.onset_ms, end_ms: end }),
        }
    }
    out
}

/// Pitch changes of a 20 fps contour: a new step starts where the pitch moves
/// half a semitone from the current step's first frame; rests end steps. Short
/// inner steps are dropped and neighbours left at the same pitch merge.
pub fn frame_steps(frames: &[f32]) -> Vec<Step> {
    let mut out: Vec<Step> = Vec::new();
    let mut open = false;
    for (i, &v) in frames.iter().enumerate() {
        let t = i as u32 * CONTOUR_MS;
        if !v.is_finite() {
            open = false;
            continue;
        }
        match out.last_mut() {
            Some(last) if open && (v - last.pitch).abs() < REF_SPLIT => last.end_ms = t + CONTOUR_MS,
            Some(last) if !open && (v - last.pitch).abs() < REF_SPLIT && t <= last.end_ms + 300 => {
                last.end_ms = t + CONTOUR_MS;
                open = true;
            }
            _ => {
                out.push(Step { pitch: v, onset_ms: t, end_ms: t + CONTOUR_MS });
                open = true;
            }
        }
    }
    let mut kept: Vec<Step> = Vec::with_capacity(out.len());
    for (i, s) in out.iter().enumerate() {
        let inner = i > 0 && i + 1 < out.len();
        if inner && s.end_ms - s.onset_ms < REF_MIN_STEP_MS {
            continue;
        }
        match kept.last_mut() {
            Some(last) if (s.pitch - last.pitch).abs() < REF_SPLIT && s.onset_ms <= last.end_ms + 300 => {
                last.end_ms = s.end_ms;
            }
            _ => kept.push(*s),
        }
    }
    kept
}

/// (interval in semitones, log2 ratio of consecutive inter-onset intervals).
pub fn shape(steps: &[Step]) -> Vec<(f32, f32)> {
    let ioi: Vec<f32> =
        steps.windows(2).map(|w| (w[1].onset_ms.saturating_sub(w[0].onset_ms)).max(10) as f32).collect();
    steps
        .windows(2)
        .enumerate()
        .map(|(i, w)| {
            let ratio = if i == 0 { 0.0 } else { (ioi[i] / ioi[i - 1]).log2() };
            (w[1].pitch - w[0].pitch, ratio)
        })
        .collect()
}

/// Reusable dynamic-programming rows.
#[derive(Default)]
pub struct AlignRows {
    rows: [Vec<f32>; 3],
}

/// Semi-global alignment of the query's pitch changes (`qs`, from [`shape`])
/// inside a reference's (`rs`): free start and end in the reference,
/// substitutions scored by interval and rhythm difference, insertions and
/// deletions, and merges of two intervals for split or missed notes. Returns
/// the best cost per query interval (None when either side is too short).
pub fn align(qs: &[(f32, f32)], rs: &[(f32, f32)], t: &mut AlignRows) -> Option<f32> {
    let (m, n) = (qs.len(), rs.len());
    if m < 2 || n < 2 {
        return None;
    }
    let sub =
        |a: (f32, f32), b: (f32, f32)| (a.0 - b.0).abs().min(4.0) / 4.0 + ALIGN_RHYTHM * (a.1 - b.1).abs().min(1.5);
    let pitch = |a: f32, b: f32| (a - b).abs().min(4.0) / 4.0;
    for r in &mut t.rows {
        r.clear();
        r.resize(n + 1, 0.0);
    }
    // rows[0] = i - 2, rows[1] = i - 1, rows[2] = i.
    for i in 1..=m {
        t.rows.rotate_left(1);
        let [p2, p1, cur] = &mut t.rows;
        cur[0] = i as f32 * ALIGN_INDEL;
        for j in 1..=n {
            let mut best = p1[j - 1] + sub(qs[i - 1], rs[j - 1]);
            best = best.min(p1[j] + ALIGN_INDEL).min(cur[j - 1] + ALIGN_INDEL);
            if i >= 2 {
                best = best.min(p2[j - 1] + ALIGN_MERGE + pitch(qs[i - 2].0 + qs[i - 1].0, rs[j - 1].0));
            }
            if j >= 2 {
                best = best.min(p1[j - 2] + ALIGN_MERGE + pitch(qs[i - 1].0, rs[j - 2].0 + rs[j - 1].0));
            }
            cur[j] = best;
        }
    }
    let last = &t.rows[2];
    last[1..].iter().copied().fold(None, |b: Option<f32>, x| Some(b.map_or(x, |b| b.min(x)))).map(|c| c / m as f32)
}

/// Dynamic-programming buffers for [`align_pairs`].
#[derive(Default)]
pub struct AlignTable {
    cost: Vec<f32>,
    from: Vec<u8>,
}

/// Like [`align`], returning the best one or two placements (at least `m` steps
/// apart in the reference) as (cost per query interval, matched (query step,
/// reference step) pairs).
pub fn align_pairs(qs: &[(f32, f32)], rs: &[(f32, f32)], t: &mut AlignTable) -> Vec<(f32, Vec<(u16, u16)>)> {
    let (m, n) = (qs.len(), rs.len());
    if m < 2 || n < 2 || m > u16::MAX as usize || n > u16::MAX as usize {
        return Vec::new();
    }
    let w = n + 1;
    t.cost.clear();
    t.cost.resize((m + 1) * w, 0.0);
    t.from.clear();
    t.from.resize((m + 1) * w, 0);
    let sub =
        |a: (f32, f32), b: (f32, f32)| (a.0 - b.0).abs().min(4.0) / 4.0 + ALIGN_RHYTHM * (a.1 - b.1).abs().min(1.5);
    let pitch = |a: f32, b: f32| (a - b).abs().min(4.0) / 4.0;
    for i in 1..=m {
        t.cost[i * w] = i as f32 * ALIGN_INDEL;
        t.from[i * w] = 2;
        for j in 1..=n {
            let mut best = (t.cost[(i - 1) * w + j - 1] + sub(qs[i - 1], rs[j - 1]), 1u8);
            let up = t.cost[(i - 1) * w + j] + ALIGN_INDEL;
            if up < best.0 {
                best = (up, 2);
            }
            let left = t.cost[i * w + j - 1] + ALIGN_INDEL;
            if left < best.0 {
                best = (left, 3);
            }
            if i >= 2 {
                let merged = t.cost[(i - 2) * w + j - 1] + ALIGN_MERGE + pitch(qs[i - 2].0 + qs[i - 1].0, rs[j - 1].0);
                if merged < best.0 {
                    best = (merged, 4);
                }
            }
            if j >= 2 {
                let merged = t.cost[(i - 1) * w + j - 2] + ALIGN_MERGE + pitch(qs[i - 1].0, rs[j - 2].0 + rs[j - 1].0);
                if merged < best.0 {
                    best = (merged, 5);
                }
            }
            t.cost[i * w + j] = best.0;
            t.from[i * w + j] = best.1;
        }
    }
    let last = &t.cost[m * w..(m + 1) * w];
    let mut ends: Vec<usize> = (1..=n).collect();
    ends.sort_by(|&a, &b| last[a].total_cmp(&last[b]));
    let mut out: Vec<(f32, Vec<(u16, u16)>)> = Vec::with_capacity(2);
    let mut used: Vec<usize> = Vec::with_capacity(2);
    for end in ends {
        if out.len() >= 2 {
            break;
        }
        if used.iter().any(|&u| u.abs_diff(end) <= m.max(8)) {
            continue;
        }
        used.push(end);
        // Backtrack; matched interval (i-1, j-1) pairs step i-1 with step j-1.
        let mut pairs: Vec<(u16, u16)> = vec![(m as u16, end as u16)];
        let (mut i, mut j) = (m, end);
        while i > 0 {
            match t.from[i * w + j] {
                1 => {
                    pairs.push(((i - 1) as u16, (j - 1) as u16));
                    i -= 1;
                    j -= 1;
                }
                2 => i -= 1,
                3 => j -= 1,
                4 => {
                    pairs.push(((i - 2) as u16, (j - 1) as u16));
                    i -= 2;
                    j -= 1;
                }
                5 => {
                    pairs.push(((i - 1) as u16, (j - 2) as u16));
                    i -= 1;
                    j -= 2;
                }
                _ => break,
            }
        }
        pairs.reverse();
        pairs.truncate(64);
        out.push((last[end] / m as f32, pairs));
    }
    out
}

/// Normalized edit distance between two pitch-change sequences (interval cost
/// min(|Δ|, 3)/3 plus 0.3 × rhythm difference, insert/delete 0.6), per query interval.
pub fn edit_distance(qs: &[(f32, f32)], rs: &[(f32, f32)]) -> f32 {
    if qs.is_empty() {
        return 1.0;
    }
    const INDEL: f32 = 0.6;
    let mut prev: Vec<f32> = (0..=rs.len()).map(|j| j as f32 * INDEL).collect();
    let mut cur = vec![0f32; rs.len() + 1];
    for a in qs {
        cur[0] = prev[0] + INDEL;
        for (j, b) in rs.iter().enumerate() {
            let sub = ((a.0 - b.0).abs().min(3.0)) / 3.0 + 0.3 * (a.1 - b.1).abs().min(1.0);
            cur[j + 1] = (prev[j] + sub).min(prev[j + 1] + INDEL).min(cur[j] + INDEL);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[rs.len()] / qs.len() as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn steps_of(p: &[f32], ms: u32) -> Vec<Step> {
        p.iter()
            .enumerate()
            .map(|(i, &x)| Step { pitch: x, onset_ms: i as u32 * ms, end_ms: i as u32 * ms + ms - 50 })
            .collect()
    }

    #[test]
    fn alignment_ignores_rhythm_and_survives_an_inserted_note() {
        let reference =
            shape(&steps_of(&[55.0, 57.0, 60.0, 62.0, 64.0, 62.0, 60.0, 59.0, 60.0, 67.0, 65.0, 64.0], 600));
        // Reference notes 1..9 a fourth up, twice as fast, with a passing 66 inserted.
        let query = shape(&steps_of(&[62.0, 65.0, 66.0, 67.0, 69.0, 67.0, 65.0, 64.0, 65.0], 300));
        let mut rows = AlignRows::default();
        let cost = align(&query, &reference, &mut rows).expect("aligned");
        assert!(cost < 0.3, "cost {cost}");
        let other = shape(&steps_of(&[60.0, 52.0, 59.0, 51.0, 58.0, 50.0, 57.0, 49.0, 56.0], 300));
        let other_cost = align(&other, &reference, &mut rows).expect("aligned");
        assert!(other_cost > cost + 0.2, "{other_cost} vs {cost}");
        assert!(align(&query[..1], &reference, &mut rows).is_none());
        let mut table = AlignTable::default();
        let found = align_pairs(&query, &reference, &mut table);
        let (c, pairs) = found.first().expect("alignment");
        assert!((c - cost).abs() < 1e-5);
        assert_eq!(pairs.first(), Some(&(0, 1)));
        assert!(edit_distance(&query, &query) < 1e-6);
    }

    #[test]
    fn steps_from_notes_and_frames() {
        let n = |p: f32, o: u32, d: u32| Note { pitch: p, onset_ms: o, dur_ms: d };
        let s = query_steps(&[n(60.0, 0, 300), n(60.4, 300, 300), n(62.0, 600, 50), n(64.0, 700, 300)]);
        assert_eq!(s.iter().map(|s| s.pitch).collect::<Vec<_>>(), vec![60.0, 64.0]);
        assert_eq!(s[0].end_ms, 600);
        let mut f = vec![60.0; 6];
        f.extend([62.0, 62.0, 62.0, 62.0, f32::NAN, 62.0, 62.0, 64.0, 64.0, 64.0, 64.0]);
        let s = frame_steps(&f);
        assert_eq!(s.iter().map(|s| s.pitch).collect::<Vec<_>>(), vec![60.0, 62.0, 64.0]);
        assert_eq!((s[1].onset_ms, s[1].end_ms), (300, 650));
        // A 100 ms ornament between two notes of the same pitch disappears.
        let g = [60.0, 60.0, 60.0, 60.0, 61.0, 61.0, 60.0, 60.0, 60.0, 60.0, 67.0, 67.0, 67.0, 67.0];
        assert_eq!(frame_steps(&g).iter().map(|s| s.pitch).collect::<Vec<_>>(), vec![60.0, 67.0]);
    }
}
