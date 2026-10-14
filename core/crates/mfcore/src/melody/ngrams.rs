//! Note n-grams: keys over a melody's pitch changes, for the rhythm-free
//! retrieval path.
//!
//! The reference's pitch changes come from its contour ([`steps::frame_steps`]),
//! the query's from its notes ([`steps::query_steps`]). Interval 4-grams (exact
//! semitones, clipped to ±12) and coarse contour 6-grams (±1–2, ±3–4, ±5+) are
//! posted per (track, step position). Sung intervals are inexact, so each query
//! n-gram expands into its likeliest readings (neighbouring semitones,
//! compressed leaps, one inserted note), weighted by penalty.

use crate::melody::steps::Step;

/// Interval n-gram length (intervals; one more note).
pub const INTERVAL_N: usize = 4;
/// Contour n-gram length (intervals).
pub const CONTOUR_N: usize = 6;
const KIND_INTERVAL: u32 = 0;
const KIND_CONTOUR: u32 = 1;
/// Query key variants per n-gram and the largest total penalty expanded.
const INTERVAL_VARIANTS: usize = 16;
const CONTOUR_VARIANTS: usize = 8;
const MAX_KEY_PENALTY: f32 = 2.0;
/// Contour n-grams are less specific than interval n-grams.
pub const CONTOUR_WEIGHT: f32 = 0.35;
/// Penalty of reading two query intervals as one (an inserted note).
const MERGE_PENALTY: f32 = 0.8;
/// Merged query intervals smaller than this are not read as one (semitones).
const MERGE_MIN: f32 = 0.7;

/// Semitone intervals between steps, clipped to ±12 (never 0).
pub fn intervals(steps: &[Step]) -> Vec<i8> {
    steps
        .windows(2)
        .map(|w| {
            let d = (w[1].pitch - w[0].pitch).round().clamp(-12.0, 12.0) as i8;
            if d == 0 { if w[1].pitch >= w[0].pitch { 1 } else { -1 } } else { d }
        })
        .collect()
}

/// Coarse contour class of an interval: ±1 (1–2 semitones), ±2 (3–4), ±3 (5+).
pub fn contour_class(d: i8) -> i8 {
    let m = match d.unsigned_abs() {
        0..=2 => 1,
        3..=4 => 2,
        _ => 3,
    };
    if d < 0 { -m } else { m }
}

pub fn interval_key(d: &[i8]) -> u32 {
    d.iter().fold(0u32, |k, &x| k * 25 + (x.clamp(-12, 12) + 12) as u32) | (KIND_INTERVAL << 30)
}

pub fn contour_key(c: &[i8]) -> u32 {
    c.iter().fold(0u32, |k, &x| k * 7 + (x.clamp(-3, 3) + 3) as u32) | (KIND_CONTOUR << 30)
}

/// Every (position, key) of a reference's steps, both kinds.
pub fn ngram_keys(steps: &[Step]) -> Vec<(u32, u32)> {
    let iv = intervals(steps);
    let mut out = Vec::with_capacity(2 * iv.len());
    for (i, w) in iv.windows(INTERVAL_N).enumerate() {
        out.push((i as u32, interval_key(w)));
    }
    let classes: Vec<i8> = iv.iter().map(|&d| contour_class(d)).collect();
    for (i, w) in classes.windows(CONTOUR_N).enumerate() {
        out.push((i as u32, contour_key(w)));
    }
    out
}

/// Key variants of the query's n-grams: (query position, key, weight).
///
/// Each interval gets alternatives with a penalty: the other neighbouring
/// semitone when it falls between two, a wider interval for large leaps
/// (singers compress them), and any ±1 error; an n-gram window may also read
/// two adjacent intervals as one (an inserted note). Each n-gram expands to its
/// cheapest combinations, weighted by e^(−penalty).
pub fn query_keys(steps: &[Step]) -> Vec<(i32, u32, f32)> {
    let raw: Vec<f32> = steps.windows(2).map(|w| w[1].pitch - w[0].pitch).collect();
    let mut out = Vec::new();
    for j in 0..raw.len() {
        for (n, contour) in [(INTERVAL_N, false), (CONTOUR_N, true)] {
            let mut windows: Vec<(Vec<f32>, f32)> = Vec::new();
            if j + n <= raw.len() {
                windows.push((raw[j..j + n].to_vec(), 0.0));
            }
            if j + n < raw.len() {
                for m in 0..n {
                    let mut w: Vec<f32> = raw[j..j + n + 1].to_vec();
                    let merged = w[m] + w[m + 1];
                    if merged.abs() < MERGE_MIN {
                        continue;
                    }
                    w[m] = merged;
                    w.remove(m + 1);
                    windows.push((w, MERGE_PENALTY));
                }
            }
            for (w, base) in windows {
                if contour {
                    let opts: Vec<Vec<(i8, f32)>> = w.iter().map(|&x| class_options(x)).collect();
                    let limit = if base > 0.0 { CONTOUR_VARIANTS / 2 } else { CONTOUR_VARIANTS };
                    for (v, p) in combinations(&opts, limit) {
                        out.push((j as i32, contour_key(&v), CONTOUR_WEIGHT * (-(p + base)).exp()));
                    }
                } else {
                    let opts: Vec<Vec<(i8, f32)>> = w.iter().map(|&x| interval_options(x)).collect();
                    let limit = if base > 0.0 { INTERVAL_VARIANTS / 4 } else { INTERVAL_VARIANTS };
                    for (v, p) in combinations(&opts, limit) {
                        out.push((j as i32, interval_key(&v), (-(p + base)).exp()));
                    }
                }
            }
        }
    }
    out
}

/// Semitone readings of a sung interval with penalties (never 0: repeats are collapsed).
fn interval_options(x: f32) -> Vec<(i8, f32)> {
    let mut o: Vec<(i8, f32)> = Vec::with_capacity(5);
    let mut add = |v: f32, p: f32| {
        let v = v.clamp(-12.0, 12.0) as i8;
        if v != 0 && !o.iter().any(|e| e.0 == v) {
            o.push((v, p));
        }
    };
    let base = if x.round() == 0.0 { x.signum() } else { x.round() };
    add(base, 0.0);
    let frac = x - x.round();
    if frac.abs() > 0.25 {
        add(base + frac.signum(), 0.5);
    }
    if x.abs() >= 4.5 {
        add(base + x.signum(), 0.6);
        add(base + 2.0 * x.signum(), 1.2);
    }
    add(base + 1.0, 1.0);
    add(base - 1.0, 1.0);
    o.sort_by(|a, b| a.1.total_cmp(&b.1));
    o
}

/// Contour-class readings of a sung interval: both classes near a boundary.
fn class_options(x: f32) -> Vec<(i8, f32)> {
    let a = x.abs();
    let class = |v: f32| contour_class(v.round().clamp(-12.0, 12.0) as i8);
    let base = if x.round() == 0.0 { x.signum() as i8 } else { class(x) };
    let mut o = vec![(base, 0.0)];
    for edge in [2.5f32, 4.5] {
        if (a - edge).abs() < 0.6 {
            let other = class(x.signum() * if a < edge { edge + 0.5 } else { edge - 0.5 });
            if other != base && other != 0 {
                o.push((other, 0.5));
            }
        }
    }
    o
}

/// Cheapest combinations of per-position options (at most `limit`), with total penalty.
fn combinations(options: &[Vec<(i8, f32)>], limit: usize) -> Vec<(Vec<i8>, f32)> {
    let mut out: Vec<(Vec<i8>, f32)> = vec![(Vec::with_capacity(options.len()), 0.0)];
    for opts in options {
        let mut next = Vec::with_capacity(out.len() * opts.len());
        for (v, p) in &out {
            for &(x, q) in opts {
                if p + q <= MAX_KEY_PENALTY {
                    let mut v = v.clone();
                    v.push(x);
                    next.push((v, p + q));
                }
            }
        }
        next.sort_by(|a, b| a.1.total_cmp(&b.1));
        next.truncate(limit * 4);
        out = next;
    }
    out.truncate(limit);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn steps(p: &[f32]) -> Vec<Step> {
        p.iter()
            .enumerate()
            .map(|(i, &x)| Step { pitch: x, onset_ms: i as u32 * 300, end_ms: i as u32 * 300 + 250 })
            .collect()
    }

    #[test]
    fn keys_are_distinct_and_positions_follow_the_steps() {
        let s = steps(&[60.0, 62.0, 64.0, 60.0, 65.0, 67.0, 62.0, 60.0]);
        assert_eq!(intervals(&s), vec![2, 2, -4, 5, 2, -5, -2]);
        assert_eq!(contour_class(-4), -2);
        assert_eq!(contour_class(7), 3);
        assert_ne!(interval_key(&[1, 2, 3, 4]), interval_key(&[4, 3, 2, 1]));
        assert_eq!(contour_key(&[1; 6]) >> 30, 1);
        let keys = ngram_keys(&s);
        assert_eq!(keys.iter().filter(|k| k.1 >> 30 == 0).count(), 4);
        assert_eq!(keys.iter().filter(|k| k.1 >> 30 == 1).count(), 2);
        assert!(keys.contains(&(1, interval_key(&[2, -4, 5, 2]))));
    }

    #[test]
    fn query_keys_cover_sung_errors() {
        // A compressed leap (+8.4 sung for +9) still produces the exact key.
        let keys = query_keys(&steps(&[60.0, 68.4, 66.1, 64.0, 66.0]));
        assert!(keys.iter().any(|&(j, k, _)| j == 0 && k == interval_key(&[9, -2, -2, 2])));
        assert!(keys.iter().any(|&(j, k, _)| j == 0 && k == interval_key(&[8, -2, -2, 2])));
        // An inserted passing note: 60 62 64 65 67 read as 60 62 65 67 too.
        let keys = query_keys(&steps(&[60.0, 62.0, 64.0, 65.0, 67.0, 69.0]));
        assert!(keys.iter().any(|&(j, k, w)| j == 0 && k == interval_key(&[2, 3, 2, 2]) && w < 1.0));
    }
}
