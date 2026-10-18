//! Frame contours shared by the index builder and the matcher.
//!
//! Matching works on 20 fps pitch contours (MIDI semitones, NaN = unvoiced)
//! rather than notes: sung and audio-derived melodies segment into notes
//! unreliably, while their contours agree frame by frame. Retrieval uses
//! key-invariant *window vectors*: a window of the contour is cut into
//! [`DIMS`] equal parts, each part's mean pitch (rests hold the previous
//! pitch) is one coordinate, and the window mean is subtracted. Reference
//! windows span [`WINDOW`] frames; the query is cut at several lengths, one
//! per candidate tempo, so the two meet at the same musical span.

use crate::melody::notes::{CONTOUR_MS, Note};

/// Frames per second of every contour.
pub const FPS: u32 = 1000 / CONTOUR_MS;
/// Reference window length in frames (3 s).
pub const WINDOW: usize = 60;
/// Coordinates per window vector.
pub const DIMS: usize = 12;
/// Coordinates per half vector (the inverted multi-index quantizes halves).
pub const HALF: usize = DIMS / 2;
/// Frames between reference windows (0.5 s).
pub const REF_HOP: usize = 10;
/// Share of voiced frames a reference window needs.
pub const REF_MIN_VOICED: f32 = 0.6;
/// Share of voiced frames a query window needs.
pub const QUERY_MIN_VOICED: f32 = 0.5;
/// Residual sub-quantizers (product quantization of the window vector minus its
/// cell's centroids) and their dimensions.
pub const SUBS: usize = 4;
pub const SUB: usize = DIMS / SUBS;
/// Coordinates are clamped to this many semitones around the window mean.
const CLAMP: f32 = 12.0;

/// A window vector.
pub type Vector = [f32; DIMS];

/// A contour with rests filled by the previous pitch and prefix sums for
/// constant-time window vectors.
pub struct Filled {
    sums: Vec<f64>,
    voiced: Vec<u32>,
}

impl Filled {
    /// None when nothing is voiced.
    pub fn new(contour: &[f32]) -> Option<Self> {
        let first = contour.iter().position(|v| v.is_finite())?;
        let mut sums = Vec::with_capacity(contour.len() + 1);
        let mut voiced = Vec::with_capacity(contour.len() + 1);
        sums.push(0.0);
        voiced.push(0);
        let mut held = contour[first];
        for &v in contour {
            if v.is_finite() {
                held = v;
            }
            sums.push(sums[sums.len() - 1] + held as f64);
            voiced.push(voiced[voiced.len() - 1] + v.is_finite() as u32);
        }
        Some(Self { sums, voiced })
    }

    /// Frames.
    pub fn len(&self) -> usize {
        self.sums.len() - 1
    }

    /// True when there are no frames.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The window vector of `len` frames starting at frame `start` (both may be
    /// fractional); None when it does not fit or has too few voiced frames.
    pub fn window(&self, start: f32, len: f32, min_voiced: f32) -> Option<Vector> {
        let n = self.len();
        if start < 0.0 || len < DIMS as f32 || start + len > n as f32 + 0.5 {
            return None;
        }
        let edge = |k: usize| ((start + len * k as f32 / DIMS as f32).round() as usize).min(n);
        let (a, b) = (edge(0), edge(DIMS));
        if b <= a || ((self.voiced[b] - self.voiced[a]) as f32) < min_voiced * (b - a) as f32 {
            return None;
        }
        let mut v = [0f32; DIMS];
        let mut lo = a;
        for (k, x) in v.iter_mut().enumerate() {
            let hi = edge(k + 1).max(lo + 1).min(n);
            let lo2 = lo.min(hi - 1);
            *x = ((self.sums[hi] - self.sums[lo2]) / (hi - lo2) as f64) as f32;
            lo = hi;
        }
        let mean = v.iter().sum::<f32>() / DIMS as f32;
        for x in &mut v {
            *x = (*x - mean).clamp(-CLAMP, CLAMP);
        }
        Some(v)
    }
}

/// Root mean square of a window vector (semitones around the window mean).
pub fn rms(v: &Vector) -> f32 {
    (v.iter().map(|x| x * x).sum::<f32>() / DIMS as f32).sqrt()
}

/// Windows flatter than this (RMS semitones) carry too little melody to index or
/// search with; they also crowd a few cells of the index.
pub const MIN_RMS: f32 = 0.8;

/// Squared distance between two half vectors.
pub fn dist2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// A note timeline as a contour: notes hold their pitch, rests shorter than
/// `hold_ms` hold the previous note, longer rests are NaN. Onsets are relative
/// to the first note.
pub fn render_notes(notes: &[Note], hold_ms: u32) -> Vec<f32> {
    let Some(first) = notes.first() else { return Vec::new() };
    let t0 = first.onset_ms;
    let end = notes.iter().map(|n| n.onset_ms.saturating_add(n.dur_ms)).max().unwrap_or(t0);
    let frames = ((end - t0) / CONTOUR_MS + 1).min(1 << 20) as usize;
    let mut out = vec![f32::NAN; frames];
    for (i, n) in notes.iter().enumerate() {
        let mut end = n.onset_ms.saturating_add(n.dur_ms);
        if let Some(next) = notes.get(i + 1)
            && next.onset_ms > end
            && next.onset_ms - end < hold_ms
        {
            end = next.onset_ms;
        }
        let a = (((n.onset_ms - t0) as f32 / CONTOUR_MS as f32).round() as usize).min(frames);
        let b = (((end - t0) as f32 / CONTOUR_MS as f32).round() as usize).clamp(a + 1, frames.max(a + 1)).min(frames);
        out[a..b].fill(n.pitch);
    }
    out
}

/// Hold the previous pitch through unvoiced gaps shorter than `max_gap` frames.
pub fn hold_short_gaps(contour: &mut [f32], max_gap: usize) {
    let mut i = 0;
    while i < contour.len() {
        if contour[i].is_finite() {
            i += 1;
            continue;
        }
        let start = i;
        while i < contour.len() && !contour[i].is_finite() {
            i += 1;
        }
        if start > 0 && i < contour.len() && i - start < max_gap {
            let held = contour[start - 1];
            contour[start..i].fill(held);
        }
    }
}

/// Lloyd's k-means with k-means++ seeding (deterministic), using
/// up to `threads` threads. Returns `k` centroids (fewer when there are fewer points).
pub fn kmeans<const N: usize>(points: &[[f32; N]], k: usize, iters: usize, threads: usize) -> Vec<[f32; N]> {
    if points.is_empty() || k == 0 {
        return Vec::new();
    }
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    // k-means++ seeding on a sample.
    let sample: Vec<&[f32; N]> = if points.len() > 20 * k {
        (0..20 * k).map(|_| &points[(next() % points.len() as u64) as usize]).collect()
    } else {
        points.iter().collect()
    };
    let mut centroids: Vec<[f32; N]> = vec![*sample[(next() % sample.len() as u64) as usize]];
    let mut nearest: Vec<f32> = sample.iter().map(|p| dist2(*p, &centroids[0])).collect();
    while centroids.len() < k.min(sample.len()) {
        let total: f64 = nearest.iter().map(|&d| d as f64).sum();
        let pick = if total <= 0.0 {
            (next() % sample.len() as u64) as usize
        } else {
            let mut r = (next() as f64 / u64::MAX as f64) * total;
            let mut at = sample.len() - 1;
            for (i, &d) in nearest.iter().enumerate() {
                r -= d as f64;
                if r <= 0.0 {
                    at = i;
                    break;
                }
            }
            at
        };
        let c = *sample[pick];
        for (d, p) in nearest.iter_mut().zip(&sample) {
            *d = d.min(dist2(*p, &c));
        }
        centroids.push(c);
    }
    let threads = threads.max(1);
    let chunk = points.len().div_ceil(threads);
    for _ in 0..iters {
        let parts: Vec<(Vec<[f64; N]>, Vec<u64>)> = std::thread::scope(|s| {
            let handles: Vec<_> = points
                .chunks(chunk)
                .map(|part| {
                    let centroids = &centroids;
                    s.spawn(move || {
                        let mut sums = vec![[0f64; N]; centroids.len()];
                        let mut counts = vec![0u64; centroids.len()];
                        for p in part {
                            let c = nearest_centroid(p, centroids).0;
                            for (s, x) in sums[c].iter_mut().zip(p) {
                                *s += *x as f64;
                            }
                            counts[c] += 1;
                        }
                        (sums, counts)
                    })
                })
                .collect();
            handles.into_iter().filter_map(|h| h.join().ok()).collect()
        });
        let mut sums = vec![[0f64; N]; centroids.len()];
        let mut counts = vec![0u64; centroids.len()];
        for (s, c) in parts {
            for (i, (a, b)) in s.iter().zip(&c).enumerate() {
                for (x, y) in sums[i].iter_mut().zip(a) {
                    *x += y;
                }
                counts[i] += b;
            }
        }
        for (i, c) in centroids.iter_mut().enumerate() {
            if counts[i] > 0 {
                for (x, s) in c.iter_mut().zip(&sums[i]) {
                    *x = (s / counts[i] as f64) as f32;
                }
            } else {
                *c = points[(next() % points.len() as u64) as usize];
            }
        }
    }
    centroids
}

/// Index and squared distance of the nearest centroid.
pub fn nearest_centroid<const N: usize>(p: &[f32; N], centroids: &[[f32; N]]) -> (usize, f32) {
    let mut best = (0, f32::INFINITY);
    for (i, c) in centroids.iter().enumerate() {
        let d = dist2(p, c);
        if d < best.1 {
            best = (i, d);
        }
    }
    best
}

/// The two halves of a window vector.
pub fn halves(v: &Vector) -> ([f32; HALF], [f32; HALF]) {
    let mut a = [0f32; HALF];
    let mut b = [0f32; HALF];
    a.copy_from_slice(&v[..HALF]);
    b.copy_from_slice(&v[HALF..]);
    (a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_are_key_invariant_and_need_voicing() {
        let c: Vec<f32> = (0..120).map(|i| 60.0 + (i / 10) as f32).collect();
        let shifted: Vec<f32> = c.iter().map(|x| x + 7.0).collect();
        let a = Filled::new(&c).and_then(|f| f.window(10.0, 60.0, 0.6)).expect("window");
        let b = Filled::new(&shifted).and_then(|f| f.window(10.0, 60.0, 0.6)).expect("window");
        assert!(a.iter().zip(&b).all(|(x, y)| (x - y).abs() < 1e-4));
        assert!(a.iter().sum::<f32>().abs() < 1e-3);
        assert!(a[0] < a[DIMS - 1]);
        // A slower rendition cut at the matching length gives the same vector.
        let slow: Vec<f32> = (0..180).map(|i| 60.0 + (i * 2 / 30) as f32).collect();
        let s = Filled::new(&slow).and_then(|f| f.window(15.0, 90.0, 0.6)).expect("window");
        assert!(a.iter().zip(&s).all(|(x, y)| (x - y).abs() < 0.35), "{a:?} {s:?}");
        let mut gappy = c.clone();
        gappy[10..50].fill(f32::NAN);
        assert!(Filled::new(&gappy).and_then(|f| f.window(10.0, 60.0, 0.6)).is_none());
        assert!(Filled::new(&c).and_then(|f| f.window(100.0, 60.0, 0.6)).is_none());
        assert!(Filled::new(&[f32::NAN; 5]).is_none());
    }

    #[test]
    fn notes_render_with_short_rests_held() {
        let n = |p: f32, o: u32, d: u32| Note { pitch: p, onset_ms: o, dur_ms: d };
        let c = render_notes(&[n(60.0, 1000, 200), n(62.0, 1300, 200), n(64.0, 2500, 100)], 300);
        assert_eq!(c.len(), 33);
        assert_eq!(c[0], 60.0);
        assert_eq!(c[5], 60.0); // 100 ms rest held
        assert_eq!(c[6], 62.0);
        assert!(c[12].is_nan()); // 1 s rest
        assert_eq!(c[30], 64.0);
        let mut g = vec![1.0, f32::NAN, f32::NAN, 2.0, f32::NAN, f32::NAN, f32::NAN, 3.0];
        hold_short_gaps(&mut g, 3);
        assert_eq!(&g[..4], &[1.0, 1.0, 1.0, 2.0]);
        assert!(g[4].is_nan());
    }

    #[test]
    fn kmeans_separates_clusters() {
        let mut pts = Vec::new();
        for i in 0..300 {
            let base = if i % 3 == 0 {
                -5.0
            } else if i % 3 == 1 {
                0.0
            } else {
                5.0
            };
            pts.push([base + (i % 7) as f32 * 0.01; HALF]);
        }
        let c = kmeans(&pts, 3, 10, 2);
        let mut centers: Vec<f32> = c.iter().map(|c| c[0]).collect();
        centers.sort_by(f32::total_cmp);
        assert!(
            (centers[0] + 5.0).abs() < 0.1 && centers[1].abs() < 0.1 && (centers[2] - 5.0).abs() < 0.1,
            "{centers:?}"
        );
        assert!(kmeans::<HALF>(&[], 4, 3, 1).is_empty());
    }
}
