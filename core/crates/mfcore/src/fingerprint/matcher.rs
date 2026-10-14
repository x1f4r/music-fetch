//! Reference index and offset voting.

use std::collections::HashMap;

use super::hash::{Landmark, Peak, landmarks};

/// Width of an offset bucket in frames (32 ms). Votes in the bucket and its two
/// neighbours count together, so small timing jitter and a residual speed
/// error of ±0.25 % over 30 s stay in one alignment.
const OFFSET_BUCKET: f32 = 4.0;

/// How query peaks map into reference coordinates: reference time = query time
/// × `time`, reference frequency = query frequency / `freq`. Vinyl-style speed
/// changes have `time == freq`; key-locked tempo changes have `freq == 1`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Warp {
    pub time: f32,
    pub freq: f32,
}

impl Warp {
    pub const IDENTITY: Warp = Warp { time: 1.0, freq: 1.0 };

    /// Speed of the query relative to the reference minus one (0.04 = 4 % faster).
    pub fn speed_change(&self) -> f32 {
        self.time - 1.0
    }

    /// True for a tempo change with the pitch kept (key lock).
    pub fn is_key_locked(&self) -> bool {
        (self.freq - 1.0).abs() < 1e-6 && (self.time - 1.0).abs() > 1e-6
    }
}

/// The identity, then vinyl-style and key-locked warps from `1 − max_change`
/// to `1 + max_change` in steps of `step`, nearest to 1 first.
pub fn warps(max_change: f32, step: f32) -> Vec<Warp> {
    let mut out = vec![Warp::IDENTITY];
    if step <= 0.0 || max_change <= 0.0 {
        return out;
    }
    let steps = (max_change / step).round() as i32;
    for k in 1..=steps {
        for sign in [1.0f32, -1.0] {
            let factor = 1.0 + sign * k as f32 * step;
            out.push(Warp { time: factor, freq: factor });
            out.push(Warp { time: factor, freq: 1.0 });
        }
    }
    out
}

/// Thresholds for accepting a match.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MatchOptions {
    /// Fewest aligned hashes.
    pub min_aligned: u32,
    /// The match must beat the best other reference by this factor.
    pub min_lead: f32,
    /// The match must beat the expected chance alignment by this factor.
    pub min_over_noise: f32,
    /// Smallest share of the reference's hashes that must align. Chance
    /// alignments (shared drum samples, repeated bars) stay far below it.
    pub min_coverage: f32,
}

impl Default for MatchOptions {
    fn default() -> Self {
        Self { min_aligned: 20, min_lead: 1.5, min_over_noise: 10.0, min_coverage: 0.012 }
    }
}

/// Best alignment of one reference inside a query.
#[derive(Debug, Clone, PartialEq)]
pub struct Match {
    /// Reference id returned by [`Index::add`].
    pub reference: usize,
    /// Aligned hashes.
    pub score: u32,
    /// Best score of any other reference.
    pub other: u32,
    /// Aligned hashes expected by chance at one offset.
    pub noise: f32,
    pub warp: Warp,
    /// Query frame where reference frame 0 sits.
    pub offset_frames: f32,
    /// First and last query frames whose hashes aligned.
    pub span: (u32, u32),
    /// Share of the reference's hashes that aligned.
    pub coverage: f32,
}

impl Match {
    /// Clear match: enough aligned hashes and a lead over other references and chance.
    pub fn is_confident(&self, options: &MatchOptions) -> bool {
        self.score >= options.min_aligned
            && self.score as f32 >= options.min_lead * self.other as f32
            && self.score as f32 >= options.min_over_noise * self.noise.max(1.0)
            && self.coverage >= options.min_coverage
    }

    /// Query time (in frames) for a reference time (in frames).
    pub fn query_frame(&self, reference_frame: f32) -> f32 {
        reference_frame / self.warp.time + self.offset_frames
    }
}

struct Reference {
    frames: u32,
    hashes: usize,
}

/// Hash index over a few reference recordings (catalog previews, mix excerpts).
#[derive(Default)]
pub struct Index {
    postings: HashMap<u32, Vec<(u32, f32)>>,
    references: Vec<Reference>,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a reference; returns its id.
    pub fn add(&mut self, peaks: &[Peak]) -> usize {
        let id = self.references.len();
        let mut marks = Vec::new();
        landmarks(peaks, 1.0, 1.0, &mut marks);
        for mark in &marks {
            self.postings.entry(mark.hash).or_default().push((id as u32, mark.t));
        }
        self.references.push(Reference { frames: peaks.last().map(|p| p.frame + 1).unwrap_or(0), hashes: marks.len() });
        id
    }

    pub fn len(&self) -> usize {
        self.references.len()
    }

    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }

    /// Number of hashes a reference contributed.
    pub fn reference_hashes(&self, reference: usize) -> usize {
        self.references.get(reference).map_or(0, |r| r.hashes)
    }

    /// Best alignment per reference over all `warps`, best first. References
    /// without any vote are left out.
    pub fn search(&self, query: &[Peak], warps: &[Warp]) -> Vec<Match> {
        if self.references.is_empty() || query.is_empty() {
            return Vec::new();
        }
        let query_frames = query.last().map(|p| p.frame + 1).unwrap_or(0) as f32;
        // Best (score, bucket, warp index, total votes) per reference.
        let mut best: Vec<Option<(u32, i32, usize, u32)>> = vec![None; self.references.len()];
        let mut marks: Vec<Landmark> = Vec::new();
        let mut votes: HashMap<(u32, i32), u32> = HashMap::new();
        let mut totals = vec![0u32; self.references.len()];
        for (w, warp) in warps.iter().enumerate() {
            landmarks(query, warp.time, warp.freq, &mut marks);
            votes.clear();
            totals.iter_mut().for_each(|t| *t = 0);
            for mark in &marks {
                let Some(list) = self.postings.get(&mark.hash) else { continue };
                for &(reference, t) in list {
                    let bucket = ((t - mark.t) / OFFSET_BUCKET).floor() as i32;
                    *votes.entry((reference, bucket)).or_default() += 1;
                    totals[reference as usize] += 1;
                }
            }
            for &(reference, bucket) in votes.keys() {
                let score = (bucket - 1..=bucket + 1).map(|b| votes.get(&(reference, b)).copied().unwrap_or(0)).sum();
                let slot = &mut best[reference as usize];
                if slot.is_none_or(|(s, ..)| score > s) {
                    *slot = Some((score, bucket, w, totals[reference as usize]));
                }
            }
        }

        let mut out: Vec<Match> = Vec::new();
        for (reference, found) in best.iter().enumerate() {
            let Some((score, bucket, w, total)) = *found else { continue };
            let warp = warps[w];
            let info = &self.references[reference];
            // Chance alignment: the other votes spread over every possible offset.
            let offsets = ((info.frames as f32 + query_frames * warp.time) / OFFSET_BUCKET).max(1.0);
            let noise = 3.0 * total.saturating_sub(score) as f32 / offsets;
            let offset_ref = bucket as f32 * OFFSET_BUCKET + OFFSET_BUCKET / 2.0;
            let span = self.span(query, warp, reference as u32, bucket);
            out.push(Match {
                reference,
                score,
                other: 0,
                noise,
                warp,
                // Reference frame 0 sits where query time × warp = −offset.
                offset_frames: -offset_ref / warp.time,
                span,
                coverage: score as f32 / info.hashes.max(1) as f32,
            });
        }
        out.sort_by_key(|m| std::cmp::Reverse(m.score));
        let scores: Vec<u32> = out.iter().map(|m| m.score).collect();
        for (i, m) in out.iter_mut().enumerate() {
            m.other = scores.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, s)| *s).max().unwrap_or(0);
        }
        out
    }

    /// First and last query frames of anchors that voted for `bucket` (± 1).
    fn span(&self, query: &[Peak], warp: Warp, reference: u32, bucket: i32) -> (u32, u32) {
        let mut marks = Vec::new();
        landmarks(query, warp.time, warp.freq, &mut marks);
        let mut lo = f32::MAX;
        let mut hi = f32::MIN;
        for mark in &marks {
            let Some(list) = self.postings.get(&mark.hash) else { continue };
            for &(r, t) in list {
                if r != reference {
                    continue;
                }
                let b = ((t - mark.t) / OFFSET_BUCKET).floor() as i32;
                if (b - bucket).abs() <= 1 {
                    lo = lo.min(mark.t);
                    hi = hi.max(mark.t);
                }
            }
        }
        if lo > hi {
            return (0, 0);
        }
        ((lo / warp.time) as u32, (hi / warp.time).ceil() as u32)
    }
}
