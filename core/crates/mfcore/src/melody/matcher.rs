//! Two-stage melody matcher: n-gram retrieval with offset voting, then linear
//! scaling + DTW + note edit distance re-ranking, fused into a calibrated score.
//!
//! 1. **Retrieval.** The query's pitch changes (repeats collapsed) give interval
//!    4-grams and coarse contour 6-grams. Sung intervals are inexact, so each
//!    n-gram expands into its likeliest readings (neighbouring semitones,
//!    compressed leaps, one inserted note), weighted by penalty and key rarity.
//!    Votes for (phrase, reference position − query position) go to a coarse
//!    per-phrase offset histogram, which shortlists phrases.
//! 2. **Placement.** Each shortlisted phrase gets candidate placements from a
//!    Hough vote over (tempo, anchor) of its matched pitch changes, and from a
//!    semi-global alignment of the query's intervals inside the phrase's (which
//!    survives the inserted, split and missed notes that break n-grams). Phrases
//!    are ranked by alignment cost into a pool.
//! 3. **Linear scaling.** Matched pitch changes give a tempo and anchor (Theil–Sen
//!    fit). A quick placement and a coarse 10 fps DTW thin the pool; the best
//!    then get 13 tempos (0.6–1.8×) plus the fitted ones and ±1 s shifts, scored
//!    with a clipped L1 distance (3 semitones) at the median key difference.
//! 4. **DTW.** Subsequence DTW between voiced frames (reference sampled at the
//!    estimated tempo, slopes ½–2 around it, band of 20% of the query), cost
//!    |q − r − k| clipped at 4; k is refined by three median passes and ±½.
//! 5. **Note edit distance** over pitch changes (interval cost min(|Δ|, 3)/3 plus
//!    0.3 × rhythm-ratio difference, insert/delete 0.6).
//! 6. **Fusion.** A conditional logit over (votes, alignment, linear scaling,
//!    DTW, edit distance) with a "none of these" option, then Platt scaling,
//!    gives a score calibrated as P(correct) (fitted on MIR-QBSH and IOACAS by
//!    tools/eval/fit_fusion.py); a popularity prior can be added through
//!    [`MatchOptions`].

use std::collections::{HashMap, HashSet};

use crate::melody::SongMeta;
use crate::melody::index::{self, MelodyIndex, Step};
use crate::melody::notes::{CONTOUR_MS, MelodyQuery, Note, median};
use crate::model::MelodyAlignment;

#[derive(Debug, Clone)]
pub struct MelodyMatch {
    pub song: SongMeta,
    /// Calibrated 0..=1.
    pub score: f32,
    /// Semitones added to the reference to meet the query.
    pub key_shift: f32,
    /// Query tempo relative to the reference.
    pub tempo: f32,
    pub alignment: MelodyAlignment,
}

/// Tuning knobs; the defaults are what the app uses.
#[derive(Debug, Clone)]
pub struct MatchOptions {
    /// Logit weight of ln((popularity + 0.05) / 0.55); 0 disables the prior.
    pub popularity_weight: f32,
    /// Phrases whose offsets are scored in detail after the coarse vote.
    pub shortlist: usize,
    /// (phrase, offset) anchors given a quick placement and a coarse DTW.
    pub pool: usize,
    /// Candidates given full linear scaling, DTW and the edit distance.
    pub rerank: usize,
}

impl Default for MatchOptions {
    fn default() -> Self {
        Self {
            popularity_weight: 0.3,
            shortlist: 3000,
            pool: 3000,
            rerank: 150,
        }
    }
}

/// Distances of one candidate, as fed to the score model.
#[derive(Debug, Clone, Copy, Default)]
pub struct Features {
    /// Retrieval votes per query n-gram position.
    pub votes: f32,
    /// Best note-alignment cost of the phrase, per query interval.
    pub align: f32,
    pub linear: f32,
    pub dtw: f32,
    pub edit: f32,
}

/// One scored candidate with its features (for evaluation and model fitting).
#[derive(Debug, Clone)]
pub struct Candidate {
    pub song: u32,
    pub phrase: u32,
    pub features: Features,
    pub score: f32,
    pub key_shift: f32,
    pub tempo: f32,
}

/// Where a song ended up in each stage (1-based ranks among distinct songs).
#[derive(Debug, Clone, Copy, Default)]
pub struct Trace {
    pub retrieval: Option<usize>,
    pub linear: Option<usize>,
    pub reranked: Option<usize>,
}

/// Logistic score model fitted on MIR-QBSH and IOACAS (tools/eval/fit_fusion.py).
/// Conditional logit over (votes, align, linear, dtw, edit): each candidate's
/// utility is MODEL_WEIGHTS · features, a "none of these" option has utility
/// MODEL_NONE, and the softmax probability goes through Platt scaling
/// σ(a + b·logit p) with CALIBRATION = (a, b).
const MODEL_WEIGHTS: [f32; 5] = [0.2757, -9.0541, -9.7386, -0.9706, -12.4091];
const MODEL_NONE: f32 = -16.8609;
const CALIBRATION: (f32, f32) = (0.7823, 1.2056);

const TEMPOS: usize = 13;
const TEMPO_MIN: f32 = 0.6;
const TEMPO_MAX: f32 = 1.8;
/// Tempos fitted from matched notes may go further (MIDI references are often
/// notated at double or half speed).
const TEMPO_FIT_MIN: f32 = 0.35;
const TEMPO_FIT_MAX: f32 = 3.0;
/// Factors tried around a fitted tempo.
const FIT_FACTORS: [f32; 5] = [0.85, 0.92, 1.0, 1.08, 1.17];
/// The quick check's tempos (with the fitted tempo when there is one) and shifts.
const QUICK_TEMPOS: [f32; 3] = [0.75, 1.0, 1.35];
const QUICK_SHIFTS_MS: [f32; 3] = [-500.0, 0.0, 500.0];
/// Linear-scaling time shifts around the anchor (ms).
const SHIFTS_MS: [f32; 9] = [
    -1000.0, -750.0, -500.0, -250.0, 0.0, 250.0, 500.0, 750.0, 1000.0,
];
const LS_CLIP: f32 = 3.0;
/// Cost of a voiced query frame meeting a reference rest.
const MISS_COST: f32 = 1.5;
const DTW_CLIP: f32 = 4.0;
/// Reference rests shorter than this hold the previous note (ms).
const REF_HOLD_MS: u32 = 200;

/// Query key variants per n-gram and the largest total penalty expanded.
const INTERVAL_VARIANTS: usize = 16;
const CONTOUR_VARIANTS: usize = 8;
const MAX_KEY_PENALTY: f32 = 2.0;
/// Contour n-grams are less specific than interval n-grams.
const CONTOUR_WEIGHT: f32 = 0.35;
/// Penalty of reading two query intervals as one (an inserted note).
const MERGE_PENALTY: f32 = 0.8;
/// Query notes shorter than this are dropped from the pitch changes (ms).
const MIN_STEP_MS: u32 = 100;
/// Query pitch drifts below this collapse into one pitch change (semitones).
const QUERY_COLLAPSE: f32 = 0.7;
/// Keys with more postings than this carry too little information to read.
const MAX_POSTINGS: u32 = 60_000;
/// Coarse offset buckets per phrase in the first vote (2 steps each, wrapping).
const BUCKETS: usize = 64;
/// Matched pitch changes considered per phrase.
const MAX_POINTS: usize = 256;
/// Alignment search over tempo (log-spaced) and anchor (bins of 400 ms).
const HOUGH_TEMPOS: usize = 12;
const HOUGH_TEMPO_MIN: f32 = 0.45;
const HOUGH_TEMPO_MAX: f32 = 2.2;
const HOUGH_BIN_MS: f32 = 400.0;
/// Note alignment costs: insertion/deletion, merge (plus its pitch cost) and the
/// weight of rhythm against pitch in a substitution.
const ALIGN_INDEL: f32 = 0.7;
const ALIGN_MERGE: f32 = 0.35;
const ALIGN_RHYTHM: f32 = 0.1;
/// Alignment cost assumed for phrases too short to align.
const ALIGN_UNKNOWN: f32 = 1.0;

/// Top `k` matches, best first.
pub fn search(index: &MelodyIndex, query: &MelodyQuery, k: usize) -> Vec<MelodyMatch> {
    search_with(index, query, k, &MatchOptions::default())
}

/// [`search`] with explicit options.
pub fn search_with(
    index: &MelodyIndex,
    query: &MelodyQuery,
    k: usize,
    options: &MatchOptions,
) -> Vec<MelodyMatch> {
    let Some(q) = Query::new(query) else {
        return Vec::new();
    };
    let mut refs = References::new(index);
    let anchors = retrieve(index, &q, &mut refs, options, |_| true);
    let ranked = rerank(&q, &mut refs, anchors, options);
    best_per_song(index, &q, &mut refs, ranked, k, options.popularity_weight)
}

/// All re-ranked candidates with their features, best first (for evaluation).
pub fn candidates(
    index: &MelodyIndex,
    query: &MelodyQuery,
    options: &MatchOptions,
) -> Vec<Candidate> {
    let Some(q) = Query::new(query) else {
        return Vec::new();
    };
    let mut refs = References::new(index);
    let anchors = retrieve(index, &q, &mut refs, options, |_| true);
    rerank(&q, &mut refs, anchors, options)
        .into_iter()
        .map(|s| Candidate {
            song: s.song,
            phrase: s.phrase,
            features: s.features,
            score: s.score,
            key_shift: s.key,
            tempo: s.tempo,
        })
        .collect()
}

/// Follow `song` through the stages (for evaluation).
pub fn trace(index: &MelodyIndex, query: &MelodyQuery, options: &MatchOptions, song: u32) -> Trace {
    let Some(q) = Query::new(query) else {
        return Trace::default();
    };
    let mut refs = References::new(index);
    let song_rank = |songs: &mut dyn Iterator<Item = u32>| {
        let mut seen = HashSet::new();
        songs
            .filter(|s| seen.insert(*s))
            .position(|s| s == song)
            .map(|p| p + 1)
    };
    let anchors = retrieve(index, &q, &mut refs, options, |_| true);
    let retrieval = song_rank(&mut anchors.iter().filter_map(|a| index.phrase_song(a.phrase)));
    let linear = linear_stage(&q, &mut refs, anchors, options);
    let linear_rank = song_rank(
        &mut linear
            .iter()
            .filter_map(|l| refs.get(l.anchor.phrase).map(|r| r.song)),
    );
    let reranked = song_rank(
        &mut dtw_stage(&q, &mut refs, linear, options.rerank)
            .iter()
            .map(|s| s.song),
    );
    Trace {
        retrieval,
        linear: linear_rank,
        reranked,
    }
}

/// Score one specific song (e.g. a lyric-search candidate) against the query.
pub fn score_song(index: &MelodyIndex, query: &MelodyQuery, song_id: u32) -> Option<MelodyMatch> {
    let q = Query::new(query)?;
    let phrases = index.song_phrases(song_id);
    if phrases.is_empty() {
        return None;
    }
    let mut refs = References::new(index);
    let options = MatchOptions {
        shortlist: 64,
        pool: 256,
        rerank: 12,
        ..MatchOptions::default()
    };
    // Anchors: offsets voted for within this song, plus a sweep of each phrase.
    let mut anchors = retrieve(index, &q, &mut refs, &options, |p| phrases.contains(&p));
    for p in phrases.clone() {
        let steps = refs.get(p).map(|r| r.steps.len()).unwrap_or(0);
        for off in (0..steps).step_by(3) {
            anchors.push(Anchor {
                phrase: p,
                offset: off as i32,
                votes: 0.0,
                align: ALIGN_UNKNOWN,
                pairs: Vec::new(),
            });
        }
    }
    let mut ranked = rerank(&q, &mut refs, anchors, &options);
    // Placements of one song do not compete: score each against "none" alone.
    for s in &mut ranked {
        score_all(std::slice::from_mut(s));
    }
    ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
    best_per_song(index, &q, &mut refs, ranked, 1, options.popularity_weight)
        .into_iter()
        .next()
}

/// The query in the forms the stages need.
struct Query {
    contour: Vec<f32>,
    /// Indices of voiced contour frames.
    voiced: Vec<usize>,
    median: f32,
    steps: Vec<Step>,
}

impl Query {
    fn new(query: &MelodyQuery) -> Option<Self> {
        let voiced: Vec<usize> = query
            .contour
            .iter()
            .enumerate()
            .filter(|(_, m)| m.is_finite())
            .map(|(i, _)| i)
            .collect();
        if voiced.len() < 10 {
            return None;
        }
        let mut v: Vec<f32> = voiced.iter().map(|&i| query.contour[i]).collect();
        let median = median(&mut v)?;
        Some(Self {
            contour: query.contour.clone(),
            voiced,
            median,
            steps: query_steps(&query.notes),
        })
    }

    fn duration_ms(&self) -> f32 {
        self.contour.len() as f32 * CONTOUR_MS as f32
    }

    /// Query n-gram positions (for normalizing votes).
    fn positions(&self) -> f32 {
        self.steps.len().saturating_sub(index::INTERVAL_N).max(1) as f32
    }
}

/// The query's pitch changes: blips shorter than 100 ms dropped and drifts of
/// less than 0.7 semitone collapsed (sung pitch wanders more than notated pitch).
fn query_steps(notes: &[Note]) -> Vec<Step> {
    let mut out: Vec<Step> = Vec::with_capacity(notes.len());
    for n in notes
        .iter()
        .filter(|n| n.dur_ms >= MIN_STEP_MS && n.pitch.is_finite())
    {
        let end = n.onset_ms.saturating_add(n.dur_ms);
        match out.last_mut() {
            Some(last) if (n.pitch - last.pitch).abs() < QUERY_COLLAPSE => {
                last.end_ms = last.end_ms.max(end)
            }
            _ => out.push(Step {
                pitch: n.pitch,
                onset_ms: n.onset_ms,
                end_ms: end,
            }),
        }
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
fn query_keys(steps: &[Step]) -> Vec<(i32, u32, f32)> {
    let raw: Vec<f32> = steps.windows(2).map(|w| w[1].pitch - w[0].pitch).collect();
    let mut out = Vec::new();
    for j in 0..raw.len() {
        for (n, contour) in [(index::INTERVAL_N, false), (index::CONTOUR_N, true)] {
            let mut windows: Vec<(Vec<f32>, f32)> = Vec::new();
            if j + n <= raw.len() {
                windows.push((raw[j..j + n].to_vec(), 0.0));
            }
            if j + n < raw.len() {
                for m in 0..n {
                    let mut w: Vec<f32> = raw[j..j + n + 1].to_vec();
                    let merged = w[m] + w[m + 1];
                    if merged.abs() < QUERY_COLLAPSE {
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
                    let limit = if base > 0.0 {
                        CONTOUR_VARIANTS / 2
                    } else {
                        CONTOUR_VARIANTS
                    };
                    for (v, p) in combinations(&opts, limit) {
                        out.push((
                            j as i32,
                            index::contour_key(&v),
                            CONTOUR_WEIGHT * (-(p + base)).exp(),
                        ));
                    }
                } else {
                    let opts: Vec<Vec<(i8, f32)>> =
                        w.iter().map(|&x| interval_options(x)).collect();
                    let limit = if base > 0.0 {
                        INTERVAL_VARIANTS / 4
                    } else {
                        INTERVAL_VARIANTS
                    };
                    for (v, p) in combinations(&opts, limit) {
                        out.push((j as i32, index::interval_key(&v), (-(p + base)).exp()));
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
    let base = if x.round() == 0.0 {
        x.signum()
    } else {
        x.round()
    };
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
    let class = |v: f32| index::contour_class(v.round().clamp(-12.0, 12.0) as i8);
    let base = if x.round() == 0.0 {
        x.signum() as i8
    } else {
        class(x)
    };
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

#[derive(Debug, Clone)]
struct Anchor {
    phrase: u32,
    /// Reference step aligned with query step 0.
    offset: i32,
    /// The phrase's coarse vote.
    votes: f32,
    /// The phrase's best note-alignment cost.
    align: f32,
    /// Matched (query step, reference step) pairs behind the offset.
    pairs: Vec<(u16, u16)>,
}

/// Stage 1: offset voting over n-gram postings.
fn retrieve(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    options: &MatchOptions,
    allow: impl Fn(u32) -> bool,
) -> Vec<Anchor> {
    let keys: Vec<(i32, u32, f32)> = query_keys(&q.steps);
    let total = index.positions() as f32;
    let looked_up: Vec<(i32, u32, f32)> = keys
        .iter()
        .filter_map(|&(j, key, weight)| {
            let (_, count) = index.postings(key)?;
            (count <= MAX_POSTINGS).then(|| (j, key, weight * (total / count as f32).ln().max(0.1)))
        })
        .collect();
    let phrases = index.phrase_count() as usize;
    // Pass 1: coarse offset histograms per phrase.
    let mut hist = vec![0f32; phrases * BUCKETS];
    let mut touched: Vec<u32> = Vec::new();
    for &(j, key, w) in &looked_up {
        let Some((postings, _)) = index.postings(key) else {
            continue;
        };
        for (phrase, at) in postings {
            let p = phrase as usize;
            if p >= phrases || !allow(phrase) {
                continue;
            }
            let bucket = ((at as i32 - j).div_euclid(2)).rem_euclid(BUCKETS as i32) as usize;
            let cell = &mut hist[p * BUCKETS..(p + 1) * BUCKETS];
            if cell.iter().all(|&v| v == 0.0) {
                touched.push(phrase);
            }
            cell[bucket] += w;
        }
    }
    let mut pre: Vec<(u32, f32)> = touched
        .iter()
        .map(|&p| {
            let h = &hist[p as usize * BUCKETS..(p as usize + 1) * BUCKETS];
            let best = (0..BUCKETS)
                .map(|b| h[(b + BUCKETS - 1) % BUCKETS] + h[b] + h[(b + 1) % BUCKETS])
                .fold(0.0, f32::max);
            (p, best)
        })
        .collect();
    drop(hist);
    if pre.len() > options.shortlist {
        pre.select_nth_unstable_by(options.shortlist, |a, b| b.1.total_cmp(&a.1));
        pre.truncate(options.shortlist);
    }
    let mut shortlisted = vec![false; phrases];
    for &(p, _) in &pre {
        shortlisted[p as usize] = true;
    }
    // Pass 2: exact votes for the shortlisted phrases.
    let mut votes: Vec<(u32, i32, i32, f32)> = Vec::new();
    for &(j, key, w) in &looked_up {
        let Some((postings, _)) = index.postings(key) else {
            continue;
        };
        for (phrase, at) in postings {
            if shortlisted.get(phrase as usize).copied().unwrap_or(false) {
                votes.push((phrase, at as i32 - j, j, w));
            }
        }
    }
    votes.sort_unstable_by(|a, b| {
        (a.0, a.1, a.2)
            .cmp(&(b.0, b.1, b.2))
            .then(b.3.total_cmp(&a.3))
    });
    // Each (phrase, offset, query position) counts once, at its best key variant.
    votes.dedup_by(|later, kept| (later.0, later.1, later.2) == (kept.0, kept.1, kept.2));
    let mut anchors = Vec::new();
    let mut points: Vec<Point> = Vec::new();
    let mut acc: Vec<f32> = Vec::new();
    let mut i = 0;
    while i < votes.len() {
        let phrase = votes[i].0;
        let start = i;
        while i < votes.len() && votes[i].0 == phrase {
            i += 1;
        }
        let Some(r) = refs.get(phrase) else { continue };
        points.clear();
        points.extend(votes[start..i].iter().filter_map(|v| {
            let qs = q.steps.get(usize::try_from(v.2).ok()?)?;
            let k = usize::try_from(v.2 + v.1).ok()?;
            let rs = r.steps.get(k)?;
            Some(Point {
                j: v.2 as u16,
                k: k as u16,
                x: qs.onset_ms as f32,
                y: rs.onset_ms as f32,
                w: v.3,
            })
        }));
        if points.len() > MAX_POINTS {
            points.select_nth_unstable_by(MAX_POINTS, |a, b| b.w.total_cmp(&a.w));
            points.truncate(MAX_POINTS);
        }
        for (votes, pairs) in hough(&points, &mut acc) {
            anchors.push(Anchor {
                phrase,
                offset: median_offset(&pairs),
                votes,
                align: ALIGN_UNKNOWN,
                pairs,
            });
        }
    }
    // Placements from aligning the query's pitch changes with each shortlisted
    // phrase's: robust to the inserted, split and missed notes that break n-grams.
    let qs = shape(&q.steps);
    let mut table = AlignTable::default();
    let mut aligned: HashMap<u32, f32> = HashMap::with_capacity(pre.len());
    for &(phrase, score) in &pre {
        let Some(r) = refs.get(phrase) else { continue };
        let rs = shape(&r.steps);
        for (cost, pairs) in align_notes(&qs, &rs, &mut table) {
            let best = aligned.entry(phrase).or_insert(f32::INFINITY);
            *best = best.min(cost);
            anchors.push(Anchor {
                phrase,
                offset: median_offset(&pairs),
                votes: score,
                align: cost,
                pairs,
            });
        }
    }
    // Rank the pool by the phrase's best note alignment, then its coarse vote.
    let pre_score: HashMap<u32, f32> = pre.iter().copied().collect();
    for a in &mut anchors {
        a.votes = pre_score.get(&a.phrase).copied().unwrap_or(a.votes);
        a.align = aligned.get(&a.phrase).copied().unwrap_or(ALIGN_UNKNOWN);
    }
    anchors.sort_by(|a, b| {
        a.align
            .total_cmp(&b.align)
            .then(b.votes.total_cmp(&a.votes))
    });
    anchors.truncate(options.pool);
    anchors
}

fn median_offset(pairs: &[(u16, u16)]) -> i32 {
    let mut offsets: Vec<f32> = pairs.iter().map(|p| p.1 as f32 - p.0 as f32).collect();
    median(&mut offsets).unwrap_or(0.0).round() as i32
}

/// Reusable dynamic-programming buffers.
#[derive(Default)]
struct AlignTable {
    cost: Vec<f32>,
    from: Vec<u8>,
}

/// Semi-global alignment of the query's pitch changes (`qs`, from [`shape`])
/// inside a phrase's (`rs`): free start and end in the phrase, substitutions
/// scored by interval and rhythm difference, insertions and deletions, and
/// merges of two intervals for split or missed notes. Returns the best one or
/// two placements as (cost per query interval, matched (query step, reference
/// step) pairs).
fn align_notes(
    qs: &[(f32, f32)],
    rs: &[(f32, f32)],
    t: &mut AlignTable,
) -> Vec<(f32, Vec<(u16, u16)>)> {
    let (m, n) = (qs.len(), rs.len());
    if m < 2 || n < 2 {
        return Vec::new();
    }
    let w = n + 1;
    t.cost.clear();
    t.cost.resize((m + 1) * w, 0.0);
    t.from.clear();
    t.from.resize((m + 1) * w, 0);
    let (indel, merge, rhythm) = (ALIGN_INDEL, ALIGN_MERGE, ALIGN_RHYTHM);
    let sub = |a: (f32, f32), b: (f32, f32)| {
        (a.0 - b.0).abs().min(4.0) / 4.0 + rhythm * (a.1 - b.1).abs().min(1.5)
    };
    let pitch = |a: f32, b: f32| (a - b).abs().min(4.0) / 4.0;
    for i in 1..=m {
        t.cost[i * w] = i as f32 * indel;
        t.from[i * w] = 2;
        for j in 1..=n {
            let mut best = (t.cost[(i - 1) * w + j - 1] + sub(qs[i - 1], rs[j - 1]), 1u8);
            let up = t.cost[(i - 1) * w + j] + indel;
            if up < best.0 {
                best = (up, 2);
            }
            let left = t.cost[i * w + j - 1] + indel;
            if left < best.0 {
                best = (left, 3);
            }
            if i >= 2 {
                let merged = t.cost[(i - 2) * w + j - 1]
                    + merge
                    + pitch(qs[i - 2].0 + qs[i - 1].0, rs[j - 1].0);
                if merged < best.0 {
                    best = (merged, 4);
                }
            }
            if j >= 2 {
                let merged = t.cost[(i - 1) * w + j - 2]
                    + merge
                    + pitch(qs[i - 1].0, rs[j - 2].0 + rs[j - 1].0);
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

/// A matched pitch change: query step `j` at `x` ms, reference step `k` at `y` ms.
#[derive(Debug, Clone, Copy)]
struct Point {
    j: u16,
    k: u16,
    x: f32,
    y: f32,
    w: f32,
}

/// The two strongest straight alignments y ≈ a + τ x among `points` (τ in
/// 0.45–2.2), by a Hough vote over (τ, a): (votes, matched pairs). Insertions
/// and deletions shift step offsets but not the line, so evidence on one
/// alignment pools here.
fn hough(points: &[Point], acc: &mut Vec<f32>) -> Vec<(f32, Vec<(u16, u16)>)> {
    if points.is_empty() {
        return Vec::new();
    }
    let x_max = points.iter().map(|p| p.x).fold(0.0, f32::max);
    let y_min = points.iter().map(|p| p.y).fold(f32::INFINITY, f32::min);
    let y_max = points.iter().map(|p| p.y).fold(0.0, f32::max);
    let a_min = y_min - HOUGH_TEMPO_MAX * x_max;
    let bins = (((y_max - a_min) / HOUGH_BIN_MS) as usize + 2).min(4096);
    // (score, tempo, first bin)
    let mut best: Vec<(f32, f32, usize)> = Vec::with_capacity(HOUGH_TEMPOS * 2);
    for t in 0..HOUGH_TEMPOS {
        let tempo = HOUGH_TEMPO_MIN
            * (HOUGH_TEMPO_MAX / HOUGH_TEMPO_MIN).powf(t as f32 / (HOUGH_TEMPOS - 1) as f32);
        acc.clear();
        acc.resize(bins, 0.0);
        for p in points {
            let b = ((p.y - tempo * p.x - a_min) / HOUGH_BIN_MS) as usize;
            if let Some(cell) = acc.get_mut(b) {
                *cell += p.w;
            }
        }
        // Two adjacent bins, so alignments on a bin edge are not split.
        let mut first = (0.0f32, 0usize);
        for b in 0..bins.saturating_sub(1) {
            let s = acc[b] + acc[b + 1];
            if s > first.0 {
                first = (s, b);
            }
        }
        best.push((first.0, tempo, first.1));
    }
    best.sort_by(|a, b| b.0.total_cmp(&a.0));
    let mut out: Vec<(f32, Vec<(u16, u16)>)> = Vec::with_capacity(2);
    let mut lines: Vec<(f32, f32)> = Vec::with_capacity(2);
    for &(score, tempo, b) in &best {
        if out.len() >= 2 || score <= 0.0 {
            break;
        }
        let a = a_min + (b as f32 + 1.0) * HOUGH_BIN_MS;
        // A second alignment must be elsewhere in the phrase.
        if lines
            .iter()
            .any(|&(a2, t2)| (a2 + t2 * x_max * 0.5 - a - tempo * x_max * 0.5).abs() < 2000.0)
        {
            continue;
        }
        let mut pairs: Vec<(u16, u16, f32)> = points
            .iter()
            .filter(|p| (p.y - tempo * p.x - a).abs() <= HOUGH_BIN_MS * 1.5)
            .map(|p| (p.j, p.k, p.w))
            .collect();
        // One reference step per query step: the heaviest.
        pairs.sort_unstable_by(|x, y| x.0.cmp(&y.0).then(y.2.total_cmp(&x.2)));
        pairs.dedup_by_key(|p| p.0);
        let votes: f32 = pairs.iter().map(|p| p.2).sum();
        lines.push((a, tempo));
        out.push((
            votes,
            pairs.into_iter().take(64).map(|p| (p.0, p.1)).collect(),
        ));
    }
    out
}

/// A reference phrase, decoded on first use.
struct Reference {
    song: u32,
    notes: Vec<Note>,
    /// End of each note, extended over rests shorter than `REF_HOLD_MS`.
    ends: Vec<u32>,
    steps: Vec<Step>,
}

impl Reference {
    fn load(index: &MelodyIndex, phrase: u32) -> Option<Self> {
        let p = index.phrase(phrase)?;
        if p.notes.is_empty() {
            return None;
        }
        let ends = p
            .notes
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let end = n.onset_ms.saturating_add(n.dur_ms);
                match p.notes.get(i + 1) {
                    Some(next) if next.onset_ms.saturating_sub(end) < REF_HOLD_MS => {
                        next.onset_ms.max(end)
                    }
                    _ => end,
                }
            })
            .collect();
        let steps = index::collapse(&p.notes);
        Some(Self {
            song: p.song,
            notes: p.notes,
            ends,
            steps,
        })
    }

    /// Pitch sounding at `t` ms (NaN in rests and outside the phrase).
    fn at(&self, t: f32) -> f32 {
        if t.is_nan() || t < 0.0 {
            return f32::NAN;
        }
        let t = t as u32;
        let i = self.notes.partition_point(|n| n.onset_ms <= t);
        if i > 0 && t < self.ends[i - 1] {
            self.notes[i - 1].pitch
        } else {
            f32::NAN
        }
    }

    /// Walks increasing times without searching from scratch.
    fn cursor(&self) -> Cursor<'_> {
        Cursor {
            r: self,
            i: 0,
            started: false,
        }
    }
}

struct Cursor<'a> {
    r: &'a Reference,
    i: usize,
    started: bool,
}

impl Cursor<'_> {
    /// Like [`Reference::at`] for non-decreasing `t`.
    fn at(&mut self, t: f32) -> f32 {
        if t.is_nan() || t < 0.0 {
            return f32::NAN;
        }
        let t = t as u32;
        let notes = &self.r.notes;
        if !self.started {
            self.i = notes.partition_point(|n| n.onset_ms <= t);
            self.started = true;
        }
        while self.i < notes.len() && notes[self.i].onset_ms <= t {
            self.i += 1;
        }
        if self.i > 0 && t < self.r.ends[self.i - 1] {
            notes[self.i - 1].pitch
        } else {
            f32::NAN
        }
    }
}

/// Phrases decoded for one search.
struct References<'a> {
    index: &'a MelodyIndex,
    loaded: HashMap<u32, Option<Reference>>,
}

impl<'a> References<'a> {
    fn new(index: &'a MelodyIndex) -> Self {
        Self {
            index,
            loaded: HashMap::new(),
        }
    }

    fn get(&mut self, phrase: u32) -> Option<&Reference> {
        let index = self.index;
        self.loaded
            .entry(phrase)
            .or_insert_with(|| Reference::load(index, phrase))
            .as_ref()
    }
}

/// An anchor after linear scaling.
struct Linear {
    anchor: Anchor,
    distance: f32,
    key: f32,
    tempo: f32,
    /// Reference time of query time 0 (ms).
    anchor_ms: f32,
}

#[derive(Debug, Clone)]
struct Scored {
    song: u32,
    phrase: u32,
    features: Features,
    score: f32,
    key: f32,
    tempo: f32,
    anchor_ms: f32,
    /// (query voiced index, reference time in ms) from the DTW path.
    path: TimePath,
}

fn tempo_grid() -> [f32; TEMPOS] {
    let mut t = [0f32; TEMPOS];
    for (i, x) in t.iter_mut().enumerate() {
        *x = TEMPO_MIN * (TEMPO_MAX / TEMPO_MIN).powf(i as f32 / (TEMPOS - 1) as f32);
    }
    t
}

/// Stages 2–5 for a set of anchors.
fn rerank(
    q: &Query,
    refs: &mut References,
    anchors: Vec<Anchor>,
    options: &MatchOptions,
) -> Vec<Scored> {
    let linear = linear_stage(q, refs, anchors, options);
    dtw_stage(q, refs, linear, options.rerank)
}

/// Quick placement and a coarse DTW over the pool, then full linear scaling of
/// the best; best first.
fn linear_stage(
    q: &Query,
    refs: &mut References,
    anchors: Vec<Anchor>,
    options: &MatchOptions,
) -> Vec<Linear> {
    let coarse_q: Vec<f32> = q.voiced.iter().step_by(2).map(|&i| q.contour[i]).collect();
    let coarse_t: Vec<f32> = q
        .voiced
        .iter()
        .step_by(2)
        .map(|&i| i as f32 * CONTOUR_MS as f32)
        .collect();
    let mut quick: Vec<(f32, Anchor)> = Vec::with_capacity(anchors.len());
    for a in anchors {
        let Some(r) = refs.get(a.phrase) else {
            continue;
        };
        let Some((_, key, tempo, anchor_ms)) = linear_scaling(q, r, &a, true) else {
            continue;
        };
        let span = q.duration_ms();
        if let Some(d) = coarse_dtw(&coarse_q, &coarse_t, span, r, key, tempo, anchor_ms) {
            quick.push((d, a));
        }
    }
    quick.sort_by(|a, b| a.0.total_cmp(&b.0));
    quick.truncate(options.rerank);
    let mut linear = Vec::with_capacity(quick.len());
    for (_, a) in quick {
        let Some(r) = refs.get(a.phrase) else {
            continue;
        };
        if let Some((distance, key, tempo, anchor_ms)) = linear_scaling(q, r, &a, false) {
            linear.push(Linear {
                anchor: a,
                distance,
                key,
                tempo,
                anchor_ms,
            });
        }
    }
    linear
}

/// DTW, edit distance and fusion for the best `keep` linear candidates.
fn dtw_stage(
    q: &Query,
    refs: &mut References,
    mut linear: Vec<Linear>,
    keep: usize,
) -> Vec<Scored> {
    linear.truncate(keep);
    let positions = q.positions();
    let mut scored: Vec<Scored> = Vec::with_capacity(linear.len());
    for l in linear {
        let Some(r) = refs.get(l.anchor.phrase) else {
            continue;
        };
        let Some((dtw, key, path)) = dtw_refine(q, r, l.key, l.tempo, l.anchor_ms) else {
            continue;
        };
        let edit = edit_distance(q, r, &path);
        scored.push(Scored {
            song: r.song,
            phrase: l.anchor.phrase,
            features: Features {
                votes: l.anchor.votes / positions,
                align: l.anchor.align,
                linear: l.distance,
                dtw,
                edit,
            },
            score: 0.0,
            key,
            tempo: l.tempo,
            anchor_ms: l.anchor_ms,
            path,
        });
    }
    score_all(&mut scored);
    scored.sort_by(|a, b| b.score.total_cmp(&a.score));
    scored
}

/// The model's logit for one candidate.
fn utility(f: &Features) -> f32 {
    let x = [
        f.votes.min(4.0),
        f.align.min(2.0),
        f.linear.min(3.0),
        f.dtw.min(4.0),
        f.edit.min(2.0),
    ];
    x.iter().zip(MODEL_WEIGHTS).map(|(x, w)| x * w).sum()
}

/// Calibrated P(correct) for competing candidates: a softmax over them and a
/// "none of these" option, then Platt scaling.
fn score_all(scored: &mut [Scored]) {
    let z: Vec<f32> = scored.iter().map(|s| utility(&s.features)).collect();
    let top = z.iter().copied().fold(MODEL_NONE, f32::max);
    // Songs compete, not placements: each song enters with its best placement.
    let mut best: HashMap<u32, f32> = HashMap::with_capacity(scored.len());
    for (s, &z) in scored.iter().zip(&z) {
        let e = best.entry(s.song).or_insert(f32::NEG_INFINITY);
        *e = e.max(z);
    }
    let total = (MODEL_NONE - top).exp() + best.values().map(|z| (z - top).exp()).sum::<f32>();
    for (s, z) in scored.iter_mut().zip(z) {
        let p = (z - top).exp() / total;
        s.score = 1.0 / (1.0 + (-(CALIBRATION.0 + CALIBRATION.1 * logit(p))).exp());
    }
}

/// Tempo and anchor from matched pitch changes by a Theil–Sen fit of reference
/// onsets against query onsets: (tempo, reference time of query time 0).
fn fit_pairs(pts: &[(f32, f32, f32)]) -> Option<(f32, f32)> {
    let mut slopes = Vec::new();
    for (a, p) in pts.iter().enumerate() {
        for s in &pts[a + 1..] {
            let dx = s.0 - p.0;
            if dx.abs() >= 300.0 {
                slopes.push((s.1 - p.1) / dx);
            }
        }
    }
    if slopes.len() < 3 {
        return None;
    }
    let tempo = median(&mut slopes)?.clamp(TEMPO_FIT_MIN, TEMPO_FIT_MAX);
    let mut a: Vec<f32> = pts.iter().map(|p| p.1 - tempo * p.0).collect();
    Some((tempo, median(&mut a)?))
}

/// Best (distance, key, tempo, anchor) over tempos and shifts around the anchor.
/// `quick` tries few placements on a sparse sample and skips the refinement.
fn linear_scaling(
    q: &Query,
    r: &Reference,
    a: &Anchor,
    quick: bool,
) -> Option<(f32, f32, f32, f32)> {
    // Matched pitch changes; without any, the first query step with a counterpart.
    let j0 = (-a.offset).max(0) as usize;
    let fallback = [(j0 as u16, (j0 as i32 + a.offset).max(0) as u16)];
    let pairs: &[(u16, u16)] = if a.pairs.is_empty() {
        &fallback
    } else {
        &a.pairs
    };
    let pts: Vec<(f32, f32, f32)> = pairs
        .iter()
        .filter_map(|&(j, k)| {
            let (qs, rs) = (q.steps.get(j as usize)?, r.steps.get(k as usize)?);
            Some((qs.onset_ms as f32, rs.onset_ms as f32, qs.pitch - rs.pitch))
        })
        .collect();
    if pts.is_empty() {
        return None;
    }
    let mut diffs: Vec<f32> = pts.iter().map(|p| p.2).collect();
    let key0 = median(&mut diffs)?;
    let fit = fit_pairs(&pts).map(|f| f.0);
    let mut trial: Vec<f32> = Vec::with_capacity(TEMPOS + FIT_FACTORS.len());
    if quick {
        trial.extend(QUICK_TEMPOS);
        if let Some(t) = fit {
            trial.extend(
                [0.92, 1.0, 1.08]
                    .iter()
                    .map(|f| (t * f).clamp(TEMPO_FIT_MIN, TEMPO_FIT_MAX)),
            );
        }
    } else {
        trial.extend(tempo_grid());
        if let Some(t) = fit {
            trial.extend(
                FIT_FACTORS
                    .iter()
                    .map(|f| (t * f).clamp(TEMPO_FIT_MIN, TEMPO_FIT_MAX)),
            );
        }
    }
    let shifts: &[f32] = if quick { &QUICK_SHIFTS_MS } else { &SHIFTS_MS };
    let sample: Vec<usize> = q
        .voiced
        .iter()
        .copied()
        .step_by(if quick { 4 } else { 2 })
        .collect();
    let mut best: Option<(f32, f32, f32)> = None;
    let mut implied = Vec::with_capacity(pts.len());
    for &tempo in &trial {
        implied.clear();
        implied.extend(pts.iter().map(|p| p.1 - tempo * p.0));
        let Some(anchor) = median(&mut implied) else {
            continue;
        };
        for &shift in shifts {
            let start = anchor + shift;
            let mut cursor = r.cursor();
            let mut cost = 0.0;
            for &i in &sample {
                let rv = cursor.at(start + tempo * (i as f32 * CONTOUR_MS as f32));
                cost += if rv.is_finite() {
                    (q.contour[i] - rv - key0).abs().min(LS_CLIP)
                } else {
                    MISS_COST
                };
            }
            if best.is_none_or(|b| cost < b.0) {
                best = Some((cost, tempo, start));
            }
        }
    }
    let (cost, tempo, anchor) = best?;
    if quick {
        return Some((cost / sample.len().max(1) as f32, key0, tempo, anchor));
    }
    // Refine the key at the best placement and score all voiced frames.
    let refs: Vec<f32> = {
        let mut cursor = r.cursor();
        q.voiced
            .iter()
            .map(|&i| cursor.at(anchor + tempo * (i as f32 * CONTOUR_MS as f32)))
            .collect()
    };
    let mut d: Vec<f32> = q
        .voiced
        .iter()
        .zip(&refs)
        .filter(|(_, rv)| rv.is_finite())
        .map(|(&i, rv)| q.contour[i] - rv)
        .collect();
    let key = median(&mut d).unwrap_or(key0);
    let cost: f32 = q
        .voiced
        .iter()
        .zip(&refs)
        .map(|(&i, &rv)| {
            if rv.is_finite() {
                (q.contour[i] - rv - key).abs().min(LS_CLIP)
            } else {
                MISS_COST
            }
        })
        .sum();
    Some((cost / q.voiced.len() as f32, key, tempo, anchor))
}

/// Voiced reference samples every `step` ms over `[from, to)`: (times, pitches).
fn ref_samples(r: &Reference, from: f32, to: f32, step: f32) -> (Vec<f32>, Vec<f32>) {
    let mut cursor = r.cursor();
    let n = ((to - from) / step).ceil().clamp(0.0, 20_000.0) as usize;
    (0..n)
        .map(|k| from + k as f32 * step)
        .map(|t| (t, cursor.at(t)))
        .filter(|(_, v)| v.is_finite())
        .unzip()
}

/// One DTW pass at 10 fps for thinning the pool. The reference is sampled at the
/// estimated tempo so the slope limits apply around it.
#[allow(clippy::too_many_arguments)]
fn coarse_dtw(
    qv: &[f32],
    qt: &[f32],
    span: f32,
    r: &Reference,
    key: f32,
    tempo: f32,
    anchor_ms: f32,
) -> Option<f32> {
    let step = tempo * 2.0 * CONTOUR_MS as f32;
    let from = (anchor_ms - 1000.0).max(0.0);
    let (times, rv) = ref_samples(r, from, anchor_ms + 1.5 * span * tempo, step);
    if rv.len() < 4 || qv.is_empty() {
        return None;
    }
    let expected: Vec<usize> = qt
        .iter()
        .map(|&t| times.partition_point(|&x| x < anchor_ms + tempo * t))
        .collect();
    let band = ((qv.len() as f32 * 0.25) as usize).max(6);
    dtw(qv, &rv, &expected, band, key).map(|(d, _)| d)
}

/// Subsequence DTW between the query's voiced frames and the reference's voiced
/// frames near the anchor, with key refinement. The reference is sampled at the
/// estimated tempo, so slopes ½–2 are relative to it. Returns (distance, key,
/// path of (query voiced index, reference time in ms)).
fn dtw_refine(
    q: &Query,
    r: &Reference,
    key: f32,
    tempo: f32,
    anchor_ms: f32,
) -> Option<(f32, f32, TimePath)> {
    let frame = CONTOUR_MS as f32;
    let span = q.duration_ms() * tempo;
    let (times, rv) = ref_samples(
        r,
        (anchor_ms - 2000.0).max(0.0),
        anchor_ms + 1.8 * span,
        tempo * frame,
    );
    if rv.len() < 4 {
        return None;
    }
    let qv: Vec<f32> = q.voiced.iter().map(|&i| q.contour[i]).collect();
    // Expected reference index of each query frame along the linear path.
    let expected: Vec<usize> = q
        .voiced
        .iter()
        .map(|&i| times.partition_point(|&t| t < anchor_ms + tempo * (i as f32 * frame)))
        .collect();
    let band = ((qv.len() as f32 * 0.2) as usize).max(10);
    let mut k = key;
    let mut best: Option<(f32, f32, IndexPath)> = None;
    for pass in 0..3 {
        let Some((d, path)) = dtw(&qv, &rv, &expected, band, k) else {
            break;
        };
        let mut diffs: Vec<f32> = path.iter().map(|&(i, j)| qv[i] - rv[j]).collect();
        let next = median(&mut diffs).unwrap_or(k);
        if best.as_ref().is_none_or(|b| d < b.0) {
            best = Some((d, k, path));
        }
        if (next - k).abs() < 0.05 || pass == 2 {
            break;
        }
        k = next;
    }
    let (mut d, mut k, mut path) = best?;
    let k0 = k;
    for dk in [-0.5, 0.5] {
        if let Some((d2, p2)) = dtw(&qv, &rv, &expected, band, k0 + dk)
            && d2 < d
        {
            d = d2;
            path = p2;
            k = k0 + dk;
        }
    }
    let path = path.into_iter().map(|(i, j)| (i, times[j])).collect();
    Some((d, k, path))
}

/// DTW path as (query index, reference index).
type IndexPath = Vec<(usize, usize)>;
/// DTW path as (query voiced index, reference time in ms).
type TimePath = Vec<(usize, f32)>;

/// Banded subsequence DTW with steps (1,1), (1,2), (2,1); every query frame is
/// charged once, so the distance is the mean clipped cost per query frame.
fn dtw(
    qv: &[f32],
    rv: &[f32],
    expected: &[usize],
    band: usize,
    key: f32,
) -> Option<(f32, Vec<(usize, usize)>)> {
    let n = qv.len();
    let m = rv.len();
    if n == 0 || m == 0 || expected.len() != n {
        return None;
    }
    let width = 2 * band + 1;
    let lo: Vec<usize> = expected.iter().map(|e| e.saturating_sub(band)).collect();
    let mut d = vec![f32::INFINITY; n * width];
    let mut from = vec![0u8; n * width];
    // Row i covers reference indices lo[i]..lo[i] + width.
    let at = |d: &[f32], i: usize, j: isize| -> f32 {
        let k = j - lo[i] as isize;
        if k < 0 || k >= width as isize {
            f32::INFINITY
        } else {
            d[i * width + k as usize]
        }
    };
    for i in 0..n {
        let l = lo[i];
        let q = qv[i] - key;
        let row = i * width;
        let end = width.min(m.saturating_sub(l));
        if i == 0 {
            for w in 0..end {
                d[row + w] = (q - rv[l + w]).abs().min(DTW_CLIP);
            }
            continue;
        }
        let q_prev = qv[i - 1] - key;
        for w in 0..end {
            let j = (l + w) as isize;
            let c = (q - rv[l + w]).abs().min(DTW_CLIP);
            let diag = at(&d, i - 1, j - 1);
            let skip_r = at(&d, i - 1, j - 2);
            let skip_q = if i >= 2 {
                at(&d, i - 2, j - 1) + (q_prev - rv[l + w]).abs().min(DTW_CLIP)
            } else {
                f32::INFINITY
            };
            let (mut best, mut f) = (diag, 1u8);
            if skip_r < best {
                best = skip_r;
                f = 2;
            }
            if skip_q < best {
                best = skip_q;
                f = 3;
            }
            d[row + w] = best + c;
            from[row + w] = f;
        }
    }
    let l = lo[n - 1];
    let (mut j, total) = (l..(l + width).min(m))
        .map(|j| (j, at(&d, n - 1, j as isize)))
        .min_by(|a, b| a.1.total_cmp(&b.1))?;
    if !total.is_finite() {
        return None;
    }
    let mut path = Vec::with_capacity(n);
    let mut i = n - 1;
    loop {
        path.push((i, j));
        match from[i * width + (j - lo[i])] {
            1 => {
                i -= 1;
                j -= 1;
            }
            2 => {
                i -= 1;
                j -= 2;
            }
            3 => {
                path.push((i - 1, j));
                i -= 2;
                j -= 1;
            }
            _ => break,
        }
    }
    path.reverse();
    Some((total / n as f32, path))
}

/// Normalized note edit distance between the query's pitch changes and the
/// reference's within the aligned span.
fn edit_distance(q: &Query, r: &Reference, path: &[(usize, f32)]) -> f32 {
    let (Some(&(_, first)), Some(&(_, last))) = (path.first(), path.last()) else {
        return 1.0;
    };
    let start_ms = first - 500.0;
    let end_ms = last + 500.0;
    let rs: Vec<Step> = r
        .steps
        .iter()
        .copied()
        .filter(|s| (s.onset_ms as f32) >= start_ms && (s.onset_ms as f32) <= end_ms)
        .collect();
    let qi = shape(&q.steps);
    let ri = shape(&rs);
    if qi.is_empty() {
        return 1.0;
    }
    const INDEL: f32 = 0.6;
    let mut prev: Vec<f32> = (0..=ri.len()).map(|j| j as f32 * INDEL).collect();
    let mut cur = vec![0f32; ri.len() + 1];
    for a in &qi {
        cur[0] = prev[0] + INDEL;
        for (j, b) in ri.iter().enumerate() {
            let sub = ((a.0 - b.0).abs().min(3.0)) / 3.0 + 0.3 * (a.1 - b.1).abs().min(1.0);
            cur[j + 1] = (prev[j] + sub).min(prev[j + 1] + INDEL).min(cur[j] + INDEL);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[ri.len()] / qi.len() as f32
}

/// (interval in semitones, log2 ratio of consecutive inter-onset intervals).
fn shape(steps: &[Step]) -> Vec<(f32, f32)> {
    let ioi: Vec<f32> = steps
        .windows(2)
        .map(|w| (w[1].onset_ms.saturating_sub(w[0].onset_ms)).max(10) as f32)
        .collect();
    steps
        .windows(2)
        .enumerate()
        .map(|(i, w)| {
            let ratio = if i == 0 {
                0.0
            } else {
                (ioi[i] / ioi[i - 1]).log2()
            };
            (w[1].pitch - w[0].pitch, ratio)
        })
        .collect()
}

/// Keep each song's best candidate, apply the popularity prior and build matches.
fn best_per_song(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    ranked: Vec<Scored>,
    k: usize,
    popularity_weight: f32,
) -> Vec<MelodyMatch> {
    let mut seen = HashSet::new();
    let mut out: Vec<(f32, Scored, SongMeta)> = Vec::new();
    for s in ranked {
        if !seen.insert(s.song) {
            continue;
        }
        let Some(meta) = index.song(s.song) else {
            continue;
        };
        let z = logit(s.score) + popularity_weight * ((meta.popularity + 0.05) / 0.55).ln();
        out.push((1.0 / (1.0 + (-z).exp()), s, meta));
    }
    out.sort_by(|a, b| b.0.total_cmp(&a.0));
    out.truncate(k);
    out.into_iter()
        .map(|(score, s, song)| {
            let alignment = alignment(q, refs.get(s.phrase), &s);
            MelodyMatch {
                song,
                score,
                key_shift: s.key,
                tempo: s.tempo,
                alignment,
            }
        })
        .collect()
}

fn logit(p: f32) -> f32 {
    let p = p.clamp(1e-6, 1.0 - 1e-6);
    (p / (1.0 - p)).ln()
}

/// Query contour and the transposed, time-aligned reference, both relative to
/// the query median.
fn alignment(q: &Query, r: Option<&Reference>, s: &Scored) -> MelodyAlignment {
    let query: Vec<f32> = q.contour.iter().map(|m| m - q.median).collect();
    let mut reference = vec![f32::NAN; q.contour.len()];
    if let Some(r) = r {
        let mut mapped: HashMap<usize, f32> = HashMap::new();
        for &(i, j) in &s.path {
            mapped.entry(q.voiced[i]).or_insert(j);
        }
        for (f, out) in reference.iter_mut().enumerate() {
            let t = match mapped.get(&f) {
                Some(&t) => t,
                None => s.anchor_ms + s.tempo * f as f32 * CONTOUR_MS as f32,
            };
            *out = r.at(t) + s.key - q.median;
        }
    }
    MelodyAlignment {
        frame_ms: CONTOUR_MS,
        query,
        reference,
    }
}

#[cfg(test)]
mod tests;
