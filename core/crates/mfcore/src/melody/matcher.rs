//! Melody matcher: window-vector retrieval with alignment voting, then linear
//! scaling and DTW on continuous contours, fused into a calibrated score.
//!
//! 1. **Retrieval.** The query contour is cut into key-invariant window vectors
//!    (see [`crate::melody::contour`]) at [`TEMPOS`] lengths, one per candidate
//!    tempo, every 0.25 s. Each vector probes the nearest cells of the index's
//!    inverted multi-index; every posting there votes, weighted by how close the
//!    cell is, for its track and the implied alignment (reference frame of
//!    query frame 0). A coarse per-track histogram over alignments shortlists
//!    tracks; their votes are then pooled per (tempo, alignment) into
//!    placements. No note segmentation is involved, so hums whose notes split,
//!    merge or slide still vote for the right place.
//! 2. **Quick check.** Each placement gets a short linear-scaling search
//!    (tempo ±8 %, ±0.3 s) on a sparse sample of the query, a coarse DTW and the
//!    rhythm-free alignment; the score model ranks them on that, and the best go
//!    on.
//! 3. **Linear scaling** over tempos around the placement and shifts up to
//!    ±0.75 s, with a clipped L1 distance (3 semitones) at the median key.
//! 4. **DTW.** Subsequence DTW between voiced frames (reference sampled at the
//!    estimated tempo, slopes ½–2 around it, band of 20 % of the query), cost
//!    |q − r − k| clipped at 4; k is refined by median passes and ±½.
//! 5. **Fusion.** A conditional logit over the stages' evidence, the reference
//!    length and the song's popularity, with a "none of these" option whose
//!    weight grows for short, narrow or simple hums, then Platt scaling, gives
//!    a score calibrated as P(correct) on real hums (fitted by
//!    tools/eval/fit_fusion.py). An extra popularity prior can be added through
//!    [`MatchOptions`].
//! 6. **Works.** Recordings with the same normalized title are one answer,
//!    shown as the most popular one that matches nearly as well as the best.

use std::collections::{HashMap, HashSet};

use crate::melody::SongMeta;
use crate::melody::contour::{self, DIMS, Filled, HALF, QUERY_MIN_VOICED, Vector, WINDOW};
use crate::melody::index::{self, CODES, MelodyIndex, fnv1a, normalize_title};
use crate::melody::ngrams;
use crate::melody::notes::{CONTOUR_MS, MelodyQuery, median};
use crate::melody::steps::{self, AlignRows, AlignTable, Step};
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
    /// Logit weight of an extra ln((popularity + 0.05) / 0.55) on top of the score
    /// model, which already weighs popularity (default 0).
    pub popularity_weight: f32,
    /// Index cells probed per query window.
    pub probe: usize,
    /// Tracks whose alignments are pooled after the coarse vote.
    pub shortlist: usize,
    /// Placements given the quick linear-scaling check.
    pub pool: usize,
    /// Candidates given full linear scaling and DTW.
    pub rerank: usize,
    /// The score model's encoder terms, for searches with encoder hints.
    pub encoder_model: EncoderModel,
}

impl Default for MatchOptions {
    fn default() -> Self {
        Self {
            popularity_weight: 0.0,
            probe: 24,
            shortlist: 2000,
            pool: 3000,
            rerank: 150,
            encoder_model: EncoderModel::default(),
        }
    }
}

/// How the learned encoder's evidence enters the score model (searches with
/// hints only): weights of [`Features::enc_best`] and [`Features::enc_hit`], the
/// shift of "none of these", and the calibration of such searches. The default
/// is the fitted model; evaluation may try others.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EncoderModel {
    pub weights: [f32; 2],
    pub none: f32,
    pub calibration: (f32, f32),
}

impl Default for EncoderModel {
    fn default() -> Self {
        Self { weights: ENC_WEIGHTS, none: ENC_NONE, calibration: CALIBRATION_ENCODER }
    }
}

/// Distances of one candidate, as fed to the score model.
#[derive(Debug, Clone, Copy, Default)]
pub struct Features {
    /// Alignment votes per query window.
    pub votes: f32,
    /// Linear-scaling distance (mean clipped semitones per voiced query frame).
    pub linear: f32,
    /// DTW distance.
    pub dtw: f32,
    /// Share of voiced query frames that meet a voiced reference frame.
    pub coverage: f32,
    /// |log2 tempo|.
    pub tempo: f32,
    /// Rhythm-free alignment cost of the pitch changes, per query interval.
    pub align: f32,
    /// Edit distance of the pitch changes within the aligned span.
    pub edit: f32,
    /// Note n-gram votes per query n-gram position.
    pub note_votes: f32,
    /// ln(voiced reference frames / 600): long references match by chance more often.
    pub reference: f32,
    /// ln((popularity + 0.05) / 0.55): the song's popularity prior.
    pub prior: f32,
    /// The learned encoder's best window cosine for the song (0 when the encoder
    /// did not run or did not rank the song among its first [`ENC_HIT_RANK`] hits).
    pub enc_best: f32,
    /// 1 when the encoder ranked the song among its first [`ENC_HIT_RANK`] hits.
    pub enc_hit: f32,
}

/// One scored candidate with its features (for evaluation and model fitting).
#[derive(Debug, Clone)]
pub struct Candidate {
    pub song: u32,
    /// The work (composition) the song belongs to, see [`work_key`].
    pub work: u64,
    pub track: u32,
    /// Voiced frames of the matched reference track.
    pub ref_frames: u32,
    pub features: Features,
    pub score: f32,
    pub key_shift: f32,
    pub tempo: f32,
}

/// Where a song ended up in each stage (1-based ranks among distinct songs).
#[derive(Debug, Clone, Copy, Default)]
pub struct Trace {
    /// Coarse window vote, before the shortlist.
    pub windows: Option<usize>,
    /// Coarse note n-gram vote, before the shortlist.
    pub notes: Option<usize>,
    /// Placements of the shortlisted tracks, before the pool limit.
    pub unpooled: Option<usize>,
    /// Placements given the quick check.
    pub retrieval: Option<usize>,
    /// The quick check's order of all of them (the first `rerank` placements go on).
    pub linear: Option<usize>,
    pub reranked: Option<usize>,
}

/// Score model fitted by tools/eval/fit_fusion.py. Conditional logit: each
/// candidate's utility is MODEL_WEIGHTS · [`Features`] (in field order, capped as in
/// [`utility`]); a "none of these" option has utility MODEL_NONE + MODEL_NONE_QUERY ·
/// [`QueryTraits::model_inputs`], so short, narrow or simple hums are less
/// conclusive; the softmax probability goes through Platt scaling σ(a + b·logit p)
/// with CALIBRATION = (a, b), fitted on real hums, whole and cut short as the
/// progressive search sees them.
const MODEL_WEIGHTS: [f32; 10] =
    [13.5952, -9.2749, -8.8543, -5.5415, -4.2559, -16.3023, -2.3667, 0.1322, -0.0354, 1.2386];
const MODEL_NONE: f32 = -24.0149;
const MODEL_NONE_QUERY: [f32; 3] = [0.7509, -6.0958, -8.1592];
const CALIBRATION: (f32, f32) = (-0.7345, 0.9035);
/// The learned encoder's evidence (fitted by tools/eval/fit_fusion.py on CHAD with
/// the other weights fixed, so searches without the encoder score exactly as
/// before): weights of [`Features::enc_best`] and [`Features::enc_hit`], the
/// shift of "none of these" when the encoder ran, and that path's calibration.
const ENC_WEIGHTS: [f32; 2] = [10.6787, -5.0795];
const ENC_NONE: f32 = 3.0519;
const CALIBRATION_ENCODER: (f32, f32) = (-0.2851, 0.6003);
/// Encoder hits up to this rank count as [`Features::enc_hit`] (the same for every
/// budget, so the weight means the same everywhere).
pub const ENC_HIT_RANK: u32 = 200;

/// Retrieval tempos (query duration over reference duration), log-spaced.
const TEMPOS: usize = 11;
const TEMPO_MIN: f32 = 0.5;
const TEMPO_MAX: f32 = 2.0;
/// Tracks each query window votes for: its nearest postings, one per track.
const WINDOW_NEIGHBOURS: usize = 800;
/// Cells with this many postings get a residual distance table.
const TABLE_MIN_POSTINGS: u32 = 128;
/// Frames between query windows (0.25 s).
const QUERY_HOP: usize = 5;
/// Codes per half considered when probing cells.
const PROBE_CODES: usize = 16;
/// Coarse alignment buckets per track (1 s each, wrapping).
const BUCKETS: usize = 64;
const BUCKET_FRAMES: f32 = 20.0;
/// Alignment bins when pooling votes into placements (0.5 s).
const BIN_FRAMES: f32 = 10.0;
/// Placements kept per track.
const PLACEMENTS_PER_TRACK: usize = 2;
/// Quick-check tempo factors and shifts (frames).
const QUICK_FACTORS: [f32; 3] = [0.92, 1.0, 1.08];
const QUICK_SHIFTS: [f32; 5] = [-6.0, -3.0, 0.0, 3.0, 6.0];
/// Linear-scaling tempo factors around the placement and shifts (frames).
const FACTORS: [f32; 9] = [0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.17, 1.25];
const SHIFTS: [f32; 7] = [-15.0, -10.0, -5.0, 0.0, 5.0, 10.0, 15.0];
const LS_CLIP: f32 = 3.0;
/// Cost of a voiced query frame meeting a reference rest.
const MISS_COST: f32 = 1.5;
const DTW_CLIP: f32 = 4.0;
/// Alignment cost assumed when either side has too few pitch changes.
const ALIGN_UNKNOWN: f32 = 1.0;
/// Note n-gram keys with more postings than this carry too little information.
const MAX_NOTE_POSTINGS: u32 = 60_000;
/// Coarse step-offset buckets per track in the first n-gram vote.
const NOTE_BUCKETS: usize = 64;
/// Tracks given exact n-gram votes and a note alignment.
const NOTE_SHORTLIST: usize = 800;
/// Matched pitch changes considered per track.
const MAX_POINTS: usize = 256;
/// Hough search over tempo (reference per query, log-spaced) and anchor (bins of 400 ms).
const HOUGH_TEMPOS: usize = 12;
const HOUGH_TEMPO_MIN: f32 = 0.45;
const HOUGH_TEMPO_MAX: f32 = 2.2;
const HOUGH_BIN_MS: f32 = 400.0;

/// Top `k` matches, best first.
pub fn search(index: &MelodyIndex, query: &MelodyQuery, k: usize) -> Vec<MelodyMatch> {
    search_with(index, query, k, &MatchOptions::default())
}

/// [`search`] with explicit options.
pub fn search_with(index: &MelodyIndex, query: &MelodyQuery, k: usize, options: &MatchOptions) -> Vec<MelodyMatch> {
    let Some(q) = Query::new(query) else {
        return Vec::new();
    };
    let mut refs = References::new(index);
    let placements = retrieve_all(index, &q, &mut refs, options, &|_| true);
    let ranked = rerank(&q, &mut refs, placements, options);
    best_per_song(index, &q, &mut refs, ranked, k, options.popularity_weight)
}

/// The work a song belongs to: recordings with the same normalized title
/// (remasters, live and radio edits, covers of a standard) are one answer to a
/// hum, which cannot tell versions apart.
pub fn work_key(index: &MelodyIndex, song: u32) -> u64 {
    match index.song(song).map(|s| normalize_title(&s.title)) {
        Some(title) if !title.is_empty() => fnv1a(&title),
        _ => fnv1a(&format!("\u{0}{song}")),
    }
}

/// All re-ranked candidates with their features, best first (for evaluation).
pub fn candidates(index: &MelodyIndex, query: &MelodyQuery, options: &MatchOptions) -> Vec<Candidate> {
    let Some(q) = Query::new(query) else {
        return Vec::new();
    };
    let mut refs = References::new(index);
    let placements = retrieve_all(index, &q, &mut refs, options, &|_| true);
    let ranked = rerank(&q, &mut refs, placements, options);
    ranked.into_iter().map(candidate).collect()
}

fn candidate(s: Scored) -> Candidate {
    Candidate {
        work: s.work,
        song: s.song,
        track: s.track,
        ref_frames: s.ref_frames,
        features: s.features,
        score: s.score,
        key_shift: s.key,
        tempo: 1.0 / s.tempo,
    }
}

/// Follow `song` through the stages (for evaluation).
pub fn trace(index: &MelodyIndex, query: &MelodyQuery, options: &MatchOptions, song: u32) -> Trace {
    let Some(q) = Query::new(query) else {
        return Trace::default();
    };
    let mut refs = References::new(index);
    let song_rank = |songs: &mut dyn Iterator<Item = u32>| {
        let mut seen = HashSet::new();
        songs.filter(|s| seen.insert(*s)).position(|s| s == song).map(|p| p + 1)
    };
    let mut coarse = Coarse::default();
    let placements = retrieve_traced(index, &q, &mut refs, options, &|_| true, Some(&mut coarse));
    let windows = song_rank(&mut coarse.windows.iter().filter_map(|&(t, _)| index.track_song(t)));
    let notes = song_rank(&mut coarse.notes.iter().filter_map(|&(t, _)| index.track_song(t)));
    let retrieval = song_rank(&mut placements.iter().filter_map(|p| index.track_song(p.track)));
    let unlimited = MatchOptions { pool: usize::MAX, ..options.clone() };
    let all = retrieve_all(index, &q, &mut refs, &unlimited, &|_| true);
    let unpooled = song_rank(&mut all.iter().filter_map(|p| index.track_song(p.track)));
    let mut quick = quick_stage(&q, &mut refs, placements, usize::MAX);
    let linear = song_rank(&mut quick.iter().filter_map(|(_, p)| refs.get(p.track).map(|r| r.song)));
    quick.truncate(options.rerank);
    let reranked = song_rank(&mut full_stage(&q, &mut refs, quick).iter().map(|s| s.song));
    Trace { windows, notes, unpooled, retrieval, linear, reranked }
}

/// Score one specific song (e.g. a lyric-search candidate) against the query.
pub fn score_song(index: &MelodyIndex, query: &MelodyQuery, song_id: u32) -> Option<MelodyMatch> {
    let q = Query::new(query)?;
    let tracks = index.song_tracks(song_id);
    if tracks.is_empty() {
        return None;
    }
    let mut refs = References::new(index);
    let options = MatchOptions { pool: 64, rerank: 12, ..MatchOptions::default() };
    // The index's own evidence for this song, so the features read as in a search...
    let wanted = tracks.clone();
    let mut placements = retrieve_all(index, &q, &mut refs, &options, &|t| wanted.contains(&t));
    // ...plus every place the song's windows resemble the query's, without vote evidence.
    for t in tracks {
        if let Some(r) = refs.get(t) {
            placements.extend(exhaustive_placements(&q, t, &r.frames));
        }
    }
    let mut ranked = rerank(&q, &mut refs, placements, &options);
    // Placements of one song do not compete: score each against "none" alone.
    for s in &mut ranked {
        score_all(std::slice::from_mut(s), &q);
    }
    ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
    best_per_song(index, &q, &mut refs, ranked, 1, options.popularity_weight).into_iter().next()
}

/// What the query itself says about how conclusive any answer can be.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct QueryTraits {
    pub voiced_ms: u32,
    /// Pitch range, 5th to 95th percentile of the voiced contour, in semitones.
    pub range: f32,
    /// Pitch changes between notes.
    pub steps: usize,
}

impl QueryTraits {
    pub fn of(query: &MelodyQuery) -> Self {
        let mut voiced: Vec<f32> = query.contour.iter().copied().filter(|m| m.is_finite()).collect();
        voiced.sort_by(f32::total_cmp);
        let pct = |p: f32| voiced.get(((voiced.len() as f32 - 1.0) * p).round() as usize).copied().unwrap_or(0.0);
        Self { voiced_ms: query.voiced_ms, range: pct(0.95) - pct(0.05), steps: steps::query_steps(&query.notes).len() }
    }

    /// The score model's inputs: log voiced length, scaled range and step count.
    fn model_inputs(&self) -> [f32; 3] {
        [
            (self.voiced_ms.max(500) as f32 / 8000.0).ln(),
            self.range.min(24.0) / 8.0 - 1.0,
            self.steps.min(60) as f32 / 20.0 - 1.0,
        ]
    }

    /// Utility of the score model's "none of these" option.
    fn none_utility(&self) -> f32 {
        MODEL_NONE + self.model_inputs().iter().zip(MODEL_NONE_QUERY).map(|(x, w)| x * w).sum::<f32>()
    }
}

/// A placement suggested by another recall stage (the learned encoder, see
/// [`crate::melody::encoder`]): reference frame `ref_frame` of `track` met query
/// frame `query_frame`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hint {
    pub track: u32,
    pub ref_frame: f32,
    pub query_frame: f32,
    /// The encoder's evidence for the track's song: best window cosine and the
    /// song's rank among its hits (1 = best).
    pub best: f32,
    pub rank: u32,
}

/// How hints become placements.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HintOptions {
    /// One placement per hint at each (query over reference tempo, shift in frames).
    pub offsets: Vec<(f32, f32)>,
    /// Also pool the index's own votes for the hinted tracks, so their placements
    /// carry evidence on the score model's scale: retrieval keeps their postings
    /// beyond its per-window limit, shortlists them and lets their placements past
    /// the pool cut (one pass, no second retrieval).
    pub votes: bool,
    /// The first this many hints always reach the full rerank (with their best
    /// quick-check placement), even when the quick stage would cut them.
    pub reserve: usize,
}

/// [`search_with`] plus hinted placements (see [`HintOptions`]). Hinted
/// placements skip the pool cut and compete in the quick stage.
pub fn search_with_hints(
    index: &MelodyIndex,
    query: &MelodyQuery,
    k: usize,
    options: &MatchOptions,
    hints: &[Hint],
    how: &HintOptions,
) -> Vec<MelodyMatch> {
    let Some(mut q) = Query::new(query) else {
        return Vec::new();
    };
    q.set_encoder(index, hints, &options.encoder_model);
    let mut refs = References::new(index);
    let placements = hinted(index, &q, &mut refs, options, hints, how);
    let ranked = rerank_reserved(&q, &mut refs, placements, options, hints, how);
    best_per_song(index, &q, &mut refs, ranked, k, options.popularity_weight)
}

/// [`candidates`] plus hinted placements (see [`search_with_hints`]).
pub fn candidates_with_hints(
    index: &MelodyIndex,
    query: &MelodyQuery,
    options: &MatchOptions,
    hints: &[Hint],
    how: &HintOptions,
) -> Vec<Candidate> {
    let Some(mut q) = Query::new(query) else {
        return Vec::new();
    };
    q.set_encoder(index, hints, &options.encoder_model);
    let mut refs = References::new(index);
    let placements = hinted(index, &q, &mut refs, options, hints, how);
    rerank_reserved(&q, &mut refs, placements, options, hints, how).into_iter().map(candidate).collect()
}

/// [`rerank`], plus the reserved hints' best quick-check placement when the
/// quick stage dropped their track.
fn rerank_reserved(
    q: &Query,
    refs: &mut References,
    placements: Vec<Placement>,
    options: &MatchOptions,
    hints: &[Hint],
    how: &HintOptions,
) -> Vec<Scored> {
    let mut quick = quick_stage(q, refs, placements, options.rerank);
    if how.reserve > 0 {
        let kept: HashSet<u32> = quick.iter().map(|(_, p)| p.track).collect();
        for h in hints.iter().take(how.reserve).filter(|h| !kept.contains(&h.track)) {
            let own: Vec<Placement> = how
                .offsets
                .iter()
                .map(|&(tempo, shift)| Placement {
                    track: h.track,
                    tempo,
                    anchor: h.ref_frame + shift - h.query_frame / tempo,
                    votes: 0.0,
                    note_votes: 0.0,
                })
                .collect();
            quick.extend(quick_stage(q, refs, own, 1));
        }
    }
    full_stage(q, refs, quick)
}

/// Retrieval's placements plus the hinted ones it has not already made (same
/// track, the query's middle within 1 s).
fn hinted(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    options: &MatchOptions,
    hints: &[Hint],
    how: &HintOptions,
) -> Vec<Placement> {
    let force: HashSet<u32> = if how.votes { hints.iter().map(|h| h.track).collect() } else { HashSet::new() };
    let mut placements = retrieve_forced(index, q, refs, options, &|_| true, &force, None);
    let mid = q.contour.len() as f32 / 2.0;
    let mut by_track: HashMap<u32, Vec<f32>> = HashMap::new();
    for p in &placements {
        by_track.entry(p.track).or_default().push(p.anchor + mid / p.tempo);
    }
    let mut add = |p: Placement, placements: &mut Vec<Placement>| {
        let middle = p.anchor + mid / p.tempo;
        let known = by_track.entry(p.track).or_default();
        if !known.iter().any(|m| (m - middle).abs() < 20.0) {
            known.push(middle);
            placements.push(p);
        }
    };
    for h in hints {
        for &(tempo, shift) in &how.offsets {
            let anchor = h.ref_frame + shift - h.query_frame / tempo;
            add(Placement { track: h.track, tempo, anchor, votes: 0.0, note_votes: 0.0 }, &mut placements);
        }
    }
    placements
}

/// The query in the forms the stages need.
struct Query {
    /// Utility of "none of these" for this query.
    none: f32,
    /// Encoder evidence per song (best window cosine, rank), when the encoder ran.
    encoder: Option<HashMap<u32, (f32, u32)>>,
    encoder_model: EncoderModel,
    contour: Vec<f32>,
    /// Indices of voiced contour frames.
    voiced: Vec<usize>,
    median: f32,
    /// (tempo index, start frame, vector).
    windows: Vec<(usize, f32, Vector)>,
    /// Pitch changes and their shape (see [`steps::shape`]).
    steps: Vec<Step>,
    shape: Vec<(f32, f32)>,
}

impl Query {
    fn new(query: &MelodyQuery) -> Option<Self> {
        let voiced: Vec<usize> =
            query.contour.iter().enumerate().filter(|(_, m)| m.is_finite()).map(|(i, _)| i).collect();
        if voiced.len() < 10 {
            return None;
        }
        let mut v: Vec<f32> = voiced.iter().map(|&i| query.contour[i]).collect();
        let median = median(&mut v)?;
        let mut windows = Vec::new();
        if let Some(filled) = Filled::new(&query.contour) {
            for (ti, tempo) in tempo_grid().into_iter().enumerate() {
                let len = WINDOW as f32 * tempo;
                let mut start = 0.0;
                while start + len <= filled.len() as f32 {
                    if let Some(vec) = filled.window(start, len, QUERY_MIN_VOICED)
                        && contour::rms(&vec) >= contour::MIN_RMS
                    {
                        windows.push((ti, start, vec));
                    }
                    start += QUERY_HOP as f32;
                }
            }
        }
        let steps = steps::query_steps(&query.notes);
        let shape = steps::shape(&steps);
        let none = QueryTraits::of(query).none_utility();
        Some(Self {
            none,
            encoder: None,
            encoder_model: EncoderModel::default(),
            contour: query.contour.clone(),
            voiced,
            median,
            windows,
            steps,
            shape,
        })
    }

    /// The encoder ran for this query and suggested `hints`: its evidence joins the
    /// score model (and "none of these" and the calibration switch to the
    /// encoder's).
    fn set_encoder(&mut self, index: &MelodyIndex, hints: &[Hint], model: &EncoderModel) {
        let mut by_song: HashMap<u32, (f32, u32)> = HashMap::with_capacity(hints.len());
        for h in hints {
            let Some(song) = index.track_song(h.track) else { continue };
            let e = by_song.entry(song).or_insert((h.best, h.rank));
            if h.best > e.0 {
                e.0 = h.best;
            }
            e.1 = e.1.min(h.rank);
        }
        if self.encoder.is_none() {
            self.none += model.none;
        }
        self.encoder = Some(by_song);
        self.encoder_model = *model;
    }

    /// The score model's logit for a candidate of this query.
    fn utility(&self, f: &Features) -> f32 {
        let w = self.encoder_model.weights;
        utility(f) + w[0] * f.enc_best + w[1] * f.enc_hit
    }

    /// [`Features::enc_best`] and [`Features::enc_hit`] of `song`. Only the first
    /// [`ENC_HIT_RANK`] hits count as evidence, whatever the budget: a larger budget's
    /// further hits are placements without encoder evidence.
    fn encoder_features(&self, song: u32) -> (f32, f32) {
        match self.encoder.as_ref().and_then(|e| e.get(&song)) {
            Some(&(best, rank)) if rank <= ENC_HIT_RANK => (best.max(0.0), 1.0),
            _ => (0.0, 0.0),
        }
    }

    /// Platt scaling of this query's scores.
    fn calibration(&self) -> (f32, f32) {
        if self.encoder.is_some() { self.encoder_model.calibration } else { CALIBRATION }
    }

    fn duration_ms(&self) -> f32 {
        self.contour.len() as f32 * CONTOUR_MS as f32
    }
}

fn tempo_grid() -> [f32; TEMPOS] {
    let mut t = [0f32; TEMPOS];
    for (i, x) in t.iter_mut().enumerate() {
        *x = TEMPO_MIN * (TEMPO_MAX / TEMPO_MIN).powf(i as f32 / (TEMPOS - 1) as f32);
    }
    t
}

/// A candidate alignment of the query with a track.
#[derive(Debug, Clone)]
struct Placement {
    track: u32,
    /// Query duration over reference duration.
    tempo: f32,
    /// Reference frame (fractional) aligned with query frame 0.
    anchor: f32,
    /// Pooled window votes per query window.
    votes: f32,
    /// Note n-gram votes per query n-gram position.
    note_votes: f32,
}

/// The coarse votes of both retrieval paths, best first (for evaluation).
#[derive(Default)]
struct Coarse {
    windows: Vec<(u32, f32)>,
    notes: Vec<(u32, f32)>,
}

/// Keep a copy of a coarse vote, best first.
fn record(coarse: Option<&mut Vec<(u32, f32)>>, pre: &[(u32, f32)]) {
    if let Some(out) = coarse {
        out.clear();
        out.extend_from_slice(pre);
        out.sort_by(|a, b| b.1.total_cmp(&a.1));
    }
}

/// Stage 1: placements from both retrieval paths, interleaved, merged where they
/// agree (same track, the query's middle within 1 s), at most `options.pool` from
/// each path, best first. Only tracks `allow` accepts take part. Another recall
/// stage can add its own placements to these before [`rerank`], which gives each
/// the quick check and the best `options.rerank` the full alignment and the score
/// model.
fn retrieve_all(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    options: &MatchOptions,
    allow: &dyn Fn(u32) -> bool,
) -> Vec<Placement> {
    retrieve_traced(index, q, refs, options, allow, None)
}

/// [`retrieve_all`], keeping the coarse votes of both paths (for evaluation).
fn retrieve_traced(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    options: &MatchOptions,
    allow: &dyn Fn(u32) -> bool,
    coarse: Option<&mut Coarse>,
) -> Vec<Placement> {
    retrieve_forced(index, q, refs, options, allow, &HashSet::new(), coarse)
}

/// [`retrieve_traced`] where the tracks in `force` (another recall stage's
/// suggestions) always get their votes: each query window keeps their nearest
/// posting even beyond its neighbour limit, the shortlists keep them, and their
/// placements pass the pool cut. Everything else is retrieved exactly as without.
fn retrieve_forced(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    options: &MatchOptions,
    allow: &dyn Fn(u32) -> bool,
    force: &HashSet<u32>,
    mut coarse: Option<&mut Coarse>,
) -> Vec<Placement> {
    let mut windows = retrieve(index, q, options, allow, force, coarse.as_mut().map(|c| &mut c.windows));
    let notes = retrieve_notes(index, q, refs, options, allow, force, coarse.map(|c| &mut c.notes));
    let mid = q.contour.len() as f32 / 2.0;
    let mut by_track: HashMap<u32, Vec<usize>> = HashMap::new();
    for (i, p) in windows.iter().enumerate() {
        by_track.entry(p.track).or_default().push(i);
    }
    let mut extra = Vec::new();
    for n in notes {
        let same = by_track.get(&n.track).and_then(|v| {
            v.iter()
                .copied()
                .find(|&i| (windows[i].anchor + mid / windows[i].tempo - n.anchor - mid / n.tempo).abs() < 20.0)
        });
        match same {
            Some(i) => windows[i].note_votes = windows[i].note_votes.max(n.note_votes),
            None => extra.push(n),
        }
    }
    // Interleave so neither path crowds out the other in the quick stage.
    let mut out = Vec::with_capacity(windows.len() + extra.len());
    let (mut a, mut b) = (windows.into_iter(), extra.into_iter());
    loop {
        match (a.next(), b.next()) {
            (None, None) => break,
            (x, y) => out.extend(x.into_iter().chain(y)),
        }
    }
    out
}

/// Stage 1b: note n-gram votes per track and step offset, then placements from a
/// Hough vote over (tempo, anchor) of the matched pitch changes and from a
/// rhythm-free alignment of the query's pitch changes with each shortlisted
/// track's. Survives hums that shorten long notes or skip rests.
fn retrieve_notes(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    options: &MatchOptions,
    allow: &dyn Fn(u32) -> bool,
    force: &HashSet<u32>,
    coarse: Option<&mut Vec<(u32, f32)>>,
) -> Vec<Placement> {
    let tracks = index.track_count() as usize;
    if tracks == 0 || q.steps.len() < 4 || index.key_count() == 0 {
        return Vec::new();
    }
    let total = index.step_count().max(1) as f32;
    let looked_up: Vec<(i32, u32, f32)> = ngrams::query_keys(&q.steps)
        .into_iter()
        .filter_map(|(j, key, weight)| {
            let (_, count) = index.note_postings(key)?;
            (count <= MAX_NOTE_POSTINGS).then(|| (j, key, weight * (total / count as f32).ln().max(0.1)))
        })
        .collect();
    // Pass 1: coarse offset histograms per track (2 steps per bucket, wrapping).
    let mut hist = vec![0f32; tracks * NOTE_BUCKETS];
    let mut seen = vec![false; tracks];
    let mut touched: Vec<u32> = Vec::new();
    for &(j, key, w) in &looked_up {
        let Some((postings, _)) = index.note_postings(key) else { continue };
        for (track, at) in postings {
            let t = track as usize;
            if t >= tracks || !allow(track) {
                continue;
            }
            if !seen[t] {
                seen[t] = true;
                touched.push(track);
            }
            let bucket = ((at as i64 - j as i64).div_euclid(2)).rem_euclid(NOTE_BUCKETS as i64) as usize;
            hist[t * NOTE_BUCKETS + bucket] += w;
        }
    }
    let mut pre: Vec<(u32, f32)> = touched
        .iter()
        .map(|&t| {
            let h = &hist[t as usize * NOTE_BUCKETS..(t as usize + 1) * NOTE_BUCKETS];
            let n = NOTE_BUCKETS;
            let best = (0..n).map(|b| h[(b + n - 1) % n] + h[b] + h[(b + 1) % n]).fold(0.0, f32::max);
            (t, best)
        })
        .collect();
    drop(hist);
    record(coarse, &pre);
    let shortlist = options.shortlist.min(NOTE_SHORTLIST);
    if pre.len() > shortlist {
        pre.select_nth_unstable_by(shortlist, |a, b| b.1.total_cmp(&a.1));
        let forced: Vec<(u32, f32)> = pre[shortlist..].iter().copied().filter(|(t, _)| force.contains(t)).collect();
        pre.truncate(shortlist);
        pre.extend(forced);
    }
    let mut keep = vec![false; tracks];
    for &(t, _) in &pre {
        keep[t as usize] = true;
    }
    // Pass 2: exact votes for the shortlisted tracks.
    let mut votes: Vec<(u32, i32, i32, f32)> = Vec::new();
    for &(j, key, w) in &looked_up {
        let Some((postings, _)) = index.note_postings(key) else { continue };
        for (track, at) in postings {
            if keep.get(track as usize).copied().unwrap_or(false) {
                votes.push((track, at as i32 - j, j, w));
            }
        }
    }
    votes.sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)).then(b.3.total_cmp(&a.3)));
    // Each (track, offset, query position) counts once, at its best key variant.
    votes.dedup_by(|later, kept| (later.0, later.1, later.2) == (kept.0, kept.1, kept.2));
    let positions = q.steps.len().saturating_sub(ngrams::INTERVAL_N).max(1) as f32;
    let pre_score: HashMap<u32, f32> = pre.iter().copied().collect();
    let mut out: Vec<(f32, Placement)> = Vec::new();
    let mut points: Vec<Point> = Vec::new();
    let mut acc: Vec<f32> = Vec::new();
    let mut i = 0;
    while i < votes.len() {
        let track = votes[i].0;
        let start = i;
        while i < votes.len() && votes[i].0 == track {
            i += 1;
        }
        let Some(r) = refs.get(track) else { continue };
        points.clear();
        points.extend(votes[start..i].iter().filter_map(|v| {
            let qs = q.steps.get(usize::try_from(v.2).ok()?)?;
            let k = usize::try_from(v.2 + v.1).ok()?;
            let rs = r.steps.get(k)?;
            Some(Point { j: v.2 as u16, k: k as u16, x: qs.onset_ms as f32, y: rs.onset_ms as f32, w: v.3 })
        }));
        if points.len() > MAX_POINTS {
            points.select_nth_unstable_by(MAX_POINTS, |a, b| b.w.total_cmp(&a.w));
            points.truncate(MAX_POINTS);
        }
        let score = pre_score.get(&track).copied().unwrap_or(0.0) / positions;
        for (_, pairs) in hough(&points, &mut acc) {
            if let Some(p) = placement_from_pairs(q, r, track, &pairs, score) {
                out.push((ALIGN_UNKNOWN, p));
            }
        }
    }
    // Placements from aligning the query's pitch changes with each shortlisted track's.
    let mut table = AlignTable::default();
    for &(track, _) in &pre {
        let Some(r) = refs.get(track) else { continue };
        let rs = steps::shape(&r.steps);
        let score = pre_score.get(&track).copied().unwrap_or(0.0) / positions;
        for (cost, pairs) in steps::align_pairs(&q.shape, &rs, &mut table) {
            if let Some(p) = placement_from_pairs(q, r, track, &pairs, score) {
                out.push((cost, p));
            }
        }
    }
    out.sort_by(|a, b| a.0.total_cmp(&b.0).then(b.1.note_votes.total_cmp(&a.1.note_votes)));
    cut_pool(&mut out, options.pool, force, |(_, p)| p.track);
    out.into_iter().map(|(_, p)| p).collect()
}

/// Keep the first `pool` items, plus any later ones of a forced track.
fn cut_pool<T>(items: &mut Vec<T>, pool: usize, force: &HashSet<u32>, track: impl Fn(&T) -> u32) {
    if items.len() <= pool {
        return;
    }
    if force.is_empty() {
        items.truncate(pool);
        return;
    }
    let mut i = 0;
    items.retain(|x| {
        i += 1;
        i <= pool || force.contains(&track(x))
    });
}

/// A placement from matched (query step, reference step) pairs: tempo and anchor
/// by a Theil–Sen fit of reference onsets against query onsets.
fn placement_from_pairs(q: &Query, r: &Reference, track: u32, pairs: &[(u16, u16)], score: f32) -> Option<Placement> {
    let pts: Vec<(f32, f32)> = pairs
        .iter()
        .filter_map(|&(j, k)| {
            Some((q.steps.get(j as usize)?.onset_ms as f32, r.steps.get(k as usize)?.onset_ms as f32))
        })
        .collect();
    let (&(x0, y0), _) = pts.split_first()?;
    // Reference ms per query ms; 1 when too few pairs to fit.
    let (slope, intercept) = fit_line(&pts).unwrap_or((1.0, y0 - x0));
    let tempo = (1.0 / slope).clamp(TEMPO_MIN * 0.7, TEMPO_MAX * 1.5);
    Some(Placement { track, tempo, anchor: intercept / CONTOUR_MS as f32, votes: 0.0, note_votes: score })
}

/// Theil–Sen fit y ≈ a + b·x over points at least 300 ms apart: (b, a).
fn fit_line(pts: &[(f32, f32)]) -> Option<(f32, f32)> {
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
    let slope = median(&mut slopes)?.clamp(0.35, 3.0);
    let mut a: Vec<f32> = pts.iter().map(|p| p.1 - slope * p.0).collect();
    Some((slope, median(&mut a)?))
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
    let mut best: Vec<(f32, f32, usize)> = Vec::with_capacity(HOUGH_TEMPOS);
    for t in 0..HOUGH_TEMPOS {
        let tempo = HOUGH_TEMPO_MIN * (HOUGH_TEMPO_MAX / HOUGH_TEMPO_MIN).powf(t as f32 / (HOUGH_TEMPOS - 1) as f32);
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
        // A second alignment must be elsewhere in the track.
        if lines.iter().any(|&(a2, t2)| (a2 + t2 * x_max * 0.5 - a - tempo * x_max * 0.5).abs() < 2000.0) {
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
        out.push((votes, pairs.into_iter().take(64).map(|p| (p.0, p.1)).collect()));
    }
    out
}

/// One vote: (track, tempo index, implied anchor frame, weight).
type Vote = (u32, u8, f32, f32);

/// Stage 1: probe the inverted multi-index and pool alignment votes.
fn retrieve(
    index: &MelodyIndex,
    q: &Query,
    options: &MatchOptions,
    allow: &dyn Fn(u32) -> bool,
    force: &HashSet<u32>,
    coarse: Option<&mut Vec<(u32, f32)>>,
) -> Vec<Placement> {
    let tracks = index.track_count() as usize;
    if tracks == 0 || q.windows.is_empty() {
        return Vec::new();
    }
    let tempos = tempo_grid();
    let books = index.codebooks();
    let avg = (index.window_count() as f32 / index::CELLS as f32).max(1.0);
    let mut hist = vec![0f32; tracks * BUCKETS];
    let mut seen = vec![false; tracks];
    let mut touched: Vec<u32> = Vec::new();
    let mut votes: Vec<Vote> = Vec::new();
    let mut probed: Vec<(usize, f32)> = Vec::with_capacity(options.probe);
    let residuals = index.residuals();
    let mut table = [[0f32; CODES]; contour::SUBS];
    // (track, anchor, distance, rarity) of one query window's nearest posting per track.
    let mut found: Vec<(u32, f32, f32, f32)> = Vec::new();
    let mut slot = vec![0u32; tracks];
    let mut stamp = vec![u32::MAX; tracks];
    // Forced tracks' postings beyond one window's neighbour limit.
    let mut extra: Vec<(u32, f32, f32, f32)> = Vec::new();
    let mut forced = vec![false; if force.is_empty() { 0 } else { tracks }];
    for &t in force {
        if let Some(f) = forced.get_mut(t as usize) {
            *f = true;
        }
    }
    for (wi, &(ti, start, ref vec)) in q.windows.iter().enumerate() {
        probe_cells(index, books, vec, options.probe, &mut probed);
        if probed.is_empty() {
            continue;
        }
        let tempo = tempos[ti];
        found.clear();
        for &(cell, _) in &probed {
            let count = index.cell_len(cell);
            // Crowded cells (flat or common shapes) say less about a track.
            let rarity = (4.0 * avg / count.max(1) as f32).min(1.0).sqrt();
            // The query's residual from this cell's centroids.
            let (ca, cb) = (cell / CODES, cell % CODES);
            let mut target = *vec;
            for (k, x) in target.iter_mut().enumerate() {
                *x -= if k < HALF { books[0][ca][k] } else { books[1][cb][k - HALF] };
            }
            let use_table = count >= TABLE_MIN_POSTINGS;
            if use_table {
                for (s, row) in table.iter_mut().enumerate() {
                    let t = &target[s * contour::SUB..(s + 1) * contour::SUB];
                    for (entry, c) in row.iter_mut().zip(&residuals[s]) {
                        *entry = contour::dist2(t, c);
                    }
                }
            }
            for (track, ref_start, code) in index.cell(cell) {
                let t = track as usize;
                if t >= tracks || !allow(track) {
                    continue;
                }
                let d: f32 = if use_table {
                    (0..contour::SUBS).map(|s| table[s][code[s] as usize]).sum()
                } else {
                    (0..contour::SUBS)
                        .map(|s| {
                            let part = &target[s * contour::SUB..(s + 1) * contour::SUB];
                            contour::dist2(part, &residuals[s][code[s] as usize])
                        })
                        .sum()
                };
                let anchor = ref_start as f32 - start / tempo;
                if stamp[t] == wi as u32 {
                    let f = &mut found[slot[t] as usize];
                    if d < f.2 {
                        *f = (track, anchor, d, rarity);
                    }
                } else {
                    stamp[t] = wi as u32;
                    slot[t] = found.len() as u32;
                    found.push((track, anchor, d, rarity));
                }
            }
        }
        extra.clear();
        if found.len() > WINDOW_NEIGHBOURS {
            found.select_nth_unstable_by(WINDOW_NEIGHBOURS, |a, b| a.2.total_cmp(&b.2));
            if !force.is_empty() {
                extra.extend(found[WINDOW_NEIGHBOURS..].iter().copied().filter(|f| forced[f.0 as usize]));
            }
            found.truncate(WINDOW_NEIGHBOURS);
        }
        let mut d: Vec<f32> = found.iter().map(|f| f.2).collect();
        let sigma = median(&mut d).unwrap_or(1.0).max(1.0);
        for &(track, anchor, dist, rarity) in found.iter().chain(&extra) {
            let t = track as usize;
            let w = (-dist / sigma).exp() * rarity;
            let bucket = ((anchor / BUCKET_FRAMES).floor() as i64).rem_euclid(BUCKETS as i64) as usize;
            if !seen[t] {
                seen[t] = true;
                touched.push(track);
            }
            hist[t * BUCKETS + bucket] += w;
            votes.push((track, ti as u8, anchor, w));
        }
    }
    // Shortlist tracks by their best alignment bucket (with neighbours).
    let mut pre: Vec<(u32, f32)> = touched
        .iter()
        .map(|&t| {
            let h = &hist[t as usize * BUCKETS..(t as usize + 1) * BUCKETS];
            let best =
                (0..BUCKETS).map(|b| h[(b + BUCKETS - 1) % BUCKETS] + h[b] + h[(b + 1) % BUCKETS]).fold(0.0, f32::max);
            (t, best)
        })
        .collect();
    drop(hist);
    record(coarse, &pre);
    if pre.len() > options.shortlist {
        pre.select_nth_unstable_by(options.shortlist, |a, b| b.1.total_cmp(&a.1));
        pre.truncate(options.shortlist);
    }
    let mut keep = vec![false; tracks];
    for &(t, _) in &pre {
        keep[t as usize] = true;
    }
    for &t in force {
        if let Some(k) = keep.get_mut(t as usize) {
            *k = true;
        }
    }
    votes.retain(|v| keep[v.0 as usize]);
    let mut placements = pool_votes(votes, q.windows.len());
    placements.sort_by(|a, b| b.votes.total_cmp(&a.votes));
    cut_pool(&mut placements, options.pool, force, |p| p.track);
    placements
}

/// The `probe` nearest non-empty cells of a window vector: (cell, squared distance
/// to the cell's code pair).
fn probe_cells(
    index: &MelodyIndex,
    books: &[[[f32; HALF]; CODES]; 2],
    v: &Vector,
    probe: usize,
    out: &mut Vec<(usize, f32)>,
) {
    out.clear();
    let (a, b) = contour::halves(v);
    let nearest = |half: &[f32; HALF], book: &[[f32; HALF]; CODES]| {
        let mut d: Vec<(usize, f32)> = book.iter().enumerate().map(|(i, c)| (i, contour::dist2(half, c))).collect();
        d.select_nth_unstable_by(PROBE_CODES, |x, y| x.1.total_cmp(&y.1));
        d.truncate(PROBE_CODES);
        d
    };
    let da = nearest(&a, &books[0]);
    let db = nearest(&b, &books[1]);
    let mut pairs: Vec<(usize, f32)> =
        da.iter().flat_map(|&(ca, xa)| db.iter().map(move |&(cb, xb)| (ca * CODES + cb, xa + xb))).collect();
    pairs.sort_by(|x, y| x.1.total_cmp(&y.1));
    for (cell, d) in pairs {
        if out.len() >= probe {
            break;
        }
        if index.cell_len(cell) > 0 {
            out.push((cell, d));
        }
    }
}

/// Pool votes per (track, tempo, alignment bin) and keep each track's best
/// placements; neighbouring bins and tempos count half.
fn pool_votes(mut votes: Vec<Vote>, windows: usize) -> Vec<Placement> {
    let tempos = tempo_grid();
    let bin = |a: f32| (a / BIN_FRAMES).floor() as i64;
    votes.sort_unstable_by(|x, y| (x.0, x.1).cmp(&(y.0, y.1)).then(bin(x.2).cmp(&bin(y.2))));
    // Votes per query window at one tempo (each window votes once per track).
    let norm = (windows as f32 / TEMPOS as f32).max(1.0);
    let mut out = Vec::new();
    let mut i = 0;
    // Per track: cells (tempo, bin) -> (weight, weighted anchor sum).
    let mut cells: HashMap<(u8, i64), (f32, f32)> = HashMap::new();
    while i < votes.len() {
        let track = votes[i].0;
        cells.clear();
        while i < votes.len() && votes[i].0 == track {
            let (_, ti, anchor, w) = votes[i];
            let e = cells.entry((ti, bin(anchor))).or_insert((0.0, 0.0));
            e.0 += w;
            e.1 += w * anchor;
            i += 1;
        }
        let at = |k: (u8, i64)| cells.get(&k).map(|c| c.0).unwrap_or(0.0);
        let mut scored: Vec<(f32, (u8, i64))> = cells
            .keys()
            .map(|&(ti, b)| {
                let mut s = at((ti, b)) + 0.5 * (at((ti, b - 1)) + at((ti, b + 1)));
                if ti > 0 {
                    s += 0.5 * at((ti - 1, b));
                }
                s += 0.5 * at((ti + 1, b));
                (s, (ti, b))
            })
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        let mut kept: Vec<(u8, i64)> = Vec::with_capacity(PLACEMENTS_PER_TRACK);
        for (s, (ti, b)) in scored {
            if kept.len() >= PLACEMENTS_PER_TRACK {
                break;
            }
            if kept.iter().any(|&(t2, b2)| (b2 - b).abs() <= 4 && (t2 as i32 - ti as i32).abs() <= 2) {
                continue;
            }
            kept.push((ti, b));
            let (w, sum) = cells[&(ti, b)];
            out.push(Placement {
                track,
                tempo: tempos[ti as usize],
                anchor: if w > 0.0 { sum / w } else { b as f32 * BIN_FRAMES },
                votes: s / norm,
                note_votes: 0.0,
            });
        }
    }
    out
}

/// Placements of the query on one track by exact window distances (no index).
fn exhaustive_placements(q: &Query, track: u32, frames: &[f32]) -> Vec<Placement> {
    let Some(filled) = Filled::new(frames) else { return Vec::new() };
    let mut refs: Vec<(f32, Vector)> = Vec::new();
    let mut start = 0;
    while start + WINDOW <= filled.len() {
        if let Some(v) = filled.window(start as f32, WINDOW as f32, contour::REF_MIN_VOICED) {
            refs.push((start as f32, v));
        }
        start += QUERY_HOP;
    }
    if refs.is_empty() {
        return Vec::new();
    }
    let tempos = tempo_grid();
    let mut votes: Vec<Vote> = Vec::new();
    let mut d: Vec<(f32, f32)> = Vec::with_capacity(refs.len());
    for &(ti, qstart, ref qv) in &q.windows {
        d.clear();
        d.extend(refs.iter().map(|(s, rv)| (contour::dist2(qv, rv), *s)));
        let keep = 8.min(d.len());
        d.select_nth_unstable_by(keep - 1, |a, b| a.0.total_cmp(&b.0));
        let mut ds: Vec<f32> = d[..keep].iter().map(|x| x.0).collect();
        let sigma = median(&mut ds).unwrap_or(1.0).max(1.0);
        for &(dist, rs) in &d[..keep] {
            votes.push((track, ti as u8, rs - qstart / tempos[ti], (-dist / sigma).exp()));
        }
    }
    let mut out = pool_votes(votes, q.windows.len());
    // These votes come from an exhaustive comparison, not the index; they carry no
    // evidence on the scale the score model expects.
    for p in &mut out {
        p.votes = 0.0;
    }
    out
}

/// A reference track, decoded on first use.
struct Reference {
    song: u32,
    /// Voiced frames.
    voiced: u32,
    frames: Vec<f32>,
    steps: Vec<Step>,
}

impl Reference {
    /// Shape of the reference's pitch changes with onsets in `[from, to)` ms.
    fn shape_between(&self, from: f32, to: f32) -> Vec<(f32, f32)> {
        let a = self.steps.partition_point(|s| (s.onset_ms as f32) < from);
        let b = self.steps.partition_point(|s| (s.onset_ms as f32) < to);
        steps::shape(&self.steps[a..b.max(a)])
    }

    /// Rhythm-free alignment cost of the query's pitch changes around a placement.
    fn align_cost(&self, q: &Query, tempo: f32, anchor_ms: f32, rows: &mut AlignRows) -> f32 {
        let span = q.duration_ms() / tempo;
        let rs = self.shape_between(anchor_ms - 1500.0 - 0.25 * span, anchor_ms + 1.25 * span + 1500.0);
        steps::align(&q.shape, &rs, rows).unwrap_or(ALIGN_UNKNOWN)
    }

    /// Pitch at `t` ms (NaN in rests and outside the track).
    fn at(&self, t: f32) -> f32 {
        if t.is_nan() || t < 0.0 {
            return f32::NAN;
        }
        self.frames.get((t / CONTOUR_MS as f32) as usize).copied().unwrap_or(f32::NAN)
    }
}

/// Tracks decoded for one search.
struct References<'a> {
    index: &'a MelodyIndex,
    loaded: HashMap<u32, Option<Reference>>,
    /// Song → work key.
    works: HashMap<u32, u64>,
}

impl<'a> References<'a> {
    fn new(index: &'a MelodyIndex) -> Self {
        Self { index, loaded: HashMap::new(), works: HashMap::new() }
    }

    fn work(&mut self, song: u32) -> u64 {
        let index = self.index;
        *self.works.entry(song).or_insert_with(|| work_key(index, song))
    }

    fn get(&mut self, track: u32) -> Option<&Reference> {
        let index = self.index;
        self.loaded
            .entry(track)
            .or_insert_with(|| {
                index.track(track).map(|t| Reference {
                    song: t.song,
                    voiced: t.contour.iter().filter(|v| v.is_finite()).count() as u32,
                    steps: steps::frame_steps(&t.contour),
                    frames: t.contour,
                })
            })
            .as_ref()
    }
}

#[derive(Debug, Clone)]
struct Scored {
    song: u32,
    /// The song's work (see [`work_key`]).
    work: u64,
    track: u32,
    ref_frames: u32,
    features: Features,
    score: f32,
    key: f32,
    /// Query duration over reference duration.
    tempo: f32,
    /// Reference time of query time 0.
    anchor_ms: f32,
    /// (query voiced index, reference time in ms) from the DTW path.
    path: TimePath,
}

/// Stages 2–5 for a set of placements (from [`retrieve_all`] and any other recall
/// stage): the quick check ranks them all, the best `options.rerank` get linear
/// scaling, DTW and the score model; best first, several per song possible.
fn rerank(q: &Query, refs: &mut References, placements: Vec<Placement>, options: &MatchOptions) -> Vec<Scored> {
    let quick = quick_stage(q, refs, placements, options.rerank);
    full_stage(q, refs, quick)
}

/// Quick linear scaling of every placement on a sparse sample; the best `keep`
/// go on (best first), each with its quick (key, tempo, anchor ms).
fn quick_stage(q: &Query, refs: &mut References, placements: Vec<Placement>, keep: usize) -> Vec<(f32, Placement)> {
    let index = refs.index;
    let sample: Vec<usize> = q.voiced.iter().copied().step_by(4).collect();
    let coarse_q: Vec<f32> = q.voiced.iter().step_by(2).map(|&i| q.contour[i]).collect();
    let coarse_t: Vec<f32> = q.voiced.iter().step_by(2).map(|&i| i as f32 * CONTOUR_MS as f32).collect();
    let mut rows = AlignRows::default();
    let mut buf = LinearBuf::default();
    let mut out: Vec<(f32, Placement)> = Vec::with_capacity(placements.len());
    for p in placements {
        let Some(r) = refs.get(p.track) else { continue };
        let mut best: Option<(f32, f32, f32, f32)> = None;
        for f in QUICK_FACTORS {
            for s in QUICK_SHIFTS {
                let tempo = p.tempo * f;
                let anchor_ms = rescaled_anchor(q, p.anchor, p.tempo, tempo, s) * CONTOUR_MS as f32;
                let (cost, key) = linear_cost(q, r, &sample, tempo, anchor_ms, &mut buf);
                if best.is_none_or(|b| cost < b.0) {
                    best = Some((cost, key, tempo, anchor_ms));
                }
            }
        }
        if let Some((cost, key, tempo, anchor_ms)) = best {
            let dtw = coarse_dtw(&coarse_q, &coarse_t, q.duration_ms(), r, key, tempo, anchor_ms).unwrap_or(cost + 1.0);
            let align = r.align_cost(q, tempo, anchor_ms, &mut rows);
            // The score model's utility on what the quick check knows (coverage and
            // the edit distance only come with the full alignment).
            let quick = Features {
                votes: p.votes,
                linear: cost,
                dtw,
                coverage: 0.0,
                tempo: tempo.log2().abs(),
                align,
                edit: 0.0,
                note_votes: p.note_votes,
                reference: reference_feature(r.voiced),
                prior: prior_feature(index.popularity(r.song)),
                enc_best: q.encoder_features(r.song).0,
                enc_hit: q.encoder_features(r.song).1,
            };
            let rank = -q.utility(&quick);
            out.push((rank, Placement { tempo, anchor: anchor_ms / CONTOUR_MS as f32, ..p }));
        }
    }
    out.sort_by(|a, b| a.0.total_cmp(&b.0));
    out.truncate(keep);
    out
}

/// Full linear scaling, DTW and fusion; best first.
fn full_stage(q: &Query, refs: &mut References, quick: Vec<(f32, Placement)>) -> Vec<Scored> {
    let index = refs.index;
    let mut scored: Vec<Scored> = Vec::with_capacity(quick.len());
    let mut rows = AlignRows::default();
    for (_, p) in quick {
        let Some(r) = refs.get(p.track) else { continue };
        let Some((linear, key, tempo, anchor_ms, coverage)) = linear_scaling(q, r, p.tempo, p.anchor) else {
            continue;
        };
        let Some((dtw, key, path)) = dtw_refine(q, r, key, tempo, anchor_ms) else {
            continue;
        };
        let align = r.align_cost(q, tempo, anchor_ms, &mut rows);
        let edit = match (path.first(), path.last()) {
            (Some(&(_, a)), Some(&(_, b))) => steps::edit_distance(&q.shape, &r.shape_between(a - 500.0, b + 500.0)),
            _ => 1.0,
        };
        scored.push(Scored {
            song: r.song,
            work: 0,
            track: p.track,
            ref_frames: r.voiced,
            features: Features {
                votes: p.votes,
                linear,
                dtw,
                coverage,
                tempo: tempo.log2().abs(),
                align,
                edit,
                note_votes: p.note_votes,
                reference: reference_feature(r.voiced),
                prior: prior_feature(index.popularity(r.song)),
                enc_best: q.encoder_features(r.song).0,
                enc_hit: q.encoder_features(r.song).1,
            },
            score: 0.0,
            key,
            tempo,
            anchor_ms,
            path,
        });
    }
    for s in &mut scored {
        s.work = refs.work(s.song);
    }
    score_all(&mut scored, q);
    scored.sort_by(|a, b| b.score.total_cmp(&a.score));
    scored
}

/// The anchor (reference frame of query frame 0) that keeps the query's middle in
/// place when the tempo changes from `from` to `to`, plus `shift` frames.
fn rescaled_anchor(q: &Query, anchor: f32, from: f32, to: f32, shift: f32) -> f32 {
    let mid = q.contour.len() as f32 / 2.0;
    anchor + mid / from - mid / to + shift
}

/// Scratch buffers of [`linear_cost`], reused across calls.
#[derive(Default)]
struct LinearBuf {
    refs: Vec<f32>,
    diffs: Vec<f32>,
}

/// Mean clipped distance of `sample` query frames to the reference placed at
/// (tempo, anchor), at the sample's median difference; returns (cost, key).
fn linear_cost(
    q: &Query,
    r: &Reference,
    sample: &[usize],
    tempo: f32,
    anchor_ms: f32,
    buf: &mut LinearBuf,
) -> (f32, f32) {
    let frame = CONTOUR_MS as f32;
    let LinearBuf { refs, diffs } = buf;
    refs.clear();
    refs.extend(sample.iter().map(|&i| r.at(anchor_ms + i as f32 * frame / tempo)));
    diffs.clear();
    diffs.extend(sample.iter().zip(refs.iter()).filter(|(_, v)| v.is_finite()).map(|(&i, v)| q.contour[i] - v));
    let key = median(diffs).unwrap_or(0.0);
    let cost: f32 = sample
        .iter()
        .zip(refs.iter())
        .map(|(&i, &v)| if v.is_finite() { (q.contour[i] - v - key).abs().min(LS_CLIP) } else { MISS_COST })
        .sum();
    (cost / sample.len().max(1) as f32, key)
}

/// Best (distance, key, tempo, anchor ms, coverage) over tempo factors and shifts
/// around a placement, refined over all voiced frames.
fn linear_scaling(q: &Query, r: &Reference, tempo: f32, anchor: f32) -> Option<(f32, f32, f32, f32, f32)> {
    let sample: Vec<usize> = q.voiced.iter().copied().step_by(2).collect();
    let mut buf = LinearBuf::default();
    let mut best: Option<(f32, f32, f32)> = None;
    for f in FACTORS {
        for s in SHIFTS {
            let t = tempo * f;
            let a = rescaled_anchor(q, anchor, tempo, t, s) * CONTOUR_MS as f32;
            let (cost, _) = linear_cost(q, r, &sample, t, a, &mut buf);
            if best.is_none_or(|b| cost < b.0) {
                best = Some((cost, t, a));
            }
        }
    }
    let (_, tempo, anchor_ms) = best?;
    let (cost, key) = linear_cost(q, r, &q.voiced, tempo, anchor_ms, &mut buf);
    let frame = CONTOUR_MS as f32;
    let met = q.voiced.iter().filter(|&&i| r.at(anchor_ms + i as f32 * frame / tempo).is_finite()).count();
    Some((cost, key, tempo, anchor_ms, met as f32 / q.voiced.len().max(1) as f32))
}

/// One DTW pass at 10 fps for thinning the pool. The reference is sampled at the
/// placement's tempo so the slope limits apply around it.
fn coarse_dtw(qv: &[f32], qt: &[f32], span: f32, r: &Reference, key: f32, tempo: f32, anchor_ms: f32) -> Option<f32> {
    let step = 2.0 * CONTOUR_MS as f32 / tempo;
    let from = (anchor_ms - 1000.0).max(0.0);
    let (times, rv) = ref_samples(r, from, anchor_ms + 1.5 * span / tempo + 500.0, step);
    if rv.len() < 4 || qv.is_empty() {
        return None;
    }
    let expected: Vec<usize> = qt.iter().map(|&t| times.partition_point(|&x| x < anchor_ms + t / tempo)).collect();
    let band = ((qv.len() as f32 * 0.25) as usize).max(6);
    dtw(qv, &rv, &expected, band, key).map(|(d, _)| d)
}

/// Voiced reference samples every `step` ms over `[from, to)`: (times, pitches).
fn ref_samples(r: &Reference, from: f32, to: f32, step: f32) -> (Vec<f32>, Vec<f32>) {
    let n = ((to - from) / step).ceil().clamp(0.0, 20_000.0) as usize;
    (0..n).map(|k| from + k as f32 * step).map(|t| (t, r.at(t))).filter(|(_, v)| v.is_finite()).unzip()
}

/// Subsequence DTW between the query's voiced frames and the reference's voiced
/// frames near the anchor, with key refinement. The reference is sampled at the
/// estimated tempo, so slopes ½–2 are relative to it. Returns (distance, key,
/// path of (query voiced index, reference time in ms)).
fn dtw_refine(q: &Query, r: &Reference, key: f32, tempo: f32, anchor_ms: f32) -> Option<(f32, f32, TimePath)> {
    let frame = CONTOUR_MS as f32;
    let span = q.duration_ms() / tempo;
    let step = frame / tempo;
    let (times, rv) = ref_samples(r, (anchor_ms - 2000.0).max(0.0), anchor_ms + 1.8 * span + 1000.0, step);
    if rv.len() < 4 {
        return None;
    }
    let qv: Vec<f32> = q.voiced.iter().map(|&i| q.contour[i]).collect();
    // Expected reference index of each query frame along the linear path.
    let expected: Vec<usize> =
        q.voiced.iter().map(|&i| times.partition_point(|&t| t < anchor_ms + i as f32 * frame / tempo)).collect();
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
fn dtw(qv: &[f32], rv: &[f32], expected: &[usize], band: usize, key: f32) -> Option<(f32, Vec<(usize, usize)>)> {
    let n = qv.len();
    let m = rv.len();
    if n == 0 || m == 0 || expected.len() != n {
        return None;
    }
    let width = 2 * band + 1;
    let lo: Vec<usize> = expected.iter().map(|e| e.saturating_sub(band)).collect();
    let mut d = vec![f32::INFINITY; n * width];
    let mut from = vec![0u8; n * width];
    // Row i covers reference indices lo[i]..lo[i] + width; outside it, infinity.
    let at =
        |row: &[f32], k: isize| -> f32 { if k < 0 || k >= width as isize { f32::INFINITY } else { row[k as usize] } };
    let l = lo[0];
    let q = qv[0] - key;
    for w in 0..width.min(m.saturating_sub(l)) {
        d[w] = (q - rv[l + w]).abs().min(DTW_CLIP);
    }
    for i in 1..n {
        let l = lo[i];
        let q = qv[i] - key;
        let q_prev = qv[i - 1] - key;
        let end = width.min(m.saturating_sub(l));
        let (done, rest) = d.split_at_mut(i * width);
        let row = &mut rest[..width];
        let moves = &mut from[i * width..(i + 1) * width];
        let prev = &done[(i - 1) * width..];
        // Offsets of this row's reference indices in the rows above.
        let shift = l as isize - lo[i - 1] as isize;
        let (prev2, shift2) = if i >= 2 {
            (&done[(i - 2) * width..(i - 1) * width], l as isize - lo[i - 2] as isize)
        } else {
            (&done[..0], 0)
        };
        for w in 0..end {
            let r = rv[l + w];
            let c = (q - r).abs().min(DTW_CLIP);
            let k = w as isize;
            let diag = at(prev, k + shift - 1);
            let skip_r = at(prev, k + shift - 2);
            let skip_q =
                if i >= 2 { at(prev2, k + shift2 - 1) + (q_prev - r).abs().min(DTW_CLIP) } else { f32::INFINITY };
            let (mut best, mut f) = (diag, 1u8);
            if skip_r < best {
                best = skip_r;
                f = 2;
            }
            if skip_q < best {
                best = skip_q;
                f = 3;
            }
            row[w] = best + c;
            moves[w] = f;
        }
    }
    let l = lo[n - 1];
    let last = &d[(n - 1) * width..];
    let (mut j, total) =
        (l..(l + width).min(m)).map(|j| (j, at(last, j as isize - l as isize))).min_by(|a, b| a.1.total_cmp(&b.1))?;
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

/// [`Features::reference`] of a reference with this many voiced frames.
fn reference_feature(voiced: u32) -> f32 {
    (voiced.max(20) as f32 / 600.0).ln()
}

/// [`Features::prior`] of a song with this popularity.
fn prior_feature(popularity: f32) -> f32 {
    ((popularity + 0.05) / 0.55).ln()
}

/// The model's logit for one candidate, before any encoder terms
/// ([`Query::utility`] adds them).
fn utility(f: &Features) -> f32 {
    let x = [
        f.votes.min(4.0),
        f.linear.min(3.0),
        f.dtw.min(4.0),
        f.coverage,
        f.tempo.min(2.0),
        f.align.min(2.0),
        f.edit.min(2.0),
        f.note_votes.min(6.0),
        f.reference,
        f.prior,
    ];
    x.iter().zip(MODEL_WEIGHTS).map(|(x, w)| x * w).sum()
}

/// Calibrated P(correct) for competing candidates: a softmax over them and a
/// "none of these" option, then Platt scaling.
fn score_all(scored: &mut [Scored], q: &Query) {
    let (none, calibration) = (q.none, q.calibration());
    let z: Vec<f32> = scored.iter().map(|s| q.utility(&s.features)).collect();
    let top = z.iter().copied().fold(none, f32::max);
    // Songs compete, not placements: each song enters with its best placement
    // (summed in first-seen order, so equal inputs give equal scores).
    let mut slot: HashMap<u32, usize> = HashMap::with_capacity(scored.len());
    let mut best: Vec<f32> = Vec::with_capacity(scored.len());
    for (s, &z) in scored.iter().zip(&z) {
        let i = *slot.entry(s.song).or_insert_with(|| {
            best.push(f32::NEG_INFINITY);
            best.len() - 1
        });
        best[i] = best[i].max(z);
    }
    let total = (none - top).exp() + best.iter().map(|z| (z - top).exp()).sum::<f32>();
    for (s, z) in scored.iter_mut().zip(z) {
        let p = (z - top).exp() / total;
        s.score = 1.0 / (1.0 + (-(calibration.0 + calibration.1 * logit(p))).exp());
    }
}

/// Songs of one work whose match is within this many logits of the work's best
/// may be shown in its place.
const WORK_MARGIN: f32 = 1.0;

/// One answer per work (see [`work_key`]), best first. `songs` holds each song's
/// best logit, best first; returns, per work, the position in `songs` of the song
/// to show and the work's best logit. The shown song is the most popular of the
/// work's songs that match nearly as well as its best: a hum cannot tell versions
/// apart, and the original is the likelier answer than a cover that happens to
/// match a little better.
pub fn group_works(index: &MelodyIndex, songs: &[(u32, f32)]) -> Vec<(usize, f32)> {
    let mut works: Vec<(u64, f32, usize)> = Vec::new();
    for (i, &(song, z)) in songs.iter().enumerate() {
        let work = work_key(index, song);
        match works.iter_mut().find(|w| w.0 == work) {
            Some(w) => {
                if z >= w.1 - WORK_MARGIN && index.popularity(song) > index.popularity(songs[w.2].0) {
                    w.2 = i;
                }
            }
            None => works.push((work, z, i)),
        }
    }
    works.into_iter().map(|(_, z, i)| (i, z)).collect()
}

/// Each song's best candidate with the popularity prior applied, one match per
/// work (see [`group_works`]).
fn best_per_song(
    index: &MelodyIndex,
    q: &Query,
    refs: &mut References,
    ranked: Vec<Scored>,
    k: usize,
    popularity_weight: f32,
) -> Vec<MelodyMatch> {
    let mut seen = HashSet::new();
    let mut songs: Vec<(f32, Scored, SongMeta)> = Vec::new();
    for s in ranked {
        if !seen.insert(s.song) {
            continue;
        }
        let Some(meta) = index.song(s.song) else {
            continue;
        };
        let z = logit(s.score) + popularity_weight * ((meta.popularity + 0.05) / 0.55).ln();
        songs.push((z, s, meta));
    }
    songs.sort_by(|a, b| b.0.total_cmp(&a.0));
    let order: Vec<(u32, f32)> = songs.iter().map(|(z, s, _)| (s.song, *z)).collect();
    let mut works = group_works(index, &order);
    works.truncate(k);
    works
        .into_iter()
        .map(|(i, z)| {
            let (_, s, song) = &songs[i];
            let alignment = alignment(q, refs.get(s.track), s);
            MelodyMatch {
                song: song.clone(),
                score: 1.0 / (1.0 + (-z).exp()),
                key_shift: s.key,
                tempo: 1.0 / s.tempo,
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
                None => s.anchor_ms + f as f32 * CONTOUR_MS as f32 / s.tempo,
            };
            *out = r.at(t) + s.key - q.median;
        }
    }
    MelodyAlignment { frame_ms: CONTOUR_MS, query, reference }
}

const _: () = assert!(DIMS == 2 * HALF);

#[cfg(test)]
mod tests;
