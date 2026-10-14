//! Long media: DJ mixes, radio shows, compilations. See docs/PIPELINES.md ("Mix strategy").
//!
//! 1. Segmenting: 1 s features → silence/speech/music labels and a novelty
//!    curve whose peaks are candidate change points.
//! 2. Hinted pass: each tracklist entry is looked up in the catalog (keeping
//!    its version) and verified against its preview by local fingerprint
//!    matching; only entries that do not verify cost one recognizer probe.
//! 3. Adaptive probing for everything still unexplained, in time order: a
//!    match at window start T with offset o and skew s puts the recording's
//!    start at T − o/(1+s) and its latest end at that plus D/(1+s); the next
//!    probe jumps ahead accordingly, and gaps that stay unexplained get probes
//!    in their middle until none qualify or the budget is spent. Tracks already
//!    identified are first looked for locally (replays cost nothing).
//! 4. Unresolved music: another probe elsewhere, then paid providers.
//! 5. Stitch, grade and emit.

pub mod segment;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use self::segment::Label;
use crate::audio::PcmFile;
use crate::audio::features;
use crate::error::{MfError, Result};
use crate::fingerprint::{self, FRAME_MS, Index, MatchOptions, Peak};
use crate::model::{Evidence, EvidenceSource, Phase, Segment, SegmentKind, Track};
use crate::pipeline::analyze::services::Services;
use crate::pipeline::emitter::Emitter;
use crate::pipeline::hints::{self, Tracklist};
use crate::pipeline::recognize::{RecognitionHit, TIGHT_SKEW};
use crate::pipeline::stitch::{self, Piece, UNVERIFIED_HINT, VERIFIED_HINT};
use crate::shazam::Priority;

/// Length of one recognizer probe.
pub const PROBE_MS: u64 = 12_000;
/// Probe starts snap to this grid, so repeated analyses of the same media hit
/// the persistent cache.
const GRID_MS: u64 = 2_000;
/// Hinted pass: how far around an entry the preview is looked for.
const HINT_MARGIN_MS: u64 = 30_000;
/// Longest jump after a match.
const MAX_STEP_MS: u64 = 150_000;
/// A recording's latest end this close is jumped to directly.
const MAX_JUMP_TO_END_MS: u64 = 240_000;
/// Shortest jump after a match.
const MIN_STEP_MS: u64 = 30_000;
/// Jump after a match whose duration is unknown.
const UNKNOWN_STEP_MS: u64 = 90_000;
/// A probe this far past a recording's latest end finds what follows it.
const END_MARGIN_MS: i64 = 10_000;
/// Jump after a probe without a match.
const MISS_STEP_MS: u64 = 25_000;
/// An unexplained stretch at least this long between two tracks gets a probe.
const GAP_PROBE_MS: u64 = 30_000;
/// Two matches further apart than this get a probe in between.
const MAX_UNPROBED_MS: u64 = 240_000;
/// An unseen stretch this long before a recording begins may hide a short track.
const HIDDEN_TRACK_MS: u64 = 60_000;
/// That check listens this long before the recording's start.
const BEFORE_START_MS: u64 = 20_000;
/// Entries looked at to decide that a tracklist stopped matching the audio...
const UNRELIABLE_WINDOW: usize = 6;
/// ...and how many of them the audio must have overruled.
const UNRELIABLE_CONTRADICTIONS: usize = 3;
/// Largest DJ speed change taken at face value.
const MAX_DJ_SKEW: f32 = 0.085;
/// How far speed and pitch (or pitch and zero) may differ in one answer.
const SKEW_TOLERANCE: f32 = 0.0015;
/// An end and a start this close describe a hard cut.
const HARD_CUT_MS: i64 = 8_000;
/// Transitions longer than this get a refining probe in their middle...
const REFINE_MIN_MS: u64 = 40_000;
/// ...while the whole analysis stays under this many probes per hour of media.
const REFINE_RATE_PER_HOUR: f64 = 60.0;
/// A replay must start no earlier than this before the first play's latest end.
const REPLAY_SLACK_MS: i64 = 30_000;
/// A lone answer whose own start lies this long before another established
/// recording was heard is dropped (DJ transitions are shorter).
const OVERLAP_SLACK_MS: i64 = 60_000;
/// A confirmation probe goes at most this far from the first answer...
const CONFIRM_REACH_MS: u64 = 40_000;
/// ...and needs this much room on that side.
const CONFIRM_ROOM_MS: u64 = 20_000;
/// A leading or trailing stretch shorter than this joins the first/last track.
const EDGE_ABSORB_MS: u64 = 30_000;
/// Paid provider calls per analysis.
const MAX_PAID: u32 = 30;
/// Largest DJ speed change searched by local matching (pitch faders go to ±8 %).
const MAX_SPEED_CHANGE: f32 = 0.08;
/// Speed step of that search.
const SPEED_STEP: f32 = 0.005;
/// Mix peaks are computed in chunks of this length (whole peak frames).
const PEAK_CHUNK_MS: u64 = 60_000;

/// Hints the mix pipeline uses.
#[derive(Debug, Clone, Default)]
pub struct MixHints {
    pub tracklist: Option<Tracklist>,
    /// Songs without times (YouTube "Music" cards).
    pub untimed: Vec<Track>,
}

/// Per-run settings.
#[derive(Debug, Clone)]
pub struct MixOptions {
    /// Playlist entry index for the segments.
    pub item_index: u32,
    /// Source page; segments get deep links into it.
    pub source_url: Option<String>,
    pub priority: Priority,
    /// Prefix for segment ids (playlist entries share one session).
    pub id_prefix: String,
}

impl Default for MixOptions {
    fn default() -> Self {
        Self { item_index: 0, source_url: None, priority: Priority::Batch, id_prefix: String::new() }
    }
}

/// A recognizer answer for a window, possibly already made by the survey.
#[derive(Debug, Clone)]
pub struct Probe {
    pub at_ms: u64,
    pub len_ms: u64,
    pub hit: Option<RecognitionHit>,
}

/// What the mix pipeline found.
#[derive(Debug, Clone, Default)]
pub struct MixReport {
    /// The final timeline.
    pub pieces: Vec<Piece>,
    /// Recognizer probes made (the survey's not included).
    pub probes: u32,
    /// Paid provider calls.
    pub paid: u32,
    /// Hints verified by local matching.
    pub verified_locally: u32,
    /// Windows explained by local matching instead of a probe.
    pub local_matches: u32,
    /// Short notes on decisions, for evaluation and debugging.
    pub log: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnchorKind {
    Recognizer,
    Local,
}

/// One observation of a recording at a place in the media.
#[derive(Debug, Clone)]
struct Anchor {
    id: String,
    at_ms: u64,
    len_ms: u64,
    track: Track,
    evidence: Evidence,
    /// Media time where the recording's beginning would sit.
    start_est: Option<i64>,
    /// Latest media time the recording can last to (needs its duration).
    end_est: Option<i64>,
    kind: AnchorKind,
    /// The recognizer reported a speed/pitch change no turntable or key-locked
    /// player produces: typical of a sample inside another record.
    odd: bool,
}

impl Anchor {
    /// Until when the recording is surely playing: a recognizer answers for
    /// the bigger part of its window, so only the window's middle is certain.
    fn seen_until(&self) -> u64 {
        match self.kind {
            AnchorKind::Recognizer => self.at_ms + self.len_ms / 2,
            AnchorKind::Local => self.at_ms + self.len_ms,
        }
    }

    /// From when the recording is surely playing.
    fn seen_from(&self) -> u64 {
        match self.kind {
            AnchorKind::Recognizer => self.at_ms + self.len_ms / 2,
            AnchorKind::Local => self.at_ms,
        }
    }
}

/// Analyze long media and emit its timeline. `prior` holds probes the survey
/// already made.
pub async fn run<S: Services>(
    services: &S,
    emitter: &Emitter,
    pcm: &PcmFile,
    hints: &MixHints,
    prior: Vec<Probe>,
    options: &MixOptions,
) -> Result<MixReport> {
    emitter.phase(Phase::Segmenting, None);
    let cancel = services.cancel().clone();
    let pcm_for_features = pcm.clone();
    let frames = tokio::task::spawn_blocking(move || features::compute(&pcm_for_features, 1000, &cancel))
        .await
        .map_err(|e| MfError::internal(format!("features: {e}")))??;
    let duration = pcm.duration_ms();
    let labels = segment::labels(&frames);
    let kernel = if duration < 20 * 60_000 { 12 } else { 24 };
    let novelty = segment::novelty(&frames, kernel);
    let novelty_peaks: Vec<u64> =
        segment::peaks(&novelty, kernel.max(10), 0.5).into_iter().map(|s| s as u64 * 1000).collect();
    drop(frames);

    let mut mix = Mix {
        services,
        emitter,
        pcm,
        options,
        duration,
        labels,
        novelty,
        novelty_peaks,
        anchors: Vec::new(),
        misses: Vec::new(),
        fixed: Vec::new(),
        report: MixReport::default(),
        budget: (duration / 45_000) as u32 + 10,
        enriched: HashMap::new(),
        bank: PreviewBank::default(),
        peaks: HashMap::new(),
        emitted: HashMap::new(),
        probe_time: Duration::ZERO,
        next_id: 0,
        limited: false,
        analyzed: 0,
        refuted: Default::default(),
    };
    for probe in prior {
        match probe.hit {
            Some(hit) => {
                let anchor = mix.anchor_from_hit(probe.at_ms, probe.len_ms, hit).await;
                mix.anchors.push(anchor);
            }
            None => mix.misses.push((probe.at_ms, probe.len_ms)),
        }
    }
    mix.publish(0);
    emitter.phase(Phase::Recognizing, None);

    if let Some(list) = hints.tracklist.as_ref().filter(|l| !l.entries.is_empty()) {
        mix.hinted_pass(list).await?;
    }
    if !hints.untimed.is_empty() {
        mix.untimed_pass(&hints.untimed).await?;
    }
    mix.adaptive_pass().await?;
    mix.fill_gaps().await?;
    mix.refine_boundaries().await?;
    mix.confirm_lone().await?;
    mix.resolve_leftovers().await?;
    emitter.phase(Phase::Finishing, None);
    mix.log_boundaries();
    let pieces = mix.timeline();
    mix.publish_pieces(&pieces, duration);
    mix.report.pieces = pieces;
    Ok(mix.report)
}

struct Mix<'a, S: Services> {
    services: &'a S,
    emitter: &'a Emitter,
    pcm: &'a PcmFile,
    options: &'a MixOptions,
    duration: u64,
    labels: Vec<Label>,
    novelty: Vec<f32>,
    novelty_peaks: Vec<u64>,
    anchors: Vec<Anchor>,
    misses: Vec<(u64, u64)>,
    /// Pieces settled by the hinted pass.
    fixed: Vec<Piece>,
    report: MixReport,
    budget: u32,
    enriched: HashMap<String, Track>,
    bank: PreviewBank,
    /// Mix peaks per minute of media (absolute frames).
    peaks: HashMap<u64, Vec<Peak>>,
    emitted: HashMap<String, Segment>,
    probe_time: Duration,
    next_id: u32,
    /// The recognizer is throttled beyond what a batch job waits for.
    limited: bool,
    /// Furthest progress reported.
    analyzed: u64,
    /// Lone recognizer answers that a second look did not support.
    refuted: std::collections::HashSet<String>,
}

/// Previews of recordings identified so far, for free local checks.
#[derive(Default)]
struct PreviewBank {
    index: Index,
    tracks: Vec<Track>,
    tried: HashMap<String, bool>,
}

impl<S: Services> Mix<'_, S> {
    fn new_id(&mut self, what: &str) -> String {
        self.next_id += 1;
        format!("{}{what}-{}", self.options.id_prefix, self.next_id)
    }

    fn log(&mut self, line: String) {
        tracing::debug!("{line}");
        self.report.log.push(line);
    }

    // -----------------------------------------------------------------------
    // Probing

    /// Snap a probe start to the grid and keep the window inside the media.
    fn snap(&self, at_ms: u64) -> u64 {
        let last = self.duration.saturating_sub(PROBE_MS.min(self.duration));
        (at_ms.min(last) / GRID_MS) * GRID_MS
    }

    /// Share of `[from, to)` labelled music.
    fn music_share(&self, from_ms: u64, to_ms: u64) -> f32 {
        self.label_share(from_ms, to_ms, Label::Music)
    }

    /// Share of `[from, to)` labelled speech.
    fn speech_share(&self, from_ms: u64, to_ms: u64) -> f32 {
        self.label_share(from_ms, to_ms, Label::Speech)
    }

    fn label_share(&self, from_ms: u64, to_ms: u64, label: Label) -> f32 {
        let lo = (from_ms / 1000) as usize;
        let hi = (to_ms.div_ceil(1000) as usize).min(self.labels.len());
        if hi <= lo {
            return 0.0;
        }
        self.labels[lo..hi].iter().filter(|l| **l == label).count() as f32 / (hi - lo) as f32
    }

    fn can_probe(&self) -> bool {
        !self.limited && self.report.probes < self.budget
    }

    /// One recognizer probe at `at_ms` (snapped). `None` when nothing matched or
    /// the window is too short.
    async fn probe(&mut self, at_ms: u64) -> Result<Option<Anchor>> {
        let at = self.snap(at_ms);
        let len = PROBE_MS.min(self.duration.saturating_sub(at));
        if len < 3_000 || !self.can_probe() {
            return Ok(None);
        }
        let samples = self.pcm.read_ms(at, len)?;
        self.report.probes += 1;
        let started = Instant::now();
        let result = self.services.shazam(&samples, self.options.priority).await;
        self.probe_time += started.elapsed();
        match result {
            Ok(Some(hit)) => {
                let skew = (hit.timeskew, hit.frequency_skew);
                let anchor = self.anchor_from_hit(at, len, hit).await;
                self.log(format!(
                    "probe {} → {} – {} (starts {}, speed {:+.1} %, pitch {:+.1} %)",
                    clock(at),
                    anchor.track.artist,
                    anchor.track.title,
                    anchor.start_est.map_or("?".into(), |s| if s < 0 {
                        format!("-{}", clock(s.unsigned_abs()))
                    } else {
                        clock(s as u64)
                    }),
                    skew.0 * 100.0,
                    skew.1 * 100.0
                ));
                Ok(Some(anchor))
            }
            Ok(None) => {
                self.log(format!("probe {} → no match", clock(at)));
                self.misses.push((at, len));
                Ok(None)
            }
            Err(MfError::RateLimited { retry_in_ms }) => {
                self.log(format!("recognizer throttled for {} s; finishing with what we have", retry_in_ms / 1000));
                self.limited = true;
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    async fn enrich(&mut self, track: Track) -> Track {
        let key = crate::identity::identity_key(&track);
        if let Some(done) = self.enriched.get(&key) {
            let mut merged = track;
            fill_from(&mut merged, done);
            return merged;
        }
        let done = self.services.enrich(track).await;
        self.enriched.insert(key, done.clone());
        done
    }

    async fn anchor_from_hit(&mut self, at_ms: u64, len_ms: u64, hit: RecognitionHit) -> Anchor {
        let track = self.enrich(hit.track.clone()).await;
        let speed = 1.0 + hit.timeskew as f64;
        let start_est = hit.offset_ms.map(|o| at_ms as i64 - (o as f64 / speed).round() as i64);
        let end_est = match (start_est, track.duration_ms) {
            (Some(s), Some(d)) => Some(s + (d as f64 / speed).round() as i64),
            _ => None,
        };
        let detail = match hit.offset_ms {
            Some(o) => format!("offset {}, skew {:.1} %", clock(o.max(0) as u64), hit.skew() * 100.0),
            None => "no offset".to_string(),
        };
        let id = self.new_id("t");
        Anchor {
            id,
            at_ms,
            len_ms,
            evidence: Evidence { source: hit.source, at_ms, score: hit.score, detail: Some(detail) },
            track,
            start_est,
            end_est,
            kind: AnchorKind::Recognizer,
            odd: !plausible_skew(hit.timeskew, hit.frequency_skew),
        }
    }

    // -----------------------------------------------------------------------
    // Local matching

    /// Mix peaks in `[from, to)`, re-based to `from`. Peaks are computed per
    /// minute of media on first use and kept.
    async fn region_peaks(&mut self, from_ms: u64, to_ms: u64) -> Result<Vec<Peak>> {
        let lo = (from_ms as f64 / FRAME_MS) as u32;
        let hi = (to_ms as f64 / FRAME_MS) as u32;
        let first = from_ms / PEAK_CHUNK_MS;
        let last = to_ms.saturating_sub(1) / PEAK_CHUNK_MS;
        let missing: Vec<u64> = (first..=last).filter(|c| !self.peaks.contains_key(c)).collect();
        if !missing.is_empty() {
            let pcm = self.pcm.clone();
            let cancel = self.services.cancel().clone();
            let computed = tokio::task::spawn_blocking(move || -> Result<Vec<(u64, Vec<Peak>)>> {
                missing
                    .into_iter()
                    .map(|c| {
                        let start = c * PEAK_CHUNK_MS;
                        let shift = (start as f64 / FRAME_MS).round() as u32;
                        let peaks = fingerprint::peaks_in(&pcm, start, PEAK_CHUNK_MS, &cancel)?;
                        Ok((c, peaks.into_iter().map(|p| Peak { frame: p.frame + shift, ..p }).collect()))
                    })
                    .collect()
            })
            .await
            .map_err(|e| MfError::internal(format!("peaks: {e}")))??;
            self.peaks.extend(computed);
        }
        let mut out = Vec::new();
        for c in first..=last {
            if let Some(chunk) = self.peaks.get(&c) {
                out.extend(
                    chunk.iter().filter(|p| p.frame >= lo && p.frame < hi).map(|p| Peak { frame: p.frame - lo, ..*p }),
                );
            }
        }
        Ok(out)
    }

    /// Where `preview` plays inside `[from, to)`, if it clearly does: the media
    /// time the preview's first frame aligns to and the matched stretch.
    async fn locate(&mut self, preview: &[i16], from_ms: u64, to_ms: u64) -> Result<Option<LocalHit>> {
        let region = self.region_peaks(from_ms, to_ms).await?;
        let reference = fingerprint::peaks(preview);
        let found = tokio::task::spawn_blocking(move || {
            let mut index = Index::new();
            index.add(&reference);
            index.search(&region, &fingerprint::warps(MAX_SPEED_CHANGE, SPEED_STEP)).into_iter().next()
        })
        .await
        .map_err(|e| MfError::internal(format!("local match: {e}")))?;
        match found {
            Some(m) if m.is_confident(&MatchOptions::default()) => Ok(Some(LocalHit::new(&m, from_ms))),
            Some(m) => {
                self.log(format!(
                    "preview not found ({} hashes, {:.1} % of the preview, chance {:.1})",
                    m.score,
                    m.coverage * 100.0,
                    m.noise
                ));
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Add the preview of `track` to the bank (once per identity).
    async fn bank_track(&mut self, track: &Track) {
        let key = crate::identity::identity_key(track);
        if self.bank.tried.contains_key(&key) {
            return;
        }
        let preview = self.services.preview(track).await;
        self.bank.tried.insert(key, preview.is_some());
        if let Some(samples) = preview {
            let peaks = fingerprint::peaks(&samples);
            self.bank.index.add(&peaks);
            self.bank.tracks.push(track.clone());
        }
    }

    /// Look for the preview of a freshly recognized recording where the
    /// recording must be playing (between its start and its latest end). A hit
    /// confirms the recognizer for free and shows more of where it plays.
    async fn confirm_locally(&mut self, a: &Anchor) -> Result<Option<Anchor>> {
        let Some(reference) = self.bank.tracks.iter().position(|t| hints::same_recording(t, &a.track)) else {
            return Ok(None);
        };
        let start = a.start_est.unwrap_or(a.at_ms as i64 - 300_000).max(0) as u64;
        let end = a.end_est.map_or(a.at_ms + 300_000, |e| e.max(0) as u64).min(start + 8 * 60_000);
        let from = start.saturating_sub(10_000);
        let to = (end + 10_000).min(self.duration);
        if to <= from + 5_000 {
            return Ok(None);
        }
        let region = self.region_peaks(from, to).await?;
        let found = self.bank.index.search(&region, &fingerprint::warps(MAX_SPEED_CHANGE, SPEED_STEP));
        let Some(m) = found.into_iter().find(|m| m.reference == reference && m.is_confident(&MatchOptions::default()))
        else {
            return Ok(None);
        };
        let hit = LocalHit::new(&m, from);
        let id = self.new_id("t");
        self.log(format!(
            "confirmed {} – {} locally at {}–{} ({} hashes)",
            a.track.artist,
            a.track.title,
            clock(hit.span.0),
            clock(hit.span.1),
            m.score
        ));
        Ok(Some(Anchor {
            id,
            at_ms: hit.span.0,
            len_ms: hit.span.1.saturating_sub(hit.span.0).max(1_000),
            evidence: Evidence {
                source: a.evidence.source,
                at_ms: hit.span.0,
                score: a.evidence.score,
                detail: Some(format!("preview matched locally ({} hashes)", m.score)),
            },
            track: a.track.clone(),
            start_est: a.start_est,
            end_est: a.end_est,
            kind: AnchorKind::Local,
            odd: false,
        }))
    }

    /// Bank a recognized recording's preview and confirm it locally (once per recording).
    async fn learn(&mut self, a: &Anchor) -> Result<()> {
        if a.kind != AnchorKind::Recognizer {
            return Ok(());
        }
        let key = crate::identity::identity_key(&a.track);
        if self.bank.tried.contains_key(&key) {
            return Ok(());
        }
        self.bank_track(&a.track).await;
        if let Some(local) = self.confirm_locally(a).await? {
            self.anchors.push(local);
        }
        Ok(())
    }

    /// A banked recording playing in `[from, to)`, found without a probe.
    async fn bank_check(&mut self, from_ms: u64, to_ms: u64) -> Result<Option<Anchor>> {
        if self.bank.index.is_empty() {
            return Ok(None);
        }
        let region = self.region_peaks(from_ms, to_ms).await?;
        let found = self.bank.index.search(&region, &fingerprint::warps(MAX_SPEED_CHANGE, SPEED_STEP));
        let Some(best) = found.into_iter().next().filter(|m| m.is_confident(&MatchOptions::default())) else {
            return Ok(None);
        };
        let hit = LocalHit::new(&best, from_ms);
        let track = self.bank.tracks[best.reference].clone();
        // The banked track's start estimate comes from its earlier anchors.
        let known = self
            .anchors
            .iter()
            .filter(|a| hints::same_recording(&a.track, &track))
            .min_by_key(|a| (a.at_ms as i64 - hit.span.0 as i64).abs());
        let start_est = known.and_then(|a| a.start_est).filter(|s| *s <= hit.span.0 as i64);
        let end_est = known.and_then(|a| a.end_est);
        let id = self.new_id("t");
        self.report.local_matches += 1;
        self.log(format!(
            "local {}–{} → {} – {} (score {}, speed {:+.1} %)",
            clock(hit.span.0),
            clock(hit.span.1),
            track.artist,
            track.title,
            best.score,
            best.warp.speed_change() * 100.0
        ));
        Ok(Some(Anchor {
            id,
            at_ms: hit.span.0,
            len_ms: hit.span.1.saturating_sub(hit.span.0).max(1_000),
            evidence: Evidence {
                source: EvidenceSource::Shazam,
                at_ms: hit.span.0,
                score: 0.72,
                detail: Some(format!("matched an earlier identification locally ({} hashes)", best.score)),
            },
            track,
            start_est,
            end_est,
            kind: AnchorKind::Local,
            odd: false,
        }))
    }

    // -----------------------------------------------------------------------
    // Hinted pass

    async fn hinted_pass(&mut self, list: &Tracklist) -> Result<()> {
        let entries = &list.entries;
        self.log(format!("tracklist from {:?}: {} entries", list.source, entries.len()));
        // Per settled entry: its piece, whether the audio overruled it, what was heard.
        let mut outcomes: Vec<(String, bool, Vec<Anchor>)> = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            self.services.cancel().check()?;
            let start = entry.start_ms.min(self.duration);
            let end = entries.get(i + 1).map_or(self.duration, |n| n.start_ms.min(self.duration));
            if end <= start {
                continue;
            }
            let id = self.new_id("h");
            let mut piece = Piece::gap(id, start, end, SegmentKind::Unidentified);
            piece.item_index = self.options.item_index;
            let mut contradicted = false;
            if entry.is_id || entry.is_talk() {
                // Unknown tracks and talk: only the audio can name a song here.
                let heard =
                    if self.music_share(start, end) >= 0.4 { self.probe(probe_point(start, end)).await? } else { None };
                match heard.clone() {
                    Some(anchor) => self.settle_with_audio(&mut piece, anchor, None),
                    None if self.speech_share(start, end) >= 0.5 => piece.kind = SegmentKind::Speech,
                    None => {}
                }
                outcomes.push((piece.id.clone(), false, heard.into_iter().collect()));
                self.fixed.push(piece);
                self.publish(end);
                continue;
            }
            let hint_track = entry.to_track();
            let found = self.services.find(&entry.title, &entry.artist, None).await;
            let in_catalog = found.is_some();
            let track = match found {
                Some(t) => self.enrich(t).await,
                None => hint_track.clone(),
            };
            let mut verified = None;
            let preview = if in_catalog { self.services.preview(&track).await } else { None };
            if let Some(preview) = preview {
                let from = start.saturating_sub(HINT_MARGIN_MS);
                let to = (end + HINT_MARGIN_MS).min(self.duration);
                verified = self.locate(&preview, from, to).await?;
                if verified.is_some() {
                    self.bank_track_samples(&track, &preview);
                }
            }
            if let Some(local) = verified {
                self.report.verified_locally += 1;
                self.log(format!(
                    "hint {} {} – {}: preview at {} (speed {:+.1} %, {} hashes, {:.1} %)",
                    clock(start),
                    entry.artist,
                    entry.title,
                    clock(local.span.0),
                    local.speed * 100.0,
                    local.score,
                    local.coverage * 100.0
                ));
                piece.kind = SegmentKind::Track;
                piece.track = Some(track);
                piece.evidence.push(Evidence {
                    source: EvidenceSource::PageHint,
                    at_ms: start,
                    score: VERIFIED_HINT,
                    detail: Some(format!("tracklist entry; preview matched at {}", clock(local.span.0))),
                });
                outcomes.push((piece.id.clone(), false, Vec::new()));
                self.fixed.push(piece);
                self.publish(end);
                continue;
            }
            // Not verified locally: one probe inside the entry.
            let mut anchor = self.probe(probe_point(start, end)).await?;
            let mut heard: Vec<Anchor> = anchor.iter().cloned().collect();
            // Another song at a speed/pitch no player produces is a sample: not evidence.
            if let Some(a) = anchor.clone().filter(|a| a.odd && !hints::same_song(&a.track, &hint_track)) {
                piece.alternates.push(a.track);
                anchor = None;
            }
            let neighbour = |a: &Anchor| {
                [i.checked_sub(1), Some(i + 1)]
                    .into_iter()
                    .flatten()
                    .filter_map(|k| entries.get(k))
                    .any(|n| hints::same_song(&a.track, &n.to_track()))
            };
            if let Some(first) = anchor.clone().filter(|a| !hints::same_song(&a.track, &hint_track))
                && end - start > 50_000
            {
                // A neighbour (the transition) or another song: listen again later
                // before overruling the tracklist. A sampled record, for one, is
                // rarely heard twice in a row.
                let later = start + (end - start) * 3 / 4 - PROBE_MS / 2;
                let again = self.probe(later).await?;
                heard.extend(again.iter().cloned());
                // Overrule only on one continuous play: the same recording twice,
                // with start estimates that agree. A sampled vocal heard twice
                // does not follow the original's timeline.
                let same_play = |b: &Anchor| {
                    hints::same_recording(&b.track, &first.track)
                        && matches!((b.start_est, first.start_est), (Some(x), Some(y)) if (x - y).abs() <= stitch::SAME_START_MS)
                };
                anchor = match again {
                    Some(b) if hints::same_song(&b.track, &hint_track) => Some(b),
                    Some(b) if neighbour(&first) => Some(b),
                    Some(b) if same_play(&b) => Some(first),
                    _ if neighbour(&first) => None,
                    _ => {
                        piece.alternates.push(first.track.clone());
                        None
                    }
                };
            }
            match anchor {
                Some(a) if hints::same_song(&a.track, &hint_track) => {
                    // The audio confirms the entry; the recognizer names the exact version.
                    let mut audio_track = a.track.clone();
                    if hints::same_version(&audio_track, &track) {
                        fill_from(&mut audio_track, &track);
                    }
                    piece.kind = SegmentKind::Track;
                    piece.track = Some(audio_track);
                    piece.track_start_ms = a.start_est;
                    piece.evidence.push(a.evidence.clone());
                    piece.evidence.push(Evidence {
                        source: EvidenceSource::PageHint,
                        at_ms: start,
                        score: VERIFIED_HINT,
                        detail: Some("tracklist entry; confirmed by recognition".into()),
                    });
                    self.bank_track(&a.track).await;
                }
                Some(a) if !neighbour(&a) => {
                    self.log(format!(
                        "hint {} {} – {} contradicted by audio: {} – {}",
                        clock(start),
                        entry.artist,
                        entry.title,
                        a.track.artist,
                        a.track.title
                    ));
                    contradicted = true;
                    self.settle_with_audio(&mut piece, a, Some(track));
                }
                _ => {
                    piece.kind = SegmentKind::Track;
                    piece.track = Some(track);
                    piece.evidence.push(Evidence {
                        source: EvidenceSource::PageHint,
                        at_ms: start,
                        score: UNVERIFIED_HINT,
                        detail: Some("tracklist entry".into()),
                    });
                }
            }
            outcomes.push((piece.id.clone(), contradicted, heard));
            self.fixed.push(piece);
            self.publish(end);
            if let Some(from) = unreliable_from(&outcomes) {
                // The tracklist stopped describing this media (re-edited video,
                // shifted times): keep what was confirmed, listen to the rest.
                let dropped: Vec<(String, bool, Vec<Anchor>)> = outcomes.drain(from..).collect();
                let since = self.fixed.iter().find(|p| p.id == dropped[0].0).map_or(0, |p| p.start_ms);
                self.log(format!("tracklist does not match the audio from {}; listening instead", clock(since)));
                for (id, _, anchors) in dropped {
                    self.fixed.retain(|p| p.id != id);
                    self.anchors.extend(anchors);
                }
                self.publish(since);
                break;
            }
        }
        Ok(())
    }

    fn settle_with_audio(&mut self, piece: &mut Piece, anchor: Anchor, hint: Option<Track>) {
        piece.kind = SegmentKind::Track;
        piece.track_start_ms = anchor.start_est;
        piece.evidence.push(anchor.evidence.clone());
        if let Some(h) = hint {
            piece.alternates.push(h);
        }
        piece.track = Some(anchor.track);
    }

    fn bank_track_samples(&mut self, track: &Track, samples: &[i16]) {
        let key = crate::identity::identity_key(track);
        if self.bank.tried.insert(key, true).is_none() {
            self.bank.index.add(&fingerprint::peaks(samples));
            self.bank.tracks.push(track.clone());
        }
    }

    // -----------------------------------------------------------------------
    // Untimed hints

    async fn untimed_pass(&mut self, untimed: &[Track]) -> Result<()> {
        for hint in untimed.iter().take(20) {
            self.services.cancel().check()?;
            let track = self.enrich(hint.clone()).await;
            self.bank_track(&track).await;
        }
        if self.bank.index.is_empty() {
            return Ok(());
        }
        // Scan the unexplained media in overlapping 90 s regions.
        let mut at = 0;
        while at < self.duration {
            let to = (at + 90_000).min(self.duration);
            if !self.is_fixed(at + 45_000)
                && let Some(anchor) = self.bank_check(at, to).await?
            {
                self.anchors.push(anchor);
            }
            at += 60_000;
        }
        self.publish(0);
        Ok(())
    }

    fn is_fixed(&self, at_ms: u64) -> bool {
        self.fixed.iter().any(|p| p.start_ms <= at_ms && at_ms < p.end_ms)
    }

    /// End of the settled piece containing `at_ms`.
    fn fixed_end(&self, at_ms: u64) -> Option<u64> {
        self.fixed.iter().find(|p| p.start_ms <= at_ms && at_ms < p.end_ms).map(|p| p.end_ms)
    }

    // -----------------------------------------------------------------------
    // Adaptive probing

    async fn adaptive_pass(&mut self) -> Result<()> {
        let mut t = self.first_music().unwrap_or(0);
        while t + 3_000 < self.duration {
            self.services.cancel().check()?;
            if let Some(end) = self.fixed_end(t) {
                t = end;
                continue;
            }
            if self.music_share(t, t + PROBE_MS) < 0.4 {
                t += 10_000;
                continue;
            }
            // Something already seen around here (survey probes, untimed hints)?
            if let Some(next) = self.known_next(t) {
                t = next;
                continue;
            }
            if !self.can_probe() {
                break;
            }
            let anchor = match self.bank_check(t.saturating_sub(5_000), (t + 25_000).min(self.duration)).await? {
                Some(a) => Some(a),
                None => self.probe(t).await?,
            };
            match anchor {
                Some(a) => {
                    let next = next_probe(&a);
                    self.anchors.push(a.clone());
                    self.learn(&a).await?;
                    t = next.max(t + MIN_STEP_MS);
                }
                None => t += MISS_STEP_MS,
            }
            self.publish(t.min(self.duration));
        }
        Ok(())
    }

    /// When an existing anchor covers `t`, the time the next probe should go.
    fn known_next(&self, t: u64) -> Option<u64> {
        self.anchors
            .iter()
            .filter(|a| a.at_ms <= t + 5_000 && t < a.at_ms + MIN_STEP_MS)
            .map(|a| next_probe(a).max(a.at_ms + MIN_STEP_MS))
            .max()
            .filter(|next| *next > t)
    }

    fn first_music(&self) -> Option<u64> {
        let mut run = 0;
        for (i, l) in self.labels.iter().enumerate() {
            if *l == Label::Music {
                run += 1;
                if run >= 5 {
                    return Some(((i + 1 - run) as u64 * 1000 + 5_000).min(self.duration));
                }
            } else {
                run = 0;
            }
        }
        None
    }

    /// Probe unexplained stretches until none qualify or the budget is spent.
    async fn fill_gaps(&mut self) -> Result<()> {
        for _round in 0..8 {
            if !self.can_probe() {
                break;
            }
            let targets = self.gap_targets();
            if targets.is_empty() {
                break;
            }
            let mut found_any = false;
            for at in targets {
                if !self.can_probe() {
                    break;
                }
                self.services.cancel().check()?;
                let anchor = match self.bank_check(at.saturating_sub(5_000), (at + 25_000).min(self.duration)).await? {
                    Some(a) => Some(a),
                    None => self.probe(at).await?,
                };
                if let Some(a) = anchor {
                    self.anchors.push(a.clone());
                    self.learn(&a).await?;
                    found_any = true;
                }
                self.publish(at);
            }
            if !found_any {
                break;
            }
        }
        Ok(())
    }

    /// Middles of unexplained stretches worth a probe, in time order.
    fn gap_targets(&self) -> Vec<u64> {
        let mut anchors: Vec<&Anchor> = self.anchors.iter().collect();
        anchors.sort_by_key(|a| a.at_ms);
        let mut out = Vec::new();
        let probed_near = |at: u64| {
            self.misses.iter().any(|(m, _)| m.abs_diff(at) < 20_000)
                || self.anchors.iter().any(|a| a.at_ms.abs_diff(at) < 20_000)
        };
        let consider = |lo: u64, hi: u64, out: &mut Vec<u64>| {
            if hi <= lo + GAP_PROBE_MS || self.is_fixed((lo + hi) / 2) {
                return;
            }
            let mid = (lo + hi) / 2 - PROBE_MS / 2;
            if self.music_share(mid, mid + PROBE_MS) >= 0.4 && !probed_near(mid) {
                out.push(mid);
            }
        };
        // Before the first match.
        if let Some(first) = anchors.first() {
            let start = first.start_est.unwrap_or(first.at_ms as i64).max(0) as u64;
            if start > EDGE_ABSORB_MS * 2 {
                consider(0, start.min(first.at_ms), &mut out);
            }
        }
        for pair in anchors.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let same = hints::same_recording(&a.track, &b.track);
            if same && !is_replay(a, b) {
                continue;
            }
            let lo = a.seen_until();
            let hi = b.seen_from().max(lo);
            // Where `b` begins (its start estimate when it is plausible).
            let b_start = b.start_est.map(|s| s.max(lo as i64) as u64).filter(|s| *s <= hi).unwrap_or(hi);
            // `a` cannot last past its own end: a stretch where neither plays.
            let a_end = a.end_est.filter(|e| *e >= 0 && (*e as u64) < b_start).map(|e| (e as u64).max(lo));
            if let Some(e) = a_end
                && b_start >= e + GAP_PROBE_MS
            {
                consider(e, b_start, &mut out);
                continue;
            }
            // A long unseen stretch before `b`: listen just before it begins.
            // A short track hidden there sits right before `b`; hearing `a`
            // instead shows that `a` lasts until `b`. Not needed when `a` ends
            // exactly where `b` begins (played in full, hard cut).
            let ends_at_b = a.end_est.is_some_and(|e| (e - b_start as i64).abs() <= 15_000);
            if !ends_at_b && b_start >= lo + HIDDEN_TRACK_MS {
                let at = b_start - BEFORE_START_MS;
                if self.music_share(at, at + PROBE_MS) >= 0.4 && !probed_near(at) && !self.is_fixed(at) {
                    out.push(at);
                }
            } else if hi.saturating_sub(lo) > MAX_UNPROBED_MS {
                consider(lo, hi, &mut out);
            }
        }
        // After the last match.
        if let Some(last) = anchors.last() {
            let end =
                last.end_est.map(|e| e.max(last.seen_until() as i64) as u64).unwrap_or(last.seen_until() + 90_000);
            if end + EDGE_ABSORB_MS * 2 < self.duration {
                consider(end, self.duration, &mut out);
            }
        }
        out.sort_unstable();
        out.dedup_by(|a, b| a.abs_diff(*b) < 20_000);
        out
    }

    /// Listen in the middle of long transitions (at most twice each) so a
    /// change lands where the next recording takes over; the whole analysis
    /// stays within `REFINE_RATE_PER_HOUR`.
    async fn refine_boundaries(&mut self) -> Result<()> {
        let cap = ((self.duration as f64 / 3_600_000.0) * REFINE_RATE_PER_HOUR).ceil() as u32;
        for _round in 0..2 {
            let mut anchors: Vec<&Anchor> = self
                .anchors
                .iter()
                .filter(|a| !self.is_fixed(a.at_ms + a.len_ms / 2) && !self.refuted.contains(&a.id))
                .collect();
            anchors.sort_by_key(|a| a.at_ms);
            let anchors = drop_contradicted(anchors);
            let mut targets = Vec::new();
            for pair in anchors.windows(2) {
                let (a, b) = (pair[0], pair[1]);
                if hints::same_recording(&a.track, &b.track) || hard_cut(a, b).is_some() {
                    continue;
                }
                let (lo, hi) = self.transition(a, b);
                if hi.saturating_sub(lo) < REFINE_MIN_MS {
                    continue;
                }
                let at = (lo + (hi - lo) / 2).saturating_sub(PROBE_MS / 2);
                if self.probed_within(at, 8_000) || self.music_share(at, at + PROBE_MS) < 0.4 {
                    continue;
                }
                targets.push(at);
            }
            if targets.is_empty() {
                break;
            }
            for at in targets {
                if self.report.probes >= cap || !self.can_probe() {
                    return Ok(());
                }
                self.services.cancel().check()?;
                if let Some(a) = self.probe(at).await? {
                    self.anchors.push(a.clone());
                    self.learn(&a).await?;
                }
                self.publish(at);
            }
        }
        Ok(())
    }

    /// A probe (answered or not) already started within `distance` of `at`.
    fn probed_within(&self, at: u64, distance: u64) -> bool {
        self.misses.iter().any(|(m, _)| m.abs_diff(at) < distance)
            || self.anchors.iter().any(|a| a.kind == AnchorKind::Recognizer && a.at_ms.abs_diff(at) < distance)
    }

    /// A recording heard only once and not found locally gets a second probe
    /// where it must still be playing. Answers caused by a sampled record or a
    /// near miss rarely repeat with a consistent position; those that do not
    /// are dropped (the stretch is then left unidentified or to the neighbours).
    async fn confirm_lone(&mut self) -> Result<()> {
        let mut sorted: Vec<Anchor> = self.anchors.clone();
        sorted.sort_by_key(|a| a.at_ms);
        for (k, a) in sorted.iter().enumerate() {
            if a.kind != AnchorKind::Recognizer
                || self.is_fixed(a.at_ms + a.len_ms / 2)
                || self.refuted.contains(&a.id)
                || self.is_supported(a)
            {
                continue;
            }
            if let Some(other) = self.overlapped_by(a) {
                self.log(format!(
                    "dropped {} – {} (would overlap {} for minutes)",
                    a.track.artist, a.track.title, other
                ));
                self.refuted.insert(a.id.clone());
                continue;
            }
            if !self.can_probe() {
                break;
            }
            self.services.cancel().check()?;
            let other = |b: &&Anchor| !hints::same_recording(&b.track, &a.track);
            let prev = sorted[..k].iter().rev().find(other).map_or(0, |b| b.seen_until());
            let next = sorted[k + 1..].iter().find(other).map_or(self.duration, |b| b.seen_from());
            let lo = prev.max(a.start_est.unwrap_or(0).max(0) as u64);
            let hi = next.min(a.end_est.map_or(self.duration, |e| e.max(0) as u64));
            let before = a.at_ms.saturating_sub(lo).min(CONFIRM_REACH_MS);
            let after = hi.saturating_sub(a.at_ms + a.len_ms).min(CONFIRM_REACH_MS);
            let at = if after >= before && after >= CONFIRM_ROOM_MS {
                Some(a.at_ms + a.len_ms + after / 2 - PROBE_MS / 2)
            } else if before >= CONFIRM_ROOM_MS {
                Some((a.at_ms - before / 2).saturating_sub(PROBE_MS / 2))
            } else {
                None
            };
            let Some(at) = at.filter(|at| self.music_share(*at, *at + PROBE_MS) >= 0.4) else {
                if a.odd {
                    self.drop_answer(a, "implausible speed and pitch");
                }
                continue;
            };
            match self.probe(at).await? {
                Some(b) if hints::same_recording(&b.track, &a.track) && consistent_start(a, &b) => {
                    self.anchors.push(b);
                }
                Some(b) => {
                    // Another version of the same song: the second answer names it better.
                    if hints::same_song(&b.track, &a.track) {
                        self.log(format!("replaced {} – {} by {}", a.track.artist, a.track.title, b.track.title));
                        self.refuted.insert(a.id.clone());
                    } else if a.odd {
                        self.drop_answer(a, "implausible speed and pitch, not heard again");
                    }
                    self.anchors.push(b.clone());
                    self.learn(&b).await?;
                }
                // Nothing recognized there (a breakdown, a transition): keep a
                // plausible answer, drop one that sounds like a sample.
                None if a.odd => self.drop_answer(a, "implausible speed and pitch, not heard again"),
                None => {}
            }
        }
        // Answers placed where another recording surely plays for minutes are
        // misidentifications (also those the confirmations added).
        let lone: Vec<Anchor> = self
            .anchors
            .iter()
            .filter(|a| a.kind == AnchorKind::Recognizer && !self.refuted.contains(&a.id) && !self.is_supported(a))
            .cloned()
            .collect();
        for a in lone {
            if let Some(other) = self.overlapped_by(&a) {
                self.drop_answer(&a, &format!("would overlap {other} for minutes"));
            } else if a.odd {
                self.drop_answer(&a, "implausible speed and pitch");
            }
        }
        Ok(())
    }

    fn drop_answer(&mut self, a: &Anchor, why: &str) {
        self.log(format!("dropped {} – {} ({why})", a.track.artist, a.track.title));
        self.refuted.insert(a.id.clone());
    }

    /// Another answer or a local match backs this recording up.
    fn is_supported(&self, a: &Anchor) -> bool {
        self.anchors.iter().any(|b| {
            b.id != a.id
                && !self.refuted.contains(&b.id)
                && hints::same_recording(&b.track, &a.track)
                && (b.kind == AnchorKind::Local || consistent_start(a, b))
        })
    }

    /// A well-established other recording heard where `a` would already have
    /// been playing for over a minute: `a`'s own start estimate makes it
    /// overlap that recording far longer than any transition.
    fn overlapped_by(&self, a: &Anchor) -> Option<String> {
        let start = a.start_est?;
        let lo = start + OVERLAP_SLACK_MS;
        let hi = a.at_ms as i64 - OVERLAP_SLACK_MS / 2;
        if hi - lo < OVERLAP_SLACK_MS {
            return None;
        }
        self.anchors
            .iter()
            .filter(|b| {
                b.id != a.id
                    && !self.refuted.contains(&b.id)
                    && !hints::same_song(&b.track, &a.track)
                    && (b.seen_until() as i64) > lo
                    && (b.seen_from() as i64) < hi
                    && (b.kind == AnchorKind::Local || self.is_supported(b))
            })
            .map(|b| format!("{} – {}", b.track.artist, b.track.title))
            .next()
    }

    /// Long unexplained music after the probes: one more probe elsewhere,
    /// then the paid providers.
    async fn resolve_leftovers(&mut self) -> Result<()> {
        let pieces = self.timeline();
        let open: Vec<(u64, u64)> = pieces
            .iter()
            .filter(|p| p.kind == SegmentKind::Unidentified && p.len_ms() >= 45_000)
            .map(|p| (p.start_ms, p.end_ms))
            .collect();
        for (lo, hi) in open {
            self.services.cancel().check()?;
            let at = lo + (hi - lo) / 3;
            if self.music_share(at, at + PROBE_MS) < 0.4 {
                continue;
            }
            let near = self.misses.iter().any(|(m, _)| m.abs_diff(at) < 15_000);
            let mut found = None;
            if !near && self.can_probe() {
                found = self.probe(at).await?;
            }
            if found.is_none() && self.services.has_paid() && self.report.paid < MAX_PAID {
                let mid = lo + (hi - lo) / 2;
                let samples = self.pcm.read_ms(self.snap(mid), PROBE_MS)?;
                self.report.paid += 1;
                if let Some(hit) = self.services.paid(&samples).await? {
                    found = Some(self.anchor_from_hit(self.snap(mid), PROBE_MS, hit).await);
                }
            }
            if let Some(a) = found {
                self.anchors.push(a);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Timeline

    /// The current timeline: settled hint pieces, anchor pieces with their
    /// boundaries, and gaps labelled from the audio; stitched.
    fn timeline(&self) -> Vec<Piece> {
        let mut pieces: Vec<Piece> = self.fixed.clone();
        let mut anchors: Vec<&Anchor> = self
            .anchors
            .iter()
            .filter(|a| !self.is_fixed(a.at_ms + a.len_ms / 2) && !self.refuted.contains(&a.id))
            .collect();
        anchors.sort_by_key(|a| a.at_ms);
        let anchors = drop_contradicted(anchors);

        // Free stretches (outside settled pieces) are handled one by one.
        for (lo, hi) in self.free_stretches() {
            let inside: Vec<&Anchor> = anchors
                .iter()
                .copied()
                .filter(|a| a.at_ms + a.len_ms / 2 >= lo && a.at_ms + a.len_ms / 2 < hi)
                .collect();
            pieces.extend(self.anchor_pieces(&inside, lo, hi));
        }
        let mut stitched = stitch::stitch(pieces).pieces;
        // Fill holes with labelled gaps.
        let mut out: Vec<Piece> = Vec::new();
        let mut cursor = 0;
        for piece in stitched.drain(..) {
            if piece.start_ms > cursor {
                out.extend(self.gap_pieces(cursor, piece.start_ms));
            }
            cursor = cursor.max(piece.end_ms);
            out.push(piece);
        }
        if cursor < self.duration {
            out.extend(self.gap_pieces(cursor, self.duration));
        }
        stitch::stitch(out).pieces
    }

    /// Stretches of the media not covered by settled pieces.
    fn free_stretches(&self) -> Vec<(u64, u64)> {
        let mut fixed: Vec<(u64, u64)> = self.fixed.iter().map(|p| (p.start_ms, p.end_ms)).collect();
        fixed.sort_unstable();
        let mut out = Vec::new();
        let mut cursor = 0;
        for (lo, hi) in fixed {
            if lo > cursor {
                out.push((cursor, lo));
            }
            cursor = cursor.max(hi);
        }
        if cursor < self.duration {
            out.push((cursor, self.duration));
        }
        out
    }

    /// Track pieces for the anchors of one free stretch `[lo, hi)`.
    fn anchor_pieces(&self, anchors: &[&Anchor], lo: u64, hi: u64) -> Vec<Piece> {
        if anchors.is_empty() {
            return Vec::new();
        }
        let mut bounds: Vec<(u64, u64)> = Vec::with_capacity(anchors.len());
        // Start of the first piece.
        let first = anchors[0];
        let mut start = first.start_est.map_or(first.at_ms, |s| s.clamp(lo as i64, first.at_ms as i64) as u64);
        if start.saturating_sub(lo) < EDGE_ABSORB_MS {
            start = lo;
        }
        let mut current_start = start;
        for k in 0..anchors.len() {
            let a = anchors[k];
            let (end, next_start) = match anchors.get(k + 1) {
                Some(b) => self.boundary(a, b),
                None => {
                    let seen = a.seen_until().min(hi);
                    let end = match a.end_est {
                        Some(e) if (e.max(seen as i64) as u64) + EDGE_ABSORB_MS < hi => e.max(seen as i64) as u64,
                        _ => hi,
                    };
                    (end, hi)
                }
            };
            let end = end.clamp(current_start, hi);
            bounds.push((current_start, end));
            current_start = next_start.clamp(end, hi);
        }
        anchors
            .iter()
            .zip(bounds)
            .map(|(a, (s, e))| Piece {
                id: a.id.clone(),
                item_index: self.options.item_index,
                start_ms: s,
                end_ms: e,
                kind: SegmentKind::Track,
                track: Some(a.track.clone()),
                evidence: vec![a.evidence.clone()],
                alternates: Vec::new(),
                track_start_ms: a.start_est,
            })
            .collect()
    }

    /// Where `a` ends and where `b` begins (equal unless an unexplained
    /// stretch lies between them).
    fn boundary(&self, a: &Anchor, b: &Anchor) -> (u64, u64) {
        let lo = a.seen_until();
        let hi = b.seen_from().max(lo);
        if hints::same_recording(&a.track, &b.track) {
            let t = if is_replay(a, b) {
                b.start_est.map_or(hi, |s| (s.max(lo as i64) as u64).min(hi))
            } else {
                lo + (hi - lo) / 2
            };
            return (t, t);
        }
        // Played in full and cut: `a` ends exactly where `b` begins.
        if let Some(s) = hard_cut(a, b).filter(|s| *s >= lo && *s <= hi) {
            return (s, s);
        }
        let in_range = |t: i64| t >= lo as i64 && t <= hi as i64;
        let a_end = a.end_est.filter(|e| in_range(*e)).map(|e| e as u64);
        let b_start = b.start_est.filter(|s| in_range(*s)).map(|s| s as u64);
        // An unexplained stretch between the two recordings' own extents.
        if let (Some(e), Some(s)) = (a_end, b_start)
            && s >= e + GAP_PROBE_MS
        {
            return (e, s);
        }
        let (lo, hi) = self.transition(a, b);
        let t = lo + (hi - lo) / 2;
        (t, t)
    }

    /// Where `b` takes over from `a`: after `a` was last heard and `b` began,
    /// before `b` was first heard and `a` ended. DJs blend for a minute or
    /// more; published tracklists mark the middle of the blend more often than
    /// its start, so the middle of this stretch is the estimate (and the place
    /// to listen when refining it).
    fn transition(&self, a: &Anchor, b: &Anchor) -> (u64, u64) {
        let lo = a.seen_until();
        let hi = b.seen_from().max(lo);
        let in_range = |t: i64| t >= lo as i64 && t <= hi as i64;
        let start = b.start_est.filter(|s| in_range(*s)).map_or(lo, |s| s as u64);
        let end = a.end_est.filter(|e| in_range(*e) && *e as u64 >= start).map_or(hi, |e| e as u64);
        (start, end.max(start))
    }

    /// Note how each change between two recordings was placed (evaluation aid).
    fn log_boundaries(&mut self) {
        let mut anchors: Vec<&Anchor> = self
            .anchors
            .iter()
            .filter(|a| !self.is_fixed(a.at_ms + a.len_ms / 2) && !self.refuted.contains(&a.id))
            .collect();
        anchors.sort_by_key(|a| a.at_ms);
        let anchors = drop_contradicted(anchors);
        let fmt = |t: Option<i64>| t.map_or("?".to_string(), |t| clock(t.max(0) as u64));
        let mut lines = Vec::new();
        for pair in anchors.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            if hints::same_recording(&a.track, &b.track) && !is_replay(a, b) {
                continue;
            }
            let (end, start) = self.boundary(a, b);
            let peaks: Vec<String> = self
                .novelty_peaks
                .iter()
                .filter(|p| **p + 60_000 >= a.seen_until() && **p <= b.seen_from() + 60_000)
                .map(|p| {
                    format!("{}({:.2})", clock(*p), self.novelty.get((*p / 1000) as usize).copied().unwrap_or(0.0))
                })
                .collect();
            lines.push(format!(
                "change {} → {}: heard until {}, from {}, ends ≤ {}, starts {} → {}{}; novelty {}",
                a.track.title,
                b.track.title,
                clock(a.seen_until()),
                clock(b.seen_from()),
                fmt(a.end_est),
                fmt(b.start_est),
                clock(end),
                if start != end { format!("/{}", clock(start)) } else { String::new() },
                peaks.join(" ")
            ));
        }
        for line in lines {
            self.log(line);
        }
    }

    /// Gap pieces for `[lo, hi)` from the labels: speech and silence runs that
    /// are long enough keep their kind, the rest is unidentified.
    fn gap_pieces(&self, lo: u64, hi: u64) -> Vec<Piece> {
        let lo_s = (lo / 1000) as usize;
        let hi_s = (hi.div_ceil(1000) as usize).min(self.labels.len());
        let mut out: Vec<Piece> = Vec::new();
        let kind_of = |l: Label, len_s: usize| match l {
            Label::Speech if len_s >= 15 => SegmentKind::Speech,
            Label::Silence if len_s >= 4 => SegmentKind::Silence,
            _ => SegmentKind::Unidentified,
        };
        if hi_s <= lo_s {
            if hi > lo {
                out.push(self.gap(lo, hi, SegmentKind::Unidentified));
            }
            return out;
        }
        for (label, s, e) in segment::runs(&self.labels[lo_s..hi_s]) {
            let kind = kind_of(label, e - s);
            let start = (lo_s + s) as u64 * 1000;
            let end = ((lo_s + e) as u64 * 1000).min(hi);
            let start = start.max(lo);
            match out.last_mut() {
                Some(last) if last.kind == kind && last.end_ms >= start => last.end_ms = end,
                _ => out.push(self.gap(start, end, kind)),
            }
        }
        out.retain(|p| p.end_ms > p.start_ms);
        out
    }

    fn gap(&self, lo: u64, hi: u64, kind: SegmentKind) -> Piece {
        let mut piece = Piece::gap(format!("{}gap-{lo}", self.options.id_prefix), lo, hi, kind);
        piece.item_index = self.options.item_index;
        piece
    }

    // -----------------------------------------------------------------------
    // Events

    /// Emit the changed parts of the timeline and progress up to `analyzed_ms`.
    fn publish(&mut self, analyzed_ms: u64) {
        let pieces = self.timeline();
        self.publish_pieces(&pieces, analyzed_ms);
    }

    fn publish_pieces(&mut self, pieces: &[Piece], analyzed_ms: u64) {
        self.analyzed = self.analyzed.max(analyzed_ms.min(self.duration));
        let analyzed_ms = self.analyzed;
        let mut seen = std::collections::HashSet::new();
        for piece in pieces {
            let link = self.options.source_url.as_deref().and_then(|u| crate::links::deep_link(u, piece.start_ms));
            let segment = piece.to_segment(link);
            seen.insert(segment.id.clone());
            if self.emitted.get(&segment.id) != Some(&segment) {
                self.emitter.segment(segment.clone());
                self.emitted.insert(segment.id.clone(), segment);
            }
        }
        let gone: Vec<String> = self.emitted.keys().filter(|id| !seen.contains(*id)).cloned().collect();
        for id in gone {
            self.emitted.remove(&id);
            self.emitter.segment_removed(&id);
        }
        let per_probe =
            if self.report.probes > 0 { self.probe_time / self.report.probes } else { Duration::from_secs(10) };
        let remaining = (self.duration.saturating_sub(analyzed_ms) / MAX_STEP_MS) as u32;
        let eta = per_probe * remaining.min(self.budget.saturating_sub(self.report.probes));
        self.emitter.progress(analyzed_ms, self.duration, Some(eta.as_millis() as u64));
    }
}

/// Where a local preview match sits in the media.
#[derive(Debug, Clone, Copy)]
struct LocalHit {
    /// Matched stretch in media time.
    span: (u64, u64),
    speed: f32,
    score: u32,
    coverage: f32,
}

impl LocalHit {
    fn new(m: &fingerprint::Match, region_start_ms: u64) -> Self {
        let to_ms = |f: u32| region_start_ms + (f as f64 * FRAME_MS) as u64;
        LocalHit {
            span: (to_ms(m.span.0), to_ms(m.span.1)),
            speed: m.warp.speed_change(),
            score: m.score,
            coverage: m.coverage,
        }
    }
}

/// Index of the first entry from which a tracklist no longer describes the
/// media: three of the last six settled entries were overruled by the audio.
fn unreliable_from(outcomes: &[(String, bool, Vec<Anchor>)]) -> Option<usize> {
    let window = outcomes.len().saturating_sub(UNRELIABLE_WINDOW);
    let recent = &outcomes[window..];
    if recent.iter().filter(|o| o.1).count() < UNRELIABLE_CONTRADICTIONS {
        return None;
    }
    recent.iter().position(|o| o.1).map(|k| window + k)
}

/// A recognizer probe point inside `[start, end)`: halfway, but 20–60 s in.
fn probe_point(start: u64, end: u64) -> u64 {
    let len = end.saturating_sub(start);
    if len < PROBE_MS + 4_000 {
        return start;
    }
    let into = (len / 2).clamp(20_000, 60_000).min(len.saturating_sub(PROBE_MS));
    start + into
}

/// Next probe after a match: just after the recording's latest end (where the
/// next one must be playing), at most 150 s and at least 30 s ahead; 90 s when
/// the duration is unknown or contradicted (heard past its supposed end).
fn next_probe(a: &Anchor) -> u64 {
    let t = a.at_ms;
    match a.end_est {
        Some(e) if e > (a.at_ms + a.len_ms) as i64 => {
            let after_end = (e + END_MARGIN_MS) as u64;
            // A near end is reached in one jump (one probe per fully played
            // track); a far one (an extended mix a DJ cuts short) is not.
            let reach = if after_end <= t + MAX_JUMP_TO_END_MS { after_end } else { t + MAX_STEP_MS };
            reach.max(t + MIN_STEP_MS)
        }
        _ => t + UNKNOWN_STEP_MS,
    }
}

/// Speed and pitch changes playback can produce: none, both together (a
/// turntable or pitch fader), or tempo alone (key lock), within ±8.5 %. A
/// recognizer answer outside these usually heard a sample inside another record.
fn plausible_skew(speed: f32, pitch: f32) -> bool {
    speed.abs() <= MAX_DJ_SKEW && ((speed - pitch).abs() <= SKEW_TOLERANCE || pitch.abs() <= SKEW_TOLERANCE)
}

/// `a` (played to its end) stops where `b` (played from its beginning) starts:
/// a hard cut, as in albums and compilations. Returns that time.
fn hard_cut(a: &Anchor, b: &Anchor) -> Option<u64> {
    let (end, start) = (a.end_est?, b.start_est?);
    ((end - start).abs() <= HARD_CUT_MS && start >= 0).then_some(start as u64)
}

/// Two answers place the recording's start within a few seconds of each other.
fn consistent_start(a: &Anchor, b: &Anchor) -> bool {
    matches!((a.start_est, b.start_est), (Some(x), Some(y)) if (x - y).abs() <= stitch::SAME_START_MS)
}

/// `b` is a fresh play of `a`'s recording: it restarts after `a` was seen and
/// after `a`'s play could have ended. A start estimate that merely jumps
/// (loops, repeated sections matching each other) is the same play.
fn is_replay(a: &Anchor, b: &Anchor) -> bool {
    match (a.start_est, b.start_est, a.end_est) {
        (Some(x), Some(y), Some(end)) => {
            y - x > stitch::SAME_START_MS
                && y >= a.seen_until() as i64 - stitch::SAME_START_MS
                && y >= end - REPLAY_SLACK_MS
        }
        _ => false,
    }
}

/// Drop a lone recognizer answer that sits inside one continuous play of
/// another recording (a sample or a misidentification).
fn drop_contradicted(anchors: Vec<&Anchor>) -> Vec<&Anchor> {
    let mut out = Vec::with_capacity(anchors.len());
    for (k, a) in anchors.iter().enumerate() {
        if k > 0 && k + 1 < anchors.len() {
            let (p, n) = (anchors[k - 1], anchors[k + 1]);
            let same_play = hints::same_recording(&p.track, &n.track)
                && !hints::same_song(&p.track, &a.track)
                && matches!((p.start_est, n.start_est), (Some(x), Some(y)) if (x - y).abs() <= stitch::SAME_START_MS);
            let loose = a.evidence.score < 0.8 || a.kind == AnchorKind::Local;
            if same_play && (loose || a.len_ms <= PROBE_MS) {
                continue;
            }
        }
        out.push(*a);
    }
    out
}

/// Copy fields `target` lacks from `source` (same recording).
fn fill_from(target: &mut Track, source: &Track) {
    macro_rules! fill {
        ($f:ident) => {
            if target.$f.is_none() {
                target.$f = source.$f.clone();
            }
        };
    }
    fill!(album);
    fill!(isrc);
    fill!(duration_ms);
    fill!(release_year);
    fill!(genre);
    fill!(artwork_url);
    fill!(preview_url);
    for (k, v) in &source.ids {
        target.ids.entry(k.clone()).or_insert_with(|| v.clone());
    }
    for link in &source.links {
        match target.links.iter_mut().find(|l| l.service == link.service) {
            Some(existing) if existing.is_search && !link.is_search => *existing = link.clone(),
            Some(_) => {}
            None => target.links.push(link.clone()),
        }
    }
    crate::identity::refresh_identity(target);
}

/// "m:ss" or "h:mm:ss".
pub fn clock(ms: u64) -> String {
    let s = ms / 1000;
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
    } else {
        format!("{}:{:02}", s / 60, s % 60)
    }
}

/// True for a single recognizer answer with a tight skew (exact recording).
pub fn is_tight(hit: &RecognitionHit) -> bool {
    hit.skew() <= TIGHT_SKEW
}

#[cfg(test)]
mod tests;
