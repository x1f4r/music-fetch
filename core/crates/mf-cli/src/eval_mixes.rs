//! `mf eval mixes` and `mf eval mix-draft`: link analysis accuracy for mixes,
//! single songs and playlists against a manifest of public cases with ground
//! truth (tools/eval/mixes/manifest.json).
//!
//! Every Shazam answer is cached in `--cache-db` (by signature) and kept, so
//! repeated runs replay from the cache; new probe positions are paced at
//! `--spacing` seconds. Downloads and decoded audio are kept in `--media-cache`
//! and previews in `--preview-cache`. Reports (Markdown + JSON) go to
//! `--report-dir`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use clap::{Args, ValueEnum};
use mfcore::media::{YtDlpSubprocess, parse_probe};
use mfcore::model::{AnalyzeMode, Segment, SegmentKind, SessionEvent, SessionKind, Track};
use mfcore::pipeline::analyze::{self, AnalyzeInput, AnalyzeOptions, Report, Services, SessionServices};
use mfcore::pipeline::ctx::{EngineCtx, SessionCtx};
use mfcore::pipeline::emitter::Emitter;
use mfcore::pipeline::hints;
use mfcore::pipeline::recognize::{self, CallBudget, RecognitionHit};
use mfcore::platform::{MediaBackend, SessionObserver};
use mfcore::shazam::{Priority, QueuePolicy, ShazamClient};
use mfcore::store::Store;
use mfcore::util::cancel::{CancelToken, TaskHandle};
use mfcore::{Credentials, EngineConfig, MediaToolsConfig, MfError};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
pub enum Mode {
    /// Hints disabled: the audio alone.
    Blind,
    /// The product default.
    Hinted,
    Both,
}

#[derive(Args)]
pub struct MixesArgs {
    /// Manifest JSON (tools/eval/mixes/manifest.json).
    manifest: PathBuf,
    #[arg(long, value_enum, default_value_t = Mode::Both)]
    mode: Mode,
    /// Only these case ids (repeatable).
    #[arg(long = "case")]
    cases: Vec<String>,
    /// Only cases of this kind: mix, single, playlist.
    #[arg(long)]
    kind: Option<String>,
    /// Kinds also run blind (repeatable; playlists without metadata are slow).
    #[arg(long = "blind-kind", default_values_t = ["mix".to_string(), "single".to_string()])]
    blind_kinds: Vec<String>,
    /// Persistent answer cache [default: $MF_EVAL_CACHE_DB or ./mix-eval/cache.sqlite3].
    #[arg(long)]
    cache_db: Option<PathBuf>,
    /// Probes and decoded audio [default: next to the cache DB, media/].
    #[arg(long)]
    media_cache: Option<PathBuf>,
    /// Decoded previews [default: next to the cache DB, previews/].
    #[arg(long)]
    preview_cache: Option<PathBuf>,
    /// Where reports go [default: $MF_EVAL_REPORT_DIR or ./mix-eval/reports].
    #[arg(long)]
    report_dir: Option<PathBuf>,
    /// Seconds between Shazam requests that miss the cache.
    #[arg(long, default_value_t = 10.0)]
    spacing: f64,
    /// Never call Shazam: uncached probes count as "no match" (and are reported).
    #[arg(long)]
    cached_only: bool,
    /// Print each case's decision log.
    #[arg(short, long)]
    verbose: bool,
    #[command(flatten)]
    tools: ToolArgs,
}

#[derive(Args)]
pub struct DraftArgs {
    /// Link to a mix whose description or comments carry a tracklist.
    url: String,
    #[command(flatten)]
    tools: ToolArgs,
}

#[derive(Args, Clone)]
pub struct ToolArgs {
    /// yt-dlp executable.
    #[arg(long, default_value = "yt-dlp")]
    yt_dlp: String,
    /// JavaScript runtime for yt-dlp, repeatable ("deno:/path/deno").
    #[arg(long = "js-runtime")]
    js_runtimes: Vec<String>,
}

impl ToolArgs {
    fn config(&self) -> MediaToolsConfig {
        MediaToolsConfig {
            python_path: None,
            yt_dlp_path: self.yt_dlp.clone(),
            js_runtimes: self.js_runtimes.clone(),
            env: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Manifest

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub cases: Vec<Case>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseKind {
    Mix,
    Single,
    Playlist,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Case {
    pub id: String,
    pub kind: CaseKind,
    pub url: String,
    #[serde(default)]
    pub note: Option<String>,
    /// Mixes: the ground-truth tracklist.
    #[serde(default)]
    pub tracks: Vec<TruthTrack>,
    /// Singles: the song.
    #[serde(default)]
    pub expect: Option<TruthSong>,
    /// Playlists: entries in order (null for an entry that is not a song).
    #[serde(default)]
    pub entries: Vec<Option<TruthSong>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TruthSong {
    pub artist: String,
    pub title: String,
    /// Other names the same recording goes by.
    #[serde(default)]
    pub aka: Vec<Alias>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Alias {
    pub artist: String,
    pub title: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TruthTrack {
    /// Start, "m:ss" or "h:mm:ss".
    pub t: String,
    pub artist: String,
    pub title: String,
    /// An unreleased/unknown track ("ID - ID") or a spoken intro: not scored.
    #[serde(default)]
    pub id: bool,
    /// Talk (an interview): a song predicted here is a false positive.
    #[serde(default)]
    pub speech: bool,
    /// The published entry could not be confirmed.
    #[serde(default)]
    pub uncertain: bool,
    #[serde(default)]
    pub aka: Vec<Alias>,
}

fn parse_clock(text: &str) -> anyhow::Result<u64> {
    let parts: Vec<u64> = text.trim().split(':').map(|p| p.parse::<u64>()).collect::<Result<_, _>>()?;
    let s = match parts.as_slice() {
        [m, s] => m * 60 + s,
        [h, m, s] => (h * 60 + m) * 60 + s,
        _ => bail!("bad time {text:?}"),
    };
    Ok(s * 1000)
}

fn song_tracks(artist: &str, title: &str, aka: &[Alias]) -> Vec<Track> {
    let mut out = vec![Track { title: title.into(), artist: artist.into(), ..Track::default() }];
    out.extend(aka.iter().map(|a| Track { title: a.title.clone(), artist: a.artist.clone(), ..Track::default() }));
    out
}

fn same_song(truth: &[Track], predicted: &Track) -> bool {
    truth.iter().any(|t| hints::same_song(t, predicted))
}

fn same_version(truth: &[Track], predicted: &Track) -> bool {
    truth.iter().any(|t| {
        hints::same_song(t, predicted)
            && hints::versions_compatible(&hints::version_key(&t.title), &hints::version_key(&predicted.title))
    })
}

// ---------------------------------------------------------------------------
// Running

/// Observer that drops events (the report carries the result).
struct Quiet;

impl SessionObserver for Quiet {
    fn on_event(&self, _event: SessionEvent) {}
}

/// Session services that can refuse network recognition and count probes.
struct EvalServices<'a> {
    inner: SessionServices<'a>,
    cached_only: bool,
    uncached: AtomicU32,
}

impl Services for EvalServices<'_> {
    async fn shazam(&self, samples: &[i16], priority: Priority) -> mfcore::error::Result<Option<RecognitionHit>> {
        match self.inner.shazam(samples, priority).await {
            Err(MfError::Network { .. }) if self.cached_only => {
                self.uncached.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            other => other,
        }
    }

    async fn paid(&self, samples: &[i16]) -> mfcore::error::Result<Option<RecognitionHit>> {
        self.inner.paid(samples).await
    }

    fn has_paid(&self) -> bool {
        self.inner.has_paid()
    }

    async fn find(&self, title: &str, artist: &str, duration_ms: Option<u64>) -> Option<Track> {
        self.inner.find(title, artist, duration_ms).await
    }

    async fn enrich(&self, track: Track) -> Track {
        self.inner.enrich(track).await
    }

    async fn preview(&self, track: &Track) -> Option<Vec<i16>> {
        self.inner.preview(track).await
    }

    fn cancel(&self) -> &CancelToken {
        self.inner.cancel()
    }
}

struct Paths {
    cache_db: PathBuf,
    media: PathBuf,
    previews: PathBuf,
    reports: PathBuf,
    work: PathBuf,
}

fn paths(args: &MixesArgs) -> Paths {
    let cache_db = args
        .cache_db
        .clone()
        .or_else(|| std::env::var_os("MF_EVAL_CACHE_DB").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("mix-eval/cache.sqlite3"));
    let base = cache_db.parent().map(Path::to_path_buf).unwrap_or_default();
    Paths {
        media: args.media_cache.clone().unwrap_or_else(|| base.join("media")),
        previews: args.preview_cache.clone().unwrap_or_else(|| base.join("previews")),
        reports: args
            .report_dir
            .clone()
            .or_else(|| std::env::var_os("MF_EVAL_REPORT_DIR").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("mix-eval/reports")),
        work: base.join("work"),
        cache_db,
    }
}

fn engine(args: &MixesArgs, paths: &Paths) -> anyhow::Result<Arc<EngineCtx>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let version = mfcore::core_version();
    let http = mfcore::links::http_client(&version)?;
    let store = Arc::new(Store::open(&paths.cache_db)?);
    // Never below 9 s between requests that reach the service; a cache-only
    // run never reaches it, so it needs no pacing at all.
    let spacing =
        if args.cached_only { Duration::from_millis(1) } else { Duration::from_secs_f64(args.spacing.max(9.0)) };
    let mut shazam = ShazamClient::new(http.clone(), "US", "en-US").with_policy(QueuePolicy {
        interactive_spacing: spacing,
        batch_spacing: spacing,
        jitter: if args.cached_only { Duration::ZERO } else { Duration::from_secs(1) },
        batch_max_wait: Duration::from_secs(30 * 60),
        interactive_max_wait: Duration::from_secs(30 * 60),
        retry_delays: if args.cached_only { Vec::new() } else { QueuePolicy::default().retry_delays },
        ..QueuePolicy::default()
    });
    if args.cached_only {
        // Nothing listens there: every uncached probe fails fast.
        shazam = shazam.with_base_url("http://127.0.0.1:9");
    }
    let tools = args.tools.config();
    let media: Arc<dyn MediaBackend> =
        Arc::new(YtDlpSubprocess::new(tools.clone()).with_temp_dir(paths.work.join("ytdlp")));
    Ok(Arc::new(EngineCtx {
        config: EngineConfig {
            data_dir: paths.work.to_string_lossy().into_owned(),
            cache_dir: paths.work.to_string_lossy().into_owned(),
            country: "US".into(),
            language: "en-US".into(),
            app_version: version,
            media_tools: Some(tools),
            melody_index_manifest_url: String::new(),
        },
        http,
        store,
        shazam: Arc::new(shazam),
        budget: if args.cached_only { CallBudget::new(1e9, spacing) } else { CallBudget::new(4.0, spacing) },
        credentials: parking_lot::RwLock::new(Credentials::default()),
        melody: parking_lot::RwLock::new(None),
        pitch: OnceLock::new(),
        media: Some(media),
        decoder: None,
        speech: None,
        runtime: tokio::runtime::Handle::current(),
    }))
}

/// One case run in one mode.
#[derive(Debug, Clone, Serialize)]
struct CaseRun {
    id: String,
    kind: String,
    mode: String,
    duration_s: f64,
    strategy: String,
    probes: u32,
    uncached: u32,
    wall_s: f64,
    error: Option<String>,
    metrics: Metrics,
    segments: Vec<SegmentOut>,
    log: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SegmentOut {
    item: u32,
    start: String,
    end: String,
    kind: String,
    artist: String,
    title: String,
    tier: String,
    confidence: f32,
}

fn segment_out(s: &Segment) -> SegmentOut {
    SegmentOut {
        item: s.item_index,
        start: clock(s.start_ms),
        end: clock(s.end_ms),
        kind: format!("{:?}", s.kind),
        artist: s.track.as_ref().map(|t| t.artist.clone()).unwrap_or_default(),
        title: s.track.as_ref().map(|t| t.title.clone()).unwrap_or_default(),
        tier: format!("{:?}", s.tier),
        confidence: s.confidence,
    }
}

pub async fn run(args: MixesArgs) -> anyhow::Result<()> {
    let manifest: Manifest = serde_json::from_str(
        &std::fs::read_to_string(&args.manifest).with_context(|| format!("reading {}", args.manifest.display()))?,
    )
    .context("parsing the manifest")?;
    let paths = paths(&args);
    for dir in [&paths.media, &paths.previews, &paths.reports, &paths.work] {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    recognize::keep_cached_answers_for(Duration::from_secs(10 * 365 * 24 * 3600));
    let engine = engine(&args, &paths)?;
    let modes: Vec<Mode> = match args.mode {
        Mode::Both => vec![Mode::Hinted, Mode::Blind],
        m => vec![m],
    };
    let cancel = CancelToken::new();
    let on_interrupt = cancel.clone();
    ctrlc::set_handler(move || {
        if on_interrupt.is_canceled() {
            std::process::exit(130);
        }
        on_interrupt.cancel();
    })?;
    let mut runs: Vec<CaseRun> = Vec::new();
    for case in &manifest.cases {
        if !args.cases.is_empty() && !args.cases.contains(&case.id) {
            continue;
        }
        let kind = format!("{:?}", case.kind).to_lowercase();
        if args.kind.as_ref().is_some_and(|k| k != &kind) {
            continue;
        }
        for mode in &modes {
            if cancel.is_canceled() {
                break;
            }
            if *mode == Mode::Blind && !args.blind_kinds.contains(&kind) {
                continue;
            }
            eprintln!("── {} [{}] {:?}", case.id, kind, mode);
            let run = run_case(&engine, &paths, &args, case, *mode, &cancel).await;
            eprintln!("   {}", summary_line(&run));
            if args.verbose {
                for line in &run.log {
                    eprintln!("     {line}");
                }
            }
            runs.push(run);
        }
    }
    write_reports(&paths.reports, &runs)?;
    Ok(())
}

async fn run_case(
    engine: &Arc<EngineCtx>,
    paths: &Paths,
    args: &MixesArgs,
    case: &Case,
    mode: Mode,
    cancel: &CancelToken,
) -> CaseRun {
    let id = mfcore::util::new_id();
    let kind = format!("{:?}", case.kind).to_lowercase();
    let mut out = CaseRun {
        id: case.id.clone(),
        kind: kind.clone(),
        mode: format!("{mode:?}").to_lowercase(),
        duration_s: 0.0,
        strategy: String::new(),
        probes: 0,
        uncached: 0,
        wall_s: 0.0,
        error: None,
        metrics: Metrics::default(),
        segments: Vec::new(),
        log: Vec::new(),
    };
    if let Err(e) =
        engine.store.create_session(&id, SessionKind::Link, &case.id, Some(&case.url), mfcore::util::now_ms())
    {
        out.error = Some(e.to_string());
        return out;
    }
    let emitter = Emitter::new(&id, Arc::new(Quiet), engine.store.clone());
    emitter.started(SessionKind::Link);
    let ctx = SessionCtx {
        engine: engine.clone(),
        id: id.clone(),
        cancel: cancel.clone(),
        credentials: Credentials::default(),
        emitter,
        work_dir: paths.work.join("sessions").join(&id),
    };
    let services = EvalServices {
        inner: SessionServices::new(&ctx).with_preview_cache(Some(paths.previews.clone())),
        cached_only: args.cached_only,
        uncached: AtomicU32::new(0),
    };
    let options = AnalyzeOptions { use_hints: mode != Mode::Blind, media_cache: Some(paths.media.clone()) };
    let started = Instant::now();
    let result =
        analyze::run_with(&ctx, &services, AnalyzeInput::Url(case.url.clone()), AnalyzeMode::Auto, &options).await;
    ctx.cleanup();
    let _ = engine.store.delete_sessions(std::slice::from_ref(&id));
    out.wall_s = started.elapsed().as_secs_f64();
    out.uncached = services.uncached.load(Ordering::Relaxed);
    match result {
        Ok(report) => {
            out.duration_s = report.duration_ms.unwrap_or(0) as f64 / 1000.0;
            out.strategy = format!("{:?}", report.strategy);
            out.probes = report.probes;
            out.segments = report.segments.iter().map(segment_out).collect();
            out.log = report.log.clone();
            out.metrics = match evaluate(case, &report) {
                Ok(m) => m,
                Err(e) => {
                    out.error = Some(format!("ground truth: {e}"));
                    Metrics::default()
                }
            };
            out.metrics.calls_per_hour =
                if out.duration_s > 0.0 { report.probes as f64 / (out.duration_s / 3600.0) } else { 0.0 };
        }
        Err(e) => out.error = Some(e.to_string()),
    }
    out
}

// ---------------------------------------------------------------------------
// Metrics

#[derive(Debug, Clone, Default, Serialize)]
pub struct Metrics {
    /// Mixes: ground-truth tracks (IDs excluded) and Track segments predicted.
    truth: usize,
    predicted: usize,
    recall: f64,
    recall_version: f64,
    precision: f64,
    precision_version: f64,
    start_error_median_s: f64,
    start_error_p90_s: f64,
    /// Median of predicted minus true start (negative: predictions come early).
    start_bias_s: f64,
    splits: usize,
    merges: usize,
    /// Singles: segments beyond one, and whether the song is right.
    single_split: bool,
    correct: bool,
    correct_version: bool,
    /// Playlists: entries right / total.
    entries: usize,
    entries_correct: usize,
    entries_correct_version: usize,
    entries_with_audio: usize,
    calls_per_hour: f64,
    /// Ground-truth tracks missed and predictions without a true counterpart.
    missed: Vec<String>,
    false_positives: Vec<String>,
    start_errors: Vec<(String, f64)>,
}

fn evaluate(case: &Case, report: &Report) -> anyhow::Result<Metrics> {
    match case.kind {
        CaseKind::Mix => evaluate_mix(case, report),
        CaseKind::Single => Ok(evaluate_single(case, report)),
        CaseKind::Playlist => Ok(evaluate_playlist(case, report)),
    }
}

struct Truth {
    start: u64,
    end: u64,
    tracks: Vec<Track>,
    /// Not a scored song (an ID, a spoken intro or talk).
    is_id: bool,
    /// Talk: predictions here are not excused.
    speech: bool,
    label: String,
}

fn overlap(a: (u64, u64), b: (u64, u64)) -> u64 {
    a.1.min(b.1).saturating_sub(a.0.max(b.0))
}

fn evaluate_mix(case: &Case, report: &Report) -> anyhow::Result<Metrics> {
    let duration = report.duration_ms.unwrap_or(0);
    let mut truth: Vec<Truth> = Vec::new();
    for (k, t) in case.tracks.iter().enumerate() {
        let start = parse_clock(&t.t)?;
        let end = match case.tracks.get(k + 1) {
            Some(n) => parse_clock(&n.t)?,
            None => duration.max(start),
        };
        truth.push(Truth {
            start,
            end,
            tracks: song_tracks(&t.artist, &t.title, &t.aka),
            is_id: t.id || t.speech,
            speech: t.speech,
            label: format!("{} {} – {}", clock(start), t.artist, t.title),
        });
    }
    let predicted: Vec<&Segment> =
        report.segments.iter().filter(|s| s.kind == SegmentKind::Track && s.track.is_some()).collect();
    let mut m = Metrics::default();
    let real: Vec<usize> = (0..truth.len()).filter(|i| !truth[*i].is_id).collect();
    m.truth = real.len();

    // One-to-one assignment by overlap, same song required.
    let mut pairs: Vec<(u64, usize, usize)> = Vec::new();
    for &g in &real {
        for (p, s) in predicted.iter().enumerate() {
            let track = s.track.as_ref().expect("filtered");
            let o = overlap((truth[g].start, truth[g].end), (s.start_ms, s.end_ms));
            if o > 0 && same_song(&truth[g].tracks, track) {
                pairs.push((o, g, p));
            }
        }
    }
    pairs.sort_by_key(|p| std::cmp::Reverse(p.0));
    let mut truth_match: HashMap<usize, usize> = HashMap::new();
    let mut pred_match: HashMap<usize, usize> = HashMap::new();
    for (_, g, p) in &pairs {
        if truth_match.contains_key(g) || pred_match.contains_key(p) {
            continue;
        }
        truth_match.insert(*g, *p);
        pred_match.insert(*p, *g);
    }
    let matched = truth_match.len();
    let matched_version = truth_match
        .iter()
        .filter(|(g, p)| same_version(&truth[**g].tracks, predicted[**p].track.as_ref().expect("filtered")))
        .count();
    // Precision: predictions inside ID regions are neither right nor wrong.
    let mut counted = 0usize;
    let mut correct = 0usize;
    let mut correct_version = 0usize;
    for (p, s) in predicted.iter().enumerate() {
        let span = (s.start_ms, s.end_ms);
        let len = (s.end_ms - s.start_ms).max(1);
        let in_ids: u64 = truth.iter().filter(|t| t.is_id && !t.speech).map(|t| overlap(span, (t.start, t.end))).sum();
        let track = s.track.as_ref().expect("filtered");
        let fits = |t: &Truth| overlap(span, (t.start, t.end)) > 0 && same_song(&t.tracks, track);
        let ok = pred_match.contains_key(&p) || truth.iter().any(|t| !t.is_id && fits(t));
        if !ok && in_ids * 2 >= len {
            continue;
        }
        counted += 1;
        if ok {
            correct += 1;
            if truth.iter().any(|t| !t.is_id && fits(t) && same_version(&t.tracks, track)) {
                correct_version += 1;
            }
        } else {
            m.false_positives.push(format!("{} {} – {}", clock(s.start_ms), track.artist, track.title));
        }
    }
    m.predicted = counted;
    let ratio = |a: usize, b: usize| if b == 0 { 1.0 } else { a as f64 / b as f64 };
    m.recall = ratio(matched, m.truth);
    m.recall_version = ratio(matched_version, m.truth);
    m.precision = ratio(correct, counted);
    m.precision_version = ratio(correct_version, counted);
    // Start error: the earliest prediction of the same song inside the true span.
    let mut errors: Vec<f64> = Vec::new();
    let mut signed: Vec<f64> = Vec::new();
    for &g in truth_match.keys() {
        let t = &truth[g];
        let first = predicted
            .iter()
            .filter(|s| {
                overlap((t.start, t.end), (s.start_ms, s.end_ms)) > 0
                    && same_song(&t.tracks, s.track.as_ref().expect("filtered"))
            })
            .map(|s| s.start_ms)
            .min()
            .unwrap_or(t.start);
        let err = (first as f64 - t.start as f64).abs() / 1000.0;
        signed.push((first as f64 - t.start as f64) / 1000.0);
        errors.push(err);
        m.start_errors.push((t.label.clone(), err));
    }
    m.start_errors.sort_by(|a, b| b.1.total_cmp(&a.1));
    errors.sort_by(f64::total_cmp);
    m.start_error_median_s = percentile(&errors, 0.5);
    m.start_error_p90_s = percentile(&errors, 0.9);
    signed.sort_by(f64::total_cmp);
    m.start_bias_s = percentile(&signed, 0.5);
    // Splits: a true track covered by more than one prediction of that song.
    for &g in &real {
        let t = &truth[g];
        let pieces = predicted
            .iter()
            .filter(|s| {
                let o = overlap((t.start, t.end), (s.start_ms, s.end_ms));
                o >= 15_000.min((t.end - t.start) / 4) && same_song(&t.tracks, s.track.as_ref().expect("filtered"))
            })
            .count();
        if pieces > 1 {
            m.splits += 1;
        }
    }
    // Merges: a prediction covering most of two or more true tracks.
    for s in &predicted {
        let covered = truth
            .iter()
            .filter(|t| {
                let len = t.end.saturating_sub(t.start);
                len > 0 && overlap((t.start, t.end), (s.start_ms, s.end_ms)) * 2 >= len
            })
            .count();
        if covered > 1 {
            m.merges += 1;
        }
    }
    for &g in &real {
        if !truth_match.contains_key(&g) {
            m.missed.push(truth[g].label.clone());
        }
    }
    Ok(m)
}

fn evaluate_single(case: &Case, report: &Report) -> Metrics {
    let mut m = Metrics::default();
    let Some(expect) = &case.expect else { return m };
    let truth = song_tracks(&expect.artist, &expect.title, &expect.aka);
    let pieces: Vec<&Segment> =
        report.segments.iter().filter(|s| matches!(s.kind, SegmentKind::Track | SegmentKind::Unidentified)).collect();
    m.single_split = pieces.len() > 1;
    m.truth = 1;
    m.predicted = pieces.iter().filter(|s| s.kind == SegmentKind::Track).count();
    let main = pieces.iter().filter(|s| s.track.is_some()).max_by_key(|s| s.end_ms - s.start_ms);
    if let Some(track) = main.and_then(|s| s.track.as_ref()) {
        m.correct = same_song(&truth, track);
        m.correct_version = same_version(&truth, track);
        if !m.correct {
            m.false_positives.push(format!("{} – {}", track.artist, track.title));
        }
    } else {
        m.missed.push(format!("{} – {}", expect.artist, expect.title));
    }
    if pieces.len() > 1 {
        m.splits = pieces.len() - 1;
    }
    m
}

fn evaluate_playlist(case: &Case, report: &Report) -> Metrics {
    let mut m = Metrics::default();
    for (index, expect) in case.entries.iter().enumerate() {
        let Some(expect) = expect else { continue };
        m.entries += 1;
        let truth = song_tracks(&expect.artist, &expect.title, &expect.aka);
        let found = report
            .segments
            .iter()
            .filter(|s| s.item_index == index as u32 && s.kind == SegmentKind::Track)
            .filter_map(|s| s.track.as_ref())
            .find(|t| same_song(&truth, t));
        match found {
            Some(t) => {
                m.entries_correct += 1;
                if same_version(&truth, t) {
                    m.entries_correct_version += 1;
                }
            }
            None => {
                let got = report
                    .segments
                    .iter()
                    .find(|s| s.item_index == index as u32 && s.track.is_some())
                    .and_then(|s| s.track.as_ref())
                    .map(|t| format!("{} – {}", t.artist, t.title))
                    .unwrap_or_else(|| "nothing".into());
                m.missed.push(format!("#{index} {} – {} (got {got})", expect.artist, expect.title));
            }
        }
    }
    // Entries settled from their title alone have a single "-meta" segment.
    let audio: std::collections::HashSet<u32> =
        report.segments.iter().filter(|s| !s.id.ends_with("-meta")).map(|s| s.item_index).collect();
    m.entries_with_audio = audio.len();
    m
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Reports

fn summary_line(run: &CaseRun) -> String {
    if let Some(e) = &run.error {
        return format!("error: {e}");
    }
    let m = &run.metrics;
    let calls = format!(
        "{} probes ({:.0}/h, {} uncached-miss), {:.0} s",
        run.probes, m.calls_per_hour, run.uncached, run.wall_s
    );
    match run.kind.as_str() {
        "mix" => format!(
            "{}: recall {:.0} % (v {:.0} %), precision {:.0} % (v {:.0} %), start err {:.1}/{:.1} s (bias {:+.1}), splits {}, merges {}; {calls}",
            run.strategy,
            m.recall * 100.0,
            m.recall_version * 100.0,
            m.precision * 100.0,
            m.precision_version * 100.0,
            m.start_error_median_s,
            m.start_error_p90_s,
            m.start_bias_s,
            m.splits,
            m.merges
        ),
        "single" => format!(
            "{}: {} segment(s), correct {} (version {}); {calls}",
            run.strategy,
            run.segments.len(),
            m.correct,
            m.correct_version
        ),
        _ => format!(
            "{}: {}/{} entries right ({} with version), {} needed audio; {calls}",
            run.strategy, m.entries_correct, m.entries, m.entries_correct_version, m.entries_with_audio
        ),
    }
}

fn write_reports(dir: &Path, runs: &[CaseRun]) -> anyhow::Result<()> {
    let stamp = chrono_like_stamp();
    let json_path = dir.join(format!("mix-eval-{stamp}.json"));
    std::fs::write(&json_path, serde_json::to_string_pretty(runs)?)?;
    let md = markdown(runs);
    let md_path = dir.join(format!("mix-eval-{stamp}.md"));
    std::fs::write(&md_path, &md)?;
    let html_path = dir.join(format!("mix-eval-{stamp}.html"));
    std::fs::write(&html_path, html(&md))?;
    println!("{md}");
    eprintln!("reports: {}, {} and {}", md_path.display(), html_path.display(), json_path.display());
    Ok(())
}

/// The Markdown report as a self-contained HTML page (headings, tables, lists).
fn html(md: &str) -> String {
    fn escape(text: &str) -> String {
        text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
    }
    fn inline(text: &str) -> String {
        let escaped = escape(text);
        let mut out = String::new();
        for (k, part) in escaped.split("**").enumerate() {
            if k % 2 == 1 {
                out += &format!("<strong>{part}</strong>");
            } else {
                out += part;
            }
        }
        out
    }
    let mut body = String::new();
    let mut table: Vec<Vec<String>> = Vec::new();
    let mut list: Vec<String> = Vec::new();
    let flush = |body: &mut String, table: &mut Vec<Vec<String>>, list: &mut Vec<String>| {
        if !table.is_empty() {
            *body += "<table>";
            for (k, row) in table.iter().enumerate() {
                let tag = if k == 0 { "th" } else { "td" };
                *body += "<tr>";
                for cell in row {
                    let class =
                        if k > 0 && cell.chars().next().is_some_and(|c| c.is_ascii_digit()) { " class=n" } else { "" };
                    *body += &format!("<{tag}{class}>{}</{tag}>", inline(cell));
                }
                *body += "</tr>";
            }
            *body += "</table>";
            table.clear();
        }
        if !list.is_empty() {
            *body += "<ul>";
            for item in list.iter() {
                *body += &format!("<li>{}</li>", inline(item));
            }
            *body += "</ul>";
            list.clear();
        }
    };
    for line in md.lines() {
        let line = line.trim_end();
        if let Some(row) = line.strip_prefix('|') {
            let cells: Vec<String> = row.trim_end_matches('|').split('|').map(|c| c.trim().to_string()).collect();
            if cells.iter().all(|c| c.chars().all(|ch| ch == '-' || ch == ':')) {
                continue;
            }
            table.push(cells);
            continue;
        }
        if let Some(item) = line.strip_prefix("- ") {
            list.push(item.to_string());
            continue;
        }
        flush(&mut body, &mut table, &mut list);
        if let Some(h) = line.strip_prefix("### ") {
            body += &format!("<h3>{}</h3>", inline(h));
        } else if let Some(h) = line.strip_prefix("## ") {
            body += &format!("<h2>{}</h2>", inline(h));
        } else if let Some(h) = line.strip_prefix("# ") {
            body += &format!("<h1>{}</h1>", inline(h));
        } else if !line.is_empty() {
            body += &format!("<p>{}</p>", inline(line));
        }
    }
    flush(&mut body, &mut table, &mut list);
    format!(
        "<!doctype html><html lang=en><meta charset=utf-8><meta name=viewport content=\"width=device-width\">\
         <title>Mix evaluation</title><style>\
         body{{font:14px/1.45 -apple-system,system-ui,sans-serif;max-width:1180px;margin:32px auto;padding:0 20px;color:#1d1d1f}}\
         h1{{font-size:26px;margin:0 0 20px}}h2{{font-size:18px;margin:32px 0 10px}}h3{{font-size:15px;margin:20px 0 6px}}\
         table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}}\
         th,td{{padding:5px 8px;border-bottom:1px solid #e5e5ea;text-align:left;white-space:nowrap}}\
         th{{font-weight:600;color:#6e6e73;font-size:12px}}td.n{{text-align:right}}\
         tr:last-child td{{border-bottom:2px solid #c7c7cc}}ul{{margin:4px 0 12px;padding-left:20px;color:#3a3a3c}}\
         @media(prefers-color-scheme:dark){{body{{background:#1c1c1e;color:#f2f2f7}}th,td{{border-color:#3a3a3c}}ul{{color:#d1d1d6}}}}\
         </style><body>{body}</body></html>"
    )
}

fn chrono_like_stamp() -> String {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    format!("{secs}")
}

fn markdown(runs: &[CaseRun]) -> String {
    let mut out = String::from("# Mix, single and playlist evaluation\n\n");
    for mode in ["hinted", "blind"] {
        let mixes: Vec<&CaseRun> = runs.iter().filter(|r| r.mode == mode && r.kind == "mix").collect();
        if !mixes.is_empty() {
            out += &format!("## Mixes ({mode})\n\n");
            out += "| case | min | true | pred | recall | recall (ver.) | precision | precision (ver.) | start err med / p90 (bias) s | splits | merges | calls/h |\n";
            out += "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
            let (mut truth, mut matched, mut matched_v, mut pred, mut correct, mut correct_v) =
                (0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
            let (mut splits, mut merges, mut probes, mut hours) = (0, 0, 0u32, 0.0);
            let mut medians = Vec::new();
            for r in &mixes {
                let m = &r.metrics;
                if let Some(e) = &r.error {
                    out += &format!("| {} | | | | error: {e} | | | | | | | |\n", r.id);
                    continue;
                }
                out += &format!(
                    "| {} | {:.0} | {} | {} | {:.1} % | {:.1} % | {:.1} % | {:.1} % | {:.1} / {:.1} ({:+.1}) | {} | {} | {:.1} |\n",
                    r.id,
                    r.duration_s / 60.0,
                    m.truth,
                    m.predicted,
                    m.recall * 100.0,
                    m.recall_version * 100.0,
                    m.precision * 100.0,
                    m.precision_version * 100.0,
                    m.start_error_median_s,
                    m.start_error_p90_s,
                    m.start_bias_s,
                    m.splits,
                    m.merges,
                    m.calls_per_hour
                );
                truth += m.truth as f64;
                matched += m.recall * m.truth as f64;
                matched_v += m.recall_version * m.truth as f64;
                pred += m.predicted as f64;
                correct += m.precision * m.predicted as f64;
                correct_v += m.precision_version * m.predicted as f64;
                splits += m.splits;
                merges += m.merges;
                probes += r.probes;
                hours += r.duration_s / 3600.0;
                medians.extend(m.start_errors.iter().map(|e| e.1));
            }
            medians.sort_by(f64::total_cmp);
            out += &format!(
                "| **all** | {:.0} | {:.0} | {:.0} | **{:.1} %** | {:.1} % | **{:.1} %** | {:.1} % | **{:.1}** / {:.1} | {} | {} | **{:.1}** |\n\n",
                hours * 60.0,
                truth,
                pred,
                100.0 * matched / truth.max(1.0),
                100.0 * matched_v / truth.max(1.0),
                100.0 * correct / pred.max(1.0),
                100.0 * correct_v / pred.max(1.0),
                percentile(&medians, 0.5),
                percentile(&medians, 0.9),
                splits,
                merges,
                probes as f64 / hours.max(1e-9)
            );
        }
        let singles: Vec<&CaseRun> = runs.iter().filter(|r| r.mode == mode && r.kind == "single").collect();
        if !singles.is_empty() {
            out += &format!(
                "## Single songs ({mode})\n\n| case | min | segments | split | correct | correct (ver.) | probes |\n|---|---:|---:|---|---|---|---:|\n"
            );
            for r in &singles {
                let m = &r.metrics;
                out += &format!(
                    "| {} | {:.1} | {} | {} | {} | {} | {} |\n",
                    r.id,
                    r.duration_s / 60.0,
                    r.segments.len(),
                    if m.single_split { "**yes**" } else { "no" },
                    m.correct,
                    m.correct_version,
                    r.probes
                );
            }
            let n = singles.iter().filter(|r| r.error.is_none()).count().max(1) as f64;
            out += &format!(
                "\nSplits: {} · correct {:.0} % · correct version {:.0} %\n\n",
                singles.iter().filter(|r| r.metrics.single_split).count(),
                100.0 * singles.iter().filter(|r| r.metrics.correct).count() as f64 / n,
                100.0 * singles.iter().filter(|r| r.metrics.correct_version).count() as f64 / n
            );
        }
        let lists: Vec<&CaseRun> = runs.iter().filter(|r| r.mode == mode && r.kind == "playlist").collect();
        if !lists.is_empty() {
            out += &format!(
                "## Playlists ({mode})\n\n| case | entries | right | right (ver.) | needed audio | probes |\n|---|---:|---:|---:|---:|---:|\n"
            );
            for r in &lists {
                let m = &r.metrics;
                out += &format!(
                    "| {} | {} | {} ({:.0} %) | {} | {} | {} |\n",
                    r.id,
                    m.entries,
                    m.entries_correct,
                    100.0 * m.entries_correct as f64 / m.entries.max(1) as f64,
                    m.entries_correct_version,
                    m.entries_with_audio,
                    r.probes
                );
            }
            out += "\n";
        }
    }
    out += "## Misses and false positives\n\n";
    for r in runs {
        let m = &r.metrics;
        if m.missed.is_empty() && m.false_positives.is_empty() && r.error.is_none() {
            continue;
        }
        out += &format!("### {} ({})\n\n", r.id, r.mode);
        if let Some(e) = &r.error {
            out += &format!("- error: {e}\n");
        }
        for x in &m.missed {
            out += &format!("- missed: {x}\n");
        }
        for x in &m.false_positives {
            out += &format!("- false positive: {x}\n");
        }
        out += "\n";
    }
    out
}

fn clock(ms: u64) -> String {
    mfcore::pipeline::mix::clock(ms)
}

// ---------------------------------------------------------------------------
// Draft ground truth

/// Print a draft `tracks` array for a mix from its published tracklist.
pub async fn draft(args: DraftArgs) -> anyhow::Result<()> {
    let backend = YtDlpSubprocess::new(args.tools.config());
    let url = args.url.clone();
    let json = tokio::task::spawn_blocking(move || backend.probe(url, true, TaskHandle::detached())).await??;
    let info = parse_probe(&json)?;
    let hints = hints::collect(&info, &[], None);
    let Some(list) = hints.tracklist else {
        bail!("no timestamped tracklist found in the chapters, description or comments");
    };
    eprintln!(
        "{} – {} entries from {:?}, {}",
        info.title.unwrap_or_default(),
        list.entries.len(),
        list.source,
        clock(info.duration_ms.unwrap_or(0))
    );
    let tracks: Vec<TruthTrack> = list
        .entries
        .iter()
        .map(|e| TruthTrack {
            t: clock(e.start_ms),
            artist: e.artist.clone(),
            title: e.title.clone(),
            id: e.is_id,
            speech: false,
            uncertain: false,
            aka: Vec::new(),
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&tracks)?);
    Ok(())
}
