//! Link and file analysis sessions. See docs/PIPELINES.md ("Analysis sessions").
//!
//! validate → resolve (links) → probe → playlist, or download → decode →
//! strategy: impression analysis, or the survey (one song or many?) → single
//! or mix. The work directory is removed at the end in every outcome.

pub mod services;
mod survey;

use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

use sha2::{Digest, Sha256};

pub use self::services::{Services, SessionServices};
pub use self::survey::{Prior, Survey, Verdict};
use crate::audio::PcmFile;
#[cfg(not(target_family = "wasm"))]
use crate::audio::decode::decode_file;
use crate::error::{MfError, Result};
use crate::links::{self, Resolution};
use crate::media::{self, MediaInfo};
use crate::model::{
    AnalyzeMode, Evidence, EvidenceSource, Outcome, Phase, Segment, SegmentKind, SessionKind, SourceInfo, Track,
};
use crate::pipeline::ctx::SessionCtx;
use crate::pipeline::hints::{self, Hints, MixWords};
use crate::pipeline::mix::{self, MixHints, MixOptions};
use crate::pipeline::stitch::{Piece, UNVERIFIED_HINT};
use crate::pipeline::{hum, playlist};
use crate::shazam::Priority;
use crate::store::SessionUpdate;
use crate::util::cancel::TaskHandle;

/// Media at least this long is a mix unless the page clearly names one song.
pub const MIX_MIN_MS: u64 = 15 * 60_000;
/// Media at least this long asks the backend for top comments (tracklists).
const COMMENTS_MIN_MS: u64 = 8 * 60_000;
/// Survey probes are interactive below this length.
const INTERACTIVE_MAX_MS: u64 = 10 * 60_000;
/// Largest direct download.
#[cfg(not(target_family = "wasm"))]
const MAX_DIRECT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// The browser decodes a direct download from memory.
#[cfg(target_family = "wasm")]
const MAX_DIRECT_BYTES: u64 = 1024 * 1024 * 1024;
/// Waits before trying a failed download again.
#[cfg(not(target_family = "wasm"))]
const DOWNLOAD_RETRIES: [Duration; 2] = [Duration::from_millis(1500), Duration::from_secs(4)];
/// Impression analysis looks at the start of the media.
const IMPRESSION_MS: u64 = 30_000;
/// Enrichment of streaming-service tracks runs this many at a time (the
/// catalogs' own rate limiters pace the requests).
const ENRICH_PARALLEL: usize = 8;

/// A validated analysis input.
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzeInput {
    /// The first http(s) URL found in the input.
    Url(String),
    /// An existing regular file.
    File(PathBuf),
}

/// Synchronous validation done by `Engine::analyze` before a session starts.
pub fn validate(input: &str) -> Result<AnalyzeInput> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(MfError::invalid("Paste a link or choose a file."));
    }
    let as_path =
        trimmed.strip_prefix("file://").map_or_else(|| PathBuf::from(trimmed), |p| PathBuf::from(decode_path(p)));
    if as_path.is_absolute() {
        return match std::fs::metadata(&as_path) {
            Ok(meta) if meta.is_file() => Ok(AnalyzeInput::File(as_path)),
            Ok(_) => Err(MfError::invalid("That is a folder, not a file.")),
            Err(_) => Err(MfError::invalid("The file does not exist.")),
        };
    }
    if let Some(url) = links::extract_url(trimmed) {
        return Ok(AnalyzeInput::Url(url));
    }
    Err(MfError::invalid("That isn't a link or a file."))
}

/// Percent-decoding for `file://` paths.
fn decode_path(path: &str) -> String {
    percent_encoding::percent_decode_str(path).decode_utf8_lossy().into_owned()
}

/// How an analysis turned out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Strategy {
    /// Streaming-service metadata, no audio.
    Tracks,
    Single,
    Mix,
    Playlist,
    Impression,
    Nothing,
}

/// Developer options (evaluation, debugging). Defaults are the product.
#[derive(Debug, Clone)]
pub struct AnalyzeOptions {
    /// Use page hints (tracklists, cards, titles). Off measures the audio alone.
    pub use_hints: bool,
    /// Keep probes and decoded audio here between runs, keyed by URL.
    pub media_cache: Option<PathBuf>,
    /// Replay from `media_cache` only: pages and probes are never fetched again
    /// when a kept probe exists (evaluation replays stay off the sites).
    pub offline: bool,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self { use_hints: true, media_cache: None, offline: false }
    }
}

/// Everything an analysis produced.
#[derive(Debug, Clone)]
pub struct Report {
    pub outcome: Outcome,
    pub strategy: Strategy,
    pub segments: Vec<Segment>,
    pub duration_ms: Option<u64>,
    /// Recognizer probes (survey and mix).
    pub probes: u32,
    pub paid: u32,
    pub log: Vec<String>,
    /// Title, artwork and primary track to store with the session.
    pub update: SessionUpdate,
}

/// Run the session to completion. Emits every event including the terminal one
/// through `ctx.emitter` and cleans up `ctx.work_dir`.
pub async fn run(ctx: Arc<SessionCtx>, input: AnalyzeInput, mode: AnalyzeMode) {
    let kind = match input {
        AnalyzeInput::Url(_) => SessionKind::Link,
        AnalyzeInput::File(_) => SessionKind::File,
    };
    ctx.emitter.started(kind);
    let services = SessionServices::new(&ctx);
    let result = run_with(&ctx, &services, input, mode, &AnalyzeOptions::default()).await;
    ctx.cleanup();
    match result {
        Ok(report) => ctx.emitter.finished(report.outcome, report.update),
        Err(MfError::Canceled) => ctx.emitter.canceled(),
        Err(e) => {
            tracing::warn!(session = %ctx.id, "analysis failed: {e}");
            ctx.emitter.failed(&e);
        }
    }
}

/// The analysis without the terminal event (the caller sends it).
pub async fn run_with<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    input: AnalyzeInput,
    mode: AnalyzeMode,
    options: &AnalyzeOptions,
) -> Result<Report> {
    let mut run = Run { ctx, services, options, log: Vec::new(), probes: 0, paid: 0 };
    let report = match input {
        AnalyzeInput::Url(url) => run.link(&url, mode).await,
        AnalyzeInput::File(path) => run.file(&path, mode).await,
    };
    let mut report = report?;
    report.log.splice(0..0, run.log);
    report.probes += run.probes;
    report.paid += run.paid;
    Ok(report)
}

struct Run<'a, S: Services> {
    ctx: &'a SessionCtx,
    services: &'a S,
    options: &'a AnalyzeOptions,
    log: Vec<String>,
    probes: u32,
    paid: u32,
}

impl<S: Services> Run<'_, S> {
    async fn link(&mut self, url: &str, mode: AnalyzeMode) -> Result<Report> {
        let ctx = self.ctx;
        ctx.emitter.phase(Phase::Resolving, None);
        let cached = cached_resolution(self.options, url);
        let resolved = match cached {
            Some(r) => Ok(r),
            None => links::resolve(&ctx.engine.http, url, ctx.engine.country(), &ctx.cancel).await,
        };
        if let Ok(r) = &resolved {
            store_resolution(self.options, url, r);
        }
        let resolution = match resolved {
            Ok(r) => r,
            // The page itself could not be read: the media backend may still fetch it.
            Err(MfError::Network { message }) if ctx.engine.media.is_some() => {
                self.log.push(format!("page unavailable ({message}); trying the media backend"));
                let url = links::canonicalize(url).unwrap_or_else(|_| url.to_string());
                let source = SourceInfo { url: Some(url.clone()), item_count: 1, ..SourceInfo::default() };
                Resolution::Media { source, url, direct: false }
            }
            Err(e) => return Err(e),
        };
        let (mut source, media_url, direct) = match resolution {
            Resolution::Tracks { source, tracks } => return self.tracks(source, tracks).await,
            Resolution::Media { source, url, direct } => (source, url, direct),
        };
        ctx.emitter.source(source.clone());
        let page_hints = std::mem::take(&mut source.hints);
        let mut info = MediaInfo::default();
        if !direct {
            let with_comments = source.duration_ms.is_some_and(|d| d >= COMMENTS_MIN_MS);
            info = probe_media(ctx, self.options, &media_url, with_comments).await?;
            // The page did not say how long the media is: ask again with comments.
            let long = info.duration_ms.is_some_and(|d| d >= COMMENTS_MIN_MS);
            if !with_comments && long && !info.is_playlist && hints::collect(&info, &[], None).tracklist.is_none() {
                info = probe_media(ctx, self.options, &media_url, true).await?;
            }
            if info.is_live {
                return Err(MfError::MediaUnavailable {
                    message: "Live streams can't be analyzed until they end.".into(),
                });
            }
            refresh_source(&mut source, &info);
            source.hints = page_hints.clone();
            ctx.emitter.source(source.clone());
            if info.is_playlist {
                let report = playlist::run(ctx, self.services, &source, &info, self.options).await?;
                return Ok(report);
            }
        }
        let dir = ctx.ensure_work_dir()?.clone();
        let pcm = fetch_audio(ctx, self.options, &media_url, direct, &dir).await?;
        let hints = if self.options.use_hints {
            hints::collect(&info, &page_hints, Some(pcm.duration_ms()))
        } else {
            Hints::default()
        };
        let source_url = source.url.clone().or(Some(media_url));
        self.analyze_audio(&pcm, &hints, &source, source_url, mode).await
    }

    async fn file(&mut self, path: &Path, mode: AnalyzeMode) -> Result<Report> {
        let ctx = self.ctx;
        let name = path.file_stem().map(|s| s.to_string_lossy().into_owned());
        let mut source = SourceInfo { title: name.clone(), item_count: 1, ..SourceInfo::default() };
        ctx.emitter.source(source.clone());
        ctx.emitter.phase(Phase::Decoding, Some(0.0));
        let out = ctx.ensure_work_dir()?.join("audio.pcm");
        let pcm = decode(ctx, path, &out).await?;
        source.duration_ms = Some(pcm.duration_ms());
        ctx.emitter.source(source.clone());
        let info = MediaInfo { title: name, duration_ms: Some(pcm.duration_ms()), ..MediaInfo::default() };
        let hints =
            if self.options.use_hints { hints::collect(&info, &[], Some(pcm.duration_ms())) } else { Hints::default() };
        self.analyze_audio(&pcm, &hints, &source, None, mode).await
    }

    /// Streaming-service tracks, no audio: every segment at once from the
    /// page's metadata, then each one again under the same id as its
    /// enrichment (artwork, ISRC, links) arrives. A single track gets every
    /// catalog; collections get the fast ones.
    async fn tracks(&mut self, source: SourceInfo, tracks: Vec<Track>) -> Result<Report> {
        use futures_util::StreamExt;
        let ctx = self.ctx;
        let mut source = source;
        source.item_count = tracks.len().max(1) as u32;
        ctx.emitter.source(source.clone());
        let total = tracks.len() as u32;
        let mut segments: Vec<Segment> = tracks
            .into_iter()
            .enumerate()
            .map(|(index, track)| metadata_segment(index, crate::catalog::with_search_links(track)))
            .collect();
        for segment in &segments {
            ctx.emitter.segment(segment.clone());
        }
        let single = segments.len() == 1;
        // Futures are built up front (no closure): the session future must stay `Send`.
        let mut pending = Vec::with_capacity(segments.len());
        for (index, segment) in segments.iter().enumerate() {
            let Some(track) = segment.track.clone() else { continue };
            let services = self.services;
            pending.push(async move {
                let enriched = if single { services.enrich(track).await } else { services.enrich_quick(track).await };
                (index, enriched)
            });
        }
        let mut enriched = futures_util::stream::iter(pending).buffer_unordered(ENRICH_PARALLEL);
        while let Some((index, track)) = enriched.next().await {
            ctx.cancel.check()?;
            let segment = &mut segments[index];
            if segment.track.as_ref() != Some(&track) {
                segment.track = Some(track);
                ctx.emitter.segment(segment.clone());
            }
        }
        drop(enriched);
        ctx.cancel.check()?;
        let update = SessionUpdate {
            title: source.title.clone(),
            track: (total == 1).then(|| segments.first().and_then(|s| s.track.clone())).flatten(),
            ..SessionUpdate::default()
        };
        Ok(Report {
            outcome: Outcome::Completed { identified: total, total },
            strategy: Strategy::Tracks,
            segments,
            duration_ms: source.duration_ms,
            probes: 0,
            paid: 0,
            log: Vec::new(),
            update,
        })
    }

    /// Strategy: impression, or survey → single / mix.
    async fn analyze_audio(
        &mut self,
        pcm: &PcmFile,
        hints: &Hints,
        source: &SourceInfo,
        source_url: Option<String>,
        mode: AnalyzeMode,
    ) -> Result<Report> {
        let duration = pcm.duration_ms();
        if duration < 1_000 {
            return Err(MfError::Decode { message: "the media has no audio".into() });
        }
        if mode == AnalyzeMode::Impression {
            return self.impression(pcm, source).await;
        }
        let report = analyze_pcm(self.ctx, self.services, pcm, hints, source, source_url, mode, 0, "").await?;
        Ok(report)
    }

    async fn impression(&mut self, pcm: &PcmFile, source: &SourceInfo) -> Result<Report> {
        let samples = pcm.read_ms(0, IMPRESSION_MS)?;
        let audio = crate::audio::resample::i16_to_f32(&samples);
        let outcome = hum::run_impression(self.ctx, &audio).await?;
        let update =
            SessionUpdate { kind: Some(SessionKind::Hum), title: source.title.clone(), ..SessionUpdate::default() };
        Ok(Report {
            outcome,
            strategy: Strategy::Impression,
            segments: Vec::new(),
            duration_ms: Some(pcm.duration_ms()),
            probes: 0,
            paid: 0,
            log: Vec::new(),
            update,
        })
    }
}

/// A streaming-service track as a segment of its collection.
fn metadata_segment(index: usize, track: Track) -> Segment {
    Segment {
        id: format!("track-{index}"),
        item_index: index as u32,
        start_ms: 0,
        end_ms: 0,
        kind: SegmentKind::Track,
        track: Some(track),
        confidence: 0.99,
        tier: crate::model::Tier::Confirmed,
        evidence: vec![Evidence { source: EvidenceSource::Metadata, at_ms: 0, score: 0.99, detail: None }],
        alternates: Vec::new(),
        source_url: None,
    }
}

/// Probe media with the backend (cached in `options.media_cache` when set).
pub(crate) async fn probe_media(
    ctx: &SessionCtx,
    options: &AnalyzeOptions,
    url: &str,
    with_comments: bool,
) -> Result<MediaInfo> {
    let cache = cache_path(options, url, if with_comments { "probe-comments.json" } else { "probe.json" });
    if let Some(path) = &cache
        && let Ok(json) = std::fs::read_to_string(path)
    {
        return media::parse_probe(&json);
    }
    // A kept probe with comments answers a request without them too.
    if !with_comments
        && let Some(path) = cache_path(options, url, "probe-comments.json")
        && let Ok(json) = std::fs::read_to_string(path)
    {
        return media::parse_probe(&json);
    }
    let Some(backend) = ctx.engine.media.clone() else {
        // Without yt-dlp (the browser), YouTube pages and InnerTube answer instead.
        if crate::links::youtube_api::handles(url) {
            return crate::links::youtube_api::probe(&ctx.engine.http, url, with_comments, &ctx.cancel).await;
        }
        return Err(MfError::Platform {
            message: "Links with media need the media download tools, which are not available.".into(),
        });
    };
    let task = TaskHandle::new(ctx.cancel.clone());
    let url_owned = url.to_string();
    let json = crate::util::rt::spawn_blocking(move || backend.probe(url_owned, with_comments, task))
        .await
        .map_err(|e| MfError::internal(format!("probe: {e}")))??;
    if let Some(path) = cache {
        let _ = std::fs::write(path, &json);
    }
    media::parse_probe(&json)
}

/// A media resolution kept in `options.media_cache` (evaluation replays never
/// touch the page again).
#[derive(serde::Serialize, serde::Deserialize)]
struct CachedMedia {
    source: SourceInfo,
    url: String,
    direct: bool,
}

fn cached_resolution(options: &AnalyzeOptions, url: &str) -> Option<Resolution> {
    if let Some(json) = cache_path(options, url, "resolution.json").and_then(|p| std::fs::read_to_string(p).ok())
        && let Ok(cached) = serde_json::from_str::<CachedMedia>(&json)
    {
        return Some(Resolution::Media { source: cached.source, url: cached.url, direct: cached.direct });
    }
    if !options.offline {
        return None;
    }
    // Offline replays: the kept probe describes the page well enough.
    let canonical = links::canonicalize(url).unwrap_or_else(|_| url.to_string());
    let (media_url, json) = [url, canonical.as_str()].iter().find_map(|u| {
        ["probe-comments.json", "probe.json"]
            .iter()
            .find_map(|what| std::fs::read_to_string(cache_path(options, u, what)?).ok())
            .map(|json| (u.to_string(), json))
    })?;
    let info = media::parse_probe(&json).ok()?;
    let mut source = SourceInfo { url: Some(media_url.clone()), ..SourceInfo::default() };
    refresh_source(&mut source, &info);
    Some(Resolution::Media { source, url: media_url, direct: false })
}

fn store_resolution(options: &AnalyzeOptions, url: &str, resolution: &Resolution) {
    let (Some(path), Resolution::Media { source, url: media_url, direct }) =
        (cache_path(options, url, "resolution.json"), resolution)
    else {
        return;
    };
    // A page that did not tell the duration was probably a consent or bot wall.
    if path.exists() || source.duration_ms.is_none() {
        return;
    }
    let cached = CachedMedia { source: source.clone(), url: media_url.clone(), direct: *direct };
    if let Ok(json) = serde_json::to_string(&cached) {
        let _ = std::fs::write(path, json);
    }
}

fn cache_path(options: &AnalyzeOptions, url: &str, what: &str) -> Option<PathBuf> {
    let dir = options.media_cache.as_ref()?;
    let _ = std::fs::create_dir_all(dir);
    let digest = Sha256::digest(url.as_bytes());
    let key: String = digest.iter().take(10).map(|b| format!("{b:02x}")).collect();
    Some(dir.join(format!("{key}.{what}")))
}

/// Download and decode the media's audio into `dir`.
pub(crate) async fn fetch_audio(
    ctx: &SessionCtx,
    options: &AnalyzeOptions,
    url: &str,
    direct: bool,
    dir: &Path,
) -> Result<PcmFile> {
    if let Some(cached) = cache_path(options, url, "pcm")
        && cached.is_file()
    {
        return PcmFile::open(&cached);
    }
    #[cfg(not(target_family = "wasm"))]
    {
        std::fs::create_dir_all(dir)?;
        ctx.emitter.phase(Phase::Downloading, Some(0.0));
        let file = if direct { download_direct(ctx, url, dir).await? } else { download(ctx, url, dir).await? };
        ctx.emitter.phase(Phase::Decoding, Some(0.0));
        let out = match cache_path(options, url, "pcm") {
            Some(path) => path.with_extension("pcm.part"),
            None => dir.join("audio.pcm"),
        };
        let decoded = decode(ctx, &file, &out).await;
        let _ = std::fs::remove_file(&file);
        let pcm = decoded?;
        if let Some(path) = cache_path(options, url, "pcm") {
            std::fs::rename(pcm.path(), &path)?;
            return PcmFile::open(&path);
        }
        Ok(pcm)
    }
    #[cfg(target_family = "wasm")]
    {
        // The browser has no media backend (capture comes with E5): direct
        // media only, downloaded into memory and decoded into a work file.
        let _ = dir;
        if !direct {
            return Err(MfError::Platform {
                message: "Links with media need the media download tools, which are not available.".into(),
            });
        }
        ctx.emitter.phase(Phase::Downloading, Some(0.0));
        let (bytes, ext) = download_direct_bytes(ctx, url).await?;
        ctx.emitter.phase(Phase::Decoding, Some(0.0));
        decode_bytes(ctx, bytes, &ext, &ctx.work_name("audio.pcm")).await
    }
}

/// Download through the media backend. Sites drop connections and refuse a
/// stream now and then, so a failed download is tried again twice, each time
/// into a fresh directory (a broken partial file is never taken for the result).
#[cfg(not(target_family = "wasm"))]
async fn download(ctx: &SessionCtx, url: &str, dir: &Path) -> Result<PathBuf> {
    let backend = ctx.engine.media.clone().ok_or_else(|| MfError::Platform {
        message: "Links with media need the media download tools, which are not available.".into(),
    })?;
    let mut attempt = 0;
    loop {
        let dest = if attempt == 0 { dir.to_path_buf() } else { dir.join(format!("retry-{attempt}")) };
        let emitter = ctx.emitter.clone();
        let task = TaskHandle::with_progress(ctx.cancel.clone(), move |f| emitter.phase(Phase::Downloading, Some(f)));
        let (url_owned, dest) = (url.to_string(), dest.to_string_lossy().into_owned());
        let backend = backend.clone();
        let result = crate::util::rt::spawn_blocking(move || backend.download_audio(url_owned, dest, task))
            .await
            .map_err(|e| MfError::internal(format!("download: {e}")))?;
        match result.map_err(MfError::from) {
            Ok(path) => return Ok(PathBuf::from(path)),
            Err(MfError::Platform { message }) if attempt < DOWNLOAD_RETRIES.len() => {
                tracing::info!(session = %ctx.id, "download failed ({message}); trying again");
                ctx.cancel.sleep(DOWNLOAD_RETRIES[attempt]).await?;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Plain HTTP download of a media file: every hop checked by the SSRF
/// guard, at most 2 GiB.
#[cfg(not(target_family = "wasm"))]
async fn download_direct(ctx: &SessionCtx, url: &str, dir: &Path) -> Result<PathBuf> {
    use tokio::io::AsyncWriteExt;
    let client = direct_client(&ctx.engine.config.app_version)?;
    let mut current = url::Url::parse(url).map_err(|_| MfError::invalid("That link isn't valid."))?;
    let mut response = None;
    for _ in 0..=5 {
        links::ensure_public_url(current.as_str()).await?;
        let r = tokio::select! {
            r = client.get(current.as_str()).send() => r?,
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
        };
        if r.status().is_redirection()
            && let Some(next) = r.headers().get(reqwest::header::LOCATION).and_then(|v| v.to_str().ok())
        {
            current = current.join(next).map_err(|_| MfError::network("the website sent a broken redirect"))?;
            continue;
        }
        response = Some(r);
        break;
    }
    let mut response = response.ok_or_else(|| MfError::network("the link redirects too many times"))?;
    if !response.status().is_success() {
        return Err(MfError::MediaUnavailable {
            message: format!("The file couldn't be downloaded ({}).", response.status()),
        });
    }
    let total = response.content_length();
    if total.is_some_and(|t| t > MAX_DIRECT_BYTES) {
        return Err(MfError::MediaUnavailable { message: "The file is too large (over 2 GB).".into() });
    }
    let ext = Path::new(current.path()).extension().and_then(|e| e.to_str()).unwrap_or("bin");
    let ext: String = ext.chars().filter(char::is_ascii_alphanumeric).take(5).collect();
    let path = dir.join(format!("download.{ext}"));
    let mut file = tokio::fs::File::create(&path).await?;
    let mut written = 0u64;
    loop {
        let chunk = tokio::select! {
            c = response.chunk() => c?,
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
        };
        let Some(chunk) = chunk else { break };
        written += chunk.len() as u64;
        if written > MAX_DIRECT_BYTES {
            return Err(MfError::MediaUnavailable { message: "The file is too large (over 2 GB).".into() });
        }
        file.write_all(&chunk).await?;
        if let Some(t) = total.filter(|t| *t > 0) {
            ctx.emitter.phase(Phase::Downloading, Some(written as f32 / t as f32));
        }
    }
    file.flush().await?;
    Ok(path)
}

/// Browser: a direct media download into memory. fetch() follows redirects
/// itself; where it ended must still be a public address. At most 1 GiB.
#[cfg(target_family = "wasm")]
async fn download_direct_bytes(ctx: &SessionCtx, url: &str) -> Result<(Vec<u8>, String)> {
    use futures_util::StreamExt;
    links::ensure_public_url(url).await?;
    let request = crate::util::http::prepare(ctx.engine.http.get(url)).send();
    let response = tokio::select! {
        r = request => r?,
        _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
    };
    links::ensure_public_url(response.url().as_str()).await?;
    if !response.status().is_success() {
        return Err(MfError::MediaUnavailable {
            message: format!("The file couldn't be downloaded ({}).", response.status()),
        });
    }
    let too_large =
        || MfError::MediaUnavailable { message: "The file is too large to analyze here (over 1 GB).".into() };
    let total = response.content_length();
    if total.is_some_and(|t| t > MAX_DIRECT_BYTES) {
        return Err(too_large());
    }
    let ext = Path::new(response.url().path()).extension().and_then(|e| e.to_str()).unwrap_or("bin");
    let ext: String = ext.chars().filter(char::is_ascii_alphanumeric).take(5).collect();
    let mut body = Vec::with_capacity(total.unwrap_or(0) as usize);
    let mut stream = response.bytes_stream();
    loop {
        let chunk = tokio::select! {
            c = stream.next() => c,
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
        };
        let Some(chunk) = chunk else { break };
        let chunk = chunk?;
        if (body.len() + chunk.len()) as u64 > MAX_DIRECT_BYTES {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
        if let Some(t) = total.filter(|t| *t > 0) {
            ctx.emitter.phase(Phase::Downloading, Some(body.len() as f32 / t as f32));
        }
    }
    Ok((body, ext))
}

/// Browser: decode `bytes` (`hint`: extension or MIME type) into the work file
/// `output`.
#[cfg(target_family = "wasm")]
pub(crate) async fn decode_bytes(ctx: &SessionCtx, bytes: Vec<u8>, hint: &str, output: &str) -> Result<PcmFile> {
    use crate::audio::PcmWriter;
    let files = ctx.engine.files.clone();
    let cancel = ctx.cancel.clone();
    let emitter = ctx.emitter.clone();
    let (hint, output) = (hint.to_string(), output.to_string());
    crate::util::rt::spawn_blocking(move || {
        let progress = move |f: f32| emitter.phase(Phase::Decoding, Some(f));
        let writer = PcmWriter::create_in(files.clone(), &output)?;
        let decoded =
            crate::audio::decode::decode_bytes_into(Arc::from(bytes), Some(&hint), writer, &cancel, &progress);
        if decoded.is_err() {
            let _ = files.remove(&output);
        }
        decoded
    })
    .await
    .map_err(|e| MfError::internal(format!("decode: {e}")))?
}

/// Browser: decode the work file `input` (a file the user picked, written by
/// the host) into the work file `output`.
#[cfg(target_family = "wasm")]
pub(crate) async fn decode(ctx: &SessionCtx, input: &Path, output: &Path) -> Result<PcmFile> {
    let name = input.to_string_lossy();
    let bytes = ctx.engine.files.read(&name)?;
    let hint = input.extension().and_then(|e| e.to_str()).unwrap_or("bin").to_string();
    decode_bytes(ctx, bytes, &hint, &output.to_string_lossy()).await
}

#[cfg(not(target_family = "wasm"))]
pub(crate) async fn decode(ctx: &SessionCtx, input: &Path, output: &Path) -> Result<PcmFile> {
    let decoder = ctx.engine.decoder.clone();
    let cancel = ctx.cancel.clone();
    let emitter = ctx.emitter.clone();
    let (input, output) = (input.to_path_buf(), output.to_path_buf());
    crate::util::rt::spawn_blocking(move || {
        let progress = move |f: f32| emitter.phase(Phase::Decoding, Some(f));
        decode_file(&input, &output, decoder.as_ref(), &cancel, &progress)
    })
    .await
    .map_err(|e| MfError::internal(format!("decode: {e}")))?
}

/// Survey and single/mix analysis of decoded audio. Shared with playlist
/// entries (`item_index`, `id_prefix`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn analyze_pcm<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    pcm: &PcmFile,
    hints: &Hints,
    source: &SourceInfo,
    source_url: Option<String>,
    mode: AnalyzeMode,
    item_index: u32,
    id_prefix: &str,
) -> Result<Report> {
    let duration = pcm.duration_ms();
    let mut log = Vec::new();
    let tracklist = hints.tracklist.clone();
    let mix_prior = tracklist.is_some() || hints.mix_words == MixWords::Strong;
    let prior = single_prior(services, hints, duration).await;
    if let Some(t) = &prior.track {
        log.push(format!("page names {} – {} ({})", t.artist, t.title, if prior.strong { "strong" } else { "weak" }));
    }
    let force_mix = mode == AnalyzeMode::Mix;
    let long = duration >= MIX_MIN_MS;
    let long_single = long && prior.strong && !mix_prior && prior.track.as_ref().and_then(|t| t.duration_ms).is_some();
    // A strong single prior outweighs mix words and a list someone else posted;
    // only a tracklist from the uploader (chapters, description, pinned or own
    // comment) makes a mix of media the catalog knows as one song.
    let uploader_list = tracklist.as_ref().is_some_and(|l| l.source != hints::HintSource::Comment);
    let go_mix = force_mix || (long && !long_single) || (mix_prior && (!prior.strong || uploader_list));
    let mut survey_probes = Vec::new();
    let mut probes = 0;
    let mut paid = 0;
    let options = MixOptions {
        item_index,
        source_url: source_url.clone(),
        priority: Priority::Batch,
        id_prefix: id_prefix.to_string(),
    };
    if !go_mix {
        ctx.emitter.phase(Phase::Recognizing, None);
        let priority = if duration < INTERACTIVE_MAX_MS { Priority::Interactive } else { Priority::Batch };
        let survey = survey::run(services, pcm, &prior, priority).await?;
        probes += survey.probes;
        paid += survey.paid;
        log.extend(survey.log);
        let mut verdict = survey.verdict;
        let missed_page_song = match (&verdict, &prior.track) {
            (Verdict::Single { track, .. }, Some(page)) => !hints::same_song(track, page),
            (Verdict::Nothing { .. }, Some(_)) => true,
            _ => false,
        };
        if missed_page_song && let Some(page) = survey::page_song_heard(services, pcm, &prior).await? {
            log.push(format!("the page's song {} – {} plays here (preview found)", page.artist, page.title));
            verdict = survey::prefer_page_song(verdict, page);
        }
        let heard = match &verdict {
            Verdict::Single { track, .. } => Some(track.clone()),
            _ => None,
        };
        verdict = survey::weigh_page(verdict, &prior, hints.official);
        if let (Some(heard), Verdict::Single { track, .. }) = (heard, &verdict)
            && !hints::same_song(&heard, track)
        {
            log.push(format!(
                "the page's {} – {} outweighs one answer ({} – {})",
                track.artist, track.title, heard.artist, heard.title
            ));
        }
        match verdict {
            Verdict::Single { track, evidence, alternates } => {
                let track = services.enrich(track).await;
                let piece = Piece {
                    id: format!("{id_prefix}single"),
                    item_index,
                    start_ms: 0,
                    end_ms: duration,
                    kind: SegmentKind::Track,
                    track: Some(track.clone()),
                    evidence,
                    alternates,
                    track_start_ms: None,
                };
                let segment = piece.to_segment(source_url.as_deref().and_then(|u| links::deep_link(u, 0)));
                ctx.emitter.segment(segment.clone());
                ctx.emitter.progress(duration, duration, Some(0));
                return Ok(Report {
                    outcome: Outcome::Completed { identified: 1, total: 1 },
                    strategy: Strategy::Single,
                    segments: vec![segment],
                    duration_ms: Some(duration),
                    probes,
                    paid,
                    log,
                    update: SessionUpdate {
                        title: source.title.clone(),
                        artwork_url: track.artwork_url.clone(),
                        track: Some(track),
                        ..SessionUpdate::default()
                    },
                });
            }
            Verdict::Nothing { hint } => {
                let segment = match hint {
                    Some(track) => {
                        let piece = Piece {
                            id: format!("{id_prefix}single"),
                            item_index,
                            start_ms: 0,
                            end_ms: duration,
                            kind: SegmentKind::Track,
                            track: Some(track),
                            evidence: vec![Evidence {
                                source: EvidenceSource::PageHint,
                                at_ms: 0,
                                score: UNVERIFIED_HINT,
                                detail: Some("from the title; the audio was not recognized".into()),
                            }],
                            alternates: Vec::new(),
                            track_start_ms: None,
                        };
                        piece.to_segment(source_url.as_deref().and_then(|u| links::deep_link(u, 0)))
                    }
                    None => Piece::gap(format!("{id_prefix}single"), 0, duration, SegmentKind::Unidentified)
                        .to_segment(None),
                };
                let mut segment = segment;
                segment.item_index = item_index;
                let identified = u32::from(segment.track.is_some());
                ctx.emitter.segment(segment.clone());
                ctx.emitter.progress(duration, duration, Some(0));
                return Ok(Report {
                    outcome: Outcome::Completed { identified, total: 1 },
                    strategy: Strategy::Nothing,
                    segments: vec![segment],
                    duration_ms: Some(duration),
                    probes,
                    paid,
                    log,
                    update: SessionUpdate { title: source.title.clone(), ..SessionUpdate::default() },
                });
            }
            Verdict::Mix { probes: seen } => survey_probes = seen,
        }
    }
    let mix_hints = MixHints { tracklist, untimed: hints.untimed.clone() };
    let report = mix::run(services, &ctx.emitter, pcm, &mix_hints, survey_probes, &options).await?;
    log.extend(report.log.iter().cloned());
    let segments: Vec<Segment> = report
        .pieces
        .iter()
        .map(|p| p.to_segment(source_url.as_deref().and_then(|u| links::deep_link(u, p.start_ms))))
        .collect();
    let identified = segments.iter().filter(|s| s.kind == SegmentKind::Track).count() as u32;
    let total =
        segments.iter().filter(|s| matches!(s.kind, SegmentKind::Track | SegmentKind::Unidentified)).count() as u32;
    Ok(Report {
        outcome: Outcome::Completed { identified, total: total.max(identified) },
        strategy: Strategy::Mix,
        segments,
        duration_ms: Some(duration),
        probes: probes + report.probes,
        paid: paid + report.paid,
        log,
        update: SessionUpdate { title: source.title.clone(), ..SessionUpdate::default() },
    })
}

/// What the page says the media is: extractor metadata (strong), or the title
/// parsed as "Artist - Title" and found in the catalog (strong when the
/// catalog duration is within 12 % of the media's).
async fn single_prior<S: Services>(services: &S, hints: &Hints, duration_ms: u64) -> Prior {
    if let Some(track) = &hints.extractor_track {
        let found = services.find(&track.title, &track.artist, None).await;
        let fits = found.as_ref().and_then(|f| f.duration_ms).is_none_or(|d| within(d, duration_ms, 0.12));
        return Prior { track: Some(found.filter(|_| fits).unwrap_or_else(|| track.clone())), strong: true };
    }
    let Some(parsed) = &hints.title_track else {
        // "Artist-Title" with a bare hyphen counts once the catalog knows the
        // song by that artist (in any version).
        let Some(loose) = &hints.loose_title_track else { return Prior::default() };
        let known = services.find(&hints::bare_title(&loose.title), &loose.artist, None).await;
        return match known {
            Some(found) if hints::same_version(&found, loose) => Prior { track: Some(found), strong: false },
            Some(_) => Prior { track: Some(loose.clone()), strong: false },
            None => Prior::default(),
        };
    };
    match services.find(&parsed.title, &parsed.artist, None).await {
        Some(found) => {
            let strong = found.duration_ms.is_some_and(|d| within(d, duration_ms, 0.12));
            Prior { track: Some(found), strong }
        }
        None => Prior { track: Some(parsed.clone()), strong: false },
    }
}

fn within(a: u64, b: u64, tolerance: f64) -> bool {
    (a as f64 - b as f64).abs() <= tolerance * b.max(1) as f64
}

/// Refresh `source` from probe data.
fn refresh_source(source: &mut SourceInfo, info: &MediaInfo) {
    if info.title.is_some() {
        source.title.clone_from(&info.title);
    }
    if info.uploader.is_some() {
        source.uploader.clone_from(&info.uploader);
    }
    if info.thumbnail.is_some() {
        source.thumbnail_url.clone_from(&info.thumbnail);
    }
    if info.duration_ms.is_some() {
        source.duration_ms = info.duration_ms;
    }
    if source.url.is_none() {
        source.url.clone_from(&info.webpage_url);
    }
    source.item_count = if info.is_playlist { info.entries.len() as u32 } else { 1 };
}

/// HTTP client for direct downloads: no automatic redirects (each hop is
/// checked), a resolver that refuses non-public addresses, long reads allowed.
#[cfg(not(target_family = "wasm"))]
fn direct_client(app_version: &str) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .tls_backend_preconfigured(crate::util::http::tls_config()?)
        .user_agent(crate::util::http::user_agent(app_version))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(PublicResolver))
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| MfError::internal(format!("http client: {e}")))
}

/// Resolves names only to public addresses (no DNS rebinding into the LAN).
#[cfg(not(target_family = "wasm"))]
struct PublicResolver;

#[cfg(not(target_family = "wasm"))]
impl reqwest::dns::Resolve for PublicResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if addrs.is_empty() || addrs.iter().any(|a| !links::is_public_ip(a.ip())) {
                let err: Box<dyn std::error::Error + Send + Sync> = "host resolves to a non-public address".into();
                return Err(err);
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

#[cfg(test)]
mod tests;
