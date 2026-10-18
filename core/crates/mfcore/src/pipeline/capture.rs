//! Analyses of media a host captures from a player: the browser plays a media
//! page (listen-through or short windows, extension plan §2.4) and pushes the
//! audio here, stamped with media time ([`CaptureSink::push`]). When the host
//! says the capture is complete ([`CaptureSink::finish`]), the unchanged survey
//! or mix pipeline runs on what was captured, with the page's own hints (title,
//! description, chapters) in place of a yt-dlp probe.
//!
//! The host decides what to play; positions it never captured read as silence.
//! Planning the capture in the engine (Quick check, gap re-capture through a
//! `CaptureHost`) comes with the browser media path (E5).

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::audio::resample::StreamResampler;
use crate::audio::{PcmFile, PcmWriter};
use crate::error::{MfError, Result};
use crate::links;
use crate::media::{Chapter, MediaInfo};
use crate::model::{AnalyzeMode, Phase, SourceInfo};
use crate::pipeline::analyze::{self, Services, SessionServices};
use crate::pipeline::ctx::SessionCtx;
use crate::pipeline::hints;
use crate::util::rt::Instant;

/// Progress events while capturing, at most this often.
const PROGRESS_EVERY: Duration = Duration::from_millis(500);
/// Media positions beyond this are refused (a broken clock, not a real mix).
const MAX_MEDIA_MS: f64 = 12.0 * 3600.0 * 1000.0;
const MIN_INPUT_RATE: f64 = 2_000.0;
const MAX_INPUT_RATE: f64 = 192_000.0;
/// Less captured audio than this cannot be analyzed.
const MIN_CAPTURED_MS: u64 = 1_000;
const WORK_FILE: &str = "capture.pcm";

/// What the page says about the media (the host's adapters read it).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PageMedia {
    /// The page's URL.
    pub url: String,
    pub title: Option<String>,
    /// Uploader, channel or artist of the page.
    pub uploader: Option<String>,
    /// Length of the media; `None` while unknown.
    pub duration_ms: Option<u64>,
    pub thumbnail_url: Option<String>,
    /// Free text that may hold a tracklist (a description, a set's notes).
    pub description: Option<String>,
    pub chapters: Vec<PageChapter>,
    /// Track and artist fields some sites fill (YouTube Music, SoundCloud, Bandcamp).
    pub track: Option<String>,
    pub artist: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PageChapter {
    pub start_ms: u64,
    pub title: String,
}

impl PageMedia {
    fn canonical_url(&self) -> String {
        links::canonicalize(&self.url).unwrap_or_else(|_| self.url.clone())
    }

    /// The page as the pipelines' probe data.
    pub fn info(&self) -> MediaInfo {
        let mut chapters: Vec<Chapter> = self
            .chapters
            .iter()
            .filter(|c| !c.title.trim().is_empty())
            .map(|c| Chapter { start_ms: c.start_ms, end_ms: None, title: c.title.trim().to_string() })
            .collect();
        chapters.sort_by_key(|c| c.start_ms);
        for i in 1..chapters.len() {
            let next = chapters[i].start_ms;
            chapters[i - 1].end_ms = Some(next);
        }
        if let (Some(last), Some(duration)) = (chapters.last_mut(), self.duration_ms) {
            last.end_ms = Some(duration);
        }
        MediaInfo {
            title: self.title.clone(),
            uploader: self.uploader.clone(),
            duration_ms: self.duration_ms,
            webpage_url: Some(self.canonical_url()),
            thumbnail: self.thumbnail_url.clone(),
            description: self.description.clone(),
            chapters,
            track: self.track.clone(),
            artist: self.artist.clone(),
            ..MediaInfo::default()
        }
    }

    /// The `Source` event for the page.
    pub fn source(&self) -> SourceInfo {
        let canonical = self.canonical_url();
        let mut source = match Url::parse(&canonical) {
            Ok(url) => {
                let host = url.host_str().unwrap_or_default().trim_end_matches('.').to_ascii_lowercase();
                links::source_info(&url, links::Site::of_host(&host))
            }
            Err(_) => SourceInfo { url: Some(self.url.clone()), item_count: 1, ..SourceInfo::default() },
        };
        source.title.clone_from(&self.title);
        source.uploader.clone_from(&self.uploader);
        source.thumbnail_url.clone_from(&self.thumbnail_url);
        source.duration_ms = self.duration_ms;
        source
    }
}

/// One continuous stretch of captured audio.
struct Run {
    /// Effective input rate (capture rate ÷ playback speed), in Hz.
    rate: u32,
    resampler: StreamResampler,
}

enum End {
    Complete,
    Failed(MfError),
}

struct SinkState {
    writer: Option<PcmWriter>,
    run: Option<Run>,
    out: Vec<f32>,
    /// Media length the host announced (the PCM gets that length).
    duration_ms: Option<u64>,
    captured_ms: f64,
    last_progress: Option<Instant>,
    end: Option<End>,
    write_error: Option<MfError>,
    warned: bool,
}

/// The audio of one captured analysis, written by media time.
pub struct CaptureSink {
    state: Mutex<SinkState>,
    ended: tokio::sync::Notify,
    ctx: Arc<SessionCtx>,
}

impl CaptureSink {
    fn new(ctx: Arc<SessionCtx>, writer: PcmWriter, duration_ms: Option<u64>) -> Self {
        Self {
            state: Mutex::new(SinkState {
                writer: Some(writer),
                run: None,
                out: Vec::new(),
                duration_ms,
                captured_ms: 0.0,
                last_progress: None,
                end: None,
                write_error: None,
                warned: false,
            }),
            ended: tokio::sync::Notify::new(),
            ctx,
        }
    }

    /// The session id.
    pub fn session_id(&self) -> &str {
        &self.ctx.id
    }

    /// Mono f32 audio captured at `sample_rate` while the media played at
    /// `speed` (1–4, pitch not preserved), starting at media time `media_ms`.
    /// It is treated as audio sampled at `sample_rate / speed`. `continuous`
    /// false starts a new stretch at `media_ms` (a seek, an ad, a stall).
    pub fn push(&self, media_ms: f64, samples: &[f32], sample_rate: u32, speed: f32, continuous: bool) {
        let mut state = self.state.lock();
        if state.end.is_some() || state.write_error.is_some() || samples.is_empty() {
            return;
        }
        let rate = f64::from(sample_rate) / f64::from(speed);
        let valid = rate.is_finite()
            && (MIN_INPUT_RATE..=MAX_INPUT_RATE).contains(&rate)
            && media_ms.is_finite()
            && (0.0..MAX_MEDIA_MS).contains(&media_ms);
        if !valid {
            if !state.warned {
                state.warned = true;
                tracing::warn!(sample_rate, speed, media_ms, "capture: unusable audio ignored");
            }
            return;
        }
        let rate = rate.round() as u32;
        let state = &mut *state;
        let new_run = !continuous || state.run.as_ref().is_none_or(|r| r.rate != rate);
        if new_run {
            match StreamResampler::new(rate, 1) {
                Ok(resampler) => state.run = Some(Run { rate, resampler }),
                Err(e) => {
                    tracing::warn!("capture: {e}");
                    state.run = None;
                    return;
                }
            }
            if let Some(writer) = state.writer.as_mut() {
                writer.seek_ms(media_ms);
            }
        }
        let (Some(run), Some(writer)) = (state.run.as_mut(), state.writer.as_mut()) else { return };
        let clean: Vec<f32> = samples.iter().map(|s| if s.is_finite() { *s } else { 0.0 }).collect();
        state.out.clear();
        run.resampler.process_into(&clean, &mut state.out);
        if let Err(e) = writer.push_f32(&state.out) {
            tracing::warn!("capture: the audio could not be stored: {e}");
            state.write_error = Some(e);
            self.ended.notify_one();
            return;
        }
        state.captured_ms += samples.len() as f64 / rate as f64 * 1000.0;
        // Capture progress is a phase fraction: `Progress` belongs to the analysis
        // that follows (it starts again at 0).
        let due = state.last_progress.is_none_or(|t| t.elapsed() >= PROGRESS_EVERY);
        if due && let Some(total) = state.duration_ms.filter(|d| *d > 0) {
            state.last_progress = Some(Instant::now());
            let fraction = (state.captured_ms / total as f64).clamp(0.0, 1.0) as f32;
            self.ctx.emitter.phase(Phase::Listening, Some(fraction));
        }
    }

    /// The media's length, once the host knows it (the analysis spans all of it).
    pub fn set_duration(&self, duration_ms: u64) {
        if duration_ms > 0 {
            self.state.lock().duration_ms = Some(duration_ms);
        }
    }

    /// Everything the host was going to capture has been pushed: analyze it.
    pub fn finish(&self) {
        let mut state = self.state.lock();
        if state.end.is_none() {
            state.end = Some(End::Complete);
        }
        drop(state);
        self.ended.notify_one();
    }

    /// The capture failed (the tab closed, the page navigated away): the
    /// session ends with `error`.
    pub fn fail(&self, error: MfError) {
        let mut state = self.state.lock();
        if state.end.is_none() {
            state.end = Some(End::Failed(error));
        }
        drop(state);
        self.ended.notify_one();
    }

    /// Waits for the end of the capture; the finished PCM, or why there is none.
    async fn captured(&self) -> Result<PcmFile> {
        loop {
            let notified = self.ended.notified();
            {
                let mut state = self.state.lock();
                if let Some(e) = state.write_error.take() {
                    return Err(e);
                }
                match state.end.take() {
                    Some(End::Failed(e)) => return Err(e),
                    Some(End::Complete) => {
                        let mut writer = state.writer.take().ok_or_else(|| MfError::internal("capture ended twice"))?;
                        // The tail the resampler still holds.
                        if let Some(run) = state.run.as_mut() {
                            let tail = run.resampler.flush();
                            writer.push_f32(&tail)?;
                        }
                        // Span the whole media: probes are planned on its length.
                        if let Some(duration) = state.duration_ms
                            && duration_samples(duration) > writer.samples_written()
                        {
                            writer.seek_sample(duration_samples(duration) - 1);
                            writer.push(&[0])?;
                        }
                        return writer.finish();
                    }
                    None => {}
                }
            }
            tokio::select! {
                _ = notified => {}
                _ = self.ctx.cancel.canceled() => return Err(MfError::Canceled),
            }
        }
    }
}

fn duration_samples(ms: u64) -> u64 {
    ms * u64::from(crate::audio::SAMPLE_RATE) / 1000
}

/// The PCM work file of a captured analysis.
fn work_writer(ctx: &SessionCtx) -> Result<PcmWriter> {
    #[cfg(not(target_family = "wasm"))]
    {
        let dir = ctx.ensure_work_dir()?;
        PcmWriter::create(&dir.join(WORK_FILE))
    }
    #[cfg(target_family = "wasm")]
    {
        PcmWriter::create_in(ctx.engine.files.clone(), &ctx.work_name(WORK_FILE))
    }
}

/// The sink for a new captured analysis of `page`.
pub fn sink(ctx: Arc<SessionCtx>, page: &PageMedia) -> Result<Arc<CaptureSink>> {
    let writer = work_writer(&ctx)?;
    Ok(Arc::new(CaptureSink::new(ctx, writer, page.duration_ms)))
}

/// The session body: wait for the capture, then the survey or mix pipeline.
/// Emits every event after `Started`, including the terminal one.
pub async fn run(ctx: Arc<SessionCtx>, sink: Arc<CaptureSink>, page: PageMedia, mode: AnalyzeMode) {
    let services = SessionServices::new(&ctx);
    run_with(&ctx, &services, &sink, &page, mode).await;
}

/// [`run`] with other services (tests).
pub async fn run_with<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    sink: &CaptureSink,
    page: &PageMedia,
    mode: AnalyzeMode,
) {
    let result = analyze(ctx, services, sink, page, mode).await;
    ctx.cleanup();
    match result {
        Ok(report) => ctx.emitter.finished(report.outcome, report.update),
        Err(MfError::Canceled) => ctx.emitter.canceled(),
        Err(e) => {
            tracing::warn!(session = %ctx.id, "captured analysis failed: {e}");
            ctx.emitter.failed(&e);
        }
    }
}

async fn analyze<S: Services>(
    ctx: &SessionCtx,
    services: &S,
    sink: &CaptureSink,
    page: &PageMedia,
    mode: AnalyzeMode,
) -> Result<analyze::Report> {
    let mut source = page.source();
    ctx.emitter.source(source.clone());
    ctx.emitter.phase(Phase::Listening, None);
    let pcm = sink.captured().await?;
    let captured = pcm.duration_ms();
    if captured < MIN_CAPTURED_MS {
        return Err(MfError::MediaUnavailable { message: "Nothing could be heard from the page.".into() });
    }
    if source.duration_ms.is_none() {
        source.duration_ms = Some(captured);
        ctx.emitter.source(source.clone());
    }
    let info = PageMedia { duration_ms: Some(captured), ..page.clone() }.info();
    let hints = hints::collect(&info, &[], Some(captured));
    // Impressions are captured from the microphone, never from a page.
    let mode = if mode == AnalyzeMode::Impression { AnalyzeMode::Auto } else { mode };
    let url = source.url.clone().or_else(|| Some(page.url.clone()));
    analyze::analyze_pcm(ctx, services, &pcm, &hints, &source, url, mode, 0, "").await
}

#[cfg(test)]
mod tests;
