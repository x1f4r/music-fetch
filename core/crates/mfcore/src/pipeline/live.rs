//! Live listening sessions (microphone, system audio). See docs/PIPELINES.md
//! ("Live sessions").
//!
//! `push_pcm` only converts the bytes and queues them ([`LiveInput`]); one
//! dedicated thread per session runs the DSP loop (resampling, level, silence,
//! pitch, impression detection) and schedules recognizer calls on the engine
//! runtime. Time is measured in captured audio, so a session behaves the same
//! whether audio arrives in real time or faster.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::audio::SAMPLE_RATE;
use crate::audio::resample::{StreamResampler, f32_to_i16};
use crate::error::{MfError, Result};
use crate::melody::{ImpressionDetector, StreamingPitch};
use crate::model::{
    CaptureSource, Evidence, ListenMode, NoMatchReason, Notice, Outcome, Phase, PitchFrame, Segment, SegmentKind,
    SessionKind, Tier,
};
use crate::pipeline::ctx::SessionCtx;
use crate::pipeline::hum;
use crate::pipeline::recognize::{self, RecognitionHit};
use crate::shazam::Priority;
use crate::store::SessionUpdate;

/// Longest capture; the session ends there.
const MAX_CAPTURE_MS: u64 = 30_000;
/// Recording queries, in captured milliseconds.
const QUERY_TIMES_MS: [u64; 5] = [4_000, 8_000, 13_000, 20_000, 28_000];
const MAX_CLIP_MS: u64 = 12_000;
/// Paid providers are tried once at this point without a match.
const PAID_AT_MS: u64 = 10_000;
const PAID_CLIP_MS: u64 = 10_000;
const MOVE_CLOSER_AT_MS: u64 = 6_000;
/// A query clip quieter than this is not sent.
const QUIET_CLIP_DB: f32 = -50.0;
const MOVE_CLOSER_DB: f32 = -40.0;
const TOO_QUIET_DB: f32 = -45.0;
/// 33 ms blocks at least this loud count as audible.
const AUDIBLE_DB: f32 = -50.0;
/// Samples below this magnitude are digital silence.
const SILENCE_AMPLITUDE: f32 = 1e-5;
const SILENT_NOTICE_MS: u64 = 3_000;
/// Level block: 33 ms at 16 kHz.
const BLOCK: usize = 528;
/// Queued input beyond this is dropped, oldest first.
const MAX_BACKLOG_MS: f64 = 5_000.0;
const MIN_INPUT_RATE: u32 = 8_000;
const MAX_INPUT_RATE: u32 = 192_000;
const MAX_INPUT_CHANNELS: u32 = 32;
/// Impression auto-stop: enough voiced audio followed by a pause, or a time limit.
const IMPRESSION_MIN_VOICED_MS: u64 = 6_000;
const IMPRESSION_PAUSE_MS: u64 = 1_500;
const IMPRESSION_MAX_MS: u64 = 20_000;
const KEEP_GOING_MS: u64 = 3_000;
const ANALYZE_MIN_VOICED_MS: u64 = 2_500;
/// Finishing runs one more query when the last one is older than this.
const FINAL_QUERY_AGE_MS: u64 = 4_000;
const MIN_AUDIBLE_MS: u64 = 3_000;
const MATCH_ENRICH_LIMIT: Duration = Duration::from_secs(3);
const POP_TIMEOUT: Duration = Duration::from_millis(20);
/// Share of speech-like windows above which a miss is reported as `MostlySpeech`.
const SPEECH_SHARE: f32 = 0.6;

/// One pushed chunk, already converted to f32.
struct Chunk {
    samples: Vec<f32>,
    rate: u32,
    channels: u32,
    ms: f64,
}

#[derive(Default)]
struct InputState {
    chunks: VecDeque<Chunk>,
    backlog_ms: f64,
    dropped_ms: f64,
    /// `finish()` was called: drain the queue, then analyze.
    finished: bool,
    /// The session stopped listening; pushes are ignored.
    closed: bool,
    warned_format: bool,
}

/// Audio handed over by the app, shared by the session handle and its DSP thread.
#[derive(Default)]
pub struct LiveInput {
    state: Mutex<InputState>,
    ready: Condvar,
    /// The DSP thread is working on a chunk.
    busy: AtomicBool,
    /// A recognizer call is running.
    inflight: AtomicBool,
}

impl LiveInput {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Queue interleaved little-endian f32 PCM. Never blocks on analysis: beyond
    /// 5 s of backlog the oldest chunks are dropped.
    pub fn push(&self, data: &[u8], sample_rate: u32, channels: u32) {
        if data.len() < 4 {
            return;
        }
        let valid = (MIN_INPUT_RATE..=MAX_INPUT_RATE).contains(&sample_rate)
            && (1..=MAX_INPUT_CHANNELS).contains(&channels);
        if !valid {
            let mut state = self.state.lock();
            if !state.warned_format {
                state.warned_format = true;
                tracing::warn!(sample_rate, channels, "live input: unsupported format; audio ignored");
            }
            return;
        }
        let samples: Vec<f32> = data.as_chunks::<4>().0.iter().map(|b| f32::from_le_bytes(*b)).collect();
        let ms = samples.len() as f64 / f64::from(channels) / f64::from(sample_rate) * 1000.0;
        {
            let mut state = self.state.lock();
            if state.finished || state.closed {
                return;
            }
            state.chunks.push_back(Chunk { samples, rate: sample_rate, channels, ms });
            state.backlog_ms += ms;
            while state.backlog_ms > MAX_BACKLOG_MS && state.chunks.len() > 1 {
                if let Some(old) = state.chunks.pop_front() {
                    state.backlog_ms -= old.ms;
                    state.dropped_ms += old.ms;
                }
            }
        }
        self.ready.notify_one();
    }

    /// The user stopped capture.
    pub fn finish(&self) {
        self.state.lock().finished = true;
        self.ready.notify_all();
    }

    /// Wake the DSP thread (after cancellation).
    pub fn wake(&self) {
        self.ready.notify_all();
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.chunks.clear();
        state.backlog_ms = 0.0;
    }

    /// Queued audio not yet processed.
    pub fn backlog_ms(&self) -> u64 {
        self.state.lock().backlog_ms.max(0.0) as u64
    }

    /// Audio dropped because the DSP thread fell more than 5 s behind.
    pub fn dropped_ms(&self) -> u64 {
        self.state.lock().dropped_ms as u64
    }

    /// True when everything pushed so far has been processed and no recognizer
    /// call is running (or the session stopped listening). Lets tools feed a file
    /// faster than real time while keeping real-time behaviour.
    pub fn is_settled(&self) -> bool {
        let state = self.state.lock();
        state.closed
            || (state.chunks.is_empty() && !self.busy.load(Ordering::SeqCst) && !self.inflight.load(Ordering::SeqCst))
    }

    /// Next chunk, waiting up to `timeout`. `Err(())` once finished and drained.
    fn pop(&self, timeout: Duration) -> std::result::Result<Option<Chunk>, ()> {
        let mut state = self.state.lock();
        if state.chunks.is_empty() && !state.finished {
            self.ready.wait_for(&mut state, timeout);
        }
        match state.chunks.pop_front() {
            Some(chunk) => {
                state.backlog_ms = (state.backlog_ms - chunk.ms).max(0.0);
                self.busy.store(true, Ordering::SeqCst);
                Ok(Some(chunk))
            }
            None if state.finished => Err(()),
            None => Ok(None),
        }
    }
}

/// Session kind for a capture source and mode.
pub fn session_kind(source: CaptureSource, mode: ListenMode) -> SessionKind {
    match (source, mode) {
        (CaptureSource::SystemAudio, _) => SessionKind::SystemAudio,
        (CaptureSource::Microphone, ListenMode::Hum) => SessionKind::Hum,
        (CaptureSource::Microphone, _) => SessionKind::Listen,
    }
}

/// Start the DSP thread of a live session. `on_exit` runs on that thread after
/// the terminal event.
pub fn spawn(
    ctx: Arc<SessionCtx>,
    source: CaptureSource,
    mode: ListenMode,
    input: Arc<LiveInput>,
    on_exit: impl FnOnce() + Send + 'static,
) -> Result<()> {
    let name = format!("mf-live-{}", ctx.id.chars().take(8).collect::<String>());
    std::thread::Builder::new()
        .name(name)
        .spawn(move || {
            let kind = session_kind(source, mode);
            ctx.emitter.started(kind);
            let outcome = catch_unwind(AssertUnwindSafe(|| Live::new(ctx.clone(), source, mode, input.clone()).run()));
            if let Err(panic) = outcome {
                tracing::error!(session = %ctx.id, "live session panicked: {}", panic_message(&panic));
                ctx.emitter.failed(&MfError::internal("the session stopped unexpectedly"));
            }
            input.close();
            if !ctx.emitter.is_terminal() {
                if ctx.cancel.is_canceled() {
                    ctx.emitter.canceled();
                } else {
                    ctx.emitter.failed(&MfError::internal("the session ended without a result"));
                }
            }
            ctx.cleanup();
            on_exit();
        })
        .map(drop)
        .map_err(|e| MfError::internal(format!("could not start the listening thread: {e}")))
}

/// Text of a panic payload.
pub(crate) fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".into())
}

/// Run `f`, turning a panic into `None` (melody code must not end a session).
fn guarded<T>(what: &str, f: impl FnOnce() -> T) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(value) => Some(value),
        Err(panic) => {
            tracing::error!("{what} panicked: {}; continuing without it", panic_message(&panic));
            None
        }
    }
}

/// Why capture stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    Matched,
    /// 30 s of audio.
    Limit,
    /// Impression auto-stop.
    Impression,
    /// The app called `finish()`.
    User,
    Canceled,
}

type QueryTask = tokio::task::JoinHandle<Result<Option<RecognitionHit>>>;

struct Live {
    ctx: Arc<SessionCtx>,
    input: Arc<LiveInput>,
    mode: ListenMode,
    resampler: Option<StreamResampler>,
    /// 16 kHz mono capture (≤ 30 s).
    capture: Vec<f32>,
    resampled: Vec<f32>,
    // Level blocks.
    block_sum: f64,
    block_peak: f32,
    block_len: usize,
    block_db: Vec<f32>,
    // Digital silence.
    heard_signal: bool,
    silent_notice: bool,
    move_closer: bool,
    // Pitch.
    pitch: Option<StreamingPitch>,
    detector: Option<ImpressionDetector>,
    frames: Vec<PitchFrame>,
    // Recording queries.
    next_query: usize,
    pending_query: bool,
    last_query_ms: Option<u64>,
    shazam: Option<QueryTask>,
    paid: Option<QueryTask>,
    paid_started: bool,
    shazam_error_noticed: bool,
    matched: Option<(RecognitionHit, u64)>,
    impression_at_ms: Option<u64>,
}

impl Live {
    fn new(ctx: Arc<SessionCtx>, source: CaptureSource, mode: ListenMode, input: Arc<LiveInput>) -> Self {
        let mode = if source == CaptureSource::SystemAudio { ListenMode::Recording } else { mode };
        Self {
            ctx,
            input,
            mode,
            resampler: None,
            capture: Vec::with_capacity(MAX_CAPTURE_MS as usize * SAMPLE_RATE as usize / 1000),
            resampled: Vec::new(),
            block_sum: 0.0,
            block_peak: 0.0,
            block_len: 0,
            block_db: Vec::new(),
            heard_signal: false,
            silent_notice: false,
            move_closer: false,
            pitch: None,
            detector: None,
            frames: Vec::new(),
            next_query: 0,
            pending_query: false,
            last_query_ms: None,
            shazam: None,
            paid: None,
            paid_started: false,
            shazam_error_noticed: false,
            matched: None,
            impression_at_ms: if mode == ListenMode::Hum { Some(0) } else { None },
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.capture.len() as u64 * 1000 / u64::from(SAMPLE_RATE)
    }

    fn run(mut self) {
        self.ctx.emitter.phase(Phase::Listening, None);
        self.start_pitch();
        let stop = self.listen();
        self.input.close();
        self.input.busy.store(false, Ordering::SeqCst);
        if stop == Stop::Canceled {
            return;
        }
        let runtime = self.ctx.engine.runtime.clone();
        runtime.block_on(self.finish(stop));
        self.input.inflight.store(false, Ordering::SeqCst);
    }

    fn start_pitch(&mut self) {
        let engine = self.ctx.engine.clone();
        match guarded("pitch tracker", || engine.pitch_tracker()) {
            Some(Ok(tracker)) => {
                self.pitch = guarded("pitch tracker", || StreamingPitch::new(tracker));
                self.detector = guarded("impression detector", ImpressionDetector::new);
            }
            Some(Err(e)) => tracing::warn!("pitch tracker unavailable: {e}"),
            None => {}
        }
    }

    /// The capture loop; returns why it stopped.
    fn listen(&mut self) -> Stop {
        loop {
            if self.ctx.cancel.is_canceled() {
                return Stop::Canceled;
            }
            self.poll_queries();
            if self.matched.is_some() {
                return Stop::Matched;
            }
            if let Some(stop) = self.stop_condition() {
                return stop;
            }
            match self.input.pop(POP_TIMEOUT) {
                Ok(Some(chunk)) => {
                    self.process(chunk);
                    self.poll_queries();
                    self.input.busy.store(false, Ordering::SeqCst);
                }
                Ok(None) => {}
                Err(()) => return Stop::User,
            }
        }
    }

    fn process(&mut self, chunk: Chunk) {
        let needs_new = self.resampler.as_ref().is_none_or(|r| r.input_rate() != chunk.rate || r.channels() != chunk.channels);
        if needs_new {
            match StreamResampler::new(chunk.rate, chunk.channels) {
                Ok(r) => self.resampler = Some(r),
                Err(e) => {
                    tracing::warn!("live input: {e}");
                    return;
                }
            }
        }
        let mut samples = chunk.samples;
        samples.iter_mut().filter(|s| !s.is_finite()).for_each(|s| *s = 0.0);
        let mut out = std::mem::take(&mut self.resampled);
        out.clear();
        if let Some(r) = self.resampler.as_mut() {
            r.process_into(&samples, &mut out);
        }
        let room = (MAX_CAPTURE_MS as usize * SAMPLE_RATE as usize / 1000).saturating_sub(self.capture.len());
        let new = &out[..out.len().min(room)];
        if !new.is_empty() {
            self.analyze(new);
            self.capture.extend_from_slice(new);
            self.after_audio();
        }
        self.resampled = out;
    }

    /// Level, silence and pitch for newly captured 16 kHz samples.
    fn analyze(&mut self, new: &[f32]) {
        if !self.heard_signal && new.iter().any(|s| s.abs() >= SILENCE_AMPLITUDE) {
            self.heard_signal = true;
        }
        for &s in new {
            self.block_sum += f64::from(s * s);
            self.block_peak = self.block_peak.max(s.abs());
            self.block_len += 1;
            if self.block_len == BLOCK {
                let rms_db = to_db((self.block_sum / BLOCK as f64).sqrt() as f32);
                let peak_db = to_db(self.block_peak);
                self.block_db.push(rms_db);
                self.ctx.emitter.level(perceptual(rms_db), perceptual(peak_db));
                self.block_sum = 0.0;
                self.block_peak = 0.0;
                self.block_len = 0;
            }
        }
        if let Some(pitch) = self.pitch.as_mut() {
            match guarded("pitch tracking", || pitch.push(new)) {
                Some(frames) => {
                    if !frames.is_empty() {
                        self.ctx.emitter.pitch(&frames);
                    }
                    if let Some(detector) = self.detector.as_mut()
                        && guarded("impression detector", || detector.push(&frames, new)).is_none()
                    {
                        self.detector = None;
                    }
                    self.frames.extend(frames);
                }
                None => {
                    self.pitch = None;
                    self.detector = None;
                }
            }
        }
    }

    fn recording_rules(&self) -> bool {
        self.impression_at_ms.is_none()
    }

    /// Notices, impression switching and query scheduling after new audio.
    fn after_audio(&mut self) {
        let elapsed = self.elapsed_ms();
        if !self.silent_notice && !self.heard_signal && elapsed >= SILENT_NOTICE_MS {
            self.silent_notice = true;
            self.ctx.emitter.notice(Notice::CaptureSilent);
        }
        if !self.move_closer && elapsed >= MOVE_CLOSER_AT_MS && self.heard_signal {
            self.move_closer = true;
            if median(&self.block_db) < MOVE_CLOSER_DB {
                self.ctx.emitter.notice(Notice::MoveCloser);
            }
        }
        if self.mode == ListenMode::Auto && self.impression_at_ms.is_none() && self.impression_verdict() == Some(true) {
            self.impression_at_ms = Some(elapsed);
            self.pending_query = false;
            self.ctx.emitter.notice(Notice::ImpressionDetected);
        }
        if self.recording_rules() {
            while self.next_query < QUERY_TIMES_MS.len() && elapsed >= QUERY_TIMES_MS[self.next_query] {
                self.next_query += 1;
                self.pending_query = true;
            }
            if self.pending_query && self.shazam.is_none() {
                self.issue_query();
            }
            let paid_configured =
                self.ctx.credentials.audd_token.is_some() || self.ctx.credentials.acr_fingerprint.is_some();
            if paid_configured && !self.paid_started && elapsed >= PAID_AT_MS {
                self.paid_started = true;
                self.issue_paid();
            }
        }
        self.update_inflight();
    }

    fn impression_verdict(&mut self) -> Option<bool> {
        let detector = self.detector.as_ref()?;
        match guarded("impression detector", || detector.verdict()) {
            Some(verdict) => verdict,
            None => {
                self.detector = None;
                None
            }
        }
    }

    /// Voiced duration (audible duration when there is no pitch tracker).
    fn voiced_ms(&mut self) -> u64 {
        if let Some(detector) = self.detector.as_ref() {
            match guarded("impression detector", || detector.voiced_ms()) {
                Some(ms) => return ms,
                None => self.detector = None,
            }
        }
        self.audible_ms()
    }

    /// Time since the last voiced frame (`None` without a pitch tracker).
    fn pause_ms(&mut self) -> Option<u64> {
        let detector = self.detector.as_ref()?;
        let pause = guarded("impression detector", || detector.silence_ms());
        if pause.is_none() {
            self.detector = None;
        }
        pause
    }

    fn audible_ms(&self) -> u64 {
        self.block_db.iter().filter(|&&db| db >= AUDIBLE_DB).count() as u64 * BLOCK as u64 * 1000
            / u64::from(SAMPLE_RATE)
    }

    fn stop_condition(&mut self) -> Option<Stop> {
        let elapsed = self.elapsed_ms();
        if elapsed >= MAX_CAPTURE_MS {
            return Some(Stop::Limit);
        }
        let started = self.impression_at_ms?;
        if elapsed.saturating_sub(started) >= IMPRESSION_MAX_MS {
            return Some(Stop::Impression);
        }
        if self.voiced_ms() >= IMPRESSION_MIN_VOICED_MS && self.pause_ms().is_some_and(|p| p >= IMPRESSION_PAUSE_MS) {
            return Some(Stop::Impression);
        }
        None
    }

    /// Clip of the newest `ms` of capture.
    fn newest(&self, ms: u64) -> &[f32] {
        let n = (ms as usize * SAMPLE_RATE as usize / 1000).min(self.capture.len());
        &self.capture[self.capture.len() - n..]
    }

    fn issue_query(&mut self) {
        self.pending_query = false;
        let elapsed = self.elapsed_ms();
        let clip = self.newest(elapsed.min(MAX_CLIP_MS));
        if rms_db(clip) < QUIET_CLIP_DB {
            tracing::debug!(elapsed, "live: query skipped, too quiet");
            return;
        }
        let samples = f32_to_i16(clip);
        let ctx = self.ctx.clone();
        self.last_query_ms = Some(elapsed);
        self.shazam = Some(
            self.ctx
                .engine
                .runtime
                .spawn(async move { recognize::shazam(&ctx, &samples, Priority::Interactive).await }),
        );
    }

    fn issue_paid(&mut self) {
        let clip = self.newest(PAID_CLIP_MS);
        if rms_db(clip) < QUIET_CLIP_DB {
            return;
        }
        let samples = f32_to_i16(clip);
        let ctx = self.ctx.clone();
        self.paid = Some(self.ctx.engine.runtime.spawn(async move { recognize::paid(&ctx, &samples).await }));
    }

    fn update_inflight(&self) {
        self.input.inflight.store(self.shazam.is_some() || self.paid.is_some(), Ordering::SeqCst);
    }

    /// Collect finished recognizer calls; issue a query that was waiting.
    fn poll_queries(&mut self) {
        let runtime = self.ctx.engine.runtime.clone();
        if self.shazam.as_ref().is_some_and(|t| t.is_finished())
            && let Some(task) = self.shazam.take()
        {
            self.on_result("Shazam", runtime.block_on(task));
        }
        if self.paid.as_ref().is_some_and(|t| t.is_finished())
            && let Some(task) = self.paid.take()
        {
            self.on_result("paid providers", runtime.block_on(task));
        }
        if self.pending_query && self.shazam.is_none() && self.recording_rules() && self.matched.is_none() {
            self.issue_query();
        }
        self.update_inflight();
    }

    fn on_result(&mut self, who: &str, result: std::result::Result<Result<Option<RecognitionHit>>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(Some(hit))) => {
                if self.matched.is_none() {
                    let at = self.last_query_ms.unwrap_or_else(|| self.elapsed_ms());
                    self.matched = Some((hit, at));
                }
            }
            Ok(Ok(None)) | Ok(Err(MfError::Canceled)) | Ok(Err(MfError::RateLimited { .. })) => {}
            Ok(Err(e)) => {
                tracing::warn!("live: {who} failed: {e}");
                if who == "Shazam" && !self.shazam_error_noticed {
                    self.shazam_error_noticed = true;
                    self.ctx
                        .emitter
                        .notice(Notice::ProviderUnavailable { provider: "Shazam".into(), message: e.to_string() });
                }
            }
            Err(e) => tracing::error!("live: {who} task failed: {e}"),
        }
    }

    /// Take results of recognizer calls that already finished, without waiting
    /// (safe inside the runtime).
    fn poll_finished(&mut self) {
        use futures_util::FutureExt;
        if self.shazam.as_ref().is_some_and(|t| t.is_finished())
            && let Some(result) = self.shazam.take().and_then(|t| t.now_or_never())
        {
            self.on_result("Shazam", result);
        }
        if self.paid.as_ref().is_some_and(|t| t.is_finished())
            && let Some(result) = self.paid.take().and_then(|t| t.now_or_never())
        {
            self.on_result("paid providers", result);
        }
    }

    /// Wait for running recognizer calls (a match may still arrive).
    async fn settle_queries(&mut self) {
        for slot in [0, 1] {
            let task = if slot == 0 { self.shazam.take() } else { self.paid.take() };
            let Some(task) = task else { continue };
            let abort = task.abort_handle();
            let result = tokio::select! {
                r = task => Some(r),
                _ = self.ctx.cancel.canceled() => None,
            };
            match result {
                Some(r) => self.on_result(if slot == 0 { "Shazam" } else { "paid providers" }, r),
                None => abort.abort(),
            }
        }
        self.update_inflight();
    }

    async fn finish(&mut self, stop: Stop) {
        if self.impression_at_ms.is_some() {
            // Humming: take answers that already arrived, don't wait for the rest.
            self.poll_finished();
            for task in [self.shazam.take(), self.paid.take()].into_iter().flatten() {
                task.abort();
            }
        } else if self.matched.is_none() && (self.shazam.is_some() || self.paid.is_some()) {
            self.ctx.emitter.phase(Phase::Recognizing, None);
            self.settle_queries().await;
        }
        if self.ctx.cancel.is_canceled() {
            return;
        }
        if let Some((hit, at)) = self.matched.take() {
            self.report_match(hit, at).await;
            return;
        }
        if self.impression_at_ms.is_some() {
            self.finish_impression(stop).await;
            return;
        }
        let elapsed = self.elapsed_ms();
        let stale = self.last_query_ms.is_none_or(|t| elapsed.saturating_sub(t) > FINAL_QUERY_AGE_MS);
        if stale && self.audible_ms() >= MIN_AUDIBLE_MS && rms_db(self.newest(MAX_CLIP_MS)) >= QUIET_CLIP_DB {
            self.ctx.emitter.phase(Phase::Recognizing, None);
            self.issue_query();
            self.settle_queries().await;
            if let Some((hit, at)) = self.matched.take() {
                self.report_match(hit, at).await;
                return;
            }
        }
        let reason = self.no_match_reason(false);
        self.ctx.emitter.finished(Outcome::NoMatch { reason }, SessionUpdate::default());
    }

    async fn finish_impression(&mut self, stop: Stop) {
        let voiced = self.voiced_ms();
        if stop == Stop::User && voiced < KEEP_GOING_MS {
            self.ctx.emitter.notice(Notice::KeepGoing);
        }
        let kind = Some(SessionKind::Hum);
        if voiced < ANALYZE_MIN_VOICED_MS {
            let reason = self.no_match_reason(true);
            self.ctx.emitter.finished(Outcome::NoMatch { reason }, SessionUpdate { kind, ..Default::default() });
            return;
        }
        self.ctx.emitter.flush_pitch();
        let frames = (self.pitch.is_some() && !self.frames.is_empty()).then(|| std::mem::take(&mut self.frames));
        let capture = std::mem::take(&mut self.capture);
        match hum::run_impression_detailed(&self.ctx, &capture, frames).await {
            Ok((outcome, candidates)) => {
                let top = candidates.first().map(|c| &c.track);
                let update = SessionUpdate {
                    kind,
                    title: top.map(|t| t.title.clone()),
                    subtitle: top.map(|t| t.artist.clone()),
                    artwork_url: top.and_then(|t| t.artwork_url.clone()),
                    ..Default::default()
                };
                self.ctx.emitter.finished(outcome, update);
            }
            Err(e) => self.ctx.emitter.failed(&e),
        }
    }

    async fn report_match(&mut self, hit: RecognitionHit, query_ms: u64) {
        let track = self.ctx.engine.enrich_within(hit.track.clone(), MATCH_ENRICH_LIMIT, &self.ctx.cancel).await;
        let detail = match hit.offset_ms {
            Some(offset) => format!("offset {}, skew {:.1}%", clock(offset.max(0) as u64), hit.skew() * 100.0),
            None => format!("skew {:.1}%", hit.skew() * 100.0),
        };
        let segment = Segment {
            id: "match".into(),
            item_index: 0,
            start_ms: 0,
            end_ms: self.elapsed_ms(),
            kind: SegmentKind::Track,
            track: Some(track.clone()),
            confidence: hit.score,
            tier: if hit.score >= 0.95 { Tier::Confirmed } else { Tier::Likely },
            evidence: vec![Evidence { source: hit.source, at_ms: query_ms, score: hit.score, detail: Some(detail) }],
            alternates: Vec::new(),
            source_url: None,
        };
        self.ctx.emitter.segment(segment);
        let update =
            SessionUpdate { title: Some(track.title.clone()), subtitle: Some(track.artist.clone()), ..Default::default() };
        self.ctx.emitter.finished(Outcome::Matched { track }, update);
    }

    fn no_match_reason(&self, impression: bool) -> NoMatchReason {
        let audible = self.audible_ms();
        if self.silent_notice && audible == 0 {
            return NoMatchReason::CaptureSilent;
        }
        if self.block_db.is_empty() {
            return NoMatchReason::TooShort;
        }
        if median(&self.block_db) < TOO_QUIET_DB {
            return NoMatchReason::TooQuiet;
        }
        if impression || audible < MIN_AUDIBLE_MS {
            return NoMatchReason::TooShort;
        }
        if speech_share(&self.block_db, &self.frames) > SPEECH_SHARE {
            return NoMatchReason::MostlySpeech;
        }
        NoMatchReason::NotFound
    }
}

/// `m:ss`
fn clock(ms: u64) -> String {
    let s = ms / 1000;
    format!("{}:{:02}", s / 60, s % 60)
}

fn to_db(x: f32) -> f32 {
    20.0 * x.max(1e-9).log10()
}

/// −60 dBFS → 0, 0 dBFS → 1, linear in dB.
fn perceptual(db: f32) -> f32 {
    ((db + 60.0) / 60.0).clamp(0.0, 1.0)
}

fn rms_db(samples: &[f32]) -> f32 {
    if samples.is_empty() {
        return -120.0;
    }
    let sum: f64 = samples.iter().map(|&s| f64::from(s * s)).sum();
    to_db((sum / samples.len() as f64).sqrt() as f32)
}

fn median(values: &[f32]) -> f32 {
    if values.is_empty() {
        return -120.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f32::total_cmp);
    sorted[sorted.len() / 2]
}

fn percentile(sorted: &[f32], p: f32) -> f32 {
    sorted[((sorted.len() - 1) as f32 * p).round() as usize]
}

/// Level blocks per second.
const BLOCK_RATE: f32 = SAMPLE_RATE as f32 / BLOCK as f32;
/// Speech analysis window and hop, in level blocks (≈ 2 s and 1 s).
const SPEECH_WINDOW: usize = 60;
const SPEECH_HOP: usize = 30;

/// Share of audible ~2 s windows that look like speech: deep syllabic level
/// modulation (3–8 Hz) and, when pitch is known, gliding, broken voicing rather
/// than held notes.
fn speech_share(block_db: &[f32], frames: &[PitchFrame]) -> f32 {
    if block_db.len() < SPEECH_WINDOW {
        return 0.0;
    }
    let block_ms = 1000.0 / BLOCK_RATE;
    let mut audible = 0;
    let mut speech = 0;
    let mut start = 0;
    while start + SPEECH_WINDOW <= block_db.len() {
        let window = &block_db[start..start + SPEECH_WINDOW];
        let loud = window.iter().filter(|&&db| db >= AUDIBLE_DB).count();
        if loud * 2 >= SPEECH_WINDOW {
            audible += 1;
            let t0 = (start as f32 * block_ms) as u64;
            let t1 = ((start + SPEECH_WINDOW) as f32 * block_ms) as u64;
            let in_window: Vec<&PitchFrame> = frames.iter().filter(|f| f.t_ms >= t0 && f.t_ms < t1).collect();
            if speech_like(window, &in_window) {
                speech += 1;
            }
        }
        start += SPEECH_HOP;
    }
    if audible == 0 { 0.0 } else { speech as f32 / audible as f32 }
}

fn speech_like(window_db: &[f32], frames: &[&PitchFrame]) -> bool {
    let mut sorted = window_db.to_vec();
    sorted.sort_by(f32::total_cmp);
    let depth = percentile(&sorted, 0.9) - percentile(&sorted, 0.1);
    if depth < 15.0 || syllabic_ratio(window_db) < 0.4 {
        return false;
    }
    if frames.len() < 20 {
        return true;
    }
    let voiced: Vec<f32> =
        frames.iter().filter(|f| f.midi.is_finite() && f.confidence >= 0.5).map(|f| f.midi).collect();
    let voiced_share = voiced.len() as f32 / frames.len() as f32;
    if !(0.1..=0.85).contains(&voiced_share) {
        return false;
    }
    let mut steps: Vec<f32> = voiced.windows(2).map(|w| (w[1] - w[0]).abs()).filter(|d| *d < 4.0).collect();
    if steps.is_empty() {
        return true;
    }
    steps.sort_by(f32::total_cmp);
    steps[steps.len() / 2] >= 0.12
}

/// Share of level-envelope modulation energy in 3–8 Hz (syllable rate) within 0.5–15 Hz.
fn syllabic_ratio(window_db: &[f32]) -> f32 {
    let env: Vec<f32> = window_db.iter().map(|&db| 10f32.powf(db.max(-90.0) / 20.0)).collect();
    let mean = env.iter().sum::<f32>() / env.len() as f32;
    let (mut band, mut total) = (0f32, 0f32);
    let mut f = 0.5f32;
    while f <= 15.0 {
        let w = 2.0 * std::f32::consts::PI * f / BLOCK_RATE;
        let (mut re, mut im) = (0f32, 0f32);
        for (n, &x) in env.iter().enumerate() {
            re += (x - mean) * (w * n as f32).cos();
            im -= (x - mean) * (w * n as f32).sin();
        }
        let power = re * re + im * im;
        total += power;
        if (3.0..=8.0).contains(&f) {
            band += power;
        }
        f += 0.5;
    }
    if total <= 0.0 { 0.0 } else { band / total }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(samples: &[f32]) -> Vec<u8> {
        samples.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    #[test]
    fn push_drops_oldest_beyond_backlog() {
        let input = LiveInput::new();
        let second = vec![0.1f32; 48_000];
        for _ in 0..8 {
            input.push(&bytes(&second), 48_000, 1);
        }
        assert!(input.backlog_ms() <= 5_000, "{}", input.backlog_ms());
        assert_eq!(input.dropped_ms(), 3_000);
        input.push(&bytes(&second), 1_000, 1); // unsupported rate: ignored
        assert_eq!(input.backlog_ms(), 5_000);
        input.finish();
        input.push(&bytes(&second), 48_000, 1); // after finish: ignored
        assert_eq!(input.backlog_ms(), 5_000);
        let mut popped = 0;
        while let Ok(Some(_)) = input.pop(Duration::ZERO) {
            popped += 1;
            input.busy.store(false, Ordering::SeqCst);
        }
        assert_eq!(popped, 5);
        assert!(input.pop(Duration::ZERO).is_err(), "finished and drained");
        assert!(input.is_settled());
    }

    #[test]
    fn perceptual_level_mapping() {
        assert_eq!(perceptual(-60.0), 0.0);
        assert_eq!(perceptual(-90.0), 0.0);
        assert_eq!(perceptual(0.0), 1.0);
        assert!((perceptual(-30.0) - 0.5).abs() < 1e-6);
        assert!((rms_db(&[0.5; 100]) - (-6.02)).abs() < 0.01);
        assert_eq!(clock(74_000), "1:14");
    }

    #[test]
    fn kinds() {
        assert_eq!(session_kind(CaptureSource::SystemAudio, ListenMode::Hum), SessionKind::SystemAudio);
        assert_eq!(session_kind(CaptureSource::Microphone, ListenMode::Hum), SessionKind::Hum);
        assert_eq!(session_kind(CaptureSource::Microphone, ListenMode::Auto), SessionKind::Listen);
    }

    /// Syllable-rate on/off level (speech-like) versus a steady level (music-like).
    #[test]
    fn speech_needs_syllabic_modulation() {
        let syllables: Vec<f32> =
            (0..300).map(|i| if (i as f32 / BLOCK_RATE * 4.0).fract() < 0.5 { -20.0 } else { -45.0 }).collect();
        let steady: Vec<f32> = (0..300).map(|i| -20.0 + (i % 3) as f32).collect();
        assert!(speech_share(&syllables, &[]) > 0.8);
        assert_eq!(speech_share(&steady, &[]), 0.0);
        // Held notes (stable pitch) are not speech.
        let notes: Vec<PitchFrame> =
            (0..600).map(|i| PitchFrame { t_ms: i * 16, midi: 60.0 + (i / 40) as f32, confidence: 0.9 }).collect();
        assert_eq!(speech_share(&syllables, &notes), 0.0);
    }
}
