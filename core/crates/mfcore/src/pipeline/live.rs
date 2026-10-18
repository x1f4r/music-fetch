//! Live listening sessions (microphone, system audio). See docs/PIPELINES.md
//! ("Live sessions").
//!
//! `push_pcm` only converts the bytes and queues them ([`LiveInput`]); one
//! dedicated thread per session runs the DSP loop (resampling, level, silence,
//! pitch, impression detection) and schedules recognizer calls on the engine
//! runtime. Time is measured in captured audio, so a session behaves the same
//! whether audio arrives in real time or faster.
//!
//! In the browser there are no threads: the same loop runs as a task on the
//! engine Worker's event loop and processes each pushed chunk as soon as the
//! push returns (DSP inline in the Worker, recognizer calls as tasks).

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use tokio::sync::watch;

use crate::audio::SAMPLE_RATE;
use crate::audio::resample::{StreamResampler, f32_to_i16};
use crate::error::{MfError, Result};
use crate::melody::pitch::FrameDetail;
use crate::melody::{ImpressionDetector, StreamingPitch};
use crate::model::{
    CaptureSource, Evidence, ListenMode, ListenOptions, NoMatchReason, Notice, Outcome, Phase, PitchFrame, Segment,
    SegmentKind, SessionKind, SpeechTranscript, Tier,
};
use crate::pipeline::ctx::SessionCtx;
use crate::pipeline::hum;
use crate::pipeline::recognize::{self, RecognitionHit};
use crate::shazam::Priority;
use crate::store::SessionUpdate;
use crate::util::rt;

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
/// Impression auto-stop: a confident answer, enough voiced audio followed by a
/// pause, a long pause, or a time limit.
const IMPRESSION_MIN_VOICED_MS: u64 = 8_000;
const IMPRESSION_PAUSE_MS: u64 = 1_500;
/// Any pause this long ends the impression once something was hummed.
const IMPRESSION_LONG_PAUSE_MS: u64 = 5_000;
const IMPRESSION_LONG_PAUSE_VOICED_MS: u64 = 1_000;
const IMPRESSION_MAX_MS: u64 = 30_000;
/// Less voicing than this gets `KeepGoing` instead of an answer.
const KEEP_GOING_MS: u64 = 4_000;
/// Progressive melody search while the user hums: every 2 s of capture (4 s in
/// low-power mode) once enough is voiced, one search at a time.
const SEARCH_EVERY_MS: u64 = 2_000;
const SEARCH_EVERY_LOW_POWER_MS: u64 = 4_000;
const SEARCH_MIN_VOICED_MS: u64 = KEEP_GOING_MS;
/// A top answer this likely, with no runner-up above `CONFIDENT_RUNNER_UP`, that
/// two searches in a row agree on, ends the capture early.
const CONFIDENT_SCORE: f32 = 0.8;
const CONFIDENT_RUNNER_UP: f32 = 0.1;
/// Finishing runs one more query when the last one is older than this.
const FINAL_QUERY_AGE_MS: u64 = 4_000;
const MIN_AUDIBLE_MS: u64 = 3_000;
const MATCH_ENRICH_LIMIT: Duration = Duration::from_secs(3);
const POP_TIMEOUT: Duration = Duration::from_millis(20);
/// Share of speech-like windows above which a miss is reported as `MostlySpeech`.
const SPEECH_SHARE: f32 = 0.6;
/// With `ListenOptions::transcripts`, how long the answer waits after capture for
/// the recognizer's final transcript.
const FINAL_TRANSCRIPT_WAIT: Duration = Duration::from_secs(2);
/// Pushed transcript text kept, at most.
const MAX_TRANSCRIPT_CHARS: usize = 2_000;
const MAX_TRANSCRIPT_ALTERNATIVES: usize = 4;

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
pub struct LiveInput {
    state: Mutex<InputState>,
    ready: Condvar,
    /// Wakes the DSP task in the browser (no threads to park there).
    #[cfg(target_family = "wasm")]
    ready_task: tokio::sync::Notify,
    /// The DSP thread is working on a chunk.
    busy: AtomicBool,
    /// A recognizer call is running.
    inflight: AtomicBool,
    /// The newest transcript the app pushed.
    transcript: watch::Sender<Option<SpeechTranscript>>,
}

impl LiveInput {
    /// An empty input queue.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            ready: Condvar::new(),
            #[cfg(target_family = "wasm")]
            ready_task: tokio::sync::Notify::new(),
            busy: AtomicBool::new(false),
            inflight: AtomicBool::new(false),
            transcript: watch::Sender::new(None),
        })
    }

    /// Replace the transcript of what the user sang so far (any thread, also
    /// shortly after capture ended).
    pub fn push_transcript(&self, mut transcript: SpeechTranscript) {
        transcript.text = transcript.text.chars().take(MAX_TRANSCRIPT_CHARS).collect();
        transcript.alternatives.truncate(MAX_TRANSCRIPT_ALTERNATIVES);
        for alternative in &mut transcript.alternatives {
            *alternative = alternative.chars().take(MAX_TRANSCRIPT_CHARS).collect();
        }
        self.transcript.send_replace(Some(transcript));
    }

    /// Follow the pushed transcripts.
    pub fn transcripts(&self) -> watch::Receiver<Option<SpeechTranscript>> {
        self.transcript.subscribe()
    }

    /// Queue interleaved little-endian f32 PCM. Never blocks on analysis: beyond
    /// 5 s of backlog the oldest chunks are dropped.
    pub fn push(&self, data: &[u8], sample_rate: u32, channels: u32) {
        if data.len() < 4 {
            return;
        }
        let valid =
            (MIN_INPUT_RATE..=MAX_INPUT_RATE).contains(&sample_rate) && (1..=MAX_INPUT_CHANNELS).contains(&channels);
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
        #[cfg(target_family = "wasm")]
        self.ready_task.notify_one();
    }

    /// The user stopped capture.
    pub fn finish(&self) {
        self.state.lock().finished = true;
        self.ready.notify_all();
        #[cfg(target_family = "wasm")]
        self.ready_task.notify_one();
    }

    /// Wake the DSP thread (after cancellation).
    pub fn wake(&self) {
        self.ready.notify_all();
        #[cfg(target_family = "wasm")]
        self.ready_task.notify_one();
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.chunks.clear();
        state.backlog_ms = 0.0;
    }

    /// False once the session stopped listening (pushes are ignored).
    pub fn is_open(&self) -> bool {
        let state = self.state.lock();
        !state.closed && !state.finished
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
    #[cfg(not(target_family = "wasm"))]
    fn pop(&self, timeout: Duration) -> std::result::Result<Option<Chunk>, ()> {
        let mut state = self.state.lock();
        if state.chunks.is_empty() && !state.finished {
            self.ready.wait_for(&mut state, timeout);
        }
        Self::take(&mut state, &self.busy)
    }

    /// [`pop`](Self::pop) for the browser's DSP task: waits without blocking.
    #[cfg(target_family = "wasm")]
    async fn pop_async(&self, timeout: Duration) -> std::result::Result<Option<Chunk>, ()> {
        let idle = {
            let state = self.state.lock();
            state.chunks.is_empty() && !state.finished
        };
        if idle {
            // A push between the check and the wait leaves a permit: no lost wake-up.
            let _ = rt::timeout(timeout, self.ready_task.notified()).await;
        }
        Self::take(&mut self.state.lock(), &self.busy)
    }

    fn take(state: &mut InputState, busy: &AtomicBool) -> std::result::Result<Option<Chunk>, ()> {
        match state.chunks.pop_front() {
            Some(chunk) => {
                state.backlog_ms = (state.backlog_ms - chunk.ms).max(0.0);
                busy.store(true, Ordering::SeqCst);
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
/// the terminal event. (In the browser: a task on the event loop.)
pub fn spawn(
    ctx: Arc<SessionCtx>,
    source: CaptureSource,
    mode: ListenMode,
    options: ListenOptions,
    input: Arc<LiveInput>,
    on_exit: impl FnOnce() + Send + 'static,
) -> Result<()> {
    #[cfg(target_family = "wasm")]
    {
        let runtime = ctx.engine.runtime.clone();
        runtime.spawn(async move {
            ctx.emitter.started(session_kind(source, mode));
            // Panics end the whole instance in the browser; nothing to catch.
            Live::new(ctx.clone(), source, mode, options, input.clone()).run_async().await;
            ended(&ctx, &input);
            on_exit();
        });
        Ok(())
    }
    #[cfg(not(target_family = "wasm"))]
    {
        let name = format!("mf-live-{}", ctx.id.chars().take(8).collect::<String>());
        std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                let kind = session_kind(source, mode);
                ctx.emitter.started(kind);
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    Live::new(ctx.clone(), source, mode, options, input.clone()).run()
                }));
                if let Err(panic) = outcome {
                    tracing::error!(session = %ctx.id, "live session panicked: {}", panic_message(&panic));
                    ctx.emitter.failed(&MfError::internal("the session stopped unexpectedly"));
                }
                ended(&ctx, &input);
                on_exit();
            })
            .map(drop)
            .map_err(|e| MfError::internal(format!("could not start the listening thread: {e}")))
    }
}

/// After the session body: no more audio, exactly one terminal event, no work files.
fn ended(ctx: &SessionCtx, input: &LiveInput) {
    input.close();
    if !ctx.emitter.is_terminal() {
        if ctx.cancel.is_canceled() {
            ctx.emitter.canceled();
        } else {
            ctx.emitter.failed(&MfError::internal("the session ended without a result"));
        }
    }
    ctx.cleanup();
}

/// The result of a task that already finished, without waiting.
fn finished<T>(runtime: &rt::Handle, task: rt::JoinHandle<T>) -> std::result::Result<T, rt::JoinError> {
    #[cfg(not(target_family = "wasm"))]
    {
        runtime.block_on(task)
    }
    #[cfg(target_family = "wasm")]
    {
        use futures_util::FutureExt;
        let _ = runtime;
        task.now_or_never().expect("a finished task has its result")
    }
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
    /// The progressive melody search is confident.
    Confident,
    /// The app called `finish()`.
    User,
    Canceled,
}

type QueryTask = rt::JoinHandle<Result<Option<RecognitionHit>>>;
/// A progressive melody search over the first `frames` pitch frames.
type SearchTask = (usize, rt::JoinHandle<Option<hum::MelodySearch>>);

/// The newest finished progressive search.
struct Searched {
    /// Pitch frames it covered.
    frames: usize,
    /// Top song and score.
    top: Option<(u32, f32)>,
    result: hum::MelodySearch,
}

struct Live {
    ctx: Arc<SessionCtx>,
    input: Arc<LiveInput>,
    mode: ListenMode,
    options: ListenOptions,
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
    details: Vec<FrameDetail>,
    // Progressive melody search.
    search: Option<SearchTask>,
    searched: Option<Searched>,
    next_search_ms: u64,
    confident: bool,
    /// Interim candidates are on screen (`ListenOptions::live_candidates`).
    shown_candidates: bool,
    keep_going_noticed: bool,
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
    fn new(
        ctx: Arc<SessionCtx>,
        source: CaptureSource,
        mode: ListenMode,
        options: ListenOptions,
        input: Arc<LiveInput>,
    ) -> Self {
        let mode = if source == CaptureSource::SystemAudio { ListenMode::Recording } else { mode };
        Self {
            ctx,
            input,
            mode,
            options,
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
            details: Vec::new(),
            search: None,
            searched: None,
            next_search_ms: 0,
            confident: false,
            shown_candidates: false,
            keep_going_noticed: false,
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

    #[cfg(not(target_family = "wasm"))]
    fn run(mut self) {
        self.begin();
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

    /// [`run`](Self::run) as a task (browser).
    #[cfg(target_family = "wasm")]
    async fn run_async(mut self) {
        self.begin();
        let stop = self.listen_async().await;
        self.input.close();
        self.input.busy.store(false, Ordering::SeqCst);
        if stop == Stop::Canceled {
            return;
        }
        self.finish(stop).await;
        self.input.inflight.store(false, Ordering::SeqCst);
    }

    fn begin(&mut self) {
        self.ctx.emitter.phase(Phase::Listening, None);
        if self.impression_at_ms.is_some() {
            self.notify_missing_index();
        }
        self.start_pitch();
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
    #[cfg(not(target_family = "wasm"))]
    fn listen(&mut self) -> Stop {
        loop {
            if let Some(stop) = self.poll_stop() {
                return stop;
            }
            match self.input.pop(POP_TIMEOUT) {
                Ok(Some(chunk)) => self.take_chunk(chunk),
                Ok(None) => {}
                Err(()) => return Stop::User,
            }
        }
    }

    /// [`listen`](Self::listen) as a task (browser): each chunk is processed
    /// when it arrives; between chunks the recognizer and search tasks run.
    #[cfg(target_family = "wasm")]
    async fn listen_async(&mut self) -> Stop {
        loop {
            if let Some(stop) = self.poll_stop() {
                return stop;
            }
            match self.input.pop_async(POP_TIMEOUT).await {
                Ok(Some(chunk)) => {
                    self.take_chunk(chunk);
                    rt::yield_to_tasks().await;
                }
                Ok(None) => {}
                Err(()) => return Stop::User,
            }
        }
    }

    /// Collects finished work; `Some` when capture must stop.
    fn poll_stop(&mut self) -> Option<Stop> {
        if self.ctx.cancel.is_canceled() {
            return Some(Stop::Canceled);
        }
        self.poll_queries();
        self.poll_search();
        if self.matched.is_some() {
            return Some(Stop::Matched);
        }
        self.stop_condition()
    }

    fn take_chunk(&mut self, chunk: Chunk) {
        self.process(chunk);
        self.poll_queries();
        self.poll_search();
        self.input.busy.store(false, Ordering::SeqCst);
    }

    fn process(&mut self, chunk: Chunk) {
        let needs_new =
            self.resampler.as_ref().is_none_or(|r| r.input_rate() != chunk.rate || r.channels() != chunk.channels);
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
            match guarded("pitch tracking", || pitch.push_detailed(new)) {
                Some(details) => {
                    let frames: Vec<PitchFrame> = details.iter().map(FrameDetail::to_pitch_frame).collect();
                    if !frames.is_empty() {
                        self.ctx.emitter.pitch(&frames);
                    }
                    if let Some(detector) = self.detector.as_mut()
                        && guarded("impression detector", || detector.push(&frames, new)).is_none()
                    {
                        self.detector = None;
                    }
                    self.frames.extend(frames);
                    self.details.extend(details);
                }
                None => {
                    self.pitch = None;
                    self.detector = None;
                }
            }
        }
    }

    /// Humming needs the melody index: tell the app as soon as an impression starts.
    fn notify_missing_index(&self) {
        if self.ctx.engine.melody_index().is_none() {
            self.ctx.emitter.notice(Notice::MelodyIndexMissing);
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
            self.notify_missing_index();
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
        } else {
            self.issue_search();
            self.keep_going_hint();
        }
        self.update_inflight();
    }

    /// Start a progressive melody search when one is due.
    fn issue_search(&mut self) {
        let elapsed = self.elapsed_ms();
        if self.search.is_some() || self.confident || elapsed < self.next_search_ms {
            return;
        }
        if self.voiced_ms() < SEARCH_MIN_VOICED_MS {
            return;
        }
        let Some(index) = self.ctx.engine.melody_index() else { return };
        let every = if self.ctx.engine.is_low_power() { SEARCH_EVERY_LOW_POWER_MS } else { SEARCH_EVERY_MS };
        self.next_search_ms = elapsed + every;
        let frames = self.details.clone();
        let n = frames.len();
        let task = self.ctx.engine.runtime.spawn_blocking(move || hum::search_frames(&index, &frames));
        self.search = Some((n, task));
    }

    /// Collect a finished progressive search: interim candidates, confidence.
    fn poll_search(&mut self) {
        if !self.search.as_ref().is_some_and(|(_, t)| t.is_finished()) {
            return;
        }
        let Some((frames, task)) = self.search.take() else { return };
        let runtime = self.ctx.engine.runtime.clone();
        match finished(&runtime, task) {
            Ok(Some(result)) => self.take_search(frames, result),
            Ok(None) => {}
            Err(e) => tracing::error!("progressive melody search failed: {e}"),
        }
        self.update_inflight();
    }

    fn take_search(&mut self, frames: usize, result: hum::MelodySearch) {
        let top = result.matches.first().map(|m| (m.song.id, m.score));
        let runner_up = result.matches.get(1).map(|m| m.score).unwrap_or(0.0);
        let agrees = self.searched.as_ref().and_then(|s| s.top).zip(top).is_some_and(|(a, b)| a.0 == b.0);
        if self.options.auto_stop
            && agrees
            && top.is_some_and(|(_, score)| score >= CONFIDENT_SCORE)
            && runner_up <= CONFIDENT_RUNNER_UP
        {
            self.confident = true;
        }
        if self.options.live_candidates {
            let candidates = hum::melody_candidates(&result.matches);
            // An empty list clears a previous one.
            if !candidates.is_empty() || self.shown_candidates {
                self.shown_candidates = !candidates.is_empty();
                self.ctx.emitter.candidates(candidates, false);
            }
        }
        self.searched = Some(Searched { frames, top, result });
    }

    /// A pause before there is enough to search with: ask for more, once.
    fn keep_going_hint(&mut self) {
        if self.keep_going_noticed {
            return;
        }
        let voiced = self.voiced_ms();
        if (IMPRESSION_LONG_PAUSE_VOICED_MS..KEEP_GOING_MS).contains(&voiced)
            && self.pause_ms().is_some_and(|p| p >= IMPRESSION_PAUSE_MS)
        {
            self.keep_going_noticed = true;
            self.ctx.emitter.notice(Notice::KeepGoing);
        }
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
        if !self.options.auto_stop {
            return None;
        }
        if self.confident {
            return Some(Stop::Confident);
        }
        let voiced = self.voiced_ms();
        let pause = self.pause_ms().unwrap_or(0);
        if voiced >= IMPRESSION_MIN_VOICED_MS && pause >= IMPRESSION_PAUSE_MS {
            return Some(Stop::Impression);
        }
        if voiced >= IMPRESSION_LONG_PAUSE_VOICED_MS && pause >= IMPRESSION_LONG_PAUSE_MS {
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
        let busy = self.shazam.is_some() || self.paid.is_some() || self.search.is_some();
        self.input.inflight.store(busy, Ordering::SeqCst);
    }

    /// Collect finished recognizer calls; issue a query that was waiting.
    fn poll_queries(&mut self) {
        let runtime = self.ctx.engine.runtime.clone();
        if self.shazam.as_ref().is_some_and(|t| t.is_finished())
            && let Some(task) = self.shazam.take()
        {
            self.on_result("Shazam", finished(&runtime, task));
        }
        if self.paid.as_ref().is_some_and(|t| t.is_finished())
            && let Some(task) = self.paid.take()
        {
            self.on_result("paid providers", finished(&runtime, task));
        }
        if self.pending_query && self.shazam.is_none() && self.recording_rules() && self.matched.is_none() {
            self.issue_query();
        }
        self.update_inflight();
    }

    fn on_result(&mut self, who: &str, result: std::result::Result<Result<Option<RecognitionHit>>, rt::JoinError>) {
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
        if let Some(pitch) = self.pitch.as_mut()
            && let Some(rest) = guarded("pitch tracking", || pitch.finish_detailed())
        {
            self.details.extend(rest);
        }
        let voiced = self.voiced_ms();
        let kind = Some(SessionKind::Hum);
        // Too little to answer with: ask for more instead of guessing.
        if voiced < KEEP_GOING_MS {
            if !self.keep_going_noticed {
                self.ctx.emitter.notice(Notice::KeepGoing);
            }
            let reason = self.no_match_reason(true);
            self.ctx.emitter.finished(Outcome::NoMatch { reason }, SessionUpdate { kind, ..Default::default() });
            return;
        }
        self.ctx.emitter.flush_pitch();
        // A search still running may cover the whole capture.
        if let Some((frames, task)) = self.search.take() {
            match task.await {
                Ok(Some(result)) => self.take_search(frames, result),
                Ok(None) => {}
                Err(e) => tracing::error!("progressive melody search failed: {e}"),
            }
        }
        tracing::debug!(?stop, voiced, "impression finished");
        // The streaming frames are the batch analysis's unless the stream took the
        // whistle path, which it cannot revise: a hissy onset, sibilants or whistles
        // near 1.2 kHz can switch it where the batch analysis, voting on the whole
        // clip, would not. Then (or when the tracker failed) the whole capture is
        // tracked again. Otherwise the last progressive search is the answer when
        // nothing was voiced after it, and the final search runs on the streaming
        // frames. After a confident stop the last search is the answer either way.
        let direct = self.pitch.as_ref().is_some_and(|p| !p.is_whistle());
        let melody = match self.searched.take() {
            Some(s)
                if (stop == Stop::Confident || direct)
                    && !self.details[s.frames.min(self.details.len())..].iter().any(|f| f.voiced) =>
            {
                hum::MelodyInput::Searched(s.result)
            }
            _ if direct && !self.details.is_empty() => hum::MelodyInput::Frames(std::mem::take(&mut self.details)),
            _ => hum::MelodyInput::Track,
        };
        let capture = std::mem::take(&mut self.capture);
        let transcripts = self.input.transcripts();
        // Transcripts the app pushed replace asking its recognizer afterwards.
        let speech = if self.options.transcripts {
            hum::SpeechInput::Pushed { transcripts, wait: FINAL_TRANSCRIPT_WAIT }
        } else if transcripts.borrow().is_some() {
            hum::SpeechInput::Pushed { transcripts, wait: Duration::ZERO }
        } else {
            hum::SpeechInput::Recognizer
        };
        match hum::run_impression_detailed(&self.ctx, &capture, melody, speech, true, self.shown_candidates).await {
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
        let update = SessionUpdate {
            title: Some(track.title.clone()),
            subtitle: Some(track.artist.clone()),
            ..Default::default()
        };
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
/// Level range (p90 − p10) of a speech window: pauses between words. Music
/// stays within ~6–16 dB, speech spans 25 dB and more.
const SPEECH_DEPTH_DB: f32 = 20.0;
/// Minimum share of envelope modulation at syllable rate.
const SPEECH_SYLLABIC: f32 = 0.2;
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
    if depth < SPEECH_DEPTH_DB || syllabic_ratio(window_db) < SPEECH_SYLLABIC {
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
