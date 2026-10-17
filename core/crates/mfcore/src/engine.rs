//! The `Engine` object and session handles exposed to the apps.
//!
//! The engine owns a tokio runtime ("mf-core", 4 workers), the HTTP client, the
//! store, the Shazam queue, the call budget and the melody index. Every session
//! gets a cancel token and an emitter; its body runs inside a panic guard so a
//! bug becomes `Failed { Internal }` instead of crossing the FFI.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use futures_util::FutureExt;
use parking_lot::{Mutex, RwLock};

use crate::config::{Credentials, EngineConfig};
use crate::error::{MfError, Result};
use crate::identity;
use crate::media::YtDlpSubprocess;
use crate::model::{
    AnalyzeMode, Candidate, CandidateTier, CaptureSource, Evidence, EvidenceSource, ExportFormat, LibraryEntry,
    LibraryQuery, ListenMode, ListenOptions, Lyrics, MelodyIndexStatus, Outcome, Segment, SegmentKind, SessionDetail,
    SessionKind, StorageUsage, Tier, Track,
};
use crate::pipeline::analyze::{self, AnalyzeInput};
use crate::pipeline::ctx::{EngineCtx, SessionCtx, preview_expired};
use crate::pipeline::emitter::Emitter;
use crate::pipeline::hum;
use crate::pipeline::live::{self, LiveInput, panic_message};
use crate::pipeline::melody_index::{self, Manifest};
use crate::pipeline::recognize::CallBudget;
use crate::platform::{MediaBackend, PlatformDecoder, SessionObserver, SpeechToText};
use crate::shazam::{QueuePolicy, ShazamClient};
use crate::store::{SessionUpdate, Store};
use crate::util::cancel::CancelToken;
use crate::util::{new_id, now_ms};

const DATABASE_FILE: &str = "musicfetch.sqlite3";
const WORKER_THREADS: usize = 4;
const MAX_BLOCKING_THREADS: usize = 64;
/// How long `delete_sessions` waits for canceled sessions to stop.
const DELETE_WAIT: Duration = Duration::from_secs(2);
const ENRICH_TRACK_LIMIT: Duration = Duration::from_secs(10);
const LYRICS_LIMIT: Duration = Duration::from_secs(15);
const LYRICS_NAMESPACE: &str = "lyrics";
const LYRICS_TTL_MS: i64 = 30 * 24 * 3600 * 1000;
const NO_LYRICS_TTL_MS: i64 = 24 * 3600 * 1000;
/// yt-dlp temp folders older than this are leftovers from a crash.
const STALE_TEMP: Duration = Duration::from_secs(3600);
const MAX_ALTERNATES: usize = 5;
const MAX_LYRIC_QUERY_CHARS: usize = 500;
const TITLE_CHARS: usize = 80;

/// Rust-only construction options for tools and tests.
#[derive(Default)]
pub struct EngineOptions {
    /// Send Shazam requests to this origin instead of the public endpoint.
    pub shazam_base_url: Option<String>,
    /// Replace the Shazam pacing policy.
    pub shazam_policy: Option<QueuePolicy>,
    /// Replace the engine-wide recognizer budget.
    pub call_budget: Option<CallBudget>,
    /// Skip catalog enrichment (offline tests).
    pub offline_catalog: bool,
}

/// A running session in the registry.
#[derive(Clone)]
struct Running {
    cancel: CancelToken,
    emitter: Emitter,
    input: Option<Arc<LiveInput>>,
}

#[derive(Default)]
struct Registry {
    sessions: Mutex<HashMap<String, Running>>,
}

impl Registry {
    fn insert(&self, id: &str, running: Running) {
        self.sessions.lock().insert(id.to_string(), running);
    }

    fn remove(&self, id: &str) {
        self.sessions.lock().remove(id);
    }

    fn get(&self, id: &str) -> Option<Running> {
        self.sessions.lock().get(id).cloned()
    }

    fn ids(&self) -> HashSet<String> {
        self.sessions.lock().keys().cloned().collect()
    }

    fn all(&self) -> Vec<Running> {
        self.sessions.lock().values().cloned().collect()
    }
}

/// Cancel a session: absolute and prompt. `Canceled` is emitted from an engine
/// thread even when the session body is stuck in a blocking call.
fn cancel_running(
    runtime: &tokio::runtime::Handle,
    cancel: &CancelToken,
    emitter: &Emitter,
    input: Option<&LiveInput>,
) {
    cancel.cancel();
    if let Some(input) = input {
        input.wake();
    }
    let emitter = emitter.clone();
    runtime.spawn(async move { emitter.canceled() });
}

#[derive(Default)]
struct IndexState {
    installing: AtomicBool,
    /// Manifest found by the last `check_melody_index`.
    available: Mutex<Option<Manifest>>,
}

/// Clears the "installing" flag however the install ends.
struct InstallGuard(Arc<IndexState>);

impl Drop for InstallGuard {
    fn drop(&mut self) {
        self.0.installing.store(false, Ordering::SeqCst);
    }
}

/// The Music Fetch engine: one per app, shared by every session.
#[derive(uniffi::Object)]
pub struct Engine {
    ctx: Arc<EngineCtx>,
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    registry: Arc<Registry>,
    index: Arc<IndexState>,
}

#[uniffi::export]
impl Engine {
    /// Create the engine. `media` overrides the built-in yt-dlp subprocess backend
    /// (`config.media_tools`); `decoder` and `speech` are optional.
    #[uniffi::constructor]
    pub fn new(
        config: EngineConfig,
        media: Option<Arc<dyn MediaBackend>>,
        decoder: Option<Arc<dyn PlatformDecoder>>,
        speech: Option<Arc<dyn SpeechToText>>,
    ) -> Result<Arc<Self>> {
        Self::with_options(config, media, decoder, speech, EngineOptions::default())
    }

    /// Replace the credentials for paid providers. Running sessions keep the
    /// credentials they started with.
    pub fn set_credentials(&self, credentials: Credentials) {
        *self.ctx.credentials.write() = credentials;
    }

    /// Start a live session; push audio with `LiveSession::push_pcm`.
    pub fn start_listening(
        &self,
        source: CaptureSource,
        mode: ListenMode,
        observer: Arc<dyn SessionObserver>,
    ) -> Result<Arc<LiveSession>> {
        let kind = live::session_kind(source, mode);
        let ctx = self.open_session(kind, "", None, observer, true)?;
        let input = LiveInput::new();
        self.registry.insert(
            &ctx.id,
            Running { cancel: ctx.cancel.clone(), emitter: ctx.emitter.clone(), input: Some(input.clone()) },
        );
        let registry = self.registry.clone();
        let id = ctx.id.clone();
        let session = Arc::new(LiveSession {
            id: ctx.id.clone(),
            input: input.clone(),
            cancel: ctx.cancel.clone(),
            emitter: ctx.emitter.clone(),
            runtime: self.ctx.runtime.clone(),
        });
        if let Err(e) = live::spawn(ctx.clone(), source, mode, input, move || registry.remove(&id)) {
            // Nothing was announced yet: report the error here only.
            self.registry.remove(&ctx.id);
            let _ = self.ctx.store.delete_sessions(std::slice::from_ref(&ctx.id));
            return Err(e);
        }
        Ok(session)
    }

    /// Start a live session with options (e.g. no impression auto-stop).
    pub fn start_listening_with(
        &self,
        source: CaptureSource,
        mode: ListenMode,
        options: ListenOptions,
        observer: Arc<dyn SessionObserver>,
    ) -> Result<Arc<LiveSession>> {
        // Options are applied by the live pipeline (wave 3); until then they are ignored.
        let _ = options;
        self.start_listening(source, mode, observer)
    }

    /// Reduce CPU and memory use (weak devices, battery saver). The engine also
    /// turns this on by itself on machines with few cores or little memory.
    pub fn set_low_power(&self, enabled: bool) {
        self.ctx.low_power.store(enabled, Ordering::Relaxed);
    }

    pub fn is_low_power(&self) -> bool {
        self.ctx.is_low_power()
    }

    /// Mark a segment as not identified ("Not this"), keeping its track as an alternate.
    pub fn reject_segment(&self, session_id: String, segment_id: String) -> Result<Segment> {
        let _ = (session_id, segment_id);
        Err(MfError::internal("not implemented"))
    }

    /// Identify sessions captured while offline (status `Waiting`). Call when the
    /// network is back; returns how many were started. Results arrive as library updates
    /// and through `observer`.
    pub fn retry_waiting(&self, observer: Arc<dyn SessionObserver>) -> Result<u32> {
        let _ = observer;
        Ok(0)
    }

    /// Analyze a link or a local file path.
    pub fn analyze(
        &self,
        input: String,
        mode: AnalyzeMode,
        observer: Arc<dyn SessionObserver>,
    ) -> Result<Arc<AnalysisSession>> {
        let validated = catch_unwind(AssertUnwindSafe(|| analyze::validate(&input)))
            .map_err(|_| MfError::internal("the input could not be checked"))??;
        let (kind, title, source_url) = match &validated {
            AnalyzeInput::Url(url) => (SessionKind::Link, url.clone(), Some(url.clone())),
            AnalyzeInput::File(path) => (SessionKind::File, file_title(path), None),
        };
        let ctx = self.open_session(kind, &title, source_url.as_deref(), observer, true)?;
        let body_ctx = ctx.clone();
        Ok(self.spawn_session(ctx, kind, async move { analyze::run(body_ctx, validated, mode).await }))
    }

    /// Find songs from typed lyrics.
    pub fn search_lyrics(&self, text: String, observer: Arc<dyn SessionObserver>) -> Result<Arc<AnalysisSession>> {
        let text: String = text.trim().chars().take(MAX_LYRIC_QUERY_CHARS).collect();
        if !text.chars().any(char::is_alphanumeric) {
            return Err(MfError::invalid("Type a few words of the lyrics."));
        }
        let title = truncate(&text, TITLE_CHARS);
        let ctx = self.open_session(SessionKind::Lyrics, &title, None, observer, true)?;
        let body_ctx = ctx.clone();
        Ok(self.spawn_session(ctx, SessionKind::Lyrics, async move {
            hum::run_lyric_search(&body_ctx, &text).await;
        }))
    }

    /// Library entries, newest first.
    pub fn library(&self, query: LibraryQuery) -> Result<Vec<LibraryEntry>> {
        self.ctx.store.library(&query)
    }

    /// Everything stored for a session, or `None` if it does not exist.
    pub fn session(&self, id: String) -> Result<Option<SessionDetail>> {
        self.ctx.store.session_detail(&id)
    }

    /// Delete sessions: running ones are canceled first; their kept recordings
    /// and cached media are removed too.
    pub fn delete_sessions(&self, ids: Vec<String>) -> Result<()> {
        // Ids name files below the app folders: accept only what the engine issues.
        let ids: Vec<String> = ids.into_iter().filter(|id| is_session_id(id)).collect();
        let mut waiting = Vec::new();
        for id in &ids {
            if let Some(running) = self.registry.get(id) {
                cancel_running(&self.ctx.runtime, &running.cancel, &running.emitter, running.input.as_deref());
                waiting.push(id.clone());
            }
        }
        let started = Instant::now();
        while !waiting.is_empty() && started.elapsed() < DELETE_WAIT {
            let running = self.registry.ids();
            waiting.retain(|id| running.contains(id));
            if !waiting.is_empty() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        self.ctx.store.delete_sessions(&ids)?;
        for id in &ids {
            remove_quietly(&self.ctx.recording_path(id));
            let dir = self.ctx.sessions_cache_dir().join(id);
            if dir.exists() {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
        Ok(())
    }

    /// Pin or unpin a session (pinned sessions keep their cached media).
    pub fn set_pinned(&self, id: String, pinned: bool) -> Result<()> {
        self.ctx.store.set_pinned(&id, pinned)
    }

    /// Replace a segment's track with the user's choice (confidence 1, tier Confirmed).
    pub fn correct_segment(&self, session_id: String, segment_id: String, track: Track) -> Result<Segment> {
        let mut segment = self
            .ctx
            .store
            .segment(&session_id, &segment_id)?
            .ok_or_else(|| MfError::invalid("The segment no longer exists."))?;
        let mut track = track;
        identity::refresh_identity(&mut track);
        if track.title.trim().is_empty() {
            return Err(MfError::invalid("The track needs a title."));
        }
        let mut alternates = std::mem::take(&mut segment.alternates);
        if let Some(previous) = segment.track.take()
            && !identity::same_track(&previous, &track)
        {
            alternates.insert(0, previous);
        }
        alternates.retain(|a| !identity::same_track(a, &track));
        let mut deduped: Vec<Track> = Vec::new();
        for alternate in alternates {
            if !deduped.iter().any(|d| identity::same_track(d, &alternate)) {
                deduped.push(alternate);
            }
        }
        deduped.truncate(MAX_ALTERNATES);
        segment.alternates = deduped;
        segment.track = Some(track);
        segment.kind = SegmentKind::Track;
        segment.confidence = 1.0;
        segment.tier = Tier::Confirmed;
        segment.evidence.retain(|e| e.source != EvidenceSource::User);
        segment.evidence.push(Evidence {
            source: EvidenceSource::User,
            at_ms: segment.start_ms,
            score: 1.0,
            detail: None,
        });
        self.ctx.store.upsert_segment(&session_id, &segment)?;
        Ok(segment)
    }

    /// Record the user's pick among impression candidates ("This is it").
    pub fn confirm_candidate(&self, session_id: String, track: Track) -> Result<()> {
        let detail =
            self.ctx.store.session_detail(&session_id)?.ok_or_else(|| MfError::invalid("Session not found."))?;
        let mut track = track;
        identity::refresh_identity(&mut track);
        let mut candidates = detail.candidates;
        let chosen = match candidates.iter().position(|c| identity::same_track(&c.track, &track)) {
            Some(i) => {
                let mut c = candidates.remove(i);
                c.tier = CandidateTier::Strong;
                c
            }
            None => Candidate {
                track: track.clone(),
                score: 1.0,
                tier: CandidateTier::Strong,
                sources: Vec::new(),
                alignment: None,
            },
        };
        let primary = chosen.track.clone();
        candidates.insert(0, chosen);
        self.ctx.store.set_candidates(&session_id, &candidates)?;
        let update = SessionUpdate {
            title: Some(primary.title.clone()),
            subtitle: Some(primary.artist.clone()),
            artwork_url: primary.artwork_url.clone(),
            track: Some(primary),
            ..Default::default()
        };
        self.ctx.store.update_session(&session_id, update)
    }

    /// Render a session as text, CSV, JSON or a CUE sheet.
    pub fn export_session(&self, id: String, format: ExportFormat) -> Result<String> {
        self.ctx.store.export(&id, format)
    }

    /// Disk use: the database files and kept recordings, the cache folder and the
    /// melody index.
    pub fn storage_usage(&self) -> StorageUsage {
        let data = PathBuf::from(&self.ctx.config.data_dir);
        let database: u64 =
            ["", "-wal", "-shm"].iter().map(|suffix| file_size(&data.join(format!("{DATABASE_FILE}{suffix}")))).sum();
        StorageUsage {
            database_bytes: database + dir_size(&self.ctx.recordings_dir()),
            cache_bytes: dir_size(Path::new(&self.ctx.config.cache_dir)),
            melody_index_bytes: dir_size(&melody_index::melody_dir(&self.ctx)),
        }
    }

    /// Delete cached downloads and decoded audio of finished, unpinned sessions.
    pub fn clear_cache(&self) -> Result<()> {
        let running = self.registry.ids();
        let pinned: HashSet<String> = self
            .ctx
            .store
            .library(&LibraryQuery { kinds: Vec::new(), search: None, pinned_only: true, limit: 0, offset: 0 })?
            .into_iter()
            .map(|e| e.id)
            .collect();
        if let Ok(entries) = std::fs::read_dir(self.ctx.sessions_cache_dir()) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !running.contains(&name) && !pinned.contains(&name) {
                    remove_path(&entry.path());
                }
            }
        }
        remove_stale(&PathBuf::from(&self.ctx.config.cache_dir).join("tmp"), STALE_TEMP);
        if let Err(e) = self.ctx.store.purge_expired_cache() {
            tracing::warn!("cache purge failed: {e}");
        }
        Ok(())
    }

    /// The installed melody index and, after `check_melody_index`, the newest
    /// published one.
    pub fn melody_index_status(&self) -> MelodyIndexStatus {
        melody_index::status(&self.ctx, self.index.available.lock().as_ref())
    }

    /// Fetch the manifest and report whether a newer index exists.
    pub fn check_melody_index(&self) -> Result<MelodyIndexStatus> {
        let ctx = self.ctx.clone();
        let url = self.ctx.config.melody_index_manifest_url.clone();
        let manifest =
            self.block_on(async move { melody_index::fetch_manifest(&ctx.http, &url, &CancelToken::new()).await })??;
        *self.index.available.lock() = Some(manifest);
        Ok(self.melody_index_status())
    }

    /// Download and install the melody index. Progress arrives as
    /// `Phase { Downloading, fraction }`; ends with `Finished` or `Failed`.
    pub fn install_melody_index(&self, observer: Arc<dyn SessionObserver>) -> Result<Arc<AnalysisSession>> {
        let url = self.ctx.config.melody_index_manifest_url.trim().to_string();
        if url.is_empty() {
            return Err(MfError::invalid("No melody index source is configured."));
        }
        if self.index.installing.swap(true, Ordering::SeqCst) {
            return Err(MfError::invalid("The melody index is already downloading."));
        }
        let guard = InstallGuard(self.index.clone());
        // Not a library session: nothing is stored for it.
        let ctx = match self.open_session(SessionKind::Hum, "", None, observer, false) {
            Ok(ctx) => ctx,
            Err(e) => {
                drop(guard);
                return Err(e);
            }
        };
        let body_ctx = ctx.clone();
        let state = self.index.clone();
        Ok(self.spawn_session(ctx, SessionKind::Hum, async move {
            let result = melody_index::install(&body_ctx, &url).await;
            // Allow the next download before anyone hears about this one ending.
            drop(guard);
            match result {
                Ok(manifest) => {
                    let mut available = state.available.lock();
                    if available.as_ref().is_some_and(|m| m.version == manifest.version) {
                        *available = None;
                    }
                    drop(available);
                    body_ctx.emitter.finished(Outcome::Completed { identified: 1, total: 1 }, SessionUpdate::default());
                }
                Err(e) => body_ctx.emitter.failed(&e),
            }
        }))
    }

    /// Remove the installed melody index.
    pub fn remove_melody_index(&self) -> Result<()> {
        if self.index.installing.load(Ordering::SeqCst) {
            return Err(MfError::invalid("The melody index is downloading; cancel the download first."));
        }
        melody_index::remove(&self.ctx)
    }

    /// Fill links, artwork and previews for a track (best effort). Also refreshes
    /// expired preview URLs.
    pub async fn enrich_track(&self, track: Track) -> Track {
        let ctx = self.ctx.clone();
        let fallback = track.clone();
        let task = self.ctx.runtime.spawn(async move {
            let mut track = track;
            let now_s = now_ms() / 1000;
            if track.preview_url.as_deref().is_some_and(|url| preview_expired(url, now_s)) {
                track.preview_url = None;
            }
            ctx.enrich_within(track, ENRICH_TRACK_LIMIT, &CancelToken::new()).await
        });
        task.await.unwrap_or(fallback)
    }

    /// Lyrics for a track, if any are known.
    pub async fn fetch_lyrics(&self, track: Track) -> Option<Lyrics> {
        let ctx = self.ctx.clone();
        let task = self.ctx.runtime.spawn(async move {
            let key = identity::identity_key(&track);
            if let Ok(Some(json)) = ctx.store.cache_get(LYRICS_NAMESPACE, &key)
                && let Ok(cached) = serde_json::from_str::<Option<Lyrics>>(&json)
            {
                return cached;
            }
            let lookup = crate::lyrics::lookup_lyrics(&ctx.http, &track);
            // Only definite answers are cached; an unreachable LRCLIB is asked again next time.
            let fetched = tokio::time::timeout(LYRICS_LIMIT, lookup).await.ok()?.ok()?;
            let ttl = if fetched.is_some() { LYRICS_TTL_MS } else { NO_LYRICS_TTL_MS };
            if let Ok(json) = serde_json::to_string(&fetched)
                && let Err(e) = ctx.store.cache_put(LYRICS_NAMESPACE, &key, &json, ttl)
            {
                tracing::warn!("lyrics cache write failed: {e}");
            }
            fetched
        });
        task.await.ok().flatten()
    }
}

impl Engine {
    /// [`Engine::new`] with tool/test options (Rust only).
    pub fn with_options(
        config: EngineConfig,
        media: Option<Arc<dyn MediaBackend>>,
        decoder: Option<Arc<dyn PlatformDecoder>>,
        speech: Option<Arc<dyn SpeechToText>>,
        options: EngineOptions,
    ) -> Result<Arc<Self>> {
        match catch_unwind(AssertUnwindSafe(|| Self::build(config, media, decoder, speech, options))) {
            Ok(result) => result,
            Err(panic) => {
                tracing::error!("engine construction panicked: {}", panic_message(&panic));
                Err(MfError::internal("the engine could not start"))
            }
        }
    }

    fn build(
        config: EngineConfig,
        media: Option<Arc<dyn MediaBackend>>,
        decoder: Option<Arc<dyn PlatformDecoder>>,
        speech: Option<Arc<dyn SpeechToText>>,
        options: EngineOptions,
    ) -> Result<Arc<Self>> {
        if config.data_dir.trim().is_empty() || config.cache_dir.trim().is_empty() {
            return Err(MfError::invalid("The data and cache folders must be set."));
        }
        let data_dir = PathBuf::from(&config.data_dir);
        let cache_dir = PathBuf::from(&config.cache_dir);
        for dir in [data_dir.clone(), cache_dir.join("sessions"), cache_dir.join("tmp")] {
            std::fs::create_dir_all(&dir).map_err(|e| MfError::Storage {
                message: format!("The folder {} cannot be created ({}).", dir.display(), e.kind()),
            })?;
        }
        let http = crate::util::http::build_client(&config.app_version)?;
        let store = Arc::new(Store::open(&data_dir.join(DATABASE_FILE))?);
        match store.recover_interrupted() {
            Ok(0) => {}
            Ok(n) => tracing::info!("marked {n} interrupted sessions as failed"),
            Err(e) => tracing::warn!("could not recover interrupted sessions: {e}"),
        }
        let mut shazam = ShazamClient::new(http.clone(), &config.country, &config.language);
        if let Some(policy) = options.shazam_policy {
            shazam = shazam.with_policy(policy);
        }
        if let Some(base) = options.shazam_base_url.as_deref() {
            shazam = shazam.with_base_url(base);
        }
        let media = media.or_else(|| {
            config.media_tools.clone().map(|mut tools| {
                // Keep yt-dlp's own cache inside the app's cache folder unless the app chose one.
                tools
                    .env
                    .entry("XDG_CACHE_HOME".into())
                    .or_insert_with(|| cache_dir.join("yt-dlp").to_string_lossy().into_owned());
                Arc::new(YtDlpSubprocess::new(tools).with_temp_dir(cache_dir.join("tmp"))) as Arc<dyn MediaBackend>
            })
        });
        // Built last: a runtime dropped on an error path would panic inside async callers.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .max_blocking_threads(MAX_BLOCKING_THREADS)
            .thread_name("mf-core")
            .enable_all()
            .build()
            .map_err(|e| MfError::internal(format!("could not start the engine runtime: {e}")))?;
        let ctx = Arc::new(EngineCtx {
            config,
            http,
            store,
            shazam: Arc::new(shazam),
            budget: options.call_budget.unwrap_or_default(),
            credentials: RwLock::new(Credentials::default()),
            melody: RwLock::new(None),
            pitch: OnceLock::new(),
            media,
            decoder,
            speech,
            runtime: runtime.handle().clone(),
            enrichment: !options.offline_catalog,
            low_power: AtomicBool::new(crate::util::system::SystemInfo::current().is_low_end()),
            pitch_low_power: OnceLock::new(),
            recognizer_url: options.shazam_base_url.clone().unwrap_or_else(|| crate::shazam::client::ENDPOINT.into()),
        });
        *ctx.melody.write() = melody_index::open_installed(&ctx);

        // Leftover session folders from earlier runs (pinned sessions keep theirs).
        let cleanup = ctx.clone();
        runtime.spawn_blocking(move || remove_leftovers(&cleanup));

        Ok(Arc::new(Self {
            ctx,
            runtime: Mutex::new(Some(runtime)),
            registry: Arc::new(Registry::default()),
            index: Arc::new(IndexState::default()),
        }))
    }

    /// The shared engine state (for tools and tests).
    pub fn context(&self) -> &Arc<EngineCtx> {
        &self.ctx
    }

    /// Run a custom pipeline as a library session with the engine's guards
    /// (`Started` first, panics become `Failed { Internal }`, cancellation).
    /// For tools and tests; the body emits its own terminal event.
    #[doc(hidden)]
    pub fn start_custom_session<F, Fut>(
        &self,
        kind: SessionKind,
        observer: Arc<dyn SessionObserver>,
        body: F,
    ) -> Result<Arc<AnalysisSession>>
    where
        F: FnOnce(Arc<SessionCtx>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let ctx = self.open_session(kind, "", None, observer, true)?;
        let future = body(ctx.clone());
        Ok(self.spawn_session(ctx, kind, future))
    }

    /// Create the session row (when `persist`), its emitter and context, and
    /// register it for cancellation.
    fn open_session(
        &self,
        kind: SessionKind,
        title: &str,
        source_url: Option<&str>,
        observer: Arc<dyn SessionObserver>,
        persist: bool,
    ) -> Result<Arc<SessionCtx>> {
        let id = new_id();
        let cancel = CancelToken::new();
        let emitter = if persist {
            self.ctx.store.create_session(&id, kind, title, source_url, now_ms())?;
            Emitter::for_session(&id, kind, observer, self.ctx.store.clone(), cancel.clone())
        } else {
            Emitter::ephemeral(&id, kind, observer, self.ctx.store.clone(), cancel.clone())
        };
        let ctx = Arc::new(SessionCtx {
            engine: self.ctx.clone(),
            work_dir: self.ctx.sessions_cache_dir().join(&id),
            id,
            cancel,
            credentials: self.ctx.credentials.read().clone(),
            emitter,
        });
        Ok(ctx)
    }

    /// Run an async session body on the runtime inside the panic guard.
    fn spawn_session(
        &self,
        ctx: Arc<SessionCtx>,
        kind: SessionKind,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> Arc<AnalysisSession> {
        self.registry
            .insert(&ctx.id, Running { cancel: ctx.cancel.clone(), emitter: ctx.emitter.clone(), input: None });
        let handle = Arc::new(AnalysisSession {
            id: ctx.id.clone(),
            cancel: ctx.cancel.clone(),
            emitter: ctx.emitter.clone(),
            runtime: self.ctx.runtime.clone(),
        });
        let registry = self.registry.clone();
        self.ctx.runtime.spawn(async move {
            ctx.emitter.started(kind);
            if let Err(panic) = AssertUnwindSafe(body).catch_unwind().await {
                tracing::error!(session = %ctx.id, "session panicked: {}", panic_message(&panic));
                ctx.emitter.failed(&MfError::internal("the session stopped unexpectedly"));
                ctx.cleanup();
            }
            if !ctx.emitter.is_terminal() {
                if ctx.cancel.is_canceled() {
                    ctx.emitter.canceled();
                } else {
                    ctx.emitter.failed(&MfError::internal("the session ended without a result"));
                }
            }
            registry.remove(&ctx.id);
        });
        handle
    }

    /// Run `future` on the engine runtime and wait for it from any thread.
    fn block_on<T: Send + 'static>(&self, future: impl Future<Output = T> + Send + 'static) -> Result<T> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.ctx.runtime.spawn(async move {
            let _ = tx.send(AssertUnwindSafe(future).catch_unwind().await);
        });
        match rx.recv() {
            Ok(Ok(value)) => Ok(value),
            _ => Err(MfError::internal("the request stopped unexpectedly")),
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        for running in self.registry.all() {
            running.cancel.cancel();
            if let Some(input) = &running.input {
                input.wake();
            }
            running.emitter.canceled();
        }
        if let Some(runtime) = self.runtime.lock().take() {
            runtime.shutdown_background();
        }
    }
}

/// A running live session (microphone or system audio).
#[derive(uniffi::Object)]
pub struct LiveSession {
    id: String,
    input: Arc<LiveInput>,
    cancel: CancelToken,
    emitter: Emitter,
    runtime: tokio::runtime::Handle,
}

#[uniffi::export]
impl LiveSession {
    /// The session id (also the library entry id).
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// Push interleaved little-endian f32 PCM. Cheap; never blocks on analysis.
    pub fn push_pcm(&self, data: &[u8], sample_rate: u32, channels: u32) {
        self.input.push(data, sample_rate, channels);
    }

    /// The user stopped capture: analyze what was captured and finish.
    pub fn finish(&self) {
        self.input.finish();
    }

    /// Stop at once; the session emits `Canceled` and is stored as canceled.
    pub fn cancel(&self) {
        cancel_running(&self.runtime, &self.cancel, &self.emitter, Some(&self.input));
    }
}

impl LiveSession {
    /// False once the session stopped taking audio (a match, auto-stop, `finish()`,
    /// cancellation).
    pub fn is_capturing(&self) -> bool {
        self.input.is_open()
    }

    /// True when all pushed audio was processed and no recognizer call is running.
    /// Tools use it to feed recordings faster than real time.
    pub fn is_settled(&self) -> bool {
        self.input.is_settled()
    }

    /// Pushed audio not yet processed, in milliseconds.
    pub fn backlog_ms(&self) -> u64 {
        self.input.backlog_ms()
    }

    /// Audio dropped because analysis fell more than 5 s behind.
    pub fn dropped_ms(&self) -> u64 {
        self.input.dropped_ms()
    }
}

/// A running analysis (link, file, lyric search, index download).
#[derive(uniffi::Object)]
pub struct AnalysisSession {
    id: String,
    cancel: CancelToken,
    emitter: Emitter,
    runtime: tokio::runtime::Handle,
}

#[uniffi::export]
impl AnalysisSession {
    /// The session id (the library entry id, except for melody index downloads).
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// Stop at once; the session emits `Canceled`.
    pub fn cancel(&self) {
        cancel_running(&self.runtime, &self.cancel, &self.emitter, None);
    }
}

/// Session ids are UUIDs; anything else (path separators, "..") is rejected
/// before it is used in a file name.
fn is_session_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn file_title(path: &Path) -> String {
    path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('\u{2026}');
    out
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Total size of the files below `path` (symlinks not followed).
fn dir_size(path: &Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(path) else { return 0 };
    if !meta.is_dir() {
        return meta.len();
    }
    std::fs::read_dir(path).map(|entries| entries.flatten().map(|e| dir_size(&e.path())).sum()).unwrap_or(0)
}

fn remove_quietly(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("could not remove {}: {e}", path.display());
    }
}

fn remove_path(path: &Path) {
    let result = if path.is_dir() { std::fs::remove_dir_all(path) } else { std::fs::remove_file(path) };
    if let Err(e) = result {
        tracing::warn!("could not remove {}: {e}", path.display());
    }
}

/// Remove entries of `dir` not modified for `age`.
fn remove_stale(dir: &Path, age: Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| now.duration_since(t).ok())
            .is_some_and(|d| d >= age);
        if old {
            remove_path(&entry.path());
        }
    }
}

/// Session folders left by earlier runs, except those of pinned sessions.
fn remove_leftovers(ctx: &EngineCtx) {
    let pinned: HashSet<String> = ctx
        .store
        .library(&LibraryQuery { kinds: Vec::new(), search: None, pinned_only: true, limit: 0, offset: 0 })
        .map(|entries| entries.into_iter().map(|e| e.id).collect())
        .unwrap_or_default();
    let started = SystemTime::now();
    if let Ok(entries) = std::fs::read_dir(ctx.sessions_cache_dir()) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            // Folders created after start-up belong to sessions of this run.
            let recent = entry.metadata().and_then(|m| m.modified()).is_ok_and(|t| t >= started);
            if !pinned.contains(&name) && !recent {
                remove_path(&entry.path());
            }
        }
    }
    remove_stale(&PathBuf::from(&ctx.config.cache_dir).join("tmp"), STALE_TEMP);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids_cannot_name_other_files() {
        assert!(is_session_id(&new_id()));
        assert!(!is_session_id("../etc"));
        assert!(!is_session_id("a/b"));
        assert!(!is_session_id(""));
    }

    #[test]
    fn truncates_titles() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("abcdefghijk", 5), "abcd\u{2026}");
    }

    #[test]
    fn sizes_directories() {
        let dir = std::env::temp_dir().join(format!("mf-size-{}", new_id()));
        std::fs::create_dir_all(dir.join("a")).unwrap();
        std::fs::write(dir.join("x"), [0u8; 10]).unwrap();
        std::fs::write(dir.join("a/y"), [0u8; 5]).unwrap();
        assert_eq!(dir_size(&dir), 15);
        assert_eq!(dir_size(&dir.join("missing")), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
