//! The `Engine` facade and its session handles.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::time::Duration;

use mfcore::engine::EngineOptions;
use mfcore::pipeline::capture::{CaptureSink, PageMedia};
use mfcore::pipeline::recognize::CallBudget;
use mfcore::shazam::QueuePolicy;
use mfcore::store::Store;
use mfcore::store::opfs::BrowserStorage;
use mfcore::util::files::FileStore;
use mfcore::util::files_sqlite::SqliteFiles;
use mfcore::{
    AnalysisSession, AnalyzeMode, CaptureSource, Credentials, EngineConfig, ExportFormat, LibraryQuery, ListenMode,
    ListenOptions, LiveSession, MfError, Track,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::config::WebEngineConfig;
use crate::convert::{error, from_js, js, to_js};
use crate::host::{self, JsObserver, Sessions};

#[wasm_bindgen(typescript_custom_section)]
const TS_IMPORTS: &str = r#"
import type {
  AnalyzeMode, Candidate, Credentials, ExportFormat, LibraryEntry, LibraryQuery, ListenMode, ListenOptions,
  Lyrics, MelodyIndexStatus, PageMedia, Segment, ServiceLink, SessionDetail, SessionEvent, SpeechTranscript,
  StorageUsage, Track, WebEngineConfig,
} from '../types.gen';

/** One header rule of the engine's requests (`util::http::HeaderRule`). */
export interface HeaderRule {
  id: 'shazam' | 'crawler' | 'youtube' | 'innertube' | 'ytmusic' | 'neutral';
  /** URL prefixes the rule covers (anchored at the start). */
  prefixes: string[];
  /** Headers to set, lower case. */
  set: Record<string, string>;
  /** Headers to remove, lower case. */
  remove: string[];
}

/** What the engine calls back into. */
export interface JsHost {
  /** Every event of every session, in order per session. */
  onEvent(sessionId: string, event: SessionEvent): void;
  /**
   * Optional: install header rule `rule` with variant `variant` (the Shazam agent for the
   * next request, see `headerRules`) and resolve once it is in place.
   */
  prepare?(rule: HeaderRule['id'], variant: number): Promise<void> | void;
}
"#;

/// The engine (one per Worker).
#[wasm_bindgen(js_name = Engine)]
pub struct WasmEngine {
    engine: RefCell<Option<Arc<mfcore::Engine>>>,
    storage: RefCell<Option<BrowserStorage>>,
    on_event: js_sys::Function,
    sessions: Rc<Sessions>,
    interrupted: Vec<String>,
}

fn closed() -> JsValue {
    error(&MfError::internal("the engine was closed"))
}

#[wasm_bindgen(js_class = Engine)]
impl WasmEngine {
    /// Opens the storage and starts the engine. Sessions an earlier Worker
    /// left running are marked failed (see `interruptedSessions`).
    #[wasm_bindgen(unchecked_return_type = "Promise<Engine>")]
    pub fn create(
        #[wasm_bindgen(unchecked_param_type = "WebEngineConfig")] config: JsValue,
        #[wasm_bindgen(unchecked_param_type = "JsHost")] host: JsValue,
    ) -> js_sys::Promise {
        future_to_promise(async move { Self::open(config, host).await.map(JsValue::from) })
    }

    async fn open(config: JsValue, host: JsValue) -> Result<WasmEngine, JsValue> {
        host::install_panic_hook();
        let config: WebEngineConfig = if config.is_undefined() || config.is_null() {
            WebEngineConfig::default()
        } else {
            from_js(config, "config")?
        };
        host::install_logging(match config.log_level.as_str() {
            "error" => tracing::Level::ERROR,
            "info" => tracing::Level::INFO,
            "debug" => tracing::Level::DEBUG,
            _ => tracing::Level::WARN,
        });
        let on_event: js_sys::Function = js_sys::Reflect::get(&host, &"onEvent".into())
            .ok()
            .and_then(|f| f.dyn_into().ok())
            .ok_or_else(|| error(&MfError::invalid("the host needs an onEvent function")))?;
        // Optional: per-request header rule variants (the Shazam agent rotation).
        let prepare: Option<js_sys::Function> =
            js_sys::Reflect::get(&host, &"prepare".into()).ok().and_then(|f| f.dyn_into().ok());
        let (store, files, storage, interrupted): (
            Arc<Store>,
            Arc<dyn FileStore>,
            Option<BrowserStorage>,
            Vec<String>,
        ) = match config.storage.as_str() {
            "memory" => {
                (Arc::new(js(Store::open_in_memory())?), Arc::new(js(SqliteFiles::open_in_memory())?), None, Vec::new())
            }
            _ => {
                let storage = js(BrowserStorage::open(&config.directory).await)?;
                let interrupted = storage.interrupted.clone();
                (storage.store.clone(), storage.file_store(), Some(storage), interrupted)
            }
        };
        let engine_config = EngineConfig {
            data_dir: format!("{}:/data", config.storage),
            cache_dir: format!("{}:/cache", config.storage),
            country: config.country,
            language: config.language,
            app_version: config.app_version,
            media_tools: None,
            melody_index_manifest_url: config.melody_index_manifest_url,
        };
        if let Some(stub) = config.test_stub.as_deref() {
            mfcore::util::endpoints::install(Some(js(mfcore::util::endpoints::TestEndpoints::stub(stub))?));
        }
        // A local stub recognizer in tests answers at once: no need to pace.
        let stubbed = config.shazam_base_url.is_some() || config.test_stub.is_some();
        let fast = config.test_pacing_ms.filter(|_| stubbed).map(Duration::from_millis);
        let options = EngineOptions {
            shazam_base_url: config.shazam_base_url,
            offline_catalog: config.offline_catalog,
            shazam_policy: fast.map(|spacing| QueuePolicy {
                interactive_spacing: spacing,
                batch_spacing: spacing,
                jitter: Duration::ZERO,
                retry_delays: vec![spacing],
                ..QueuePolicy::default()
            }),
            call_budget: fast.map(|spacing| CallBudget::new(1e6, spacing)),
            header_host: prepare.map(|f| host::JsRuleHost::new(f) as Arc<dyn mfcore::util::http::HeaderRuleHost>),
        };
        let engine = js(mfcore::Engine::in_browser(engine_config, store, files, None, config.low_power, options))?;
        Ok(WasmEngine {
            engine: RefCell::new(Some(engine)),
            storage: RefCell::new(storage),
            on_event,
            sessions: Rc::new(Sessions::default()),
            interrupted,
        })
    }

    fn engine(&self) -> Result<Arc<mfcore::Engine>, JsValue> {
        self.engine.borrow().clone().ok_or_else(closed)
    }

    fn observer(&self) -> Arc<JsObserver> {
        JsObserver::new(self.on_event.clone(), self.sessions.clone())
    }

    /// The engine core's version.
    #[wasm_bindgen(js_name = coreVersion)]
    pub fn core_version() -> String {
        mfcore::core_version()
    }

    /// Sessions the previous Worker left running, now marked failed.
    #[wasm_bindgen(getter, js_name = interruptedSessions)]
    pub fn interrupted_sessions(&self) -> Vec<String> {
        self.interrupted.clone()
    }

    /// Bytes of linear memory the module holds (it never shrinks).
    #[wasm_bindgen(js_name = memoryBytes)]
    pub fn memory_bytes() -> f64 {
        (core::arch::wasm32::memory_size(0) * 65_536) as f64
    }

    /// Start a live session; push its audio with `LiveSession.pushPcm`.
    #[wasm_bindgen(js_name = startListening)]
    pub fn start_listening(
        &self,
        #[wasm_bindgen(unchecked_param_type = "'Microphone' | 'SystemAudio'")] source: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ListenMode")] mode: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ListenOptions")] options: JsValue,
    ) -> Result<LiveSessionHandle, JsValue> {
        let source: CaptureSource = from_js(source, "the source")?;
        let mode: ListenMode = from_js(mode, "the mode")?;
        let options: ListenOptions =
            if options.is_undefined() { ListenOptions::default() } else { from_js(options, "the options")? };
        let observer = self.observer();
        let session = js(self.engine()?.start_listening_with(source, mode, options, observer.clone()))?;
        let id = session.id();
        let handle = LiveSessionHandle { id: id.clone(), session: Arc::downgrade(&session) };
        self.sessions.live.borrow_mut().insert(id, session);
        Ok(handle)
    }

    /// Analyze a link; returns the session id.
    pub fn analyze(
        &self,
        input: String,
        #[wasm_bindgen(unchecked_param_type = "AnalyzeMode")] mode: JsValue,
    ) -> Result<String, JsValue> {
        let mode: AnalyzeMode = from_js(mode, "the mode")?;
        let session = js(self.engine()?.analyze(input, mode, self.observer()))?;
        Ok(self.keep(session))
    }

    /// Analyze a file the user picked: its bytes go to a work file, which is
    /// deleted when the session ends.
    #[wasm_bindgen(js_name = analyzeFile)]
    pub fn analyze_file(
        &self,
        name: String,
        bytes: &[u8],
        #[wasm_bindgen(unchecked_param_type = "AnalyzeMode")] mode: JsValue,
    ) -> Result<String, JsValue> {
        let mode: AnalyzeMode = from_js(mode, "the mode")?;
        if bytes.is_empty() {
            return Err(error(&MfError::invalid("The file is empty.")));
        }
        let engine = self.engine()?;
        let work = format!("imports/{}/{}", mfcore::util::new_id(), file_name(&name));
        js(engine.context().files.write(&work, bytes))?;
        let title = name.rsplit(['/', '\\']).next().unwrap_or(&name).to_string();
        let title = title.rsplit_once('.').map_or(title.as_str(), |(stem, _)| stem).to_string();
        match engine.analyze_work_file(work.clone(), title, mode, self.observer()) {
            Ok(session) => Ok(self.keep(session)),
            Err(e) => {
                let _ = engine.context().files.remove(&work);
                Err(error(&e))
            }
        }
    }

    /// Analyze media the extension captures from a page's player: push the
    /// audio with `CaptureSession.push`, then `finish()` (or `fail()`).
    #[wasm_bindgen(js_name = analyzeCaptured)]
    pub fn analyze_captured(
        &self,
        #[wasm_bindgen(unchecked_param_type = "PageMedia")] page: JsValue,
        #[wasm_bindgen(unchecked_param_type = "AnalyzeMode")] mode: JsValue,
    ) -> Result<CaptureSessionHandle, JsValue> {
        let page: PageMedia = from_js(page, "the page")?;
        let mode: AnalyzeMode = from_js(mode, "the mode")?;
        let (session, sink) = js(self.engine()?.analyze_captured(page, mode, self.observer()))?;
        let handle =
            CaptureSessionHandle { id: session.id(), session: Arc::downgrade(&session), sink: Arc::downgrade(&sink) };
        self.sessions.captures.borrow_mut().insert(handle.id.clone(), sink);
        self.keep(session);
        Ok(handle)
    }

    /// Find songs from typed lyrics; returns the session id.
    #[wasm_bindgen(js_name = searchLyrics)]
    pub fn search_lyrics(&self, text: String) -> Result<String, JsValue> {
        let session = js(self.engine()?.search_lyrics(text, self.observer()))?;
        Ok(self.keep(session))
    }

    fn keep(&self, session: Arc<AnalysisSession>) -> String {
        let id = session.id();
        self.sessions.analyses.borrow_mut().insert(id.clone(), session);
        id
    }

    /// Stop a session at once (it ends `Canceled`); false when it is not running.
    #[wasm_bindgen(js_name = cancelSession)]
    pub fn cancel_session(&self, id: String) -> bool {
        if let Some(live) = self.sessions.live.borrow().get(&id).cloned() {
            live.cancel();
            return true;
        }
        if let Some(analysis) = self.sessions.analyses.borrow().get(&id).cloned() {
            analysis.cancel();
            return true;
        }
        false
    }

    /// The user stopped a live session: identify what was heard. False when it
    /// is not a running live session.
    #[wasm_bindgen(js_name = finishSession)]
    pub fn finish_session(&self, id: String) -> bool {
        match self.sessions.live.borrow().get(&id).cloned() {
            Some(live) => {
                live.finish();
                true
            }
            None => false,
        }
    }

    /// Ids of the sessions that are still running.
    #[wasm_bindgen(js_name = runningSessions)]
    pub fn running_sessions(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.sessions.live.borrow().keys().cloned().collect();
        ids.extend(self.sessions.analyses.borrow().keys().cloned());
        ids
    }

    #[wasm_bindgen(unchecked_return_type = "LibraryEntry[]")]
    pub fn library(
        &self,
        #[wasm_bindgen(unchecked_param_type = "LibraryQuery")] query: JsValue,
    ) -> Result<JsValue, JsValue> {
        let query: LibraryQuery = from_js(query, "the query")?;
        to_js(&js(self.engine()?.library(query))?)
    }

    #[wasm_bindgen(unchecked_return_type = "SessionDetail | null")]
    pub fn session(&self, id: String) -> Result<JsValue, JsValue> {
        to_js(&js(self.engine()?.session(id))?)
    }

    /// Delete sessions (running ones are canceled first).
    #[wasm_bindgen(js_name = deleteSessions, unchecked_return_type = "Promise<void>")]
    pub fn delete_sessions(&self, ids: Vec<String>) -> Result<js_sys::Promise, JsValue> {
        let engine = self.engine()?;
        Ok(future_to_promise(async move { js(engine.delete_sessions_async(ids).await).map(|()| JsValue::UNDEFINED) }))
    }

    #[wasm_bindgen(js_name = setPinned)]
    pub fn set_pinned(&self, id: String, pinned: bool) -> Result<(), JsValue> {
        js(self.engine()?.set_pinned(id, pinned))
    }

    #[wasm_bindgen(js_name = setPinnedMany)]
    pub fn set_pinned_many(&self, ids: Vec<String>, pinned: bool) -> Result<(), JsValue> {
        js(self.engine()?.set_pinned_many(ids, pinned))
    }

    #[wasm_bindgen(js_name = correctSegment, unchecked_return_type = "Segment")]
    pub fn correct_segment(
        &self,
        session_id: String,
        segment_id: String,
        #[wasm_bindgen(unchecked_param_type = "Track")] track: JsValue,
    ) -> Result<JsValue, JsValue> {
        let track: Track = from_js(track, "the track")?;
        to_js(&js(self.engine()?.correct_segment(session_id, segment_id, track))?)
    }

    #[wasm_bindgen(js_name = rejectSegment, unchecked_return_type = "Segment")]
    pub fn reject_segment(&self, session_id: String, segment_id: String) -> Result<JsValue, JsValue> {
        to_js(&js(self.engine()?.reject_segment(session_id, segment_id))?)
    }

    #[wasm_bindgen(js_name = confirmCandidate)]
    pub fn confirm_candidate(
        &self,
        session_id: String,
        #[wasm_bindgen(unchecked_param_type = "Track")] track: JsValue,
    ) -> Result<(), JsValue> {
        let track: Track = from_js(track, "the track")?;
        js(self.engine()?.confirm_candidate(session_id, track))
    }

    #[wasm_bindgen(js_name = exportSession)]
    pub fn export_session(
        &self,
        id: String,
        #[wasm_bindgen(unchecked_param_type = "ExportFormat")] format: JsValue,
    ) -> Result<String, JsValue> {
        let format: ExportFormat = from_js(format, "the format")?;
        js(self.engine()?.export_session(id, format))
    }

    #[wasm_bindgen(js_name = storageUsage, unchecked_return_type = "StorageUsage")]
    pub fn storage_usage(&self) -> Result<JsValue, JsValue> {
        to_js(&self.engine()?.storage_usage())
    }

    #[wasm_bindgen(js_name = clearCache)]
    pub fn clear_cache(&self) -> Result<(), JsValue> {
        js(self.engine()?.clear_cache())
    }

    /// Identify sessions captured while offline; returns how many started.
    #[wasm_bindgen(js_name = retryWaiting)]
    pub fn retry_waiting(&self) -> Result<u32, JsValue> {
        js(self.engine()?.retry_waiting(self.observer()))
    }

    #[wasm_bindgen(js_name = melodyIndexStatus, unchecked_return_type = "MelodyIndexStatus")]
    pub fn melody_index_status(&self) -> Result<JsValue, JsValue> {
        to_js(&self.engine()?.melody_index_status())
    }

    /// Fetch the manifest and report whether a newer index exists.
    #[wasm_bindgen(js_name = checkMelodyIndex, unchecked_return_type = "Promise<MelodyIndexStatus>")]
    pub fn check_melody_index(&self) -> Result<js_sys::Promise, JsValue> {
        let engine = self.engine()?;
        Ok(future_to_promise(async move { to_js(&js(engine.check_melody_index_async().await)?) }))
    }

    /// Download and install the index; progress arrives as that session's
    /// `Phase(Downloading)` events. Returns the session id.
    #[wasm_bindgen(js_name = installMelodyIndex)]
    pub fn install_melody_index(&self) -> Result<String, JsValue> {
        let session = js(self.engine()?.install_melody_index(self.observer()))?;
        Ok(self.keep(session))
    }

    #[wasm_bindgen(js_name = removeMelodyIndex)]
    pub fn remove_melody_index(&self) -> Result<(), JsValue> {
        js(self.engine()?.remove_melody_index())
    }

    /// Read the installed melody index into memory (humming needs it); false
    /// when none is installed. A few hundred milliseconds for the full index.
    #[wasm_bindgen(js_name = loadMelodyIndex, unchecked_return_type = "Promise<boolean>")]
    pub fn load_melody_index(&self) -> Result<js_sys::Promise, JsValue> {
        let engine = self.engine()?;
        Ok(future_to_promise(async move { Ok(JsValue::from_bool(engine.load_melody_index().await)) }))
    }

    /// Drop the melody index from memory (it stays installed).
    #[wasm_bindgen(js_name = unloadMelodyIndex)]
    pub fn unload_melody_index(&self) -> Result<(), JsValue> {
        self.engine()?.unload_melody_index();
        Ok(())
    }

    #[wasm_bindgen(js_name = melodyIndexLoaded)]
    pub fn melody_index_loaded(&self) -> Result<bool, JsValue> {
        Ok(self.engine()?.melody_index_loaded())
    }

    /// Fill links, artwork and previews for a track (best effort).
    #[wasm_bindgen(js_name = enrichTrack, unchecked_return_type = "Promise<Track>")]
    pub fn enrich_track(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Track")] track: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let track: Track = from_js(track, "the track")?;
        let engine = self.engine()?;
        Ok(future_to_promise(async move { to_js(&engine.enrich_track(track).await) }))
    }

    /// Lyrics for a track, if any are known.
    #[wasm_bindgen(js_name = fetchLyrics, unchecked_return_type = "Promise<Lyrics | null>")]
    pub fn fetch_lyrics(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Track")] track: JsValue,
    ) -> Result<js_sys::Promise, JsValue> {
        let track: Track = from_js(track, "the track")?;
        let engine = self.engine()?;
        Ok(future_to_promise(async move { to_js(&engine.fetch_lyrics(track).await) }))
    }

    #[wasm_bindgen(js_name = setLowPower)]
    pub fn set_low_power(&self, enabled: bool) -> Result<(), JsValue> {
        self.engine()?.set_low_power(enabled);
        Ok(())
    }

    #[wasm_bindgen(js_name = isLowPower)]
    pub fn is_low_power(&self) -> Result<bool, JsValue> {
        Ok(self.engine()?.is_low_power())
    }

    /// Keys for the optional paid providers (never stored by the engine).
    #[wasm_bindgen(js_name = setCredentials)]
    pub fn set_credentials(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Credentials")] credentials: JsValue,
    ) -> Result<(), JsValue> {
        let credentials: Credentials = from_js(credentials, "the credentials")?;
        self.engine()?.set_credentials(credentials);
        Ok(())
    }

    /// Links for a track on every supported provider.
    #[wasm_bindgen(js_name = providerLinks, unchecked_return_type = "ServiceLink[]")]
    pub fn provider_links(#[wasm_bindgen(unchecked_param_type = "Track")] track: JsValue) -> Result<JsValue, JsValue> {
        let track: Track = from_js(track, "the track")?;
        to_js(&mfcore::provider_links(track))
    }

    /// A custom provider template filled for a track; null when the result is
    /// not an http(s) URL.
    #[wasm_bindgen(js_name = customProviderLink)]
    pub fn custom_provider_link(
        #[wasm_bindgen(unchecked_param_type = "Track")] track: JsValue,
        template: String,
    ) -> Result<Option<String>, JsValue> {
        let track: Track = from_js(track, "the track")?;
        Ok(mfcore::custom_provider_link(track, template))
    }

    /// Test tooling (stub recognizers): the peaks of a Shazam signature
    /// (`data:audio/vnd.shazam.sig;base64,…` or raw bytes), as `[frame, fbin,
    /// magnitude]` per band (8 ms frames, `fbin × 0.1220703125` Hz).
    #[wasm_bindgen(js_name = decodeSignature, unchecked_return_type = "{ sample_rate: number; sample_count: number; bands: [number, number, number][][] }")]
    pub fn decode_signature(signature: &[u8]) -> Result<JsValue, JsValue> {
        let decoded = js(mfcore::shazam::signature::decode(signature))?;
        let bands: Vec<Vec<[u32; 3]>> = decoded
            .bands
            .iter()
            .map(|band| band.iter().map(|p| [p.frame, u32::from(p.fbin), u32::from(p.magnitude)]).collect())
            .collect();
        to_js(&serde_json::json!({
            "sample_rate": decoded.sample_rate,
            "sample_count": decoded.sample_count,
            "bands": bands,
        }))
    }

    /// The header policy of the engine's requests (`util::http::header_rules`): the
    /// rules the extension installs as declarativeNetRequest session rules. The Shazam
    /// rule carries agent `shazam_agent` (of eight, wrapping).
    #[wasm_bindgen(js_name = headerRules, unchecked_return_type = "HeaderRule[]")]
    pub fn header_rules(app_version: String, shazam_agent: u32) -> Result<JsValue, JsValue> {
        to_js(&mfcore::util::http::header_rules(&app_version, shazam_agent as usize))
    }

    /// Test tooling (stub recognizers): the Shazam signature the engine would
    /// send for 16 kHz mono audio (its middle 12 s at most).
    #[wasm_bindgen(js_name = shazamSignature)]
    pub fn shazam_signature(samples: &[i16]) -> Vec<u8> {
        mfcore::shazam::signature::generate(samples).bytes
    }

    /// True when `text` looks like a link the engine can analyze.
    #[wasm_bindgen(js_name = isLink)]
    pub fn is_link(text: String) -> bool {
        mfcore::is_link(text)
    }

    /// Stop every session, close both databases and release the OPFS pool, so
    /// the next Worker can open it at once. The engine is unusable afterwards.
    #[wasm_bindgen(unchecked_return_type = "Promise<void>")]
    pub fn close(&self) -> js_sys::Promise {
        let engine = self.engine.borrow_mut().take();
        let storage = self.storage.borrow_mut().take();
        self.sessions.clear();
        future_to_promise(async move {
            drop(engine);
            // Canceled sessions unwind on the event loop before the pool can pause.
            for _ in 0..50 {
                mfcore::util::rt::yield_now().await;
                let Some(storage) = storage.as_ref() else { break };
                if Arc::strong_count(&storage.store) == 1 && Arc::strong_count(&storage.files) == 1 {
                    break;
                }
            }
            if let Some(storage) = storage {
                storage.close().map_err(|(_, e)| error(&e))?;
            }
            Ok(JsValue::UNDEFINED)
        })
    }
}

impl WasmEngine {
    /// `Engine.create` for Rust callers (the browser test suites).
    pub async fn open_with(config: JsValue, host: JsValue) -> Result<WasmEngine, JsValue> {
        Self::open(config, host).await
    }
}

/// A safe last path segment for a work-file name.
fn file_name(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let clean: String = base
        .chars()
        .map(|c| if c.is_alphanumeric() || matches!(c, '.' | '-' | '_') { c } else { '_' })
        .take(80)
        .collect();
    let clean = clean.trim_matches('.').to_string();
    if clean.is_empty() { "file".into() } else { clean }
}

/// A running live session. Once it ended, its methods do nothing (and
/// `isCapturing` is false, `isSettled` true).
#[wasm_bindgen(js_name = LiveSession)]
pub struct LiveSessionHandle {
    id: String,
    session: Weak<LiveSession>,
}

impl LiveSessionHandle {
    fn running(&self) -> Option<Arc<LiveSession>> {
        self.session.upgrade()
    }
}

#[wasm_bindgen(js_class = LiveSession)]
impl LiveSessionHandle {
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// Mono or interleaved f32 PCM. Cheap: queued and analyzed as the Worker
    /// gets to it.
    #[wasm_bindgen(js_name = pushPcm)]
    pub fn push_pcm(&self, samples: &[f32], sample_rate: u32, channels: u32) {
        let Some(session) = self.running() else { return };
        let mut bytes = Vec::with_capacity(samples.len() * 4);
        for s in samples {
            bytes.extend_from_slice(&s.to_le_bytes());
        }
        session.push_pcm(&bytes, sample_rate, channels);
    }

    /// The user stopped capture: analyze what was heard and finish.
    pub fn finish(&self) {
        if let Some(session) = self.running() {
            session.finish();
        }
    }

    pub fn cancel(&self) {
        if let Some(session) = self.running() {
            session.cancel();
        }
    }

    /// What the user sang so far, from an on-device recognizer (sessions
    /// started with `transcripts: true`); replaces the previous transcript.
    #[wasm_bindgen(js_name = pushTranscript)]
    pub fn push_transcript(
        &self,
        #[wasm_bindgen(unchecked_param_type = "SpeechTranscript")] transcript: JsValue,
    ) -> Result<(), JsValue> {
        let transcript: mfcore::SpeechTranscript = from_js(transcript, "the transcript")?;
        if let Some(session) = self.running() {
            session.push_transcript(transcript);
        }
        Ok(())
    }

    /// False once the session stopped taking audio.
    #[wasm_bindgen(js_name = isCapturing)]
    pub fn is_capturing(&self) -> bool {
        self.running().is_some_and(|s| s.is_capturing())
    }

    /// Pushed audio not processed yet, in ms.
    #[wasm_bindgen(js_name = backlogMs)]
    pub fn backlog_ms(&self) -> f64 {
        self.running().map_or(0.0, |s| s.backlog_ms() as f64)
    }

    /// True when all pushed audio was processed and no recognizer call runs.
    #[wasm_bindgen(js_name = isSettled)]
    pub fn is_settled(&self) -> bool {
        self.running().is_none_or(|s| s.is_settled())
    }
}

/// A running analysis of captured media. Once it ended, its methods do nothing.
#[wasm_bindgen(js_name = CaptureSession)]
pub struct CaptureSessionHandle {
    id: String,
    session: Weak<AnalysisSession>,
    sink: Weak<CaptureSink>,
}

#[wasm_bindgen(js_class = CaptureSession)]
impl CaptureSessionHandle {
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// Mono f32 audio captured at `sampleRate` while the media played at
    /// `speed`, from media time `mediaMs`; `continuous` false after a seek, an
    /// ad or a stall.
    pub fn push(&self, media_ms: f64, samples: &[f32], sample_rate: u32, speed: f32, continuous: bool) {
        if let Some(sink) = self.sink.upgrade() {
            sink.push(media_ms, samples, sample_rate, speed, continuous);
        }
    }

    /// The media's length, once known.
    #[wasm_bindgen(js_name = setDuration)]
    pub fn set_duration(&self, duration_ms: f64) {
        if let Some(sink) = self.sink.upgrade().filter(|_| duration_ms.is_finite() && duration_ms > 0.0) {
            sink.set_duration(duration_ms as u64);
        }
    }

    /// The capture is complete: analyze it.
    pub fn finish(&self) {
        if let Some(sink) = self.sink.upgrade() {
            sink.finish();
        }
    }

    /// The capture failed; the session ends `Failed` with this code and message.
    pub fn fail(&self, code: String, message: String) {
        let Some(sink) = self.sink.upgrade() else { return };
        let error = match code.as_str() {
            "Network" => MfError::Network { message },
            "InvalidInput" => MfError::InvalidInput { message },
            "Platform" => MfError::Platform { message },
            _ => MfError::MediaUnavailable { message },
        };
        sink.fail(error);
    }

    pub fn cancel(&self) {
        if let Some(session) = self.session.upgrade() {
            session.cancel();
        }
    }
}
