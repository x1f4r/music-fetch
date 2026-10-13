//! The `Engine` object and session handles exposed to the apps.

use std::sync::Arc;

use crate::config::{Credentials, EngineConfig};
use crate::error::{MfError, Result};
use crate::model::{
    AnalyzeMode, CaptureSource, ExportFormat, LibraryEntry, LibraryQuery, ListenMode, Lyrics, MelodyIndexStatus,
    Segment, SessionDetail, StorageUsage, Track,
};
use crate::platform::{MediaBackend, PlatformDecoder, SessionObserver, SpeechToText};

#[derive(uniffi::Object)]
pub struct Engine {}

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
        let _ = (config, media, decoder, speech);
        Err(MfError::internal("not implemented"))
    }

    pub fn set_credentials(&self, credentials: Credentials) {
        let _ = credentials;
    }

    /// Start a live session; push audio with `LiveSession::push_pcm`.
    pub fn start_listening(
        &self,
        source: CaptureSource,
        mode: ListenMode,
        observer: Arc<dyn SessionObserver>,
    ) -> Result<Arc<LiveSession>> {
        let _ = (source, mode, observer);
        Err(MfError::internal("not implemented"))
    }

    /// Analyze a link or a local file path.
    pub fn analyze(
        &self,
        input: String,
        mode: AnalyzeMode,
        observer: Arc<dyn SessionObserver>,
    ) -> Result<Arc<AnalysisSession>> {
        let _ = (input, mode, observer);
        Err(MfError::internal("not implemented"))
    }

    /// Find songs from typed lyrics.
    pub fn search_lyrics(&self, text: String, observer: Arc<dyn SessionObserver>) -> Result<Arc<AnalysisSession>> {
        let _ = (text, observer);
        Err(MfError::internal("not implemented"))
    }

    pub fn library(&self, query: LibraryQuery) -> Result<Vec<LibraryEntry>> {
        let _ = query;
        Err(MfError::internal("not implemented"))
    }

    pub fn session(&self, id: String) -> Result<Option<SessionDetail>> {
        let _ = id;
        Err(MfError::internal("not implemented"))
    }

    pub fn delete_sessions(&self, ids: Vec<String>) -> Result<()> {
        let _ = ids;
        Err(MfError::internal("not implemented"))
    }

    pub fn set_pinned(&self, id: String, pinned: bool) -> Result<()> {
        let _ = (id, pinned);
        Err(MfError::internal("not implemented"))
    }

    /// Replace a segment's track with the user's choice (confidence 1, tier Confirmed).
    pub fn correct_segment(&self, session_id: String, segment_id: String, track: Track) -> Result<Segment> {
        let _ = (session_id, segment_id, track);
        Err(MfError::internal("not implemented"))
    }

    /// Record the user's pick among impression candidates ("This is it").
    pub fn confirm_candidate(&self, session_id: String, track: Track) -> Result<()> {
        let _ = (session_id, track);
        Err(MfError::internal("not implemented"))
    }

    pub fn export_session(&self, id: String, format: ExportFormat) -> Result<String> {
        let _ = (id, format);
        Err(MfError::internal("not implemented"))
    }

    pub fn storage_usage(&self) -> StorageUsage {
        StorageUsage::default()
    }

    /// Delete cached downloads and decoded audio of finished, unpinned sessions.
    pub fn clear_cache(&self) -> Result<()> {
        Err(MfError::internal("not implemented"))
    }

    pub fn melody_index_status(&self) -> MelodyIndexStatus {
        MelodyIndexStatus::default()
    }

    /// Fetch the manifest and report whether a newer index exists.
    pub fn check_melody_index(&self) -> Result<MelodyIndexStatus> {
        Err(MfError::internal("not implemented"))
    }

    /// Download and install the melody index. Progress arrives as
    /// `Phase { Downloading, fraction }`; ends with `Finished` or `Failed`.
    pub fn install_melody_index(&self, observer: Arc<dyn SessionObserver>) -> Result<Arc<AnalysisSession>> {
        let _ = observer;
        Err(MfError::internal("not implemented"))
    }

    pub fn remove_melody_index(&self) -> Result<()> {
        Err(MfError::internal("not implemented"))
    }

    /// Fill links, artwork and previews for a track (best effort). Also refreshes
    /// expired preview URLs.
    pub async fn enrich_track(&self, track: Track) -> Track {
        track
    }

    /// Lyrics for a track, if any are known.
    pub async fn fetch_lyrics(&self, track: Track) -> Option<Lyrics> {
        let _ = track;
        None
    }
}

/// A running live session (microphone or system audio).
#[derive(uniffi::Object)]
pub struct LiveSession {}

#[uniffi::export]
impl LiveSession {
    pub fn id(&self) -> String {
        String::new()
    }

    /// Push interleaved little-endian f32 PCM. Cheap; never blocks on analysis.
    pub fn push_pcm(&self, data: &[u8], sample_rate: u32, channels: u32) {
        let _ = (data, sample_rate, channels);
    }

    /// The user stopped capture: analyze what was captured and finish.
    pub fn finish(&self) {}

    pub fn cancel(&self) {}
}

/// A running analysis (link, file, lyric search, index download).
#[derive(uniffi::Object)]
pub struct AnalysisSession {}

#[uniffi::export]
impl AnalysisSession {
    pub fn id(&self) -> String {
        String::new()
    }

    pub fn cancel(&self) {}
}
