//! Shared engine state and per-session context handed to every pipeline.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

use crate::config::{Credentials, EngineConfig};
use crate::error::Result;
use crate::melody::{MelodyIndex, PitchTracker};
use crate::pipeline::emitter::Emitter;
use crate::platform::{MediaBackend, PlatformDecoder, SpeechToText};
use crate::shazam::ShazamClient;
use crate::store::Store;
use crate::util::cancel::CancelToken;

/// Engine-wide state shared by all sessions.
pub struct EngineCtx {
    pub config: EngineConfig,
    pub http: reqwest::Client,
    pub store: Arc<Store>,
    /// One serial Shazam queue per engine (rate limits are per install/IP).
    pub shazam: Arc<ShazamClient>,
    pub credentials: RwLock<Credentials>,
    pub melody: RwLock<Option<Arc<MelodyIndex>>>,
    pub pitch: OnceLock<Arc<PitchTracker>>,
    pub media: Option<Arc<dyn MediaBackend>>,
    pub decoder: Option<Arc<dyn PlatformDecoder>>,
    pub speech: Option<Arc<dyn SpeechToText>>,
    pub runtime: tokio::runtime::Handle,
}

impl EngineCtx {
    /// The shared pitch tracker, created on first use.
    pub fn pitch_tracker(&self) -> Result<Arc<PitchTracker>> {
        if let Some(tracker) = self.pitch.get() {
            return Ok(tracker.clone());
        }
        let tracker = Arc::new(PitchTracker::new()?);
        Ok(self.pitch.get_or_init(|| tracker).clone())
    }

    pub fn melody_index(&self) -> Option<Arc<MelodyIndex>> {
        self.melody.read().clone()
    }

    pub fn country(&self) -> &str {
        &self.config.country
    }

    pub fn language(&self) -> &str {
        &self.config.language
    }
}

/// Everything one session needs.
pub struct SessionCtx {
    pub engine: Arc<EngineCtx>,
    pub id: String,
    pub cancel: CancelToken,
    /// Snapshot taken when the session started.
    pub credentials: Credentials,
    pub emitter: Emitter,
    /// `cache_dir/sessions/<id>`; created lazily, removed when the session ends.
    pub work_dir: PathBuf,
}

impl SessionCtx {
    /// Create the work directory if needed and return it.
    pub fn ensure_work_dir(&self) -> Result<&PathBuf> {
        std::fs::create_dir_all(&self.work_dir)?;
        Ok(&self.work_dir)
    }

    /// Remove the work directory (best effort).
    pub fn cleanup(&self) {
        if self.work_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&self.work_dir) {
                tracing::warn!("could not remove {}: {e}", self.work_dir.display());
            }
        }
    }
}
