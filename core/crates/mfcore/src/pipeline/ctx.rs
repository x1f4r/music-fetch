//! Shared engine state and per-session context handed to every pipeline.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::RwLock;

use crate::config::{Credentials, EngineConfig};
use crate::error::Result;
use crate::melody::{MelodyIndex, PitchTracker};
use crate::model::Track;
use crate::pipeline::emitter::Emitter;
use crate::pipeline::recognize::CallBudget;
use crate::platform::{MediaBackend, PlatformDecoder, SpeechToText};
use crate::shazam::ShazamClient;
use crate::store::Store;
use crate::util::cancel::CancelToken;

/// Store cache namespace for enriched tracks, keyed by identity key.
const ENRICH_NAMESPACE: &str = "enrich";
const ENRICH_TTL_MS: i64 = 30 * 24 * 3600 * 1000;
/// Signed preview URLs closer than this to their expiry count as expired.
const PREVIEW_MARGIN_S: i64 = 300;

/// Engine-wide state shared by all sessions.
pub struct EngineCtx {
    pub config: EngineConfig,
    pub http: reqwest::Client,
    pub store: Arc<Store>,
    /// One serial Shazam queue per engine (rate limits are per install/IP).
    pub shazam: Arc<ShazamClient>,
    /// Engine-wide recognizer call budget shared by all sessions.
    pub budget: CallBudget,
    pub credentials: RwLock<Credentials>,
    pub melody: RwLock<Option<Arc<MelodyIndex>>>,
    pub pitch: OnceLock<Arc<PitchTracker>>,
    pub media: Option<Arc<dyn MediaBackend>>,
    pub decoder: Option<Arc<dyn PlatformDecoder>>,
    pub speech: Option<Arc<dyn SpeechToText>>,
    pub runtime: tokio::runtime::Handle,
    /// Catalog enrichment (Deezer/iTunes) of results; off only in offline tests.
    pub enrichment: bool,
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

    /// `data_dir/recordings`: kept impression captures, one WAV per session.
    pub fn recordings_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.data_dir).join("recordings")
    }

    /// The kept capture of a session (may not exist).
    pub fn recording_path(&self, session_id: &str) -> PathBuf {
        self.recordings_dir().join(format!("{session_id}.wav"))
    }

    /// `cache_dir/sessions`: per-session downloads and decoded audio.
    pub fn sessions_cache_dir(&self) -> PathBuf {
        PathBuf::from(&self.config.cache_dir).join("sessions")
    }

    /// Best-effort catalog enrichment (artwork, preview, links, ISRC) through the
    /// persistent `enrich` cache. Never fails; returns the input on errors.
    pub async fn enrich(&self, track: Track, cancel: &CancelToken) -> Track {
        if !self.enrichment || track.title.trim().is_empty() {
            return track;
        }
        let key = crate::identity::identity_key(&track);
        if let Ok(Some(json)) = self.store.cache_get(ENRICH_NAMESPACE, &key)
            && let Ok(cached) = serde_json::from_str::<Track>(&json)
            && !cached.preview_url.as_deref().is_some_and(|url| preview_expired(url, crate::util::now_ms() / 1000))
        {
            return merge_cached(track, cached);
        }
        let mut track = track;
        if track.preview_url.as_deref().is_some_and(|url| preview_expired(url, crate::util::now_ms() / 1000)) {
            track.preview_url = None;
        }
        let enriched = crate::catalog::enrich(&self.http, track, self.country(), cancel).await;
        let found = enriched.ids.contains_key("deezer") || enriched.ids.contains_key("apple");
        if found
            && !cancel.is_canceled()
            && let Ok(json) = serde_json::to_string(&enriched)
            && let Err(e) = self.store.cache_put(ENRICH_NAMESPACE, &key, &json, ENRICH_TTL_MS)
        {
            tracing::warn!("enrich cache write failed: {e}");
        }
        enriched
    }

    /// [`enrich`](Self::enrich) bounded by `limit`; the input comes back when it
    /// runs out of time.
    pub async fn enrich_within(&self, track: Track, limit: Duration, cancel: &CancelToken) -> Track {
        let fallback = track.clone();
        tokio::time::timeout(limit, self.enrich(track, cancel)).await.unwrap_or(fallback)
    }
}

/// The cached enrichment with the ids and links the fresh track already had.
fn merge_cached(fresh: Track, mut cached: Track) -> Track {
    for (key, value) in fresh.ids {
        cached.ids.entry(key).or_insert(value);
    }
    for link in fresh.links {
        if !cached.links.iter().any(|l| l.service == link.service) {
            cached.links.push(link);
        }
    }
    cached
}

/// True for signed preview URLs (Deezer `hdnea=exp=<unix seconds>`) that have
/// expired or expire within a few minutes of `now_s`.
pub fn preview_expired(url: &str, now_s: i64) -> bool {
    let Some(at) = url.find("exp=") else { return false };
    let digits: String = url[at + 4..].chars().take_while(char::is_ascii_digit).collect();
    match digits.parse::<i64>() {
        Ok(expiry) => expiry - PREVIEW_MARGIN_S <= now_s,
        Err(_) => false,
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
        if self.work_dir.exists()
            && let Err(e) = std::fs::remove_dir_all(&self.work_dir)
        {
            tracing::warn!("could not remove {}: {e}", self.work_dir.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_previews_expire() {
        let url = "https://cdnt-preview.dzcdn.net/api/1/1/a/b/c/0/abc.mp3?hdnea=exp=1700000000~acl=/api/1/1/a*~hmac=f";
        assert!(preview_expired(url, 1_700_000_000));
        assert!(preview_expired(url, 1_699_999_800), "within the safety margin");
        assert!(!preview_expired(url, 1_699_990_000));
        assert!(!preview_expired("https://audio-ssl.itunes.apple.com/x/preview.m4a", 1_700_000_000));
        assert!(!preview_expired("https://example.com/?exp=soon", 1_700_000_000));
    }

    #[test]
    fn cached_enrichment_keeps_fresh_ids() {
        let mut fresh = Track { title: "a".into(), ..Track::default() };
        fresh.ids.insert("shazam".into(), "1".into());
        let mut cached = Track { title: "a".into(), ..Track::default() };
        cached.ids.insert("deezer".into(), "2".into());
        let merged = merge_cached(fresh, cached);
        assert_eq!(merged.ids.len(), 2);
    }
}
