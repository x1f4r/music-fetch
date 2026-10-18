//! What the analysis pipelines need from the outside world: recognizers,
//! catalog lookups and previews. [`SessionServices`] is the real thing; tests
//! substitute fakes.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sha2::{Digest, Sha256};

#[cfg(not(target_family = "wasm"))]
use crate::audio::decode::decode_file;
use crate::catalog;
use crate::error::{MfError, Result};
use crate::model::Track;
use crate::pipeline::ctx::SessionCtx;
use crate::pipeline::hints;
use crate::pipeline::recognize::{self, RecognitionHit};
use crate::shazam::Priority;
use crate::util::cancel::CancelToken;
use crate::util::rt;

/// Largest preview download.
const MAX_PREVIEW_BYTES: usize = 6 * 1024 * 1024;
const PREVIEW_TIMEOUT: Duration = Duration::from_secs(20);

/// Recognizers, catalog and previews.
pub trait Services: Send + Sync {
    /// Shazam on 3–12 s of 16 kHz mono audio (persistent cache, shared budget).
    fn shazam(
        &self,
        samples: &[i16],
        priority: Priority,
    ) -> impl Future<Output = Result<Option<RecognitionHit>>> + Send;

    /// The configured paid providers, if any.
    fn paid(&self, samples: &[i16]) -> impl Future<Output = Result<Option<RecognitionHit>>> + Send;

    /// True when a paid provider is configured.
    fn has_paid(&self) -> bool;

    /// Catalog track for a title and artist in the same version (a remix is
    /// not its original), if one is found with confidence.
    fn find(&self, title: &str, artist: &str, duration_ms: Option<u64>) -> impl Future<Output = Option<Track>> + Send;

    /// Fill links, artwork, duration and preview (best effort).
    fn enrich(&self, track: Track) -> impl Future<Output = Track> + Send;

    /// [`enrich`](Self::enrich) for one of many tracks of a collection: fast
    /// catalogs only.
    fn enrich_quick(&self, track: Track) -> impl Future<Output = Track> + Send {
        self.enrich(track)
    }

    /// A short excerpt of the recording as 16 kHz mono, when a preview exists.
    fn preview(&self, track: &Track) -> impl Future<Output = Option<Vec<i16>>> + Send;

    fn cancel(&self) -> &CancelToken;
}

/// Production services for one session.
pub struct SessionServices<'a> {
    ctx: &'a SessionCtx,
    /// Developer option: keep decoded previews here between runs.
    preview_cache: Option<PathBuf>,
    shazam_calls: AtomicU32,
}

impl<'a> SessionServices<'a> {
    pub fn new(ctx: &'a SessionCtx) -> Self {
        Self { ctx, preview_cache: None, shazam_calls: AtomicU32::new(0) }
    }

    /// Keep decoded previews in `dir` (evaluation runs replay without downloads).
    pub fn with_preview_cache(mut self, dir: Option<PathBuf>) -> Self {
        self.preview_cache = dir;
        self
    }

    /// Shazam requests made through these services (cache hits included).
    pub fn shazam_calls(&self) -> u32 {
        self.shazam_calls.load(Ordering::Relaxed)
    }

    async fn fetch_preview(&self, url: &str) -> Result<Vec<i16>> {
        crate::links::ensure_public_url(url).await?;
        let cancel = &self.ctx.cancel;
        let request = crate::util::http::prepare(self.ctx.engine.http.get(url).timeout(PREVIEW_TIMEOUT)).send();
        let response = tokio::select! {
            r = request => r?,
            _ = cancel.canceled() => return Err(MfError::Canceled),
        };
        if !response.status().is_success() {
            return Err(MfError::network(format!("preview answered {}", response.status())));
        }
        if response.content_length().is_some_and(|n| n as usize > MAX_PREVIEW_BYTES) {
            return Err(MfError::network("preview too large"));
        }
        let bytes = tokio::select! {
            b = response.bytes() => b?,
            _ = cancel.canceled() => return Err(MfError::Canceled),
        };
        if bytes.len() > MAX_PREVIEW_BYTES {
            return Err(MfError::network("preview too large"));
        }
        let ext = if url.split(['?', '#']).next().is_some_and(|p| p.ends_with(".mp3")) { "mp3" } else { "m4a" };
        let cancel = cancel.clone();
        // The browser decodes from memory (no platform decoder, no files).
        #[cfg(target_family = "wasm")]
        {
            rt::spawn_blocking(move || crate::audio::decode::decode_bytes(&bytes, Some(ext), &cancel))
                .await
                .map_err(|e| MfError::internal(format!("preview decode: {e}")))?
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let dir = self.ctx.ensure_work_dir()?.clone();
            let name = crate::util::new_id();
            let input = dir.join(format!("preview-{name}.{ext}"));
            let output = dir.join(format!("preview-{name}.pcm"));
            std::fs::write(&input, &bytes)?;
            let decoder = self.ctx.engine.decoder.clone();
            let (input2, output2) = (input.clone(), output.clone());
            let decoded = rt::spawn_blocking(move || -> Result<Vec<i16>> {
                let pcm = decode_file(&input2, &output2, decoder.as_ref(), &cancel, &|_| {})?;
                pcm.read_samples(0, pcm.len_samples() as usize)
            })
            .await
            .map_err(|e| MfError::internal(format!("preview decode: {e}")));
            let _ = std::fs::remove_file(&input);
            let _ = std::fs::remove_file(&output);
            decoded?
        }
    }

    fn cache_path(&self, track: &Track) -> Option<PathBuf> {
        let dir = self.preview_cache.as_ref()?;
        let key = ["deezer", "apple", "shazam"]
            .iter()
            .find_map(|p| track.ids.get(*p).map(|id| format!("{p}:{id}")))
            .unwrap_or_else(|| crate::identity::identity_key(track));
        let digest = Sha256::digest(key.as_bytes());
        let hex: String = digest.iter().take(12).map(|b| format!("{b:02x}")).collect();
        Some(dir.join(format!("{hex}.pcm")))
    }
}

// `rt::sendable`: in the browser these futures hold fetch() futures, which are
// not `Send`; natively it changes nothing.
impl Services for SessionServices<'_> {
    fn shazam(
        &self,
        samples: &[i16],
        priority: Priority,
    ) -> impl Future<Output = Result<Option<RecognitionHit>>> + Send {
        self.shazam_calls.fetch_add(1, Ordering::Relaxed);
        rt::sendable(recognize::shazam(self.ctx, samples, priority))
    }

    fn paid(&self, samples: &[i16]) -> impl Future<Output = Result<Option<RecognitionHit>>> + Send {
        rt::sendable(recognize::paid(self.ctx, samples))
    }

    fn has_paid(&self) -> bool {
        self.ctx.credentials.audd_token.as_deref().is_some_and(|t| !t.trim().is_empty())
            || self.ctx.credentials.acr_fingerprint.is_some()
    }

    fn find(&self, title: &str, artist: &str, duration_ms: Option<u64>) -> impl Future<Output = Option<Track>> + Send {
        let engine = &self.ctx.engine;
        rt::sendable(find_version(&engine.http, title, artist, duration_ms, engine.country(), &self.ctx.cancel))
    }

    fn enrich(&self, track: Track) -> impl Future<Output = Track> + Send {
        rt::sendable(self.ctx.engine.enrich(track, &self.ctx.cancel))
    }

    fn enrich_quick(&self, track: Track) -> impl Future<Output = Track> + Send {
        rt::sendable(self.ctx.engine.enrich_quick(track, &self.ctx.cancel))
    }

    fn preview(&self, track: &Track) -> impl Future<Output = Option<Vec<i16>>> + Send {
        rt::sendable(self.preview_samples(track))
    }

    fn cancel(&self) -> &CancelToken {
        &self.ctx.cancel
    }
}

impl SessionServices<'_> {
    async fn preview_samples(&self, track: &Track) -> Option<Vec<i16>> {
        let cached = self.cache_path(track);
        if let Some(path) = &cached
            && let Ok(bytes) = std::fs::read(path)
        {
            return Some(bytes.as_chunks::<2>().0.iter().map(|c| i16::from_le_bytes(*c)).collect());
        }
        let mut url = track.preview_url.clone();
        if url.is_none() {
            url = self.enrich(track.clone()).await.preview_url;
        }
        let samples = match self.fetch_preview(url.as_deref()?).await {
            Ok(s) if s.len() >= 5 * crate::audio::SAMPLE_RATE as usize => s,
            Ok(_) => return None,
            Err(e) => {
                tracing::debug!("preview unavailable: {e}");
                return None;
            }
        };
        if let Some(path) = cached {
            let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
            if let Some(dir) = path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let _ = std::fs::write(path, bytes);
        }
        Some(samples)
    }
}

/// Catalog lookup that keeps the version: "Song (X Remix)" must not resolve
/// to the original "Song", and vice versa.
pub async fn find_version(
    http: &reqwest::Client,
    title: &str,
    artist: &str,
    duration_ms: Option<u64>,
    country: &str,
    cancel: &CancelToken,
) -> Option<Track> {
    let artists = hints::split_artists(artist);
    let primary = artists.first().cloned().unwrap_or_else(|| artist.to_string());
    let wanted = hints::version_key(title);
    let probe = Track { title: title.to_string(), artist: artist.to_string(), ..Track::default() };
    if let Some(found) = catalog::find(http, title, &primary, duration_ms, country, cancel).await
        && hints::versions_compatible(&wanted, &hints::version_key(&found.title))
        && hints::same_song(&probe, &found)
    {
        return Some(found);
    }
    if cancel.is_canceled() {
        return None;
    }
    // The best match was another version: look through the candidates.
    let candidates = catalog::deezer::search_plain(http, title, &primary, cancel).await.ok()?;
    candidates
        .into_iter()
        .filter(|c| hints::same_song(&probe, c))
        .find(|c| {
            hints::versions_compatible(&wanted, &hints::version_key(&c.title))
                && duration_ms.is_none_or(|d| c.duration_ms.is_none_or(|cd| cd.abs_diff(d) <= 3_000))
        })
        .map(|mut t| {
            crate::identity::refresh_identity(&mut t);
            t
        })
}

/// Shared handle to services for spawned tasks.
pub type SharedServices<S> = Arc<S>;
