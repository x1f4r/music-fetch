//! Recording recognition shared by every pipeline: persistent cache, the
//! engine-wide call budget ([`CallBudget`], see `shazam/budget.rs`), rate-limit
//! notices and the optional paid providers.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::audio::wav::encode_wav;
use crate::error::{MfError, Result};
use crate::model::{EvidenceSource, Notice, Track};
use crate::pipeline::ctx::SessionCtx;
use crate::providers::ProviderMatch;
use crate::providers::acrcloud::AcrCloud;
use crate::providers::audd::AudD;
pub use crate::shazam::budget::CallBudget;
use crate::shazam::{self, Priority, ShazamMatch};

const CACHE_NAMESPACE: &str = "shazam";
const POSITIVE_TTL_MS: i64 = 30 * 24 * 3600 * 1000;
const NEGATIVE_TTL_MS: i64 = 24 * 3600 * 1000;
/// Set by evaluation tools that replay every answer from the cache: answers
/// are then kept for this long, "no match" included.
static REPLAY_TTL_MS: AtomicI64 = AtomicI64::new(0);

/// Keep every cached answer (matches and "no match") for `ttl` in this process.
/// For evaluation runs that must replay from the cache; apps never call it.
pub fn keep_cached_answers_for(ttl: Duration) {
    REPLAY_TTL_MS.store(ttl.as_millis().min(i64::MAX as u128) as i64, Ordering::Relaxed);
}

fn ttl_ms(matched: bool) -> i64 {
    match REPLAY_TTL_MS.load(Ordering::Relaxed) {
        0 if matched => POSITIVE_TTL_MS,
        0 => NEGATIVE_TTL_MS,
        replay => replay,
    }
}
/// Skew below which a single Shazam match counts as an exact recording match.
pub const TIGHT_SKEW: f32 = 0.002;

/// One recognizer answer for a clip.
#[derive(Debug, Clone)]
pub struct RecognitionHit {
    pub track: Track,
    pub source: EvidenceSource,
    /// Calibrated 0..=1 for this single answer.
    pub score: f32,
    /// Position in the reference track where the clip starts, when known.
    pub offset_ms: Option<i64>,
    pub timeskew: f32,
    pub frequency_skew: f32,
}

impl RecognitionHit {
    /// max(|timeskew|, |frequency_skew|)
    pub fn skew(&self) -> f32 {
        self.timeskew.abs().max(self.frequency_skew.abs())
    }
}

fn signature_key(signature: &shazam::Signature) -> String {
    let digest = Sha256::digest(&signature.bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn hit_from_shazam(m: ShazamMatch) -> RecognitionHit {
    let skew = m.timeskew.abs().max(m.frequency_skew.abs());
    RecognitionHit {
        score: if skew <= TIGHT_SKEW { 0.88 } else { 0.72 },
        track: m.track,
        source: EvidenceSource::Shazam,
        offset_ms: Some(m.offset_ms),
        timeskew: m.timeskew,
        frequency_skew: m.frequency_skew,
    }
}

/// Recognize 16 kHz mono audio (3–12 s) with Shazam through the persistent cache
/// and the shared budget. Emits `Notice::RateLimited` while throttled.
/// `Err(RateLimited)` means the cool-down is longer than `priority` allows.
pub async fn shazam(ctx: &SessionCtx, samples: &[i16], priority: Priority) -> Result<Option<RecognitionHit>> {
    ctx.cancel.check()?;
    let signature = shazam::generate(samples);
    let key = signature_key(&signature);
    let store = &ctx.engine.store;
    match store.cache_get(CACHE_NAMESPACE, &key) {
        Ok(Some(value)) => {
            if value == "null" {
                return Ok(None);
            }
            if let Ok(m) = serde_json::from_str::<ShazamMatch>(&value) {
                return Ok(Some(hit_from_shazam(m)));
            }
        }
        Ok(None) => {}
        Err(e) => tracing::warn!("shazam cache read failed: {e}"),
    }

    let client = &ctx.engine.shazam;
    if let Some(cooldown) = client.cooldown() {
        ctx.emitter.notice(Notice::RateLimited { retry_in_ms: cooldown.as_millis() as u64 });
    }
    ctx.engine.budget.take(priority, &ctx.cancel).await?;
    let was_cool = client.cooldown().is_none();
    let result = client.recognize(&signature, priority, &ctx.cancel).await;
    if was_cool && client.cooldown().is_some() {
        ctx.engine.budget.penalize();
    }
    match result {
        Ok(found) => {
            let (value, ttl) = match &found {
                Some(m) => (serde_json::to_string(m).unwrap_or_else(|_| "null".into()), ttl_ms(true)),
                None => ("null".to_string(), ttl_ms(false)),
            };
            if let Err(e) = store.cache_put(CACHE_NAMESPACE, &key, &value, ttl) {
                tracing::warn!("shazam cache write failed: {e}");
            }
            Ok(found.map(hit_from_shazam))
        }
        Err(MfError::RateLimited { retry_in_ms }) => {
            ctx.engine.budget.penalize();
            ctx.emitter.notice(Notice::RateLimited { retry_in_ms });
            Err(MfError::RateLimited { retry_in_ms })
        }
        Err(e) => Err(e),
    }
}

fn hit_from_provider(m: ProviderMatch, source: EvidenceSource) -> RecognitionHit {
    RecognitionHit {
        track: m.track,
        source,
        score: m.score,
        offset_ms: m.offset_ms,
        timeskew: 0.0,
        frequency_skew: 0.0,
    }
}

/// Recognize with the configured paid providers (AudD, then ACRCloud), if any.
/// Provider failures become a `ProviderUnavailable` notice and `Ok(None)`.
pub async fn paid(ctx: &SessionCtx, samples: &[i16]) -> Result<Option<RecognitionHit>> {
    let creds = &ctx.credentials;
    if creds.audd_token.is_none() && creds.acr_fingerprint.is_none() {
        return Ok(None);
    }
    let window = &samples[..samples.len().min(12 * crate::audio::SAMPLE_RATE as usize)];
    let wav = encode_wav(window, crate::audio::SAMPLE_RATE);
    if let Some(token) = creds.audd_token.clone().filter(|t| !t.trim().is_empty()) {
        match AudD::new(ctx.engine.http.clone(), token).recognize(wav.clone(), &ctx.cancel).await {
            Ok(Some(m)) => return Ok(Some(hit_from_provider(m, EvidenceSource::AudD))),
            Ok(None) => {}
            Err(MfError::Canceled) => return Err(MfError::Canceled),
            Err(e) => {
                ctx.emitter.notice(Notice::ProviderUnavailable { provider: "AudD".into(), message: e.to_string() })
            }
        }
    }
    if let Some(acr) = creds.acr_fingerprint.clone() {
        match AcrCloud::new(ctx.engine.http.clone(), acr).recognize(wav, &ctx.cancel).await {
            Ok(Some(m)) => return Ok(Some(hit_from_provider(m, EvidenceSource::AcrCloud))),
            Ok(None) => {}
            Err(MfError::Canceled) => return Err(MfError::Canceled),
            Err(e) => {
                ctx.emitter.notice(Notice::ProviderUnavailable { provider: "ACRCloud".into(), message: e.to_string() })
            }
        }
    }
    Ok(None)
}
