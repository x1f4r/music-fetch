//! Recording recognition shared by every pipeline: persistent cache, the
//! engine-wide call budget, rate-limit notices and the optional paid providers.
//!
//! Budget: a token bucket shared by all sessions (capacity 4, one token per 9 s,
//! half that rate for an hour after any throttling). Interactive calls may use
//! every token; batch calls leave 2 in reserve so a person listening never waits
//! behind a mix. Cache hits cost nothing.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::audio::wav::encode_wav;
use crate::error::{MfError, Result};
use crate::model::{EvidenceSource, Notice, Track};
use crate::pipeline::ctx::SessionCtx;
use crate::providers::ProviderMatch;
use crate::providers::acrcloud::AcrCloud;
use crate::providers::audd::AudD;
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

/// Engine-wide token bucket for recognizer calls.
pub struct CallBudget {
    state: Mutex<BudgetState>,
    capacity: f64,
    refill: Duration,
}

struct BudgetState {
    tokens: f64,
    updated: Instant,
    slow_until: Option<Instant>,
}

impl Default for CallBudget {
    fn default() -> Self {
        Self::new(4.0, Duration::from_secs(9))
    }
}

impl CallBudget {
    pub fn new(capacity: f64, refill: Duration) -> Self {
        Self {
            state: Mutex::new(BudgetState { tokens: capacity, updated: Instant::now(), slow_until: None }),
            capacity,
            refill,
        }
    }

    /// Time until a token is available for `priority`, taking it when available now.
    fn try_take(&self, priority: Priority) -> Option<Duration> {
        let mut state = self.state.lock();
        let now = Instant::now();
        let slow = state.slow_until.is_some_and(|t| t > now);
        let per_token = if slow { self.refill * 2 } else { self.refill };
        let gained = now.duration_since(state.updated).as_secs_f64() / per_token.as_secs_f64();
        state.tokens = (state.tokens + gained).min(self.capacity);
        state.updated = now;
        let reserve = match priority {
            Priority::Interactive => 0.0,
            Priority::Batch => 2.0,
        };
        if state.tokens >= 1.0 + reserve {
            state.tokens -= 1.0;
            None
        } else {
            let missing = 1.0 + reserve - state.tokens;
            Some(per_token.mul_f64(missing))
        }
    }

    /// Wait for a token.
    pub async fn take(&self, priority: Priority, cancel: &crate::util::cancel::CancelToken) -> Result<()> {
        while let Some(wait) = self.try_take(priority) {
            cancel.sleep(wait.max(Duration::from_millis(50))).await?;
        }
        Ok(())
    }

    /// Slow the refill down for an hour after the service throttled us.
    pub fn penalize(&self) {
        let mut state = self.state.lock();
        state.slow_until = Some(Instant::now() + Duration::from_secs(3600));
        state.tokens = state.tokens.min(1.0);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_keeps_a_reserve_for_interactive() {
        let budget = CallBudget::new(4.0, Duration::from_secs(9));
        assert!(budget.try_take(Priority::Batch).is_none());
        assert!(budget.try_take(Priority::Batch).is_none());
        // Two tokens left: batch must wait, interactive may proceed.
        assert!(budget.try_take(Priority::Batch).is_some());
        assert!(budget.try_take(Priority::Interactive).is_none());
        assert!(budget.try_take(Priority::Interactive).is_none());
        assert!(budget.try_take(Priority::Interactive).is_some());
    }

    #[test]
    fn penalty_slows_refill() {
        let budget = CallBudget::new(4.0, Duration::from_secs(9));
        budget.penalize();
        assert!(budget.try_take(Priority::Interactive).is_none());
        let wait = budget.try_take(Priority::Interactive).unwrap();
        assert!(wait > Duration::from_secs(9));
    }
}
