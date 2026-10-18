//! Spacing-based rate limiter with jitter and a circuit breaker.
//!
//! The state lock is only held to reserve a slot; waiting happens without it, so
//! `cooldown`, `success` and `throttled` never block behind a sleeping caller.
//! [`RateLimiter::snapshot`] and [`RateLimiter::restore`] carry the state across
//! a restart (the Shazam client persists it).

use std::time::Duration;

use parking_lot::Mutex;

use crate::error::{MfError, Result};
use crate::util::cancel::CancelToken;
use crate::util::rt::Instant;

/// A limiter's state relative to the moment it was taken.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LimiterSnapshot {
    /// Until the next slot opens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_in_ms: Option<u64>,
    /// Until the breaker closes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_for_ms: Option<u64>,
    /// Throttles since the last success (the back-off exponent).
    #[serde(default)]
    pub failures: u32,
}

/// Serializes calls to one remote service with a minimum spacing.
pub struct RateLimiter {
    state: Mutex<State>,
}

struct State {
    spacing: Duration,
    jitter: Duration,
    /// Earliest start of the next call.
    next_at: Option<Instant>,
    /// Breaker deadline while throttled.
    open_until: Option<Instant>,
    failures: u32,
}

impl RateLimiter {
    pub fn new(min_spacing: Duration, jitter: Duration) -> Self {
        Self { state: Mutex::new(State { spacing: min_spacing, jitter, next_at: None, open_until: None, failures: 0 }) }
    }

    pub fn set_spacing(&self, spacing: Duration) {
        self.state.lock().spacing = spacing;
    }

    /// Wait for the next slot using the configured spacing.
    pub async fn acquire(&self, cancel: &CancelToken, max_wait: Duration) -> Result<()> {
        let spacing = self.state.lock().spacing;
        self.acquire_with(spacing, cancel, max_wait).await
    }

    /// Wait for the next slot and keep `spacing` (+ jitter) free after it.
    /// Returns `RateLimited` without waiting when the breaker is open for longer
    /// than `max_wait`; ordinary spacing is always waited out.
    pub async fn acquire_with(&self, spacing: Duration, cancel: &CancelToken, max_wait: Duration) -> Result<()> {
        let start_at = {
            let mut state = self.state.lock();
            let now = Instant::now();
            let breaker = state.open_until.filter(|t| *t > now);
            let slot = state.next_at.filter(|t| *t > now);
            if let Some(open_until) = breaker {
                let wait = open_until.saturating_duration_since(now);
                if wait > max_wait {
                    return Err(MfError::RateLimited { retry_in_ms: wait.as_millis() as u64 });
                }
            }
            let start_at = match (breaker, slot) {
                (Some(a), Some(b)) => a.max(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
                (None, None) => now,
            };
            let jitter_ms = if state.jitter.is_zero() { 0 } else { fastrand::u64(0..=state.jitter.as_millis() as u64) };
            state.next_at = Some(start_at + spacing + Duration::from_millis(jitter_ms));
            start_at
        };
        let wait = start_at.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            cancel.sleep(wait).await?;
        }
        cancel.check()
    }

    /// Record a successful call; closes the breaker.
    pub fn success(&self) {
        let mut state = self.state.lock();
        state.failures = 0;
        state.open_until = None;
    }

    /// Record a throttling response; opens the breaker with exponential backoff
    /// (`base` doubling up to `max`, at least `retry_after`). Returns the cool-down.
    pub fn throttled(&self, base: Duration, max: Duration, retry_after: Option<Duration>) -> Duration {
        let mut state = self.state.lock();
        state.failures = state.failures.saturating_add(1);
        let exp = base.saturating_mul(1u32 << (state.failures - 1).min(12));
        let mut wait = exp.min(max);
        if let Some(ra) = retry_after {
            wait = wait.max(ra);
        }
        let wait = wait + Duration::from_millis(fastrand::u64(0..=(wait.as_millis() as u64 / 10).max(1)));
        state.open_until = Some(Instant::now() + wait);
        wait
    }

    /// Remaining cool-down, if the breaker is open.
    pub fn cooldown(&self) -> Option<Duration> {
        self.state.lock().open_until.and_then(|t| t.checked_duration_since(Instant::now()))
    }

    /// The state now, for [`restore`](Self::restore) after a restart.
    pub fn snapshot(&self) -> LimiterSnapshot {
        let state = self.state.lock();
        let now = Instant::now();
        let left = |t: Option<Instant>| {
            t.and_then(|t| t.checked_duration_since(now)).filter(|d| !d.is_zero()).map(|d| d.as_millis() as u64)
        };
        LimiterSnapshot {
            next_in_ms: left(state.next_at),
            open_for_ms: left(state.open_until),
            failures: state.failures,
        }
    }

    /// Take over a snapshot taken `elapsed` ago. Waits only ever get longer: a
    /// slot or breaker later than the restored one stays.
    pub fn restore(&self, snapshot: &LimiterSnapshot, elapsed: Duration) {
        let mut state = self.state.lock();
        let now = Instant::now();
        let at = |ms: Option<u64>| {
            ms.and_then(|ms| Duration::from_millis(ms).checked_sub(elapsed)).filter(|d| !d.is_zero()).map(|d| now + d)
        };
        state.next_at = state.next_at.max(at(snapshot.next_in_ms));
        state.open_until = state.open_until.max(at(snapshot.open_for_ms));
        state.failures = state.failures.max(snapshot.failures);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spaces_calls_and_reports_breaker() {
        let limiter = RateLimiter::new(Duration::from_millis(50), Duration::ZERO);
        let cancel = CancelToken::new();
        let t0 = Instant::now();
        limiter.acquire(&cancel, Duration::from_secs(1)).await.unwrap();
        limiter.acquire(&cancel, Duration::from_secs(1)).await.unwrap();
        assert!(t0.elapsed() >= Duration::from_millis(50));
        let wait = limiter.throttled(Duration::from_secs(30), Duration::from_secs(60), None);
        assert!(wait >= Duration::from_secs(30));
        assert!(limiter.cooldown().is_some());
        let err = limiter.acquire(&cancel, Duration::from_secs(1)).await.unwrap_err();
        assert!(matches!(err, MfError::RateLimited { .. }));
        limiter.success();
        assert!(limiter.cooldown().is_none());
    }

    #[tokio::test]
    async fn snapshot_restores_spacing_breaker_and_backoff() {
        let limiter = RateLimiter::new(Duration::from_secs(20), Duration::ZERO);
        let cancel = CancelToken::new();
        limiter.acquire(&cancel, Duration::from_secs(1)).await.unwrap();
        limiter.throttled(Duration::from_secs(30), Duration::from_secs(600), None);
        let snap = limiter.snapshot();
        assert_eq!(snap.failures, 1);
        assert!(snap.next_in_ms.unwrap() > 19_000);
        assert!(snap.open_for_ms.unwrap() >= 30_000);
        let json = serde_json::to_string(&snap).unwrap();
        let back: LimiterSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snap);

        let fresh = RateLimiter::new(Duration::from_secs(20), Duration::ZERO);
        fresh.restore(&back, Duration::from_secs(10));
        let cooldown = fresh.cooldown().expect("breaker restored");
        assert!(cooldown >= Duration::from_secs(19) && cooldown <= Duration::from_secs(24), "{cooldown:?}");
        assert!(fresh.acquire(&cancel, Duration::from_secs(1)).await.is_err(), "still open");
        // Exponential back-off continues where it was.
        let wait = fresh.throttled(Duration::from_secs(30), Duration::from_secs(600), None);
        assert!(wait >= Duration::from_secs(60), "{wait:?}");

        // A snapshot older than its waits restores nothing but the failure count.
        let later = RateLimiter::new(Duration::from_secs(20), Duration::ZERO);
        later.restore(&back, Duration::from_secs(3600));
        assert!(later.cooldown().is_none());
        assert_eq!(later.snapshot(), LimiterSnapshot { failures: 1, ..Default::default() });
    }

    #[tokio::test]
    async fn cancel_interrupts_wait() {
        let limiter = RateLimiter::new(Duration::from_secs(10), Duration::ZERO);
        let cancel = CancelToken::new();
        limiter.acquire(&cancel, Duration::from_secs(20)).await.unwrap();
        let c2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            c2.cancel();
        });
        let err = limiter.acquire(&cancel, Duration::from_secs(20)).await.unwrap_err();
        assert_eq!(err, MfError::Canceled);
    }
}
