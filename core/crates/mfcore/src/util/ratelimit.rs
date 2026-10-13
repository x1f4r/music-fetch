//! Spacing-based rate limiter with jitter and a circuit breaker.
//!
//! The state lock is only held to reserve a slot; waiting happens without it, so
//! `cooldown`, `success` and `throttled` never block behind a sleeping caller.

use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::error::{MfError, Result};
use crate::util::cancel::CancelToken;

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
