//! Spacing-based rate limiter with jitter and a circuit breaker.

use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::error::{MfError, Result};
use crate::util::cancel::CancelToken;

/// Serializes calls to one remote service with a minimum spacing.
pub struct RateLimiter {
    state: tokio::sync::Mutex<State>,
    min_spacing: Mutex<Duration>,
    jitter: Duration,
}

struct State {
    next_at: Option<Instant>,
    open_until: Option<Instant>,
    failures: u32,
}

impl RateLimiter {
    pub fn new(min_spacing: Duration, jitter: Duration) -> Self {
        Self {
            state: tokio::sync::Mutex::new(State { next_at: None, open_until: None, failures: 0 }),
            min_spacing: Mutex::new(min_spacing),
            jitter,
        }
    }

    pub fn set_spacing(&self, spacing: Duration) {
        *self.min_spacing.lock() = spacing;
    }

    /// Wait for the next slot. Returns `RateLimited` while the breaker is open and
    /// the wait would exceed `max_wait`.
    pub async fn acquire(&self, cancel: &CancelToken, max_wait: Duration) -> Result<()> {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        if let Some(open_until) = state.open_until {
            if open_until > now {
                let wait = open_until - now;
                if wait > max_wait {
                    return Err(MfError::RateLimited { retry_in_ms: wait.as_millis() as u64 });
                }
                cancel.sleep(wait).await?;
            }
        }
        if let Some(next_at) = state.next_at {
            let now = Instant::now();
            if next_at > now {
                cancel.sleep(next_at - now).await?;
            }
        }
        let jitter_ms = if self.jitter.is_zero() { 0 } else { fastrand::u64(0..=self.jitter.as_millis() as u64) };
        state.next_at = Some(Instant::now() + *self.min_spacing.lock() + Duration::from_millis(jitter_ms));
        Ok(())
    }

    /// Record a successful call; closes the breaker.
    pub async fn success(&self) {
        let mut state = self.state.lock().await;
        state.failures = 0;
        state.open_until = None;
    }

    /// Record a throttling response; opens the breaker with exponential backoff
    /// (base doubling up to `max`). Returns the cool-down.
    pub async fn throttled(&self, base: Duration, max: Duration, retry_after: Option<Duration>) -> Duration {
        let mut state = self.state.lock().await;
        state.failures = state.failures.saturating_add(1);
        let exp = base.saturating_mul(1u32 << (state.failures - 1).min(12));
        let mut wait = exp.min(max);
        if let Some(ra) = retry_after {
            wait = wait.max(ra);
        }
        let jitter = Duration::from_millis(fastrand::u64(0..=(wait.as_millis() as u64 / 10).max(1)));
        let wait = wait + jitter;
        state.open_until = Some(Instant::now() + wait);
        wait
    }

    /// Remaining cool-down, if the breaker is open.
    pub async fn cooldown(&self) -> Option<Duration> {
        let state = self.state.lock().await;
        state.open_until.and_then(|t| t.checked_duration_since(Instant::now()))
    }
}
