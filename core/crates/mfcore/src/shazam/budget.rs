//! The engine-wide recognizer call budget: a token bucket shared by all sessions
//! (capacity 4, one token per 9 s, half that rate for an hour after any
//! throttling). Interactive calls may use every token; batch calls leave 2 in
//! reserve so a person listening never waits behind a mix. Cache hits cost
//! nothing. The bucket and the slow hour persist across restarts
//! ([`CallBudget::with_state`]).

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::client::Priority;
use super::state::{self, StateStore};
use crate::error::Result;
use crate::util::cancel::CancelToken;
use crate::util::rt::Instant;

/// How long the refill stays halved after a throttle.
const SLOW_FOR: Duration = Duration::from_secs(3600);
const STATE_VERSION: u32 = 1;

/// Engine-wide token bucket for recognizer calls.
pub struct CallBudget {
    state: Mutex<BudgetState>,
    capacity: f64,
    refill: Duration,
    store: Option<(Arc<dyn StateStore>, String)>,
}

struct BudgetState {
    tokens: f64,
    updated: Instant,
    slow_until: Option<Instant>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Saved {
    v: u32,
    saved_ms: i64,
    tokens: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    slow_left_ms: Option<u64>,
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
            store: None,
        }
    }

    /// Keep the bucket in `store` for the recognizer at `base_url`: restore what an
    /// earlier run left (refilled for the time since, at the rate that applied) and
    /// save after every change.
    pub fn with_state(mut self, store: Arc<dyn StateStore>, base_url: &str) -> Self {
        let key = state::key("budget", base_url);
        if let Some(saved) = store.load(&key).and_then(|s| serde_json::from_str::<Saved>(&s).ok())
            && saved.v == STATE_VERSION
            && saved.tokens.is_finite()
        {
            let elapsed = state::elapsed_since(saved.saved_ms);
            let now = Instant::now();
            let slow_left = Duration::from_millis(saved.slow_left_ms.unwrap_or(0));
            let in_slow = elapsed.min(slow_left);
            let after = elapsed.saturating_sub(slow_left);
            let gained = in_slow.as_secs_f64() / (self.refill.as_secs_f64() * 2.0).max(1e-9)
                + after.as_secs_f64() / self.refill.as_secs_f64().max(1e-9);
            let restored = BudgetState {
                tokens: (saved.tokens.max(0.0) + gained).min(self.capacity),
                updated: now,
                slow_until: state::deadline(saved.slow_left_ms, elapsed, now),
            };
            tracing::debug!(
                tokens = restored.tokens,
                slow = restored.slow_until.is_some(),
                "recognizer budget restored"
            );
            *self.state.get_mut() = restored;
        }
        self.store = Some((store, key));
        self
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
            self.save(&state, now);
            None
        } else {
            let missing = 1.0 + reserve - state.tokens;
            Some(per_token.mul_f64(missing))
        }
    }

    /// Wait for a token.
    pub async fn take(&self, priority: Priority, cancel: &CancelToken) -> Result<()> {
        while let Some(wait) = self.try_take(priority) {
            cancel.sleep(wait.max(Duration::from_millis(50))).await?;
        }
        Ok(())
    }

    /// Slow the refill down for an hour after the service throttled us.
    pub fn penalize(&self) {
        let mut state = self.state.lock();
        let now = Instant::now();
        state.slow_until = Some(now + SLOW_FOR);
        state.tokens = state.tokens.min(1.0);
        self.save(&state, now);
    }

    fn save(&self, state: &BudgetState, now: Instant) {
        let Some((store, key)) = &self.store else { return };
        let saved = Saved {
            v: STATE_VERSION,
            saved_ms: crate::util::now_ms(),
            tokens: state.tokens,
            slow_left_ms: state::left(state.slow_until, now),
        };
        if let Ok(json) = serde_json::to_string(&saved) {
            store.save(key, &json);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shazam::state::MemoryState;

    #[test]
    fn batch_keeps_two_tokens_in_reserve() {
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

    #[test]
    fn a_restart_keeps_the_spent_bucket_and_the_slow_hour() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryState::default());
        let url = "https://amp.shazam.com";
        let budget = CallBudget::new(4.0, Duration::from_secs(9)).with_state(store.clone(), url);
        for _ in 0..4 {
            assert!(budget.try_take(Priority::Interactive).is_none());
        }
        assert!(budget.try_take(Priority::Interactive).is_some(), "spent");
        drop(budget);

        // A fresh engine would start with a full bucket; the restored one is still spent.
        let restarted = CallBudget::new(4.0, Duration::from_secs(9)).with_state(store.clone(), url);
        assert!(restarted.try_take(Priority::Interactive).is_some(), "still spent after the restart");
        restarted.penalize();
        drop(restarted);

        let slow = CallBudget::new(4.0, Duration::from_secs(9)).with_state(store.clone(), url);
        let wait = slow.try_take(Priority::Interactive).unwrap();
        assert!(wait > Duration::from_secs(17), "the slow hour survived: {wait:?}");

        // Another endpoint (a stub) has its own bucket.
        let other = CallBudget::new(4.0, Duration::from_secs(9)).with_state(store, "http://127.0.0.1:9/shazam");
        assert!(other.try_take(Priority::Batch).is_none());
    }

    #[test]
    fn time_since_the_save_refills() {
        let store = Arc::new(MemoryState::default());
        let saved =
            Saved { v: STATE_VERSION, saved_ms: crate::util::now_ms() - 20_000, tokens: 0.0, slow_left_ms: None };
        store.save(&state::key("budget", "u"), &serde_json::to_string(&saved).unwrap());
        let budget = CallBudget::new(4.0, Duration::from_secs(9)).with_state(store.clone(), "u");
        // 20 s at 9 s per token: two tokens.
        assert!(budget.try_take(Priority::Interactive).is_none());
        assert!(budget.try_take(Priority::Interactive).is_none());
        assert!(budget.try_take(Priority::Interactive).is_some());

        // A clock that went backwards refills nothing.
        let future =
            Saved { v: STATE_VERSION, saved_ms: crate::util::now_ms() + 3_600_000, tokens: 0.0, slow_left_ms: None };
        store.save(&state::key("budget", "u"), &serde_json::to_string(&future).unwrap());
        let budget = CallBudget::new(4.0, Duration::from_secs(9)).with_state(store, "u");
        assert!(budget.try_take(Priority::Interactive).is_some());
    }
}
