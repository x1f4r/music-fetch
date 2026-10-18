//! Persistence of the recognizer's back-off: the circuit breaker, the request
//! spacing and the call budget survive an engine restart (an app relaunch, the
//! browser extension recycling its engine Worker), so a restart can never send
//! sooner or faster than the running engine would have.
//!
//! State is saved as relative durations plus the wall-clock time of the save.
//! On restore the time since the save counts as elapsed; a clock that went
//! backwards counts as no time at all, so a restored wait is never shorter than
//! what was left at the save, and never longer.

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::Mutex;

use crate::util::rt::Instant;

/// Where the state lives between runs (the store's `kv` entries in the engine).
pub trait StateStore: Send + Sync {
    fn load(&self, key: &str) -> Option<String>;
    fn save(&self, key: &str, value: &str);
}

#[cfg(feature = "store")]
impl StateStore for crate::store::Store {
    fn load(&self, key: &str) -> Option<String> {
        match self.kv_get(key) {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!("recognizer state unreadable: {e}");
                None
            }
        }
    }

    fn save(&self, key: &str, value: &str) {
        if let Err(e) = self.kv_put(key, value) {
            tracing::warn!("recognizer state not saved: {e}");
        }
    }
}

/// In-memory [`StateStore`] (tests and tools).
#[derive(Default)]
pub struct MemoryState(Mutex<HashMap<String, String>>);

impl StateStore for MemoryState {
    fn load(&self, key: &str) -> Option<String> {
        self.0.lock().get(key).cloned()
    }

    fn save(&self, key: &str, value: &str) {
        self.0.lock().insert(key.to_string(), value.to_string());
    }
}

/// Key of one piece of state for the recognizer at `base_url`. Test endpoints
/// make the effective destination differ from the configured one, so stub runs
/// never share state with the public service.
pub(crate) fn key(kind: &str, base_url: &str) -> String {
    let target = crate::util::endpoints::route(base_url).unwrap_or_else(|| base_url.to_string());
    format!("shazam.{kind} {}", target.trim_end_matches('/'))
}

/// Milliseconds since the save, never negative.
pub(crate) fn elapsed_since(saved_ms: i64) -> Duration {
    Duration::from_millis(crate::util::now_ms().saturating_sub(saved_ms).max(0) as u64)
}

/// Time left until `deadline`, if it is still ahead.
pub(crate) fn left(deadline: Option<Instant>, now: Instant) -> Option<u64> {
    deadline.and_then(|t| t.checked_duration_since(now)).filter(|d| !d.is_zero()).map(|d| d.as_millis() as u64)
}

/// The deadline `left_ms` after the save, seen from now.
pub(crate) fn deadline(left_ms: Option<u64>, elapsed: Duration, now: Instant) -> Option<Instant> {
    let rest = Duration::from_millis(left_ms?).checked_sub(elapsed).filter(|d| !d.is_zero())?;
    now.checked_add(rest)
}
