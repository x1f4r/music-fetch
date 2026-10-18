//! Clocks and timers that work on every target.
//!
//! `std::time::Instant::now()` and `SystemTime::now()` compile for
//! wasm32-unknown-unknown but panic when called, and tokio's timers need a tokio
//! runtime, which the browser does not have. Code that also builds for wasm32
//! takes its clocks and timers from here: natively they are std's clocks and
//! tokio's timer, in the browser `performance.now()` and `Date.now()` (through
//! `web-time`) and `setTimeout` (through `gloo-timers`). clippy.toml bans the std
//! clocks in wasm32 builds.

use std::future::Future;
use std::time::Duration;

#[cfg(not(target_family = "wasm"))]
pub use std::time::{Instant, SystemTime, UNIX_EPOCH};
#[cfg(target_family = "wasm")]
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};

/// Waits for `duration`.
pub async fn sleep(duration: Duration) {
    #[cfg(not(target_family = "wasm"))]
    tokio::time::sleep(duration).await;
    // The timer future holds a JS callback, which is not `Send`. The browser
    // engine is single-threaded, so it is always polled and dropped on the
    // thread that made it.
    #[cfg(target_family = "wasm")]
    send_wrapper::SendWrapper::new(gloo_timers::future::sleep(duration)).await;
}

/// The deadline of [`timeout_at`] or [`timeout`] passed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Runs `fut` until `deadline`, like `tokio::time::timeout_at`.
pub async fn timeout_at<F: Future>(deadline: Instant, fut: F) -> Result<F::Output, Elapsed> {
    #[cfg(not(target_family = "wasm"))]
    {
        tokio::time::timeout_at(deadline.into(), fut).await.map_err(|_| Elapsed)
    }
    #[cfg(target_family = "wasm")]
    {
        let wait = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            biased;
            out = fut => Ok(out),
            _ = sleep(wait) => Err(Elapsed),
        }
    }
}

/// Runs `fut` for at most `duration`, like `tokio::time::timeout`.
pub async fn timeout<F: Future>(duration: Duration, fut: F) -> Result<F::Output, Elapsed> {
    #[cfg(not(target_family = "wasm"))]
    {
        tokio::time::timeout(duration, fut).await.map_err(|_| Elapsed)
    }
    #[cfg(target_family = "wasm")]
    {
        timeout_at(Instant::now() + duration, fut).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sleep_and_deadlines() {
        let t0 = Instant::now();
        sleep(Duration::from_millis(20)).await;
        assert!(t0.elapsed() >= Duration::from_millis(20));
        assert_eq!(timeout(Duration::from_secs(5), async { 7 }).await, Ok(7));
        let slow = sleep(Duration::from_secs(5));
        assert_eq!(timeout_at(Instant::now() + Duration::from_millis(10), slow).await, Err(Elapsed));
        assert!(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() > 1_700_000_000);
    }
}
