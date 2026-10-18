//! Clocks, timers and tasks that work on every target.
//!
//! `std::time::Instant::now()` and `SystemTime::now()` compile for
//! wasm32-unknown-unknown but panic when called, and tokio's timers and tasks need
//! a tokio runtime, which the browser does not have. Code that also builds for
//! wasm32 takes its clocks, timers and tasks from here:
//!
//! - natively they are std's clocks, tokio's timer and tokio's tasks (the same
//!   types, re-exported, so native behaviour does not change);
//! - in the browser `performance.now()` and `Date.now()` (through `web-time`),
//!   `setTimeout` (through `gloo-timers`) and tasks on the JS event loop
//!   (`wasm_bindgen_futures::spawn_local`). The engine there runs in one
//!   dedicated Worker on one thread, so "blocking" work runs inline in a task,
//!   and futures that hold JS values (fetch, timers) are not `Send`:
//!   [`sendable`] wraps them where an interface asks for `Send`.
//!
//! clippy.toml bans the std clocks in wasm32 builds.

use std::future::Future;
use std::time::Duration;

#[cfg(not(target_family = "wasm"))]
pub use std::time::{Instant, SystemTime, UNIX_EPOCH};
#[cfg(target_family = "wasm")]
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(not(target_family = "wasm"))]
pub use tokio::runtime::Handle;
#[cfg(not(target_family = "wasm"))]
pub use tokio::task::{AbortHandle, JoinError, JoinHandle, spawn, spawn_blocking, yield_now};

#[cfg(target_family = "wasm")]
pub use local::{AbortHandle, Handle, JoinError, JoinHandle, spawn, spawn_blocking, yield_now};

/// Lets other ready tasks run before continuing. Natively tokio's
/// `yield_now`; in the browser a microtask hop, which does not wait for the
/// event loop (see [`yield_now`] for that).
pub async fn yield_to_tasks() {
    #[cfg(not(target_family = "wasm"))]
    tokio::task::yield_now().await;
    #[cfg(target_family = "wasm")]
    {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return std::task::Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        })
        .await;
    }
}

/// Marks `fut` as `Send` where an interface needs it: natively the future
/// itself (it must be `Send`); in the browser a [`send_wrapper::SendWrapper`],
/// because fetch and timer futures hold JS values. The engine's Worker is
/// single-threaded, so the wrapper is always polled and dropped on the thread
/// that made it.
#[cfg(not(target_family = "wasm"))]
#[inline]
pub fn sendable<F: Future + Send>(fut: F) -> F {
    fut
}

/// See the native version.
#[cfg(target_family = "wasm")]
#[inline]
pub fn sendable<F: Future>(fut: F) -> send_wrapper::SendWrapper<F> {
    send_wrapper::SendWrapper::new(fut)
}

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

/// Ticks every `period`, like `tokio::time::interval` with
/// `MissedTickBehavior::Skip`: the first tick completes at once; ticks missed
/// while the caller was busy are skipped, not replayed.
pub struct Interval {
    period: Duration,
    next: Instant,
}

/// A new [`Interval`]. `period` must not be zero.
pub fn interval(period: Duration) -> Interval {
    assert!(!period.is_zero(), "interval period must be positive");
    Interval { period, next: Instant::now() }
}

impl Interval {
    /// Waits for the next tick and returns when it was due.
    pub async fn tick(&mut self) -> Instant {
        let now = Instant::now();
        if self.next > now {
            sleep(self.next - now).await;
        }
        let due = self.next;
        let now = Instant::now();
        self.next = due + self.period;
        if self.next <= now {
            // Busy for more than a period: skip to the next tick in the future.
            let behind = now.duration_since(due).as_nanos() / self.period.as_nanos().max(1);
            self.next = due + self.period * (behind as u32 + 1);
        }
        due
    }

    pub fn period(&self) -> Duration {
        self.period
    }
}

/// Tasks on the browser's event loop.
#[cfg(target_family = "wasm")]
mod local {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures_util::future::{AbortHandle as Abort, Abortable};
    use tokio::sync::oneshot;

    /// Stands in for tokio's runtime handle: tasks go to the JS event loop.
    #[derive(Debug, Clone, Default)]
    pub struct Handle;

    impl Handle {
        pub fn current() -> Self {
            Self
        }

        pub fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
        where
            F: Future + 'static,
            F::Output: 'static,
        {
            spawn(fut)
        }

        pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
        where
            F: FnOnce() -> R + 'static,
            R: 'static,
        {
            spawn_blocking(f)
        }
    }

    /// Why a task gave no result: it was aborted (a panic ends the whole
    /// instance in the browser, so it never shows up here).
    #[derive(Debug)]
    pub struct JoinError {
        cancelled: bool,
    }

    impl JoinError {
        pub fn is_cancelled(&self) -> bool {
            self.cancelled
        }

        pub fn is_panic(&self) -> bool {
            !self.cancelled
        }
    }

    impl std::fmt::Display for JoinError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(if self.cancelled { "task was cancelled" } else { "task stopped without a result" })
        }
    }

    impl std::error::Error for JoinError {}

    /// Aborts a task without owning its [`JoinHandle`].
    #[derive(Clone)]
    pub struct AbortHandle(Abort);

    impl AbortHandle {
        pub fn abort(&self) {
            self.0.abort();
        }

        pub fn is_finished(&self) -> bool {
            self.0.is_aborted()
        }
    }

    /// A spawned task, like tokio's: await it for the output; dropping it
    /// detaches the task.
    pub struct JoinHandle<T> {
        rx: oneshot::Receiver<T>,
        abort: Abort,
        done: Arc<AtomicBool>,
    }

    impl<T> JoinHandle<T> {
        /// True once the task completed or was aborted.
        pub fn is_finished(&self) -> bool {
            self.done.load(Ordering::SeqCst)
        }

        pub fn abort(&self) {
            self.abort.abort();
        }

        pub fn abort_handle(&self) -> AbortHandle {
            AbortHandle(self.abort.clone())
        }
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.rx).poll(cx) {
                Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
                Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError { cancelled: self.abort.is_aborted() })),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    /// Runs `fut` on the event loop.
    pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (tx, rx) = oneshot::channel();
        let (abort, registration) = Abort::new_pair();
        let done = Arc::new(AtomicBool::new(false));
        let flag = done.clone();
        let task = Abortable::new(fut, registration);
        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(value) = task.await {
                let _ = tx.send(value);
            }
            flag.store(true, Ordering::SeqCst);
        });
        JoinHandle { rx, abort, done }
    }

    /// There are no threads: `f` runs in a task of its own, after the caller
    /// yields. It holds the Worker until it returns, so long work should be cut
    /// into pieces with [`yield_now`] in between.
    pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + 'static,
        R: 'static,
    {
        spawn(async move { f() })
    }

    /// Lets the event loop run (messages from the page, timers) before
    /// continuing: a `setTimeout(0)`, not just a microtask.
    pub async fn yield_now() {
        super::sleep(Duration::ZERO).await;
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

    #[tokio::test]
    async fn tasks_and_intervals() {
        let task = spawn(async { 3 });
        assert_eq!(task.await.unwrap(), 3);
        let blocking = spawn_blocking(|| 4);
        assert_eq!(blocking.await.unwrap(), 4);
        let slow = spawn(sleep(Duration::from_secs(5)));
        slow.abort();
        assert!(slow.await.unwrap_err().is_cancelled());
        assert_eq!(sendable(async { 5 }).await, 5);
        yield_now().await;

        let mut ticks = interval(Duration::from_millis(10));
        let t0 = Instant::now();
        ticks.tick().await;
        assert!(t0.elapsed() < Duration::from_millis(10), "the first tick is immediate");
        ticks.tick().await;
        assert!(t0.elapsed() >= Duration::from_millis(10));
        sleep(Duration::from_millis(35)).await;
        let due = ticks.tick().await;
        assert!(due.elapsed() >= Duration::from_millis(25), "a missed tick completes at once");
        let next = ticks.tick().await;
        assert!(next > Instant::now() - Duration::from_millis(15), "missed ticks are skipped");
    }
}
