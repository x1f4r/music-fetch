//! Cooperative cancellation shared between the engine and platform services.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::error::{MfError, Result};

type ProgressFn = Box<dyn Fn(f32) + Send + Sync>;

/// Handed to platform services for one blocking call: lets them observe
/// cancellation and report progress.
#[derive(uniffi::Object)]
pub struct TaskHandle {
    token: CancelToken,
    progress: Mutex<Option<ProgressFn>>,
}

impl TaskHandle {
    pub fn new(token: CancelToken) -> Arc<Self> {
        Arc::new(Self { token, progress: Mutex::new(None) })
    }

    pub fn with_progress(token: CancelToken, f: impl Fn(f32) + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self { token, progress: Mutex::new(Some(Box::new(f))) })
    }
}

#[uniffi::export]
impl TaskHandle {
    /// True once the session was canceled; stop work and return `PlatformError::Canceled`.
    pub fn is_canceled(&self) -> bool {
        self.token.is_canceled()
    }

    /// Report progress in 0..=1.
    pub fn report_progress(&self, fraction: f32) {
        if let Some(f) = self.progress.lock().as_ref() {
            f(fraction.clamp(0.0, 1.0));
        }
    }
}

/// Clonable cancellation flag with async wake-up.
#[derive(Clone, Default)]
pub struct CancelToken {
    inner: Arc<CancelInner>,
}

#[derive(Default)]
struct CancelInner {
    canceled: AtomicBool,
    notify: tokio::sync::Notify,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.canceled.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    pub fn is_canceled(&self) -> bool {
        self.inner.canceled.load(Ordering::SeqCst)
    }

    /// `Err(MfError::Canceled)` once canceled. Call between pipeline steps.
    pub fn check(&self) -> Result<()> {
        if self.is_canceled() { Err(MfError::Canceled) } else { Ok(()) }
    }

    /// Resolves when the token is canceled.
    pub async fn canceled(&self) {
        while !self.is_canceled() {
            let notified = self.inner.notify.notified();
            if self.is_canceled() {
                break;
            }
            notified.await;
        }
    }

    /// Sleep that ends early (with `Err(Canceled)`) on cancellation.
    pub async fn sleep(&self, duration: std::time::Duration) -> Result<()> {
        tokio::select! {
            _ = tokio::time::sleep(duration) => self.check(),
            _ = self.canceled() => Err(MfError::Canceled),
        }
    }
}
