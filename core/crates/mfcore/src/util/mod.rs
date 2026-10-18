//! Shared helpers: cancellation, work files, HTTP, rate limiting, system info, clocks and timers.

pub mod cancel;
pub mod files;
#[cfg(feature = "store")]
pub mod files_sqlite;
#[cfg(feature = "net")]
pub mod http;
pub mod ratelimit;
pub mod rt;
pub mod system;

/// Current Unix time in milliseconds.
pub fn now_ms() -> i64 {
    rt::SystemTime::now().duration_since(rt::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// New random id (UUID v4, lowercase, hyphenated).
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
