//! Shared helpers: cancellation, HTTP, rate limiting, system info, time.

pub mod cancel;
pub mod http;
pub mod ratelimit;
pub mod system;

/// Current Unix time in milliseconds.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// New random id (UUID v4, lowercase, hyphenated).
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
