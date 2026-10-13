//! HTTP client factory. One shared client per engine.
//!
//! Requirements: rustls with webpki roots on every platform (no JNI setup on
//! Android), gzip/brotli, a neutral `MusicFetch/<version>` user agent, sane
//! timeouts. Never put personal data in headers.

use std::time::Duration;

use crate::error::Result;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub fn user_agent(app_version: &str) -> String {
    format!("MusicFetch/{app_version} (+https://github.com/x1f4r/music-fetch)")
}

/// A browser-like user agent for sites that refuse unknown clients.
pub const BROWSER_UA: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36";

/// Crawler user agent that makes many sites return Open Graph metadata.
pub const CRAWLER_UA: &str = "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)";

pub fn build_client(app_version: &str) -> Result<reqwest::Client> {
    let client = reqwest::Client::builder()
        .user_agent(user_agent(app_version))
        .timeout(DEFAULT_TIMEOUT)
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;
    Ok(client)
}
