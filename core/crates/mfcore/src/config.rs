//! Engine configuration supplied by the app.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct EngineConfig {
    /// Persistent app data (database, melody index).
    pub data_dir: String,
    /// Disposable cache (downloads, decoded PCM).
    pub cache_dir: String,
    /// ISO 3166-1 alpha-2 storefront, e.g. "US", "DE".
    pub country: String,
    /// BCP-47 language for metadata and speech, e.g. "en-US".
    pub language: String,
    /// App version, used in the neutral User-Agent.
    pub app_version: String,
    /// Built-in yt-dlp subprocess backend (macOS, CLI). Ignored when the app
    /// supplies its own `MediaBackend`.
    pub media_tools: Option<MediaToolsConfig>,
    /// URL of the melody-index manifest (JSON).
    pub melody_index_manifest_url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct MediaToolsConfig {
    /// Python interpreter to run `yt_dlp_path` with. None runs `yt_dlp_path` directly.
    pub python_path: Option<String>,
    /// yt-dlp executable or zipimport file.
    pub yt_dlp_path: String,
    /// Values for `--js-runtimes`, e.g. "deno:/opt/homebrew/bin/deno", "quickjs:/path/qjs".
    pub js_runtimes: Vec<String>,
    /// Extra environment for the subprocess (e.g. TMPDIR, SSL_CERT_FILE).
    pub env: HashMap<String, String>,
}

/// Optional paid providers. Never persisted by the core.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, uniffi::Record)]
pub struct Credentials {
    pub audd_token: Option<String>,
    /// ACRCloud project with the fingerprint engine (recordings).
    pub acr_fingerprint: Option<AcrCredentials>,
    /// ACRCloud project with "Cover Song (humming) Identification".
    pub acr_humming: Option<AcrCredentials>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, uniffi::Record)]
pub struct AcrCredentials {
    /// e.g. "identify-eu-west-1.acrcloud.com"
    pub host: String,
    pub access_key: String,
    pub access_secret: String,
}
