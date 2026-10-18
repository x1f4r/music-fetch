//! The engine's start-up options (shared with the type generator, which runs natively).

use serde::{Deserialize, Serialize};

/// How the engine starts (`Engine.create`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebEngineConfig {
    /// ISO 3166-1 alpha-2 storefront, e.g. "US".
    pub country: String,
    /// BCP-47 language, e.g. "en-US".
    pub language: String,
    /// Extension version, for the neutral user agent where the browser lets us set it.
    pub app_version: String,
    pub melody_index_manifest_url: String,
    /// "opfs" (the default: SQLite in the Origin Private File System, dedicated
    /// Workers only) or "memory" (tests, Node).
    pub storage: String,
    /// OPFS directory of the database pool.
    pub directory: String,
    /// Low-power mode from the start (the host decides from its system info).
    pub low_power: bool,
    /// Tests only: send recognizer requests to this origin (a local stub).
    pub shazam_base_url: Option<String>,
    /// Tests only: no catalog enrichment.
    pub offline_catalog: bool,
    /// Tests only: pace recognizer requests this many ms apart instead of the
    /// product's spacing (only with `shazam_base_url`).
    pub test_pacing_ms: Option<u64>,
    /// Console log level: "error", "warn" (default), "info", "debug".
    pub log_level: String,
}

impl Default for WebEngineConfig {
    fn default() -> Self {
        Self {
            country: "US".into(),
            language: "en-US".into(),
            app_version: mfcore::core_version(),
            melody_index_manifest_url: String::new(),
            storage: "opfs".into(),
            directory: ".musicfetch".into(),
            low_power: false,
            shazam_base_url: None,
            offline_catalog: false,
            test_pacing_ms: None,
            log_level: "warn".into(),
        }
    }
}
