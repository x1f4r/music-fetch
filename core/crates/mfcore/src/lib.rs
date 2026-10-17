//! Music Fetch engine.
//!
//! See `docs/ARCHITECTURE.md` and `docs/CORE_API.md` in the repository.

uniffi::setup_scaffolding!();

pub mod audio;
pub mod catalog;
pub mod config;
pub mod engine;
pub mod error;
pub mod fingerprint;
pub mod identity;
pub mod links;
pub mod lyrics;
pub mod media;
pub mod melody;
pub mod model;
pub mod pipeline;
pub mod platform;
pub mod providers;
pub mod shazam;
pub mod store;
pub mod util;

pub use config::{AcrCredentials, Credentials, EngineConfig, MediaToolsConfig};
pub use engine::{AnalysisSession, Engine, LiveSession};
pub use error::{MfError, PlatformError};
pub use model::*;

/// Engine version.
#[uniffi::export]
pub fn core_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// True when `text` looks like a link the engine can analyze.
#[uniffi::export]
pub fn is_link(text: String) -> bool {
    links::looks_like_url(&text)
}

/// Links for a track on every supported provider: the exact track page when known,
/// otherwise a search for "artist title" (version qualifiers kept).
#[uniffi::export]
pub fn provider_links(track: Track) -> Vec<ServiceLink> {
    catalog::search_links(&track)
}

/// Fill a custom provider template: `{artist}`, `{title}`, `{query}` (artist and title)
/// and `{isrc}` are replaced with URL-encoded values. None when the template is not an
/// http(s) URL after substitution.
#[uniffi::export]
pub fn custom_provider_link(track: Track, template: String) -> Option<String> {
    catalog::custom_link(&track, &template)
}
