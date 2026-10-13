//! Music Fetch engine.
//!
//! See `docs/ARCHITECTURE.md` and `docs/CORE_API.md` in the repository.

uniffi::setup_scaffolding!();

pub mod audio;
pub mod catalog;
pub mod config;
pub mod engine;
pub mod error;
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
