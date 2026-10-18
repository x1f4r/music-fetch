//! Music Fetch engine.
//!
//! See `docs/ARCHITECTURE.md` and `docs/CORE_API.md` in the repository.
//!
//! Cargo features (see `Cargo.toml`): the default `native` build is what the apps
//! and `mf` use. Without default features the crate is the pure core and also
//! builds for `wasm32-unknown-unknown`; `net`, `store` and `decode` add HTTP, the
//! SQLite store and file decoding on both targets.

// std's clocks compile for wasm32-unknown-unknown but panic there at run time;
// clippy.toml bans them, and the ban applies to wasm32 builds (natively
// `util::rt` simply is std).
#![cfg_attr(target_family = "wasm", warn(clippy::disallowed_methods))]

#[cfg(feature = "ffi")]
uniffi::setup_scaffolding!();

pub mod audio;
pub mod catalog;
pub mod config;
#[cfg(feature = "engine")]
pub mod engine;
pub mod error;
pub mod fingerprint;
pub mod identity;
pub mod links;
#[cfg(feature = "net")]
pub mod lyrics;
#[cfg(feature = "engine")]
pub mod media;
pub mod melody;
pub mod model;
#[cfg(feature = "engine")]
pub mod pipeline;
#[cfg(feature = "engine")]
pub mod platform;
#[cfg(feature = "engine")]
pub mod providers;
pub mod shazam;
#[cfg(feature = "store")]
pub mod store;
pub mod util;

pub use config::{AcrCredentials, Credentials, EngineConfig, MediaToolsConfig};
#[cfg(feature = "engine")]
pub use engine::{AnalysisSession, Engine, LiveSession};
pub use error::{MfError, PlatformError};
pub use model::*;

/// Engine version.
#[cfg_attr(feature = "ffi", uniffi::export)]
pub fn core_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// True when `text` looks like a link the engine can analyze.
#[cfg_attr(feature = "ffi", uniffi::export)]
pub fn is_link(text: String) -> bool {
    links::looks_like_url(&text)
}

/// Links for a track on every supported provider: the exact track page when known,
/// otherwise a search for "artist title" (version qualifiers kept).
#[cfg_attr(feature = "ffi", uniffi::export)]
pub fn provider_links(track: Track) -> Vec<ServiceLink> {
    catalog::search_links(&track)
}

/// Fill a custom provider template: `{artist}`, `{title}`, `{query}` (artist and title)
/// and `{isrc}` are replaced with URL-encoded values. None when the template is not an
/// http(s) URL after substitution.
#[cfg_attr(feature = "ffi", uniffi::export)]
pub fn custom_provider_link(track: Track, template: String) -> Option<String> {
    catalog::custom_link(&track, &template)
}
