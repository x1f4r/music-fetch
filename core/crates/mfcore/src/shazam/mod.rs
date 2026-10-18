//! Shazam-compatible recognition: clean-room signature generator and client,
//! the engine-wide call budget and their persisted back-off state.

#[cfg(feature = "net")]
pub mod budget;
#[cfg(feature = "net")]
pub mod client;
#[cfg(feature = "net")]
mod response;
pub mod signature;
#[cfg(feature = "net")]
pub mod state;

#[cfg(all(test, target_os = "macos"))]
mod oracle;
#[cfg(all(test, feature = "net"))]
pub(crate) mod test_server;

#[cfg(feature = "net")]
pub use budget::CallBudget;
#[cfg(feature = "net")]
pub use client::{Priority, QueuePolicy, ShazamClient, ShazamMatch};
#[cfg(feature = "net")]
pub use response::parse_match;
pub use signature::{Signature, generate};
#[cfg(feature = "net")]
pub use state::{MemoryState, StateStore};
