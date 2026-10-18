//! Shazam-compatible recognition: clean-room signature generator and client.

#[cfg(feature = "net")]
pub mod client;
#[cfg(feature = "net")]
mod response;
pub mod signature;

#[cfg(all(test, target_os = "macos"))]
mod oracle;
#[cfg(all(test, feature = "net"))]
pub(crate) mod test_server;

#[cfg(feature = "net")]
pub use client::{Priority, QueuePolicy, ShazamClient, ShazamMatch};
pub use signature::{Signature, generate};
