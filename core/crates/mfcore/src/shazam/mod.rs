//! Shazam-compatible recognition: clean-room signature generator and client.

pub mod client;
mod response;
pub mod signature;

#[cfg(all(test, target_os = "macos"))]
mod oracle;
#[cfg(test)]
pub(crate) mod test_server;

pub use client::{Priority, QueuePolicy, ShazamClient, ShazamMatch};
pub use signature::{Signature, generate};
