//! Shazam-compatible recognition: clean-room signature generator and client.

pub mod client;
pub mod signature;

pub use client::{Priority, ShazamClient, ShazamMatch};
pub use signature::{Signature, generate};
