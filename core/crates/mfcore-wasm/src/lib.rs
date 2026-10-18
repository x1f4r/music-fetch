//! The Music Fetch engine in the browser (extension plan §2.8): a wasm-bindgen
//! facade that mirrors the UniFFI `Engine` one to one, so the extension's
//! TypeScript models match the apps' Kotlin and Swift ones.
//!
//! - It runs in the extension's dedicated engine Worker, on that Worker's event
//!   loop; storage is SQLite on OPFS ("sahpool"), or in memory for tests.
//! - JS names are camelCase; records are plain objects in serde's default shape
//!   (see `extension/src/engine/types.gen.ts`, generated from the core's types by
//!   `tests/types.rs`); calls that wait return Promises.
//! - Session events go to `host.onEvent(sessionId, event)`, exactly the core's
//!   `SessionEvent`s with its guarantees (`Started` first, one terminal event).
//! - Rejections are `Error`s with a `code`: the core's `ErrorCode` name.
//!
//! Natively only [`config`] builds (the type generator, `tests/types.rs`, reads it).

pub mod config;
#[cfg(target_family = "wasm")]
mod convert;
#[cfg(target_family = "wasm")]
mod engine;
#[cfg(target_family = "wasm")]
mod host;

#[cfg(target_family = "wasm")]
pub use engine::{CaptureSessionHandle, LiveSessionHandle, WasmEngine};
