//! Session pipelines. Each session runs on the engine runtime, emits events to
//! its observer and persists its results.

pub mod analyze;
pub mod capture;
pub mod ctx;
pub mod emitter;
pub mod hints;
pub mod hum;
pub mod live;
pub mod melody_index;
pub mod mix;
pub mod playlist;
pub mod recognize;
pub mod stitch;

#[cfg(test)]
pub(crate) mod testutil;
