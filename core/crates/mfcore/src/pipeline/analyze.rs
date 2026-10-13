//! Link and file analysis sessions. See docs/PIPELINES.md ("Analysis sessions").

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;
use crate::model::AnalyzeMode;
use crate::pipeline::ctx::SessionCtx;

/// A validated analysis input.
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzeInput {
    /// The first http(s) URL found in the input.
    Url(String),
    /// An existing regular file.
    File(PathBuf),
}

/// Synchronous validation done by `Engine::analyze` before a session starts.
pub fn validate(input: &str) -> Result<AnalyzeInput> {
    let _ = input;
    todo!("pipeline::analyze")
}

/// Run the session to completion. Emits every event including the terminal one
/// through `ctx.emitter` and cleans up `ctx.work_dir`.
pub async fn run(ctx: Arc<SessionCtx>, input: AnalyzeInput, mode: AnalyzeMode) {
    let _ = (ctx, input, mode);
    todo!("pipeline::analyze")
}
