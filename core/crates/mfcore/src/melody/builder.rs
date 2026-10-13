//! Builds a melody index file from the offline builder's JSONL output.

use std::path::Path;

use crate::error::Result;

#[derive(Debug, Clone, Default)]
pub struct BuildStats {
    pub songs: u32,
    pub phrases: u32,
    pub notes: u64,
    pub bytes: u64,
}

pub fn build_index(input_jsonl: &Path, output: &Path, version: &str) -> Result<BuildStats> {
    let _ = (input_jsonl, output, version);
    todo!("melody::builder")
}
