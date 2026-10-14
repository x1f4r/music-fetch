//! Test helpers shared by the audio and media tests.

use std::f32::consts::TAU;
use std::path::{Path, PathBuf};

/// A unique temporary directory removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("mfcore-{tag}-{}", crate::util::new_id()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Mono sine at `rate`, `amp` peak.
pub fn sine(rate: u32, freq: f32, secs: f32, amp: f32) -> Vec<f32> {
    let n = (rate as f32 * secs) as usize;
    (0..n).map(|i| amp * (TAU * freq * i as f32 / rate as f32).sin()).collect()
}

/// Deterministic white noise in -amp..amp.
pub fn noise(n: usize, amp: f32, seed: u64) -> Vec<f32> {
    let mut rng = fastrand::Rng::with_seed(seed);
    (0..n).map(|_| (rng.f32() * 2.0 - 1.0) * amp).collect()
}

/// True when an executable named `name` is on PATH.
pub fn have_tool(name: &str) -> bool {
    std::process::Command::new(name)
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}
