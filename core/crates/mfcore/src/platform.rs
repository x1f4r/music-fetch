//! Services the apps implement for the engine (UniFFI foreign traits).
//!
//! All methods are blocking and are always called from engine worker threads,
//! never from the thread that called into the engine.

use std::sync::Arc;

use crate::error::PlatformError;
use crate::util::cancel::TaskHandle;

/// Fetches media with yt-dlp (or an equivalent).
#[uniffi::export(with_foreign)]
pub trait MediaBackend: Send + Sync {
    /// Return yt-dlp `--dump-single-json --flat-playlist` output for `url`.
    fn probe(&self, url: String, task: Arc<TaskHandle>) -> Result<String, PlatformError>;

    /// Download the best audio-only stream of `url` into `dest_dir` (m4a preferred,
    /// no post-processing) and return the absolute file path. Report progress with
    /// `task.report_progress` and stop promptly when `task.is_canceled()`.
    fn download_audio(&self, url: String, dest_dir: String, task: Arc<TaskHandle>) -> Result<String, PlatformError>;
}

/// Decodes files the built-in decoder cannot handle (WebM/Opus on macOS, HE-AAC, ...).
#[uniffi::export(with_foreign)]
pub trait PlatformDecoder: Send + Sync {
    /// Decode the first audio track of `input_path` to a 16-bit PCM mono WAV at
    /// `sample_rate`, written to `output_path`.
    fn decode_to_wav(
        &self,
        input_path: String,
        output_path: String,
        sample_rate: u32,
        task: Arc<TaskHandle>,
    ) -> Result<(), PlatformError>;
}

/// On-device speech recognition, used to read sung lyrics.
#[uniffi::export(with_foreign)]
pub trait SpeechToText: Send + Sync {
    /// Transcribe a 16 kHz mono 16-bit WAV. Return up to 5 hypotheses, best first;
    /// an empty list when there is no speech.
    fn transcribe(&self, wav_path: String, language: String, task: Arc<TaskHandle>) -> Result<Vec<String>, PlatformError>;
}

/// Receives session events. Called from engine threads; hop to the UI thread yourself.
#[uniffi::export(with_foreign)]
pub trait SessionObserver: Send + Sync {
    fn on_event(&self, event: crate::model::SessionEvent);
}
