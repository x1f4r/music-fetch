//! yt-dlp run as a subprocess (macOS app with the bundled runtime, and the CLI).

use std::sync::Arc;

use crate::config::MediaToolsConfig;
use crate::error::PlatformError;
use crate::platform::MediaBackend;
use crate::util::cancel::TaskHandle;

pub struct YtDlpSubprocess {
    _config: MediaToolsConfig,
}

impl YtDlpSubprocess {
    pub fn new(config: MediaToolsConfig) -> Self {
        Self { _config: config }
    }
}

impl MediaBackend for YtDlpSubprocess {
    fn probe(&self, url: String, task: Arc<TaskHandle>) -> Result<String, PlatformError> {
        let _ = (url, task);
        todo!("media::ytdlp")
    }

    fn download_audio(&self, url: String, dest_dir: String, task: Arc<TaskHandle>) -> Result<String, PlatformError> {
        let _ = (url, dest_dir, task);
        todo!("media::ytdlp")
    }
}
