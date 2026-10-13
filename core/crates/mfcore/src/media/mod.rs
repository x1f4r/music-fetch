//! The yt-dlp contract: probe parsing and the built-in subprocess backend.

pub mod probe;
pub mod ytdlp;

pub use probe::{Chapter, MediaEntry, MediaInfo, description_timestamps, parse_probe};
pub use ytdlp::YtDlpSubprocess;
