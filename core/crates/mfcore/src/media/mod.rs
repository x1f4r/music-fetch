//! The yt-dlp contract: probe parsing and the built-in subprocess backend
//! (native only: the browser has no processes).

pub mod probe;
#[cfg(not(target_family = "wasm"))]
pub mod ytdlp;

pub use probe::{Chapter, Comment, MediaEntry, MediaInfo, description_timestamps, parse_probe};
#[cfg(not(target_family = "wasm"))]
pub use ytdlp::YtDlpSubprocess;
