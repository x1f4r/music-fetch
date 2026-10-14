//! Local landmark matching (constellation hashing) on the Shazam peak extractor.
//!
//! Used by the mix and playlist pipelines to check, without any recognizer
//! call, whether a known recording plays in a stretch of the media: tracklist
//! hints are verified against catalog previews, and replays of tracks already
//! identified are found. See docs/PIPELINES.md ("Local fingerprint matching").
//!
//! Peaks come from [`crate::shazam::signature::peaks`]. Each anchor peak is
//! paired with up to [`FANOUT`] later peaks 0.08–1.5 s ahead within a bounded
//! frequency span; a hash is (log-frequency of the anchor, log-frequency
//! difference, time difference). Matching votes on (reference, time offset).
//!
//! DJs change speed: vinyl-style (tempo and pitch together) or with key lock
//! (tempo only). Rather than resampling audio, the query's peaks are mapped
//! into reference time and frequency for each candidate factor
//! ([`Warp`]); log-frequency differences survive a pitch change, so only the
//! anchor bucket and the time differences move.

mod hash;
mod matcher;

pub use hash::{FRAME_MS, Peak, peaks, peaks_in};
pub use matcher::{Index, Match, MatchOptions, Warp, warps};

#[cfg(test)]
mod tests;
