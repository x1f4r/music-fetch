//! Query by humming, whistling and singing: pitch tracking, note segmentation,
//! the melody index and the matcher.

pub mod builder;
pub mod detect;
pub mod index;
pub mod matcher;
pub mod notes;
pub mod pitch;
pub mod swiftf0;

pub use detect::ImpressionDetector;
pub use index::{MelodyIndex, SongMeta};
pub use matcher::MelodyMatch;
pub use notes::{MelodyQuery, Note};
pub use pitch::{PitchTracker, StreamingPitch};
