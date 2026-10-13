//! Optional paid recognizers.

pub mod acrcloud;
pub mod audd;

use crate::model::Track;

#[derive(Debug, Clone)]
pub struct ProviderMatch {
    pub track: Track,
    /// Calibrated 0..=1.
    pub score: f32,
    /// Position in the reference track where the clip starts, when reported.
    pub offset_ms: Option<i64>,
}
