//! Streaming conversion from interleaved f32 (any rate, any channel count) to
//! 16 kHz mono f32.

use crate::error::Result;

pub struct StreamResampler {
    _input_rate: u32,
    _channels: u32,
}

impl StreamResampler {
    pub fn new(input_rate: u32, channels: u32) -> Result<Self> {
        Ok(Self { _input_rate: input_rate, _channels: channels })
    }

    /// Convert one chunk; output length varies with internal buffering.
    pub fn process(&mut self, interleaved: &[f32]) -> Vec<f32> {
        let _ = interleaved;
        todo!("audio::resample")
    }

    pub fn flush(&mut self) -> Vec<f32> {
        todo!("audio::resample")
    }
}

pub fn f32_to_i16(samples: &[f32]) -> Vec<i16> {
    samples.iter().map(|s| (s.clamp(-1.0, 1.0) * 32767.0).round() as i16).collect()
}

pub fn i16_to_f32(samples: &[i16]) -> Vec<f32> {
    samples.iter().map(|&s| s as f32 / 32768.0).collect()
}
