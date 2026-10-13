//! Signature generation (16 kHz mono i16 input).

/// Encoded signature ready to send.
#[derive(Debug, Clone)]
pub struct Signature {
    pub bytes: Vec<u8>,
    pub sample_count: u32,
}

impl Signature {
    /// `data:audio/vnd.shazam.sig;base64,...`
    pub fn data_uri(&self) -> String {
        todo!("shazam::signature")
    }

    pub fn sample_ms(&self) -> u64 {
        self.sample_count as u64 * 1000 / 16_000
    }
}

/// Generate a signature from at most 12 s of 16 kHz mono audio.
pub fn generate(samples: &[i16]) -> Signature {
    let _ = samples;
    todo!("shazam::signature")
}
