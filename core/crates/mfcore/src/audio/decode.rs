//! Decode audio/video files to a 16 kHz mono `PcmFile` (symphonia first, then
//! the platform decoder).

use std::path::Path;
use std::sync::Arc;

use crate::audio::PcmFile;
use crate::error::Result;
use crate::platform::PlatformDecoder;
use crate::util::cancel::CancelToken;

pub fn decode_file(
    input: &Path,
    output: &Path,
    fallback: Option<&Arc<dyn PlatformDecoder>>,
    cancel: &CancelToken,
    progress: &(dyn Fn(f32) + Send + Sync),
) -> Result<PcmFile> {
    let _ = (input, output, fallback, cancel, progress);
    todo!("audio::decode")
}
