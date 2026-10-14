//! Opus for symphonia, backed by the pure-Rust `opus-decoder` crate.
//!
//! symphonia demuxes Opus from Ogg and Matroska/WebM but has no decoder. This
//! adapter decodes mono and stereo streams (channel mapping family 0, which is
//! what YouTube and every common encoder produce) at 48 kHz. Surround streams
//! are reported as unsupported so the platform decoder can take over.
//!
//! Neither demuxer trims the encoder pre-skip, so the decoder drops it, carried
//! across packets when it spans more than one.

use symphonia::core::audio::{
    AsGenericAudioBufferRef, AudioBuffer, AudioMut, AudioSpec, Channels, GenericAudioBufferRef, layouts,
};
use symphonia::core::codecs::CodecInfo;
use symphonia::core::codecs::audio::well_known::CODEC_ID_OPUS;
use symphonia::core::codecs::audio::{AudioCodecParameters, AudioDecoder, AudioDecoderOptions, FinalizeResult};
use symphonia::core::codecs::registry::{RegisterableAudioDecoder, SupportedAudioCodec};
use symphonia::core::errors::{Error, Result};
use symphonia::core::packet::PacketRef;

const RATE: u32 = 48_000;
/// 120 ms at 48 kHz: the longest Opus packet.
const MAX_FRAMES: usize = 5_760;

const SUPPORTED: &[SupportedAudioCodec] = &[SupportedAudioCodec {
    id: CODEC_ID_OPUS,
    info: CodecInfo { short_name: "opus", long_name: "Opus (opus-decoder)", profiles: &[] },
}];

/// Fields of an `OpusHead` identification header (RFC 7845 §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpusHead {
    pub channels: u8,
    pub pre_skip: u16,
    pub mapping_family: u8,
}

impl OpusHead {
    pub(crate) fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 19 || &data[..8] != b"OpusHead" {
            return None;
        }
        Some(Self { channels: data[9], pre_skip: u16::from_le_bytes([data[10], data[11]]), mapping_family: data[18] })
    }
}

pub(crate) struct OpusAudioDecoder {
    params: AudioCodecParameters,
    decoder: opus_decoder::OpusDecoder,
    channels: usize,
    pcm: Vec<i16>,
    buf: AudioBuffer<i16>,
    pre_skip_left: usize,
}

impl OpusAudioDecoder {
    fn try_new(params: &AudioCodecParameters) -> Result<Self> {
        let head = params.extra_data.as_deref().and_then(OpusHead::parse);
        let channels = params
            .channels
            .as_ref()
            .map(Channels::count)
            .filter(|&c| c > 0)
            .or(head.map(|h| h.channels as usize))
            .unwrap_or(2);
        if head.is_some_and(|h| h.mapping_family != 0 && h.channels > 2) || !(1..=2).contains(&channels) {
            return Err(Error::Unsupported("opus: multichannel streams"));
        }
        let decoder =
            opus_decoder::OpusDecoder::new(RATE, channels).map_err(|_| Error::Unsupported("opus: decoder setup"))?;
        let layout = if channels == 1 { layouts::CHANNEL_LAYOUT_MONO } else { layouts::CHANNEL_LAYOUT_STEREO };
        Ok(Self {
            params: params.clone(),
            decoder,
            channels,
            pcm: vec![0; MAX_FRAMES * channels],
            buf: AudioBuffer::new(AudioSpec::new(RATE, layout), MAX_FRAMES),
            pre_skip_left: head.map(|h| h.pre_skip as usize).unwrap_or(0),
        })
    }
}

impl AudioDecoder for OpusAudioDecoder {
    fn reset(&mut self) {
        self.decoder.reset();
    }

    fn codec_info(&self) -> &CodecInfo {
        &SUPPORTED[0].info
    }

    fn codec_params(&self) -> &AudioCodecParameters {
        &self.params
    }

    fn decode_ref(&mut self, packet: &PacketRef<'_>) -> Result<GenericAudioBufferRef<'_>> {
        self.buf.clear();
        if packet.data.is_empty() {
            return Ok(self.buf.as_generic_audio_buffer_ref());
        }
        let frames = self
            .decoder
            .decode(packet.data, &mut self.pcm, false)
            .map_err(|_| Error::DecodeError("opus: invalid packet"))?;
        if frames > MAX_FRAMES {
            return Err(Error::DecodeError("opus: packet too long"));
        }
        self.buf.render_uninit(Some(frames));
        for ch in 0..self.channels {
            if let Some(plane) = self.buf.plane_mut(ch) {
                for (dst, src) in plane.iter_mut().zip(self.pcm[ch..].iter().step_by(self.channels)) {
                    *dst = *src;
                }
            }
        }
        let skip = self.pre_skip_left.min(frames);
        self.pre_skip_left -= skip;
        let trim_start = (packet.trim_start.get() as usize).saturating_add(skip);
        let trim_end = packet.trim_end.get() as usize;
        self.buf.trim(trim_start, trim_end);
        Ok(self.buf.as_generic_audio_buffer_ref())
    }

    fn finalize(&mut self) -> FinalizeResult {
        FinalizeResult::default()
    }

    fn last_decoded(&self) -> GenericAudioBufferRef<'_> {
        self.buf.as_generic_audio_buffer_ref()
    }
}

impl RegisterableAudioDecoder for OpusAudioDecoder {
    fn try_registry_new(params: &AudioCodecParameters, _opts: &AudioDecoderOptions) -> Result<Box<dyn AudioDecoder>>
    where
        Self: Sized,
    {
        Ok(Box::new(Self::try_new(params)?))
    }

    fn supported_codecs() -> &'static [SupportedAudioCodec] {
        SUPPORTED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_opus_head() {
        let mut head = b"OpusHead".to_vec();
        head.extend_from_slice(&[1, 2, 0x38, 0x01, 0x80, 0xbb, 0, 0, 0, 0, 0]);
        assert_eq!(OpusHead::parse(&head), Some(OpusHead { channels: 2, pre_skip: 312, mapping_family: 0 }));
        assert_eq!(OpusHead::parse(&head[..18]), None);
        assert_eq!(OpusHead::parse(b"OpusTags..........."), None);
    }

    #[test]
    fn rejects_surround_and_garbage() {
        let mut head = b"OpusHead".to_vec();
        head.extend_from_slice(&[1, 6, 0, 0, 0x80, 0xbb, 0, 0, 0, 0, 1]);
        let mut params = AudioCodecParameters::new();
        params.for_codec(CODEC_ID_OPUS).with_sample_rate(RATE).with_extra_data(head.into_boxed_slice());
        assert!(OpusAudioDecoder::try_new(&params).is_err());

        let mut params = AudioCodecParameters::new();
        params.for_codec(CODEC_ID_OPUS).with_channels(layouts::CHANNEL_LAYOUT_STEREO);
        let mut dec = OpusAudioDecoder::try_new(&params).unwrap();
        // Garbage packets are decode errors, never panics.
        for len in [1usize, 2, 3, 17, 400, 1500] {
            let data: Vec<u8> = (0..len).map(|i| (i * 37 % 251) as u8).collect();
            let packet = PacketRef::new(0, 0.into(), 960u32.into(), &data);
            let _ = dec.decode_ref(&packet);
        }
    }
}
