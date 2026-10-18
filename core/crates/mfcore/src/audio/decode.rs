//! Decode audio/video files to a 16 kHz mono `PcmFile` (symphonia first, then
//! the platform decoder).
//!
//! The file is streamed packet by packet: decode → downmix → resample →
//! `PcmWriter`, so memory stays flat for hours of audio. Packets that fail to
//! decode are replaced by silence of the same length to keep the timeline
//! aligned, and MP4 edit lists (encoder priming and padding) are applied.
//!
//! When symphonia cannot open or decode a file (explicit HE-AAC, multichannel
//! AAC, AC-3, odd containers, or a decoder panic), the platform decoder writes
//! a temporary WAV next to `output` that is converted instead. When there is a
//! platform decoder, Opus (the OS decoders are ~10x faster than the pure-Rust
//! one) and implicitly signalled HE-AAC (symphonia decodes only the AAC core,
//! without SBR) go to it first; the built-in decoders are the safety net.
//!
//! [`decode_bytes`] decodes media held in memory (previews, recordings from the
//! browser's MediaRecorder, downloaded files) with the same built-in decoders;
//! it is what the browser build has (there is no platform decoder there).

use std::fs::File;
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use std::path::PathBuf;
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use symphonia::core::audio::{Audio, GenericAudioBufferRef};
use symphonia::core::codecs::CodecParameters;
use symphonia::core::codecs::audio::well_known::{
    CODEC_ID_AAC, CODEC_ID_AC3, CODEC_ID_ATRAC3, CODEC_ID_DCA, CODEC_ID_EAC3, CODEC_ID_OPUS, CODEC_ID_WMA,
};
use symphonia::core::codecs::audio::{AudioCodecId, AudioCodecParameters, AudioDecoder, AudioDecoderOptions};
use symphonia::core::codecs::registry::CodecRegistry;
use symphonia::core::errors::Error as SymError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::well_known::FORMAT_ID_ISOMP4;
use symphonia::core::formats::{FormatOptions, FormatReader, Track, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;

use crate::audio::mp4;
use crate::audio::opus::OpusAudioDecoder;
use crate::audio::resample::StreamResampler;
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use crate::audio::resample::downmix_into;
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use crate::audio::wav::WavReader;
use crate::audio::{PcmFile, PcmWriter, SAMPLE_RATE};
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use crate::error::PlatformError;
use crate::error::{MfError, Result};
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use crate::platform::PlatformDecoder;
use crate::util::cancel::CancelToken;
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
use crate::util::cancel::TaskHandle;
use crate::util::rt::Instant;

/// Consecutive undecodable packets before the stream counts as broken.
const MAX_ERRORS_IN_ROW: u32 = 64;
/// Undecodable packets tolerated before any audio came out.
const MAX_ERRORS_BEFORE_AUDIO: u32 = 16;
/// Track resets (chained Ogg streams) followed before giving up.
const MAX_RESETS: u32 = 64;
const PROGRESS_STEP: f32 = 0.005;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);
/// Share of the progress bar for the platform decoder; the WAV conversion gets the rest.
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
const PLATFORM_SHARE: f32 = 0.9;
/// Output delay of an SBR (HE-AAC) decoder, in output samples (ISO/IEC 14496-3).
const SBR_DECODER_DELAY: u64 = 962;

/// Decode the first audio track of `input` to a 16 kHz mono PCM file at `output`.
///
/// `progress` receives 0..=1. On failure or cancellation `output` is removed.
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
pub fn decode_file(
    input: &Path,
    output: &Path,
    fallback: Option<&Arc<dyn PlatformDecoder>>,
    cancel: &CancelToken,
    progress: &(dyn Fn(f32) + Send + Sync),
) -> Result<PcmFile> {
    let meta = std::fs::metadata(input).map_err(|e| match e.kind() {
        ErrorKind::NotFound => MfError::invalid("The file does not exist."),
        ErrorKind::PermissionDenied => MfError::invalid("The file cannot be read (permission denied)."),
        _ => MfError::invalid(format!("The file cannot be read: {e}")),
    })?;
    if !meta.is_file() {
        return Err(MfError::invalid("The path is not a file."));
    }
    if meta.len() == 0 {
        return Err(MfError::Decode { message: "the file is empty".into() });
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| MfError::internal(format!("cannot create {}: {e}", parent.display())))?;
    }
    cancel.check()?;

    let reporter = Progress::new(progress);
    let prefer = if fallback.is_some() { Prefer::Platform } else { Prefer::Builtin };
    let (reason, deferred) = match run_symphonia(input, output, cancel, &reporter, prefer) {
        Ok(pcm) => {
            reporter.finish();
            return Ok(pcm);
        }
        Err(Failure::Canceled) => return Err(MfError::Canceled),
        Err(Failure::Fatal(e)) => return Err(e),
        Err(Failure::Unsupported(reason)) => (reason, false),
        Err(Failure::Deferred(what)) => (format!("{what} audio could not be decoded"), true),
    };
    tracing::debug!(input = %input.display(), %reason, "trying the platform decoder");

    let Some(decoder) = fallback else {
        return Err(MfError::Decode { message: reason });
    };
    let platform = decode_with_platform(decoder, input, output, cancel, &reporter);
    if platform.is_err() {
        remove_quietly(output);
    }
    match platform {
        Ok(pcm) => {
            reporter.finish();
            Ok(pcm)
        }
        Err(PlatformFailure::Canceled) => Err(MfError::Canceled),
        Err(PlatformFailure::Fatal(e)) => Err(e),
        // The OS could not do it after all; the built-in decoder can.
        Err(_) if deferred => match run_symphonia(input, output, cancel, &reporter, Prefer::Builtin) {
            Ok(pcm) => {
                reporter.finish();
                Ok(pcm)
            }
            Err(Failure::Canceled) => Err(MfError::Canceled),
            Err(Failure::Fatal(e)) => Err(e),
            Err(Failure::Unsupported(reason)) => Err(MfError::Decode { message: reason }),
            Err(Failure::Deferred(_)) => Err(MfError::Decode { message: reason }),
        },
        Err(PlatformFailure::Declined) => Err(MfError::Decode { message: reason }),
        Err(PlatformFailure::Failed(message)) => Err(MfError::Decode { message: format!("{reason} ({message})") }),
    }
}

/// Decodes the first audio track of a file held in memory (any format the
/// built-in decoders read: MP3, AAC/MP4, FLAC, Vorbis, Opus in Ogg or WebM/
/// Matroska, WAV, ALAC, AIFF, CAF) to 16 kHz mono samples.
///
/// `hint` is a file extension (`"m4a"`) or a MIME type (`"audio/webm;codecs=opus"`)
/// that helps pick the container; the content decides in the end.
pub fn decode_bytes(bytes: &[u8], hint: Option<&str>, cancel: &CancelToken) -> Result<Vec<i16>> {
    let pcm = decode_bytes_into(Arc::from(bytes), hint, PcmWriter::in_memory(), cancel, &|_| {})?;
    Ok(pcm.into_samples().unwrap_or_default())
}

/// Like [`decode_bytes`], writing into `writer` (a work file for long media).
/// On failure the caller removes what `writer` wrote.
pub fn decode_bytes_into(
    bytes: Arc<[u8]>,
    hint: Option<&str>,
    writer: PcmWriter,
    cancel: &CancelToken,
    progress: &(dyn Fn(f32) + Send + Sync),
) -> Result<PcmFile> {
    if bytes.is_empty() {
        return Err(MfError::Decode { message: "the file is empty".into() });
    }
    cancel.check()?;
    let reporter = Progress::new(progress);
    let input = Input::Bytes { data: bytes, hint };
    let make = move || Ok(writer);
    match guarded(|| decode_with_symphonia(&input, make, cancel, &reporter, Prefer::Builtin)) {
        Ok(pcm) => {
            reporter.finish();
            Ok(pcm)
        }
        Err(Failure::Canceled) => Err(MfError::Canceled),
        Err(Failure::Fatal(e)) => Err(e),
        Err(Failure::Unsupported(reason)) => Err(MfError::Decode { message: reason }),
        Err(Failure::Deferred(what)) => Err(MfError::Decode { message: format!("{what} audio could not be decoded") }),
    }
}

/// What to decode.
enum Input<'a> {
    #[cfg_attr(not(all(feature = "engine", not(target_family = "wasm"))), allow(dead_code))]
    Path(&'a Path),
    Bytes {
        data: Arc<[u8]>,
        hint: Option<&'a str>,
    },
}

impl Input<'_> {
    fn source(&self) -> std::result::Result<CountingSource, Failure> {
        match self {
            Input::Path(path) => {
                let file = File::open(path)
                    .map_err(|e| Failure::Fatal(MfError::invalid(format!("The file cannot be read: {e}"))))?;
                let len = file.metadata().ok().map(|m| m.len());
                Ok(CountingSource::new(Box::new(file), len))
            }
            Input::Bytes { data, .. } => {
                let len = data.len() as u64;
                Ok(CountingSource::new(Box::new(Cursor::new(data.clone())), Some(len)))
            }
        }
    }

    fn hint(&self) -> Hint {
        let mut hint = Hint::new();
        match self {
            Input::Path(path) => {
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    hint.with_extension(ext);
                }
            }
            Input::Bytes { hint: Some(h), .. } => {
                let h = h.trim();
                if h.contains('/') {
                    hint.mime_type(h.split(';').next().unwrap_or(h).trim());
                } else {
                    hint.with_extension(h.trim_start_matches('.'));
                }
            }
            Input::Bytes { hint: None, .. } => {}
        }
        hint
    }

    fn mp4_edit(&self, track_id: Option<u32>) -> Option<mp4::Mp4Edit> {
        match self {
            Input::Path(path) => mp4::sound_track_edit(path, track_id),
            Input::Bytes { data, .. } => {
                mp4::sound_track_edit_in(&mut Cursor::new(&data[..]), data.len() as u64, track_id)
            }
        }
    }

    fn name(&self) -> String {
        match self {
            Input::Path(path) => path.display().to_string(),
            Input::Bytes { .. } => "the media".into(),
        }
    }
}

/// Runs a decode, turning a decoder panic into `Unsupported`. (Browser builds
/// abort on panics instead; their Worker is restarted.)
fn guarded(decode: impl FnOnce() -> std::result::Result<PcmFile, Failure>) -> std::result::Result<PcmFile, Failure> {
    catch_unwind(AssertUnwindSafe(decode))
        .unwrap_or_else(|_| Err(Failure::Unsupported("the built-in decoder failed on this file".into())))
}

/// Which decoder gets codecs that both can handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(all(feature = "engine", not(target_family = "wasm"))), allow(dead_code))]
enum Prefer {
    Builtin,
    /// Leave Opus (much faster natively) and HE-AAC (symphonia skips SBR) to the
    /// platform decoder.
    Platform,
}

enum Failure {
    Canceled,
    /// Not a decoding problem (output I/O); no fallback.
    Fatal(MfError),
    /// symphonia cannot handle the file; another decoder might.
    Unsupported(String),
    /// Left to the platform decoder (`Prefer::Platform`); names the codec.
    Deferred(&'static str),
}

impl From<MfError> for Failure {
    fn from(e: MfError) -> Self {
        match e {
            MfError::Canceled => Failure::Canceled,
            e => Failure::Fatal(e),
        }
    }
}

/// symphonia decode guarded against panics; removes `output` on any failure.
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
fn run_symphonia(
    input: &Path,
    output: &Path,
    cancel: &CancelToken,
    progress: &Progress<'_>,
    prefer: Prefer,
) -> std::result::Result<PcmFile, Failure> {
    let result =
        guarded(|| decode_with_symphonia(&Input::Path(input), || PcmWriter::create(output), cancel, progress, prefer));
    if result.is_err() {
        remove_quietly(output);
    }
    result
}

fn codecs() -> &'static CodecRegistry {
    static REGISTRY: OnceLock<CodecRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut registry = CodecRegistry::new();
        symphonia::default::register_enabled_codecs(&mut registry);
        registry.register_audio_decoder::<OpusAudioDecoder>();
        registry
    })
}

/// The audio track being decoded.
struct OpenTrack {
    id: u32,
    params: AudioCodecParameters,
    track: Track,
    decoder: Box<dyn AudioDecoder>,
}

/// The first audio track symphonia can decode, the container's default first.
fn open_track(format: &dyn FormatReader, prefer: Prefer) -> std::result::Result<OpenTrack, Failure> {
    let default_id = format.default_track(TrackType::Audio).map(|t| t.id);
    let mut tracks: Vec<(u32, AudioCodecParameters, Track)> = format
        .tracks()
        .iter()
        .filter_map(|t| match &t.codec_params {
            Some(CodecParameters::Audio(params)) => Some((t.id, params.clone(), t.clone())),
            _ => None,
        })
        .collect();
    tracks.sort_by_key(|(id, ..)| Some(*id) != default_id);
    let mut first_error = None;
    for (id, params, track) in tracks {
        if params.codec == CODEC_ID_OPUS && prefer == Prefer::Platform {
            return Err(Failure::Deferred("Opus"));
        }
        match make_decoder(&params) {
            Ok(decoder) => return Ok(OpenTrack { id, params, track, decoder }),
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }
    Err(first_error.unwrap_or_else(|| Failure::Unsupported("the file has no audio track".into())))
}

fn unsupported_codec(id: AudioCodecId) -> String {
    let name = match id {
        CODEC_ID_AAC => "HE-AAC or multichannel AAC",
        CODEC_ID_AC3 => "AC-3",
        CODEC_ID_EAC3 => "E-AC-3",
        CODEC_ID_DCA => "DTS",
        CODEC_ID_WMA => "WMA",
        CODEC_ID_ATRAC3 => "ATRAC3",
        CODEC_ID_OPUS => "multichannel Opus",
        _ => return "unsupported audio codec".into(),
    };
    format!("unsupported audio codec ({name})")
}

fn make_decoder(params: &AudioCodecParameters) -> std::result::Result<Box<dyn AudioDecoder>, Failure> {
    codecs()
        .make_audio_decoder(params, &AudioDecoderOptions::default())
        .map_err(|_| Failure::Unsupported(unsupported_codec(params.codec)))
}

/// Where the current track's timeline stands, for progress and gap filling.
struct Timeline {
    start_ts: i64,
    total_ticks: Option<u64>,
    /// Seconds per timestamp tick.
    tick_secs: Option<f64>,
}

impl Timeline {
    fn new(track: &Track) -> Self {
        let tick_secs = track.time_base.map(|tb| tb.numer.get() as f64 / tb.denom.get() as f64);
        let total_ticks = track.duration.map(|d| d.get()).or(track.num_frames).filter(|&t| t > 0);
        Self { start_ts: track.start_ts.get(), total_ticks, tick_secs }
    }

    fn fraction(&self, pts: i64) -> Option<f32> {
        let total = self.total_ticks? as f64;
        Some((((pts - self.start_ts).max(0) as f64) / total).clamp(0.0, 1.0) as f32)
    }

    /// Seconds covered by `ticks` of the track.
    fn ticks_to_secs(&self, ticks: u64) -> f64 {
        self.tick_secs.map(|s| ticks as f64 * s).unwrap_or(0.0)
    }
}

/// Mono audio at the decoded rate → audible window → 16 kHz → PCM file.
///
/// The window (MP4 edit list: encoder priming and padding) is applied before
/// resampling, so trimming is sample-exact at the source rate.
struct Pipeline {
    writer: PcmWriter,
    resampler: Option<StreamResampler>,
    edit: Option<mp4::Mp4Edit>,
    /// Rate the window counters are expressed in (set by the first audio).
    window_rate: Option<u32>,
    skip: u64,
    remaining: u64,
    /// Rate for silence before any audio was decoded.
    default_rate: Option<u32>,
    /// The track is AAC (see `apply_window`).
    aac: bool,
    out: Vec<f32>,
    zeros: Vec<f32>,
}

impl Pipeline {
    fn new(writer: PcmWriter, edit: Option<mp4::Mp4Edit>, params: &AudioCodecParameters) -> Self {
        Self {
            writer,
            resampler: None,
            edit,
            window_rate: None,
            skip: 0,
            remaining: u64::MAX,
            default_rate: params.sample_rate,
            aac: params.codec == CODEC_ID_AAC,
            out: Vec::new(),
            zeros: Vec::new(),
        }
    }

    fn samples_written(&self) -> u64 {
        self.writer.samples_written()
    }

    fn push(&mut self, mono: &mut Vec<f32>, rate: u32) -> std::result::Result<(), Failure> {
        self.apply_window(mono, rate);
        if mono.is_empty() {
            return Ok(());
        }
        if self.resampler.as_ref().is_none_or(|r| r.input_rate() != rate) {
            self.flush()?;
            self.resampler = Some(
                StreamResampler::offline(rate, 1)
                    .map_err(|_| Failure::Unsupported(format!("unsupported sample rate ({rate} Hz)")))?,
            );
        }
        if let Some(r) = self.resampler.as_mut() {
            self.out.clear();
            r.process_mono_into(mono, &mut self.out);
            self.writer.push_f32(&self.out)?;
        }
        Ok(())
    }

    /// Silence for `secs` of lost audio, at the current rate.
    fn push_silence(&mut self, secs: f64) -> std::result::Result<(), Failure> {
        let rate = self.resampler.as_ref().map(|r| r.input_rate()).or(self.default_rate).unwrap_or(SAMPLE_RATE);
        let frames = (secs.clamp(0.0, 10.0) * rate as f64).round() as usize;
        let mut zeros = std::mem::take(&mut self.zeros);
        zeros.clear();
        zeros.resize(frames, 0.0);
        let result = self.push(&mut zeros, rate);
        self.zeros = zeros;
        result
    }

    fn apply_window(&mut self, mono: &mut Vec<f32>, rate: u32) {
        let Some(mut edit) = self.edit else { return };
        match self.window_rate {
            None => {
                // AAC decoding at half the track rate is HE-AAC without SBR: the
                // edit counts the SBR decoder's delay, which the core never adds.
                if self.aac && rate.saturating_mul(2) == edit.timescale {
                    edit = edit.with_less_skip(SBR_DECODER_DELAY);
                }
                self.skip = edit.skip_samples(rate);
                self.remaining = edit.duration_samples(rate).unwrap_or(u64::MAX);
            }
            // The rate changed mid-stream: keep the window in time, not samples.
            Some(old) if old != rate => {
                let rescale = |v: u64| if v == u64::MAX { v } else { (v as u128 * rate as u128 / old as u128) as u64 };
                self.skip = rescale(self.skip);
                self.remaining = rescale(self.remaining);
            }
            Some(_) => {}
        }
        self.window_rate = Some(rate);
        let drop = (self.skip.min(mono.len() as u64)) as usize;
        if drop > 0 {
            mono.drain(..drop);
            self.skip -= drop as u64;
        }
        let keep = (self.remaining.min(mono.len() as u64)) as usize;
        mono.truncate(keep);
        if self.remaining != u64::MAX {
            self.remaining -= keep as u64;
        }
    }

    fn flush(&mut self) -> std::result::Result<(), Failure> {
        if let Some(r) = self.resampler.as_mut() {
            self.out.clear();
            r.flush_into(&mut self.out);
            self.writer.push_f32(&self.out)?;
        }
        Ok(())
    }

    fn finish(mut self) -> std::result::Result<PcmFile, Failure> {
        self.flush()?;
        Ok(self.writer.finish()?)
    }
}

/// Decodes `input` into the writer `make_writer` creates (once the input turned
/// out to be readable media).
fn decode_with_symphonia(
    input: &Input<'_>,
    make_writer: impl FnOnce() -> Result<PcmWriter>,
    cancel: &CancelToken,
    progress: &Progress<'_>,
    prefer: Prefer,
) -> std::result::Result<PcmFile, Failure> {
    let source = input.source()?;
    let bytes_read = source.position.clone();
    let byte_len = source.len;
    let mss = MediaSourceStream::new(Box::new(source), Default::default());
    let hint = input.hint();
    let mut format = symphonia::default::get_probe()
        .probe(&hint, mss, FormatOptions::default(), MetadataOptions::default())
        .map_err(|e| match e {
            SymError::IoError(e) if e.kind() != ErrorKind::UnexpectedEof => {
                Failure::Fatal(MfError::internal(format!("cannot read {}: {e}", input.name())))
            }
            _ => Failure::Unsupported("unrecognized file format".into()),
        })?;

    let OpenTrack { id, params, track, mut decoder } = open_track(format.as_ref(), prefer)?;
    let mut track_id = id;
    let mut timeline = Timeline::new(&track);
    let edit = (format.format_info().format == FORMAT_ID_ISOMP4).then(|| input.mp4_edit(Some(id))).flatten();

    let mut pipeline = Pipeline::new(make_writer()?, edit, &params);
    let mut planes: Vec<Vec<f32>> = Vec::new();
    let mut mono: Vec<f32> = Vec::new();
    let mut errors_in_row = 0u32;
    let mut errors_total = 0u32;
    let mut resets = 0u32;
    let mut sbr_checked = false;

    loop {
        if cancel.is_canceled() {
            return Err(Failure::Canceled);
        }
        let packet = match format.next_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(SymError::ResetRequired) => {
                // A new logical stream (chained Ogg): pick the track again.
                resets += 1;
                if resets > MAX_RESETS {
                    break;
                }
                pipeline.flush()?;
                let next = open_track(format.as_ref(), prefer)?;
                track_id = next.id;
                decoder = next.decoder;
                timeline = Timeline::new(&next.track);
                continue;
            }
            // A truncated or damaged tail: keep what was decoded.
            Err(e) if pipeline.samples_written() > 0 => {
                tracing::debug!("stopping at demuxer error: {e}");
                break;
            }
            Err(SymError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Failure::Unsupported(format!("the file cannot be read ({e})"))),
        };
        if packet.track_id != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(buf) => {
                let rate = buf.spec().rate();
                downmix_generic(&buf, &mut planes, &mut mono);
                Ok(rate)
            }
            Err(e) => Err(e),
        };
        match decoded {
            Ok(rate) => {
                // AAC at half the track's rate is HE-AAC decoded without SBR.
                if !sbr_checked {
                    sbr_checked = true;
                    let half_rate = |r: Option<u32>| r == Some(rate.saturating_mul(2));
                    if params.codec == CODEC_ID_AAC
                        && prefer == Prefer::Platform
                        && (half_rate(params.sample_rate) || half_rate(edit.map(|e| e.timescale)))
                    {
                        return Err(Failure::Deferred("HE-AAC"));
                    }
                }
                errors_in_row = 0;
                pipeline.push(&mut mono, rate)?;
            }
            Err(SymError::DecodeError(_)) | Err(SymError::IoError(_)) => {
                errors_in_row += 1;
                errors_total += 1;
                if errors_in_row > MAX_ERRORS_IN_ROW
                    || (pipeline.samples_written() == 0 && errors_total > MAX_ERRORS_BEFORE_AUDIO)
                {
                    return Err(Failure::Unsupported("the audio stream is damaged".into()));
                }
                // Keep the timeline: silence for the lost packet.
                pipeline.push_silence(timeline.ticks_to_secs(packet.dur.get()))?;
            }
            Err(SymError::ResetRequired) => {
                pipeline.flush()?;
                decoder.reset();
            }
            Err(e) => {
                if pipeline.samples_written() > 0 {
                    tracing::debug!("stopping at decoder error: {e}");
                    break;
                }
                return Err(Failure::Unsupported(format!("unsupported audio stream ({e})")));
            }
        }

        let fraction = timeline
            .fraction(packet.pts.get())
            .or_else(|| byte_len.map(|len| (bytes_read.load(Ordering::Relaxed) as f64 / len.max(1) as f64) as f32));
        if let Some(f) = fraction {
            progress.report(f);
        }
    }

    if errors_total > 0 {
        tracing::debug!(errors_total, "replaced undecodable packets with silence");
    }
    let pcm = pipeline.finish()?;
    if pcm.len_samples() == 0 {
        return Err(Failure::Unsupported("no audio could be decoded".into()));
    }
    Ok(pcm)
}

/// Average all channels of a decoded buffer into `mono`.
fn downmix_generic(buf: &GenericAudioBufferRef<'_>, planes: &mut Vec<Vec<f32>>, mono: &mut Vec<f32>) {
    mono.clear();
    match buf {
        GenericAudioBufferRef::F32(b) => {
            let n = b.num_planes();
            match n {
                0 => {}
                1 => mono.extend_from_slice(b.plane(0).unwrap_or(&[])),
                2 => {
                    if let (Some(l), Some(r)) = (b.plane(0), b.plane(1)) {
                        mono.extend(l.iter().zip(r).map(|(a, b)| (a + b) * 0.5));
                    }
                }
                _ => average_planes((0..n).filter_map(|i| b.plane(i)), b.frames(), mono),
            }
        }
        other => {
            other.copy_to_vecs_planar::<f32>(planes);
            average_planes(planes.iter().map(Vec::as_slice), other.frames(), mono);
        }
    }
}

fn average_planes<'a>(planes: impl Iterator<Item = &'a [f32]>, frames: usize, mono: &mut Vec<f32>) {
    mono.clear();
    mono.resize(frames, 0.0);
    let mut count = 0usize;
    for plane in planes {
        count += 1;
        for (m, s) in mono.iter_mut().zip(plane) {
            *m += *s;
        }
    }
    if count > 1 {
        let scale = 1.0 / count as f32;
        mono.iter_mut().for_each(|m| *m *= scale);
    }
}

#[cfg(all(feature = "engine", not(target_family = "wasm")))]
enum PlatformFailure {
    Canceled,
    Fatal(MfError),
    /// The platform has no decoder for this file either.
    Declined,
    Failed(String),
}

#[cfg(all(feature = "engine", not(target_family = "wasm")))]
impl From<MfError> for PlatformFailure {
    fn from(e: MfError) -> Self {
        match e {
            MfError::Canceled => PlatformFailure::Canceled,
            MfError::Decode { message } => PlatformFailure::Failed(message),
            e => PlatformFailure::Fatal(e),
        }
    }
}

/// Removes a file when dropped.
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
struct TempPath(PathBuf);

#[cfg(all(feature = "engine", not(target_family = "wasm")))]
impl Drop for TempPath {
    fn drop(&mut self) {
        remove_quietly(&self.0);
    }
}

#[cfg(all(feature = "engine", not(target_family = "wasm")))]
fn decode_with_platform(
    decoder: &Arc<dyn PlatformDecoder>,
    input: &Path,
    output: &Path,
    cancel: &CancelToken,
    progress: &Progress<'_>,
) -> std::result::Result<PcmFile, PlatformFailure> {
    let tmp = TempPath(sibling_path(output, "platform.wav"));
    remove_quietly(&tmp.0);

    // The platform reports progress on any thread; relay it from here.
    let latest = Arc::new(AtomicU32::new(f32::to_bits(-1.0)));
    let task = {
        let latest = latest.clone();
        TaskHandle::with_progress(cancel.clone(), move |f| latest.store(f.to_bits(), Ordering::Relaxed))
    };
    let done = AtomicBool::new(false);
    let started = Instant::now();
    let result = std::thread::scope(|scope| {
        scope.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                let f = f32::from_bits(latest.load(Ordering::Relaxed));
                if f >= 0.0 {
                    progress.report(f * PLATFORM_SHARE);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        let result = decoder.decode_to_wav(
            input.to_string_lossy().into_owned(),
            tmp.0.to_string_lossy().into_owned(),
            SAMPLE_RATE,
            task,
        );
        done.store(true, Ordering::Relaxed);
        result
    });
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        ok = result.is_ok(),
        "platform decoder finished"
    );
    match result {
        Ok(()) => {}
        Err(PlatformError::Canceled) => return Err(PlatformFailure::Canceled),
        Err(PlatformError::NotSupported) | Err(PlatformError::Unavailable { .. }) => {
            return Err(PlatformFailure::Declined);
        }
        Err(PlatformError::Failed { message }) => return Err(PlatformFailure::Failed(message)),
    }
    if cancel.is_canceled() {
        return Err(PlatformFailure::Canceled);
    }
    progress.report(PLATFORM_SHARE);
    let pcm = wav_to_pcm(&tmp.0, output, cancel)?;
    if pcm.len_samples() == 0 {
        return Err(PlatformFailure::Failed("no audio could be decoded".into()));
    }
    Ok(pcm)
}

/// `output` with `suffix` appended to its file name.
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
fn sibling_path(output: &Path, suffix: &str) -> PathBuf {
    let mut name = output.file_name().map(|n| n.to_os_string()).unwrap_or_else(|| "decode".into());
    name.push(".");
    name.push(suffix);
    output.with_file_name(name)
}

/// Convert a WAV written by a platform decoder into a 16 kHz mono PCM file.
/// 16-bit PCM is streamed directly (resampled if the decoder ignored the
/// requested format); anything else goes through symphonia.
#[cfg(all(feature = "engine", not(target_family = "wasm")))]
fn wav_to_pcm(wav: &Path, output: &Path, cancel: &CancelToken) -> Result<PcmFile> {
    let mut reader = match WavReader::open_any(wav) {
        Ok(r) if r.spec().is_pcm16() => r,
        _ => {
            let noop = |_: f32| {};
            let progress = Progress::new(&noop);
            return match run_symphonia(wav, output, cancel, &progress, Prefer::Builtin) {
                Ok(pcm) => Ok(pcm),
                Err(Failure::Canceled) => Err(MfError::Canceled),
                Err(Failure::Fatal(e)) => Err(e),
                Err(Failure::Unsupported(reason)) => Err(MfError::Decode { message: reason }),
                Err(Failure::Deferred(what)) => Err(MfError::Decode { message: format!("unexpected {what} data") }),
            };
        }
    };
    let spec = reader.spec();
    let channels = spec.channels as usize;
    let mut writer = PcmWriter::create(output)?;
    let passthrough = spec.sample_rate == SAMPLE_RATE && channels == 1;
    let mut resampler =
        if passthrough {
            None
        } else {
            Some(StreamResampler::offline(spec.sample_rate, 1).map_err(|_| MfError::Decode {
                message: format!("unsupported sample rate ({} Hz)", spec.sample_rate),
            })?)
        };
    let mut buf = vec![0i16; 64 * 1024 * channels.max(1)];
    let mut interleaved = Vec::new();
    let mut mono = Vec::new();
    let mut out = Vec::new();
    loop {
        cancel.check()?;
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let Some(r) = resampler.as_mut() else {
            writer.push(&buf[..n])?;
            continue;
        };
        interleaved.clear();
        interleaved.extend(buf[..n - n % channels].iter().map(|&s| s as f32 / 32768.0));
        mono.clear();
        downmix_into(&interleaved, channels, &mut mono);
        out.clear();
        r.process_mono_into(&mono, &mut out);
        writer.push_f32(&out)?;
    }
    if let Some(r) = resampler.as_mut() {
        out.clear();
        r.flush_into(&mut out);
        writer.push_f32(&out)?;
    }
    writer.finish()
}

#[cfg(all(feature = "engine", not(target_family = "wasm")))]
fn remove_quietly(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != ErrorKind::NotFound
    {
        tracing::debug!("cannot remove {}: {e}", path.display());
    }
}

/// Throttled, monotonic progress reporting (thread-safe).
struct Progress<'a> {
    sink: &'a (dyn Fn(f32) + Send + Sync),
    state: Mutex<(f32, Option<Instant>)>,
}

impl<'a> Progress<'a> {
    fn new(sink: &'a (dyn Fn(f32) + Send + Sync)) -> Self {
        Self { sink, state: Mutex::new((0.0, None)) }
    }

    fn report(&self, fraction: f32) {
        let fraction = if fraction.is_nan() { 0.0 } else { fraction.clamp(0.0, 1.0) };
        let mut state = self.state.lock();
        let (last, at) = *state;
        if fraction <= last {
            return;
        }
        let due = at.is_none_or(|t| t.elapsed() >= PROGRESS_INTERVAL);
        if fraction - last >= PROGRESS_STEP || due {
            *state = (fraction, Some(Instant::now()));
            drop(state);
            (self.sink)(fraction);
        }
    }

    fn finish(&self) {
        let mut state = self.state.lock();
        if state.0 < 1.0 {
            *state = (1.0, Some(Instant::now()));
            drop(state);
            (self.sink)(1.0);
        }
    }
}

trait ReadSeek: Read + Seek + Send + Sync {}

impl<T: Read + Seek + Send + Sync> ReadSeek for T {}

/// A media source that publishes its read position (progress when the
/// container has no duration).
struct CountingSource {
    file: Box<dyn ReadSeek>,
    len: Option<u64>,
    position: Arc<AtomicU64>,
}

impl CountingSource {
    fn new(file: Box<dyn ReadSeek>, len: Option<u64>) -> Self {
        Self { file, len, position: Arc::new(AtomicU64::new(0)) }
    }
}

impl Read for CountingSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read(buf)?;
        self.position.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

impl Seek for CountingSource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let at = self.file.seek(pos)?;
        self.position.store(at, Ordering::Relaxed);
        Ok(at)
    }
}

impl MediaSource for CountingSource {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        self.len
    }
}

#[cfg(all(test, feature = "engine"))]
mod tests;

/// The codec fixtures decode the same everywhere (see tests/wasm_decode.rs).
#[cfg(test)]
mod codec_fixtures {
    use super::decode_bytes;
    use crate::util::cancel::CancelToken;

    include!("../../tests/fixtures/codecs/check.rs");

    #[test]
    fn decoded_pcm_matches_expected() {
        let bad = codec_mismatches();
        assert!(bad.is_empty(), "{}", bad.join("\n"));
    }

    #[test]
    fn every_fixture_has_audio() {
        for (name, hint, bytes, _) in CODEC_FIXTURES {
            let samples = decode_bytes(bytes, Some(hint), &CancelToken::new()).unwrap();
            let summary = codec_summary(&samples);
            assert!(samples.len() > 7000, "{name}: {summary}");
            assert!(summary["peak"].as_u64().unwrap_or(0) > 3000, "{name}: {summary}");
        }
    }

    /// Rewrites expected.json and expected/*.pcm after the fixtures change.
    #[test]
    #[ignore = "regenerates tests/fixtures/codecs/expected*"]
    fn write_expected() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codecs");
        std::fs::create_dir_all(dir.join("expected")).unwrap();
        let mut all = serde_json::Map::new();
        for (name, hint, bytes, _) in CODEC_FIXTURES {
            let samples = decode_bytes(bytes, Some(hint), &CancelToken::new()).unwrap();
            let start = codec_window_start(samples.len());
            let window: Vec<u8> = samples[start..(start + CODEC_WINDOW).min(samples.len())]
                .iter()
                .flat_map(|s| s.to_le_bytes())
                .collect();
            std::fs::write(dir.join(format!("expected/{name}.pcm")), window).unwrap();
            all.insert(name.to_string(), codec_summary(&samples));
        }
        std::fs::write(dir.join("expected.json"), serde_json::to_string_pretty(&all).unwrap() + "\n").unwrap();
    }

    /// The file path (the apps) and the in-memory path (the browser) decode alike.
    #[cfg(all(feature = "engine", not(target_family = "wasm")))]
    #[test]
    fn files_and_bytes_decode_alike() {
        let dir = crate::audio::testutil::TempDir::new("decbytes");
        for (name, hint, bytes, _) in CODEC_FIXTURES {
            let input = dir.path().join(name);
            std::fs::write(&input, bytes).unwrap();
            let output = dir.path().join(format!("{name}.pcm"));
            let pcm = super::decode_file(&input, &output, None, &CancelToken::new(), &|_| {})
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let from_file = pcm.read_samples(0, usize::MAX).unwrap();
            assert_eq!(from_file, decode_bytes(bytes, Some(hint), &CancelToken::new()).unwrap(), "{name}");
        }
    }

    #[test]
    fn hints_and_errors() {
        let (_, _, mp3, _) = CODEC_FIXTURES[1];
        let samples = decode_bytes(mp3, None, &CancelToken::new()).unwrap();
        assert_eq!(decode_bytes(mp3, Some("audio/mpeg"), &CancelToken::new()).unwrap(), samples, "MIME hint");
        assert_eq!(decode_bytes(mp3, Some(".mp3"), &CancelToken::new()).unwrap(), samples, "extension hint");
        assert!(matches!(decode_bytes(b"", None, &CancelToken::new()), Err(crate::error::MfError::Decode { .. })));
        assert!(matches!(
            decode_bytes(&b"not media ".repeat(300), Some("mp3"), &CancelToken::new()),
            Err(crate::error::MfError::Decode { .. })
        ));
        let cancel = CancelToken::new();
        cancel.cancel();
        assert_eq!(decode_bytes(mp3, None, &cancel), Err(crate::error::MfError::Canceled));
    }
}
