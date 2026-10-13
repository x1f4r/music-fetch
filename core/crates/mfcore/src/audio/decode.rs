//! Decode audio/video files to a 16 kHz mono `PcmFile` (symphonia first, then
//! the platform decoder).
//!
//! The file is streamed packet by packet: decode → downmix → resample →
//! `PcmWriter`, so memory stays flat for hours of audio. Packets that fail to
//! decode are replaced by silence of the same length to keep the timeline
//! aligned. When symphonia cannot open or decode a file (HE-AAC v2, AC-3, odd
//! containers, surround Opus, or a decoder panic), the platform decoder writes a
//! temporary WAV next to `output` that is converted instead.

use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use symphonia::core::audio::{Audio, GenericAudioBufferRef};
use symphonia::core::codecs::CodecParameters;
use symphonia::core::codecs::audio::well_known::{
    CODEC_ID_AC3, CODEC_ID_ATRAC3, CODEC_ID_DCA, CODEC_ID_EAC3, CODEC_ID_OPUS, CODEC_ID_WMA,
};
use symphonia::core::codecs::audio::{AudioCodecId, AudioCodecParameters, AudioDecoder, AudioDecoderOptions};
use symphonia::core::codecs::registry::CodecRegistry;
use symphonia::core::errors::Error as SymError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, FormatReader, Track, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;

use crate::audio::opus::OpusAudioDecoder;
use crate::audio::resample::{StreamResampler, downmix_into};
use crate::audio::wav::WavReader;
use crate::audio::{PcmFile, PcmWriter, SAMPLE_RATE};
use crate::error::{MfError, PlatformError, Result};
use crate::platform::PlatformDecoder;
use crate::util::cancel::{CancelToken, TaskHandle};

/// Consecutive undecodable packets before the stream counts as broken.
const MAX_ERRORS_IN_ROW: u32 = 64;
/// Undecodable packets tolerated before any audio came out.
const MAX_ERRORS_BEFORE_AUDIO: u32 = 16;
/// Track resets (chained Ogg streams) followed before giving up.
const MAX_RESETS: u32 = 64;
const PROGRESS_STEP: f32 = 0.005;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

/// Decode the first audio track of `input` to a 16 kHz mono PCM file at `output`.
///
/// `progress` receives 0..=1. On failure or cancellation `output` is removed.
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
    let attempt = catch_unwind(AssertUnwindSafe(|| decode_with_symphonia(input, output, cancel, &reporter)));
    let reason = match attempt {
        Ok(Ok(pcm)) => {
            reporter.finish();
            return Ok(pcm);
        }
        Ok(Err(Failure::Canceled)) => {
            remove_quietly(output);
            return Err(MfError::Canceled);
        }
        Ok(Err(Failure::Fatal(e))) => {
            remove_quietly(output);
            return Err(e);
        }
        Ok(Err(Failure::Unsupported(reason))) => reason,
        Err(_) => "the built-in decoder failed on this file".to_string(),
    };
    remove_quietly(output);
    tracing::debug!(input = %input.display(), %reason, "symphonia could not decode; trying the platform decoder");

    let Some(decoder) = fallback else {
        return Err(MfError::Decode { message: reason });
    };
    match decode_with_platform(decoder, input, output, cancel, &reporter) {
        Ok(pcm) => {
            reporter.finish();
            Ok(pcm)
        }
        Err(e) => {
            remove_quietly(output);
            Err(match e {
                PlatformFailure::Canceled => MfError::Canceled,
                PlatformFailure::Fatal(e) => e,
                PlatformFailure::Declined => MfError::Decode { message: reason },
                PlatformFailure::Failed(message) => MfError::Decode { message: format!("{reason} ({message})") },
            })
        }
    }
}

enum Failure {
    Canceled,
    /// Not a decoding problem (output I/O); no fallback.
    Fatal(MfError),
    /// symphonia cannot handle the file; another decoder might.
    Unsupported(String),
}

impl From<MfError> for Failure {
    fn from(e: MfError) -> Self {
        match e {
            MfError::Canceled => Failure::Canceled,
            e => Failure::Fatal(e),
        }
    }
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

/// The track to decode: the container's default audio track.
fn audio_track(format: &dyn FormatReader) -> Option<(u32, AudioCodecParameters, Track)> {
    let track = format.default_track(TrackType::Audio).or_else(|| format.first_track(TrackType::Audio))?;
    match &track.codec_params {
        Some(CodecParameters::Audio(params)) => Some((track.id, params.clone(), track.clone())),
        _ => None,
    }
}

fn codec_name(id: AudioCodecId) -> &'static str {
    match id {
        CODEC_ID_AC3 => "AC-3",
        CODEC_ID_EAC3 => "E-AC-3",
        CODEC_ID_DCA => "DTS",
        CODEC_ID_WMA => "WMA",
        CODEC_ID_ATRAC3 => "ATRAC3",
        CODEC_ID_OPUS => "multichannel Opus",
        _ => "this codec",
    }
}

fn make_decoder(params: &AudioCodecParameters) -> std::result::Result<Box<dyn AudioDecoder>, Failure> {
    codecs()
        .make_audio_decoder(params, &AudioDecoderOptions::default())
        .map_err(|_| Failure::Unsupported(format!("unsupported audio codec ({})", codec_name(params.codec))))
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

    /// Output samples (16 kHz) covering `ticks` of the track.
    fn ticks_to_samples(&self, ticks: u64) -> usize {
        self.tick_secs.map(|s| (ticks as f64 * s * SAMPLE_RATE as f64).round() as usize).unwrap_or(0)
    }
}

fn decode_with_symphonia(
    input: &Path,
    output: &Path,
    cancel: &CancelToken,
    progress: &Progress<'_>,
) -> std::result::Result<PcmFile, Failure> {
    let file =
        File::open(input).map_err(|e| Failure::Fatal(MfError::invalid(format!("The file cannot be read: {e}"))))?;
    let source = CountingSource::new(file);
    let bytes_read = source.position.clone();
    let byte_len = source.len;
    let mss = MediaSourceStream::new(Box::new(source), Default::default());
    let mut hint = Hint::new();
    if let Some(ext) = input.extension().and_then(|e| e.to_str()) {
        hint.with_extension(ext);
    }
    let mut format = symphonia::default::get_probe()
        .probe(&hint, mss, FormatOptions::default(), MetadataOptions::default())
        .map_err(|e| match e {
            SymError::IoError(e) if e.kind() != ErrorKind::UnexpectedEof => {
                Failure::Fatal(MfError::internal(format!("cannot read {}: {e}", input.display())))
            }
            _ => Failure::Unsupported("unrecognized file format".into()),
        })?;

    let (mut track_id, params, track) =
        audio_track(format.as_ref()).ok_or_else(|| Failure::Unsupported("the file has no audio track".into()))?;
    let mut decoder = make_decoder(&params)?;
    let mut timeline = Timeline::new(&track);

    let mut writer = PcmWriter::create(output)?;
    let mut resampler: Option<StreamResampler> = None;
    let mut planes: Vec<Vec<f32>> = Vec::new();
    let mut mono: Vec<f32> = Vec::new();
    let mut out: Vec<f32> = Vec::new();
    let mut errors_in_row = 0u32;
    let mut errors_total = 0u32;
    let mut resets = 0u32;

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
                flush_resampler(&mut resampler, &mut out, &mut writer)?;
                let Some((id, params, track)) = audio_track(format.as_ref()) else { break };
                track_id = id;
                decoder = make_decoder(&params)?;
                timeline = Timeline::new(&track);
                continue;
            }
            // A truncated or damaged tail: keep what was decoded.
            Err(e) if writer.samples_written() > 0 => {
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
                errors_in_row = 0;
                if !mono.is_empty() {
                    if resampler.as_ref().is_none_or(|r| r.input_rate() != rate) {
                        flush_resampler(&mut resampler, &mut out, &mut writer)?;
                        resampler = Some(
                            StreamResampler::offline(rate, 1)
                                .map_err(|_| Failure::Unsupported(format!("unsupported sample rate ({rate} Hz)")))?,
                        );
                    }
                    if let Some(r) = resampler.as_mut() {
                        out.clear();
                        r.process_mono_into(&mono, &mut out);
                        writer.push_f32(&out)?;
                    }
                }
            }
            Err(SymError::DecodeError(_)) | Err(SymError::IoError(_)) => {
                errors_in_row += 1;
                errors_total += 1;
                if errors_in_row > MAX_ERRORS_IN_ROW
                    || (writer.samples_written() == 0 && errors_total > MAX_ERRORS_BEFORE_AUDIO)
                {
                    return Err(Failure::Unsupported("the audio stream is damaged".into()));
                }
                // Keep the timeline: silence for the lost packet.
                flush_resampler(&mut resampler, &mut out, &mut writer)?;
                let gap = timeline.ticks_to_samples(packet.dur.get()).min(SAMPLE_RATE as usize * 10);
                writer.push(&vec![0i16; gap])?;
            }
            Err(SymError::ResetRequired) => {
                flush_resampler(&mut resampler, &mut out, &mut writer)?;
                decoder.reset();
            }
            Err(e) => {
                if writer.samples_written() > 0 {
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

    flush_resampler(&mut resampler, &mut out, &mut writer)?;
    if errors_total > 0 {
        tracing::debug!(errors_total, "replaced undecodable packets with silence");
    }
    if writer.samples_written() == 0 {
        return Err(Failure::Unsupported("no audio could be decoded".into()));
    }
    Ok(writer.finish()?)
}

fn flush_resampler(
    resampler: &mut Option<StreamResampler>,
    out: &mut Vec<f32>,
    writer: &mut PcmWriter,
) -> std::result::Result<(), Failure> {
    if let Some(r) = resampler.as_mut() {
        out.clear();
        r.flush_into(out);
        writer.push_f32(out)?;
    }
    Ok(())
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

enum PlatformFailure {
    Canceled,
    Fatal(MfError),
    /// The platform has no decoder for this file either.
    Declined,
    Failed(String),
}

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
struct TempPath(PathBuf);

impl Drop for TempPath {
    fn drop(&mut self) {
        remove_quietly(&self.0);
    }
}

fn decode_with_platform(
    decoder: &Arc<dyn PlatformDecoder>,
    input: &Path,
    output: &Path,
    cancel: &CancelToken,
    progress: &Progress<'_>,
) -> std::result::Result<PcmFile, PlatformFailure> {
    let tmp = TempPath(sibling_path(output, "platform.wav"));
    remove_quietly(&tmp.0);
    // The platform decoder covers the first 90%; the WAV conversion the rest.
    let task = TaskHandle::new(cancel.clone());
    let started = Instant::now();
    let result = decoder.decode_to_wav(
        input.to_string_lossy().into_owned(),
        tmp.0.to_string_lossy().into_owned(),
        SAMPLE_RATE,
        task,
    );
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
    progress.report(0.9);
    let pcm = wav_to_pcm(&tmp.0, output, cancel)?;
    if pcm.len_samples() == 0 {
        return Err(PlatformFailure::Failed("no audio could be decoded".into()));
    }
    Ok(pcm)
}

/// `output` with `suffix` appended to its file name.
fn sibling_path(output: &Path, suffix: &str) -> PathBuf {
    let mut name = output.file_name().map(|n| n.to_os_string()).unwrap_or_else(|| "decode".into());
    name.push(".");
    name.push(suffix);
    output.with_file_name(name)
}

/// Convert a WAV written by a platform decoder into a 16 kHz mono PCM file.
/// 16-bit PCM is streamed directly (resampled if the decoder ignored the
/// requested format); anything else goes through symphonia.
fn wav_to_pcm(wav: &Path, output: &Path, cancel: &CancelToken) -> Result<PcmFile> {
    let mut reader = match WavReader::open_any(wav) {
        Ok(r) if r.spec().is_pcm16() => r,
        _ => {
            let noop = |_: f32| {};
            let progress = Progress::new(&noop);
            return match decode_with_symphonia(wav, output, cancel, &progress) {
                Ok(pcm) => Ok(pcm),
                Err(Failure::Canceled) => Err(MfError::Canceled),
                Err(Failure::Fatal(e)) => Err(e),
                Err(Failure::Unsupported(reason)) => Err(MfError::Decode { message: reason }),
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

fn remove_quietly(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != ErrorKind::NotFound
    {
        tracing::debug!("cannot remove {}: {e}", path.display());
    }
}

/// Throttled, monotonic progress reporting.
struct Progress<'a> {
    sink: &'a (dyn Fn(f32) + Send + Sync),
    state: std::cell::Cell<(f32, Option<Instant>)>,
}

impl<'a> Progress<'a> {
    fn new(sink: &'a (dyn Fn(f32) + Send + Sync)) -> Self {
        Self { sink, state: std::cell::Cell::new((0.0, None)) }
    }

    fn report(&self, fraction: f32) {
        let (last, at) = self.state.get();
        let fraction = fraction.clamp(0.0, 1.0);
        if fraction <= last {
            return;
        }
        let due = at.is_none_or(|t| t.elapsed() >= PROGRESS_INTERVAL);
        if fraction - last >= PROGRESS_STEP || due {
            self.state.set((fraction, Some(Instant::now())));
            (self.sink)(fraction);
        }
    }

    fn finish(&self) {
        if self.state.get().0 < 1.0 {
            self.state.set((1.0, Some(Instant::now())));
            (self.sink)(1.0);
        }
    }
}

/// A file source that publishes its read position (progress when the
/// container has no duration).
struct CountingSource {
    file: File,
    len: Option<u64>,
    position: Arc<AtomicU64>,
}

impl CountingSource {
    fn new(file: File) -> Self {
        let len = file.metadata().ok().map(|m| m.len());
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
