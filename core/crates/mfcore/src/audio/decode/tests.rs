use std::process::Command;
use std::sync::atomic::AtomicUsize;

use super::*;
use crate::audio::testutil::{TempDir, have_tool, sine};
use crate::audio::wav::{encode_wav, encode_wav_interleaved};

fn interleave(mono: &[f32], channels: usize) -> Vec<i16> {
    mono.iter().flat_map(|&s| std::iter::repeat_n(crate::audio::resample::f32_sample_to_i16(s), channels)).collect()
}

fn write_wav_file(path: &Path, rate: u32, channels: u16, secs: f32, freq: f32) {
    let samples = interleave(&sine(rate, freq, secs, 0.5), channels as usize);
    std::fs::write(path, encode_wav_interleaved(&samples, rate, channels)).unwrap();
}

fn zero_cross_freq(x: &[i16]) -> f32 {
    let crossings = x.windows(2).filter(|w| (w[0] < 0) != (w[1] < 0)).count();
    crossings as f32 / 2.0 / (x.len() as f32 / SAMPLE_RATE as f32)
}

fn decode(input: &Path, output: &Path, fallback: Option<&Arc<dyn PlatformDecoder>>) -> Result<PcmFile> {
    decode_file(input, output, fallback, &CancelToken::new(), &|_| {})
}

#[test]
fn decodes_wav_to_16k_mono() {
    let dir = TempDir::new("dec");
    let input = dir.path().join("in.wav");
    write_wav_file(&input, 44_100, 2, 3.0, 440.0);
    let seen = Mutex::new(Vec::new());
    let pcm =
        decode_file(&input, &dir.path().join("out.pcm"), None, &CancelToken::new(), &|f| seen.lock().push(f)).unwrap();
    assert_eq!(pcm.len_samples(), 48_000);
    let samples = pcm.read_ms(500, 2000).unwrap();
    assert!((zero_cross_freq(&samples) - 440.0).abs() < 3.0);
    let peak = samples.iter().map(|s| s.unsigned_abs()).max().unwrap();
    assert!((15_500..17_000).contains(&peak), "peak {peak}");
    let seen = seen.into_inner();
    assert_eq!(seen.last(), Some(&1.0));
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
}

#[test]
fn input_errors() {
    let dir = TempDir::new("decerr");
    let out = dir.path().join("o.pcm");
    assert!(matches!(decode(&dir.path().join("missing.wav"), &out, None), Err(MfError::InvalidInput { .. })));
    assert!(matches!(decode(dir.path(), &out, None), Err(MfError::InvalidInput { .. })));
    let empty = dir.path().join("empty.mp3");
    std::fs::write(&empty, b"").unwrap();
    assert!(matches!(decode(&empty, &out, None), Err(MfError::Decode { .. })));
    let garbage = dir.path().join("garbage.bin");
    std::fs::write(&garbage, b"plain text, not media. ".repeat(500)).unwrap();
    match decode(&garbage, &out, None) {
        Err(MfError::Decode { message }) => assert_eq!(message, "unrecognized file format"),
        other => panic!("{other:?}"),
    }
    assert!(!out.exists());
}

#[test]
fn truncated_file_keeps_decoded_audio() {
    let dir = TempDir::new("dectrunc");
    let input = dir.path().join("t.wav");
    write_wav_file(&input, 48_000, 2, 2.0, 300.0);
    let bytes = std::fs::read(&input).unwrap();
    // Cut mid-file and mid-frame.
    std::fs::write(&input, &bytes[..bytes.len() / 2 + 3]).unwrap();
    let pcm = decode(&input, &dir.path().join("t.pcm"), None).unwrap();
    assert!((15_900..=16_100).contains(&pcm.len_samples()), "{}", pcm.len_samples());
}

#[test]
fn cancellation_removes_output() {
    let dir = TempDir::new("deccancel");
    let input = dir.path().join("long.wav");
    write_wav_file(&input, 48_000, 2, 30.0, 300.0);
    let out = dir.path().join("long.pcm");

    let cancel = CancelToken::new();
    cancel.cancel();
    assert_eq!(decode_file(&input, &out, None, &cancel, &|_| {}).unwrap_err(), MfError::Canceled);

    let cancel = CancelToken::new();
    let trigger = cancel.clone();
    let result = decode_file(&input, &out, None, &cancel, &move |f| {
        if f > 0.3 {
            trigger.cancel();
        }
    });
    assert_eq!(result.unwrap_err(), MfError::Canceled);
    assert!(!out.exists());
}

/// Fake platform decoder with scripted behavior.
struct FakeDecoder {
    calls: AtomicUsize,
    behavior: Behavior,
}

enum Behavior {
    /// Write a sine WAV with this rate and channel count.
    Write {
        rate: u32,
        channels: u16,
        secs: f32,
    },
    /// Write a WAV that is not 16-bit PCM (32-bit float).
    WriteFloat,
    Fail(PlatformError),
    /// Report progress, then wait to be canceled.
    WaitForCancel,
}

impl FakeDecoder {
    fn new(behavior: Behavior) -> Arc<Self> {
        Arc::new(Self { calls: AtomicUsize::new(0), behavior })
    }
}

impl PlatformDecoder for FakeDecoder {
    fn decode_to_wav(
        &self,
        input_path: String,
        output_path: String,
        sample_rate: u32,
        task: Arc<TaskHandle>,
    ) -> std::result::Result<(), PlatformError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(sample_rate, SAMPLE_RATE);
        assert!(Path::new(&input_path).exists());
        assert!(output_path.ends_with(".platform.wav"));
        match &self.behavior {
            Behavior::Write { rate, channels, secs } => {
                task.report_progress(0.5);
                std::thread::sleep(Duration::from_millis(250));
                write_wav_file(Path::new(&output_path), *rate, *channels, *secs, 500.0);
                Ok(())
            }
            Behavior::WriteFloat => {
                let mut bytes = encode_wav(&vec![0; 32_000], 16_000);
                // Re-label as 32-bit float with half the frames.
                bytes[20..22].copy_from_slice(&3u16.to_le_bytes());
                bytes[34..36].copy_from_slice(&32u16.to_le_bytes());
                bytes[32..34].copy_from_slice(&4u16.to_le_bytes());
                bytes[28..32].copy_from_slice(&64_000u32.to_le_bytes());
                std::fs::write(&output_path, bytes).unwrap();
                Ok(())
            }
            Behavior::Fail(e) => Err(e.clone()),
            Behavior::WaitForCancel => {
                task.report_progress(0.2);
                while !task.is_canceled() {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(PlatformError::Canceled)
            }
        }
    }
}

fn garbage_file(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("odd.xyz");
    std::fs::write(&path, b"\x00\x01\x02 not a known container \x03".repeat(100)).unwrap();
    path
}

#[test]
fn falls_back_to_the_platform_decoder() {
    let dir = TempDir::new("decfb");
    let input = garbage_file(&dir);
    let out = dir.path().join("fb.pcm");
    let fake = FakeDecoder::new(Behavior::Write { rate: 16_000, channels: 1, secs: 2.0 });
    let decoder: Arc<dyn PlatformDecoder> = fake.clone();
    let seen = Mutex::new(Vec::new());
    let pcm = decode_file(&input, &out, Some(&decoder), &CancelToken::new(), &|f| seen.lock().push(f)).unwrap();
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    assert_eq!(pcm.len_samples(), 32_000);
    assert!((zero_cross_freq(&pcm.read_ms(0, 2000).unwrap()) - 500.0).abs() < 3.0);
    // The temporary WAV is gone.
    let leftovers: Vec<_> =
        std::fs::read_dir(dir.path()).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name()).collect();
    assert_eq!(leftovers.len(), 2, "{leftovers:?}");
    // Platform progress (0.5) was relayed, scaled to its share.
    let seen = seen.into_inner();
    assert!(seen.contains(&(0.5 * PLATFORM_SHARE)), "{seen:?}");
    assert_eq!(seen.last(), Some(&1.0));
}

#[test]
fn platform_wav_in_other_formats_is_converted() {
    let dir = TempDir::new("decfb2");
    let input = garbage_file(&dir);
    let decoder: Arc<dyn PlatformDecoder> = FakeDecoder::new(Behavior::Write { rate: 44_100, channels: 2, secs: 1.5 });
    let pcm = decode(&input, &dir.path().join("a.pcm"), Some(&decoder)).unwrap();
    assert_eq!(pcm.len_samples(), 24_000);
    assert!((zero_cross_freq(&pcm.read_ms(200, 1000).unwrap()) - 500.0).abs() < 3.0);

    let decoder: Arc<dyn PlatformDecoder> = FakeDecoder::new(Behavior::WriteFloat);
    let pcm = decode(&input, &dir.path().join("b.pcm"), Some(&decoder)).unwrap();
    assert_eq!(pcm.len_samples(), 16_000);
}

#[test]
fn platform_failures_map_to_decode_errors() {
    let dir = TempDir::new("decfb3");
    let input = garbage_file(&dir);
    let out = dir.path().join("x.pcm");
    let cases = [
        (PlatformError::NotSupported, "unrecognized file format".to_string()),
        (PlatformError::Unavailable { message: "no".into() }, "unrecognized file format".to_string()),
        (PlatformError::Failed { message: "boom".into() }, "unrecognized file format (boom)".to_string()),
    ];
    for (error, expected) in cases {
        let decoder: Arc<dyn PlatformDecoder> = FakeDecoder::new(Behavior::Fail(error));
        assert_eq!(decode(&input, &out, Some(&decoder)).unwrap_err(), MfError::Decode { message: expected });
        assert!(!out.exists());
    }
    let decoder: Arc<dyn PlatformDecoder> = FakeDecoder::new(Behavior::Fail(PlatformError::Canceled));
    assert_eq!(decode(&input, &out, Some(&decoder)).unwrap_err(), MfError::Canceled);
}

#[test]
fn platform_decoder_sees_cancellation() {
    let dir = TempDir::new("decfb4");
    let input = garbage_file(&dir);
    let decoder: Arc<dyn PlatformDecoder> = FakeDecoder::new(Behavior::WaitForCancel);
    let cancel = CancelToken::new();
    let trigger = cancel.clone();
    let canceler = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        trigger.cancel();
    });
    let result = decode_file(&input, &dir.path().join("c.pcm"), Some(&decoder), &cancel, &|_| {});
    canceler.join().unwrap();
    assert_eq!(result.unwrap_err(), MfError::Canceled);
}

#[test]
fn readable_files_never_reach_the_platform_decoder() {
    let dir = TempDir::new("decfb5");
    let input = dir.path().join("ok.wav");
    write_wav_file(&input, 22_050, 1, 1.0, 440.0);
    let fake = FakeDecoder::new(Behavior::Fail(PlatformError::NotSupported));
    let decoder: Arc<dyn PlatformDecoder> = fake.clone();
    assert_eq!(decode(&input, &dir.path().join("ok.pcm"), Some(&decoder)).unwrap().len_samples(), 16_000);
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
}

fn params(rate: u32) -> AudioCodecParameters {
    let mut params = AudioCodecParameters::new();
    params.with_sample_rate(rate);
    params
}

#[test]
fn pipeline_applies_the_edit_window_before_resampling() {
    let dir = TempDir::new("pipe");
    let writer = PcmWriter::create(&dir.path().join("p.pcm")).unwrap();
    // 1024 frames of priming, then exactly one second of audio at 44.1 kHz.
    let edit = mp4::Mp4Edit { timescale: 44_100, skip: 1024, duration: Some(44_100) };
    let mut pipeline = Pipeline::new(writer, Some(edit), &params(44_100));
    let mut signal = vec![0.9f32; 1024];
    signal.extend(std::iter::repeat_n(0.25f32, 60_000));
    for chunk in signal.chunks(777) {
        pipeline.push(&mut chunk.to_vec(), 44_100).ok().unwrap();
    }
    let pcm = pipeline.finish().ok().unwrap();
    assert_eq!(pcm.len_samples(), 16_000);
    // No trace of the loud priming; away from the edges the level is steady.
    let samples = pcm.read_samples(0, 16_000).unwrap();
    assert!(samples[100..15_900].iter().all(|&s| (s - 8_192).abs() < 120), "level drifts");
}

#[test]
fn pipeline_silence_keeps_time() {
    let dir = TempDir::new("pipe2");
    let writer = PcmWriter::create(&dir.path().join("s.pcm")).unwrap();
    let mut pipeline = Pipeline::new(writer, None, &params(48_000));
    // A lost packet before any audio, then 0.5 s of audio, then another gap.
    pipeline.push_silence(0.25).ok().unwrap();
    pipeline.push(&mut sine(48_000, 440.0, 0.5, 0.5), 48_000).ok().unwrap();
    pipeline.push_silence(0.25).ok().unwrap();
    let pcm = pipeline.finish().ok().unwrap();
    assert_eq!(pcm.len_samples(), 16_000);
    let samples = pcm.read_samples(0, 16_000).unwrap();
    assert!(samples[..3_900].iter().all(|&s| s.abs() < 50));
    assert!(samples[12_100..].iter().all(|&s| s.abs() < 50));
    assert!(samples[5_000..11_000].iter().any(|&s| s > 15_000));
}

// ---- ffmpeg-generated media (needs ffmpeg/ffprobe; uses the sung dataset when present) ----

fn sung_source() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let path = PathBuf::from(home).join("Documents/projects/music-fetch-data/datasets/sung/amazing.wav");
    path.exists().then_some(path)
}

fn ffmpeg(args: &[&str]) -> bool {
    Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-y"])
        .args(args)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Duration of the first audio stream according to ffprobe.
fn ffprobe_audio_secs(path: &Path) -> Option<f64> {
    let out = Command::new("ffprobe")
        .args(["-v", "error", "-select_streams", "a:0", "-show_entries", "stream=duration", "-of", "csv=p=0"])
        .arg(path)
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// (file name, ffmpeg output options, has a reliable stream duration)
const MATRIX: &[(&str, &[&str], bool)] = &[
    ("aac_lc.m4a", &["-ar", "44100", "-ac", "2", "-c:a", "aac", "-b:a", "128k"], true),
    ("he_aac.m4a", &["-ar", "44100", "-ac", "2", "-c:a", "aac_at", "-profile:a", "4", "-b:a", "48k"], true),
    ("mp3.mp3", &["-ar", "44100", "-ac", "2", "-c:a", "libmp3lame", "-b:a", "192k"], true),
    ("flac.flac", &["-ar", "44100", "-ac", "2", "-c:a", "flac"], true),
    ("vorbis.ogg", &["-ar", "44100", "-ac", "2", "-c:a", "vorbis", "-strict", "-2"], true),
    ("opus.ogg", &["-ar", "48000", "-ac", "2", "-c:a", "libopus", "-b:a", "96k"], true),
    ("opus.webm", &["-ar", "48000", "-ac", "2", "-c:a", "libopus", "-b:a", "96k"], false),
    ("wav.wav", &["-ar", "48000", "-ac", "2", "-c:a", "pcm_s24le"], true),
    ("alac.m4a", &["-ar", "44100", "-ac", "2", "-c:a", "alac"], true),
    ("aiff.aiff", &["-ar", "44100", "-ac", "2", "-c:a", "pcm_s16be"], true),
    ("caf.caf", &["-ar", "44100", "-ac", "2", "-c:a", "pcm_s16le"], true),
    ("adpcm.wav", &["-ar", "44100", "-ac", "2", "-c:a", "adpcm_ms"], true),
    ("flac.mka", &["-ar", "44100", "-ac", "2", "-c:a", "flac"], false),
];

/// Encode `source` into every matrix format (plus video containers) in `dir`.
fn build_matrix(source: &Path, dir: &Path) -> Vec<(PathBuf, bool)> {
    let src = source.to_str().unwrap();
    let mut files = Vec::new();
    for (name, opts, reliable) in MATRIX {
        let out = dir.join(name);
        let mut args = vec!["-i", src];
        args.extend_from_slice(opts);
        args.push(out.to_str().unwrap());
        if ffmpeg(&args) {
            files.push((out, *reliable));
        } else {
            eprintln!("skipping {name}: ffmpeg could not encode it");
        }
    }
    let video = ["-f", "lavfi", "-i", "testsrc=size=160x120:rate=15", "-i", src, "-shortest"];
    for (name, codecs) in [
        ("video.mp4", &["-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac"][..]),
        ("video.mkv", &["-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac"][..]),
    ] {
        let out = dir.join(name);
        let mut args = video.to_vec();
        args.extend_from_slice(codecs);
        args.extend_from_slice(&["-ar", "48000", "-ac", "2"]);
        args.push(out.to_str().unwrap());
        if ffmpeg(&args) {
            files.push((out, name.ends_with(".mp4")));
        }
    }
    files
}

#[test]
#[ignore = "needs ffmpeg/ffprobe: cargo test -p mfcore decode -- --ignored"]
fn ffmpeg_matrix_durations_match_ffprobe() {
    assert!(have_tool("ffmpeg") && have_tool("ffprobe"), "ffmpeg and ffprobe must be on PATH");
    let dir = TempDir::new("matrix");
    let generated = dir.path().join("source.wav");
    let source = sung_source().unwrap_or_else(|| {
        assert!(ffmpeg(&["-f", "lavfi", "-i", "sine=frequency=330:duration=90", generated.to_str().unwrap()]));
        generated.clone()
    });
    let source_secs = ffprobe_audio_secs(&source).unwrap();
    let files = build_matrix(&source, dir.path());
    assert!(files.len() >= 12, "only {} files encoded", files.len());
    let mut report = Vec::new();
    for (file, reliable) in &files {
        let expected = if *reliable { ffprobe_audio_secs(file).unwrap_or(source_secs) } else { source_secs };
        let started = Instant::now();
        let pcm = decode(file, &file.with_extension("pcm"), None).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        let ours = pcm.len_samples() as f64 / SAMPLE_RATE as f64;
        let diff_ms = (ours - expected) * 1000.0;
        report.push(format!(
            "{:<12} expected {expected:>9.3} s  ours {ours:>9.3} s  diff {diff_ms:>6.1} ms  ({:.2} s)",
            file.file_name().unwrap().to_string_lossy(),
            started.elapsed().as_secs_f64()
        ));
        assert!(diff_ms.abs() <= 50.0, "{}: {ours} s vs {expected} s", file.display());
    }
    eprintln!("{}", report.join("\n"));
}

#[test]
#[ignore = "needs ffmpeg (aac_at for HE-AAC): cargo test -p mfcore decode -- --ignored"]
fn opus_and_he_aac_prefer_the_platform_decoder_then_fall_back() {
    assert!(have_tool("ffmpeg"), "ffmpeg must be on PATH");
    let dir = TempDir::new("policy");
    let sine_src = ["-f", "lavfi", "-i", "sine=frequency=440:duration=3", "-ac", "2"];
    let opus = dir.path().join("a.webm");
    let he_aac = dir.path().join("b.m4a");
    let mut inputs = Vec::new();
    if ffmpeg(&[&sine_src[..], &["-c:a", "libopus", opus.to_str().unwrap()]].concat()) {
        inputs.push(opus);
    }
    let he_args = ["-ar", "44100", "-c:a", "aac_at", "-profile:a", "4", "-b:a", "48k", he_aac.to_str().unwrap()];
    if ffmpeg(&[&sine_src[..], &he_args[..]].concat()) {
        inputs.push(he_aac);
    }
    assert!(!inputs.is_empty());
    for input in inputs {
        let expected_ms = (ffprobe_audio_secs(&input).unwrap_or(3.0) * 1000.0) as i64;
        // A platform decoder that works is used first.
        let fake = FakeDecoder::new(Behavior::Write { rate: 16_000, channels: 1, secs: 3.0 });
        let decoder: Arc<dyn PlatformDecoder> = fake.clone();
        let pcm = decode(&input, &input.with_extension("p.pcm"), Some(&decoder)).unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1, "{}", input.display());
        assert!((zero_cross_freq(&pcm.read_ms(500, 2000).unwrap()) - 500.0).abs() < 3.0);
        // One that cannot (macOS has no WebM) hands back to the built-in decoder.
        let fake = FakeDecoder::new(Behavior::Fail(PlatformError::Failed { message: "no".into() }));
        let decoder: Arc<dyn PlatformDecoder> = fake.clone();
        let pcm = decode(&input, &input.with_extension("b.pcm"), Some(&decoder)).unwrap();
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        assert!((pcm.duration_ms() as i64 - expected_ms).abs() <= 30, "{}: {}", input.display(), pcm.duration_ms());
        assert!((zero_cross_freq(&pcm.read_ms(500, 2000).unwrap()) - 440.0).abs() < 3.0);
        // Without a platform decoder the built-in one is used directly.
        let pcm = decode(&input, &input.with_extension("c.pcm"), None).unwrap();
        assert!((pcm.duration_ms() as i64 - expected_ms).abs() <= 30);
    }
}

/// `cargo test -p mfcore --release decode_one_hour -- --ignored --nocapture`
#[test]
#[ignore = "performance: needs ffmpeg, run in release"]
fn decode_one_hour_m4a() {
    assert!(have_tool("ffmpeg"), "ffmpeg must be on PATH");
    let dir = TempDir::new("hour");
    let input = dir.path().join("hour.m4a");
    let source = sung_source().map(|p| p.to_string_lossy().into_owned());
    let mut args: Vec<String> = match &source {
        // Loop the sung clip to 60 minutes, as 44.1 kHz stereo AAC-LC.
        Some(src) => vec!["-stream_loop".into(), "-1".into(), "-i".into(), src.clone()],
        None => vec!["-f".into(), "lavfi".into(), "-i".into(), "sine=frequency=330:duration=3600".into()],
    };
    args.extend(["-t", "3600", "-ar", "44100", "-ac", "2", "-c:a", "aac", "-b:a", "128k"].map(String::from));
    args.push(input.to_string_lossy().into_owned());
    assert!(ffmpeg(&args.iter().map(String::as_str).collect::<Vec<_>>()));
    let started = Instant::now();
    let pcm = decode(&input, &dir.path().join("hour.pcm"), None).unwrap();
    let elapsed = started.elapsed();
    eprintln!("decoded {:.1} min of AAC in {:.2} s", pcm.duration_ms() as f64 / 60_000.0, elapsed.as_secs_f64());
    assert!((pcm.duration_ms() as i64 - 3_600_000).abs() < 100);
    if !cfg!(debug_assertions) {
        assert!(elapsed < Duration::from_secs(15), "took {elapsed:?}");
    }
}
