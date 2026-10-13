//! `mf listen`, `mf analyze`, `mf session` and `mf index install|check|status|remove`:
//! drive the engine the way the apps do and print its events as JSON lines.
//!
//! The library and caches default to the app's folders; pass `--data-dir` /
//! `--cache-dir` (or set `MF_DATA_DIR` / `MF_CACHE_DIR`) to keep experiments
//! apart. Paid providers are read from `MF_AUDD_TOKEN`, `MF_ACR_HOST`,
//! `MF_ACR_KEY`, `MF_ACR_SECRET` and `MF_ACR_HUM_HOST`, `MF_ACR_HUM_KEY`,
//! `MF_ACR_HUM_SECRET`.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use mfcore::audio::decode::decode_file;
use mfcore::audio::wav::WavReader;
use mfcore::engine::{Engine, EngineOptions, LiveSession};
use mfcore::platform::{SessionObserver, SpeechToText};
use mfcore::util::cancel::{CancelToken, TaskHandle};
use mfcore::{
    AcrCredentials, AnalyzeMode, CaptureSource, Credentials, EngineConfig, LibraryQuery, ListenMode, MediaToolsConfig,
    PlatformError, SessionEvent,
};
use serde_json::json;

#[derive(Subcommand)]
pub enum Command {
    /// Feed audio files through live sessions, as the apps do with the microphone.
    Listen(ListenArgs),
    /// Analyze a link or a local file.
    Analyze(AnalyzeArgs),
    /// Print a stored session as JSON.
    Session(SessionArgs),
    /// Install, check or remove the melody index.
    #[command(subcommand)]
    Index(IndexCommand),
}

#[derive(Args, Clone)]
pub struct EngineArgs {
    /// Library folder [default: $MF_DATA_DIR, else the app's data folder].
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,
    /// Cache folder [default: $MF_CACHE_DIR, else the app's cache folder].
    #[arg(long, value_name = "DIR")]
    cache_dir: Option<PathBuf>,
    /// Storefront country (ISO 3166-1 alpha-2).
    #[arg(long, default_value = "US")]
    country: String,
    /// Language for metadata and speech (BCP-47).
    #[arg(long, default_value = "en-US")]
    language: String,
    /// Melody index manifest URL [default: $MF_MELODY_MANIFEST].
    #[arg(long, value_name = "URL")]
    manifest: Option<String>,
    /// Skip catalog enrichment (no Deezer/iTunes lookups).
    #[arg(long)]
    offline_catalog: bool,
}

#[derive(Args)]
pub struct ListenArgs {
    /// Audio files (WAV is pushed at its own rate and channel count; other
    /// formats are decoded to 16 kHz mono first). Several files run as
    /// concurrent sessions.
    #[arg(required = true)]
    files: Vec<PathBuf>,
    #[arg(long, value_enum, default_value_t = ModeArg::Auto)]
    mode: ModeArg,
    #[arg(long, value_enum, default_value_t = SourceArg::Mic)]
    source: SourceArg,
    /// Push in real time instead of as fast as the engine settles.
    #[arg(long)]
    realtime: bool,
    /// Start this many seconds into the file.
    #[arg(long, default_value_t = 0.0)]
    start: f64,
    /// Push at most this many seconds.
    #[arg(long)]
    duration: Option<f64>,
    /// Mix in white noise at this signal-to-noise ratio (dB).
    #[arg(long, value_name = "DB", allow_hyphen_values = true)]
    snr: Option<f32>,
    /// Scale the audio by this gain in dB (e.g. -40 for a very quiet room).
    #[arg(long, value_name = "DB", allow_hyphen_values = true)]
    gain: Option<f32>,
    /// Chunk length in milliseconds.
    #[arg(long, default_value_t = 20)]
    chunk_ms: u32,
    /// Cancel the session after this much pushed audio (milliseconds).
    #[arg(long, value_name = "MS")]
    cancel_after: Option<u64>,
    /// Keep pushing until the engine ends the session instead of calling
    /// `finish()` when the file ends.
    #[arg(long)]
    no_finish: bool,
    /// Speech-to-text double: return this transcript for sung audio (repeatable,
    /// best first).
    #[arg(long)]
    transcript: Vec<String>,
    /// Also print Level events.
    #[arg(long)]
    levels: bool,
    /// Also print Pitch events.
    #[arg(long)]
    pitch: bool,
    #[command(flatten)]
    engine: EngineArgs,
}

#[derive(Args)]
pub struct AnalyzeArgs {
    /// A link or a local file.
    input: String,
    #[arg(long, value_enum, default_value_t = AnalyzeModeArg::Auto)]
    mode: AnalyzeModeArg,
    /// yt-dlp executable (or zipapp when --python is set).
    #[arg(long, default_value = "yt-dlp")]
    yt_dlp: String,
    /// Python interpreter to run yt-dlp with.
    #[arg(long)]
    python: Option<String>,
    /// JavaScript runtime for yt-dlp, repeatable ("deno", "quickjs:/path/qjs").
    #[arg(long = "js-runtime")]
    js_runtimes: Vec<String>,
    #[command(flatten)]
    engine: EngineArgs,
}

#[derive(Args)]
pub struct SessionArgs {
    /// Session id or a unique prefix of it.
    id: String,
    #[command(flatten)]
    engine: EngineArgs,
}

#[derive(Subcommand)]
pub enum IndexCommand {
    /// Download, verify and install the index named by a manifest.
    Install(EngineArgs),
    /// Fetch the manifest and report whether a newer index exists.
    Check(EngineArgs),
    /// Show the installed index.
    Status(EngineArgs),
    /// Remove the installed index.
    Remove(EngineArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum ModeArg {
    Auto,
    Recording,
    Hum,
}

#[derive(Clone, Copy, ValueEnum)]
enum SourceArg {
    Mic,
    System,
}

#[derive(Clone, Copy, ValueEnum)]
enum AnalyzeModeArg {
    Auto,
    Recording,
    Impression,
    Mix,
}

pub fn run(command: Command) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match command {
        Command::Listen(args) => listen(args),
        Command::Analyze(args) => analyze(args),
        Command::Session(args) => session(args),
        Command::Index(command) => run_index(command),
    }
}

/// `$var`, else `<platform dir>/<name>`.
fn default_dir(var: &str, cache: bool) -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os(var) {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var_os("HOME").map(PathBuf::from).context("HOME is not set; pass --data-dir")?;
    Ok(match (cfg!(target_os = "macos"), cache) {
        (true, false) => home.join("Library/Application Support/Music Fetch"),
        (true, true) => home.join("Library/Caches/Music Fetch"),
        (false, false) => std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/share"))
            .join("music-fetch"),
        (false, true) => std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cache"))
            .join("music-fetch"),
    })
}

fn credentials_from_env() -> Credentials {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let acr = |prefix: &str| {
        Some(AcrCredentials {
            host: var(&format!("{prefix}_HOST"))?,
            access_key: var(&format!("{prefix}_KEY"))?,
            access_secret: var(&format!("{prefix}_SECRET"))?,
        })
    };
    Credentials { audd_token: var("MF_AUDD_TOKEN"), acr_fingerprint: acr("MF_ACR"), acr_humming: acr("MF_ACR_HUM") }
}

fn open_engine(
    args: &EngineArgs,
    media_tools: Option<MediaToolsConfig>,
    speech: Option<Arc<dyn SpeechToText>>,
) -> Result<Arc<Engine>> {
    let data_dir = match &args.data_dir {
        Some(dir) => dir.clone(),
        None => default_dir("MF_DATA_DIR", false)?,
    };
    let cache_dir = match &args.cache_dir {
        Some(dir) => dir.clone(),
        None => default_dir("MF_CACHE_DIR", true)?,
    };
    let manifest = args.manifest.clone().or_else(|| std::env::var("MF_MELODY_MANIFEST").ok()).unwrap_or_default();
    let config = EngineConfig {
        data_dir: data_dir.to_string_lossy().into_owned(),
        cache_dir: cache_dir.to_string_lossy().into_owned(),
        country: args.country.clone(),
        language: args.language.clone(),
        app_version: mfcore::core_version(),
        media_tools,
        melody_index_manifest_url: manifest,
    };
    let options = EngineOptions { offline_catalog: args.offline_catalog, ..EngineOptions::default() };
    let engine = Engine::with_options(config, None, None, speech, options)?;
    engine.set_credentials(credentials_from_env());
    Ok(engine)
}

/// Prints events as JSON lines and reports the terminal one.
struct Printer {
    label: Option<usize>,
    levels: bool,
    pitch: bool,
    audio_ms: Arc<AtomicU64>,
    started: Instant,
    done: Mutex<Option<Sender<SessionEvent>>>,
}

impl SessionObserver for Printer {
    fn on_event(&self, event: SessionEvent) {
        let terminal =
            matches!(event, SessionEvent::Finished { .. } | SessionEvent::Failed { .. } | SessionEvent::Canceled);
        let quiet = match event {
            SessionEvent::Level { .. } => !self.levels,
            SessionEvent::Pitch { .. } => !self.pitch,
            _ => false,
        };
        if !quiet {
            let mut line = json!({
                "audio_ms": self.audio_ms.load(Ordering::SeqCst),
                "wall_ms": self.started.elapsed().as_millis() as u64,
                "event": event,
            });
            if let Some(label) = self.label {
                line["file"] = json!(label);
            }
            let mut out = std::io::stdout().lock();
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        }
        if terminal && let Some(tx) = self.done.lock().ok().and_then(|mut d| d.take()) {
            let _ = tx.send(event);
        }
    }
}

/// Speech-to-text test double returning fixed hypotheses.
struct FixedTranscript(Vec<String>);

impl SpeechToText for FixedTranscript {
    fn transcribe(
        &self,
        wav_path: String,
        _language: String,
        task: Arc<TaskHandle>,
    ) -> Result<Vec<String>, PlatformError> {
        if task.is_canceled() {
            return Err(PlatformError::Canceled);
        }
        if !Path::new(&wav_path).exists() {
            return Err(PlatformError::Failed { message: "the clip is missing".into() });
        }
        Ok(self.0.clone())
    }
}

/// Audio to push: interleaved f32 at the file's own format.
struct Clip {
    samples: Vec<f32>,
    rate: u32,
    channels: u32,
}

fn load_clip(path: &Path, start_s: f64, duration_s: Option<f64>) -> Result<Clip> {
    let mut clip = match WavReader::open(path) {
        Ok(mut reader) => {
            let spec = reader.spec();
            let mut samples = Vec::new();
            let mut buf = vec![0i16; 64 * 1024];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                samples.extend(buf[..n].iter().map(|&s| f32::from(s) / 32768.0));
            }
            Clip { samples, rate: spec.sample_rate, channels: u32::from(spec.channels) }
        }
        Err(_) => {
            let temp = std::env::temp_dir().join(format!("mf-listen-{}.pcm", mfcore::util::new_id()));
            let decoded = decode_file(path, &temp, None, &CancelToken::new(), &|_| {});
            let pcm = decoded.with_context(|| format!("cannot decode {}", path.display()))?;
            let samples = pcm.read_samples(0, pcm.len_samples() as usize)?;
            let _ = std::fs::remove_file(&temp);
            Clip { samples: samples.iter().map(|&s| f32::from(s) / 32768.0).collect(), rate: 16_000, channels: 1 }
        }
    };
    let frame = clip.channels as usize;
    let first = ((start_s.max(0.0) * f64::from(clip.rate)) as usize * frame).min(clip.samples.len());
    let last = match duration_s {
        Some(d) => (first + (d.max(0.0) * f64::from(clip.rate)) as usize * frame).min(clip.samples.len()),
        None => clip.samples.len(),
    };
    clip.samples = clip.samples[first..last].to_vec();
    Ok(clip)
}

/// Deterministic white noise at `snr_db` below the clip's RMS.
fn add_noise(samples: &mut [f32], snr_db: f32) {
    let rms = (samples.iter().map(|s| s * s).sum::<f32>() / samples.len().max(1) as f32).sqrt();
    let noise_rms = rms / 10f32.powf(snr_db / 20.0);
    // Uniform noise in [-a, a] has RMS a / sqrt(3).
    let amplitude = noise_rms * 3f32.sqrt();
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for s in samples.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let uniform = (state >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0;
        *s = (*s + uniform * amplitude).clamp(-1.0, 1.0);
    }
}

fn listen(args: ListenArgs) -> Result<()> {
    let speech: Option<Arc<dyn SpeechToText>> =
        (!args.transcript.is_empty()).then(|| Arc::new(FixedTranscript(args.transcript.clone())) as _);
    let engine = open_engine(&args.engine, None, speech)?;
    let interrupted = Arc::new(AtomicBool::new(false));
    let flag = interrupted.clone();
    ctrlc::set_handler(move || {
        if flag.swap(true, Ordering::SeqCst) {
            std::process::exit(130);
        }
    })
    .context("installing the Ctrl-C handler")?;

    let mode = match args.mode {
        ModeArg::Auto => ListenMode::Auto,
        ModeArg::Recording => ListenMode::Recording,
        ModeArg::Hum => ListenMode::Hum,
    };
    let source = match args.source {
        SourceArg::Mic => CaptureSource::Microphone,
        SourceArg::System => CaptureSource::SystemAudio,
    };
    let mut clips = Vec::new();
    for path in &args.files {
        let mut clip = load_clip(path, args.start, args.duration)?;
        if let Some(gain) = args.gain {
            let factor = 10f32.powf(gain / 20.0);
            clip.samples.iter_mut().for_each(|s| *s *= factor);
        }
        if let Some(snr) = args.snr {
            add_noise(&mut clip.samples, snr);
        }
        clips.push(clip);
    }
    let labelled = clips.len() > 1;
    let args = Arc::new(args);
    let mut workers = Vec::new();
    for (i, clip) in clips.into_iter().enumerate() {
        let engine = engine.clone();
        let args = args.clone();
        let interrupted = interrupted.clone();
        workers.push(std::thread::spawn(move || {
            run_listen(&engine, &args, labelled.then_some(i), clip, mode, source, &interrupted)
        }));
    }
    let mut failed = false;
    for worker in workers {
        match worker.join() {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => failed = true,
            Ok(Err(e)) => {
                eprintln!("error: {e:#}");
                failed = true;
            }
            Err(_) => failed = true,
        }
    }
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

/// One live session fed from `clip`; true when it finished normally.
fn run_listen(
    engine: &Arc<Engine>,
    args: &ListenArgs,
    label: Option<usize>,
    clip: Clip,
    mode: ListenMode,
    source: CaptureSource,
    interrupted: &AtomicBool,
) -> Result<bool> {
    let audio_ms = Arc::new(AtomicU64::new(0));
    let (tx, rx) = mpsc::channel();
    let printer = Arc::new(Printer {
        label,
        levels: args.levels,
        pitch: args.pitch,
        audio_ms: audio_ms.clone(),
        started: Instant::now(),
        done: Mutex::new(Some(tx)),
    });
    let session = engine.start_listening(source, mode, printer)?;
    let frame = clip.channels as usize;
    let chunk_frames = (clip.rate as usize * args.chunk_ms.max(1) as usize / 1000).max(1);
    let started = Instant::now();
    let mut pushed_frames = 0usize;
    let mut terminal = None;
    for chunk in clip.samples.chunks(chunk_frames * frame) {
        if let Ok(event) = rx.try_recv() {
            terminal = Some(event);
            break;
        }
        if interrupted.load(Ordering::SeqCst) {
            session.cancel();
            break;
        }
        if !session.is_capturing() {
            break;
        }
        let bytes: Vec<u8> = chunk.iter().flat_map(|s| s.to_le_bytes()).collect();
        session.push_pcm(&bytes, clip.rate, clip.channels);
        pushed_frames += chunk.len() / frame;
        let pushed_ms = pushed_frames as u64 * 1000 / u64::from(clip.rate);
        audio_ms.store(pushed_ms, Ordering::SeqCst);
        if args.realtime {
            let due = Duration::from_millis(pushed_ms);
            if let Some(wait) = due.checked_sub(started.elapsed()) {
                std::thread::sleep(wait);
            }
        } else {
            wait_settled(&session, interrupted);
        }
        if args.cancel_after.is_some_and(|limit| pushed_ms >= limit) {
            session.cancel();
            break;
        }
    }
    if terminal.is_none() {
        if args.no_finish && session.is_capturing() {
            // Silence until the engine decides (impression auto-stop, 30 s limit).
            let silence = vec![0u8; chunk_frames * frame * 4];
            while session.is_capturing() && !interrupted.load(Ordering::SeqCst) {
                session.push_pcm(&silence, clip.rate, clip.channels);
                pushed_frames += chunk_frames;
                audio_ms.store(pushed_frames as u64 * 1000 / u64::from(clip.rate), Ordering::SeqCst);
                if args.realtime {
                    std::thread::sleep(Duration::from_millis(u64::from(args.chunk_ms)));
                } else {
                    wait_settled(&session, interrupted);
                }
                if pushed_frames as u64 * 1000 / u64::from(clip.rate) > 60_000 {
                    break;
                }
            }
        }
        session.finish();
        terminal = loop {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(event) => break Some(event),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if interrupted.load(Ordering::SeqCst) {
                        session.cancel();
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break None,
            }
        };
    }
    if session.dropped_ms() > 0 {
        eprintln!("note: {} ms of audio were dropped (analysis fell behind)", session.dropped_ms());
    }
    Ok(matches!(terminal, Some(SessionEvent::Finished { .. }) | Some(SessionEvent::Canceled)))
}

/// Wait until the engine processed everything and no recognizer call runs.
fn wait_settled(session: &LiveSession, interrupted: &AtomicBool) {
    while !session.is_settled() && !interrupted.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Run a session started by `start` and print its events until the terminal one.
fn follow(
    engine: &Arc<Engine>,
    start: impl FnOnce(&Engine, Arc<dyn SessionObserver>) -> mfcore::error::Result<Arc<mfcore::engine::AnalysisSession>>,
) -> Result<bool> {
    let (tx, rx) = mpsc::channel();
    let printer = Arc::new(Printer {
        label: None,
        levels: false,
        pitch: false,
        audio_ms: Arc::new(AtomicU64::new(0)),
        started: Instant::now(),
        done: Mutex::new(Some(tx)),
    });
    let session = start(engine, printer)?;
    let interrupted = Arc::new(AtomicBool::new(false));
    let flag = interrupted.clone();
    ctrlc::set_handler(move || {
        if flag.swap(true, Ordering::SeqCst) {
            std::process::exit(130);
        }
    })
    .context("installing the Ctrl-C handler")?;
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(event) => return Ok(matches!(event, SessionEvent::Finished { .. })),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if interrupted.load(Ordering::SeqCst) {
                    session.cancel();
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(false),
        }
    }
}

fn analyze(args: AnalyzeArgs) -> Result<()> {
    let tools = MediaToolsConfig {
        python_path: args.python.clone(),
        yt_dlp_path: args.yt_dlp.clone(),
        js_runtimes: args.js_runtimes.clone(),
        env: HashMap::new(),
    };
    let engine = open_engine(&args.engine, Some(tools), None)?;
    let mode = match args.mode {
        AnalyzeModeArg::Auto => AnalyzeMode::Auto,
        AnalyzeModeArg::Recording => AnalyzeMode::Recording,
        AnalyzeModeArg::Impression => AnalyzeMode::Impression,
        AnalyzeModeArg::Mix => AnalyzeMode::Mix,
    };
    let input = args.input.clone();
    if !follow(&engine, |engine, observer| engine.analyze(input, mode, observer))? {
        std::process::exit(1);
    }
    Ok(())
}

fn session(args: SessionArgs) -> Result<()> {
    let engine = open_engine(&args.engine, None, None)?;
    let id = args.id.trim();
    if id.is_empty() {
        bail!("session id is empty");
    }
    let detail = match engine.session(id.to_string())? {
        Some(detail) => detail,
        None => {
            let all = LibraryQuery { kinds: vec![], search: None, pinned_only: false, limit: 0, offset: 0 };
            let matches: Vec<String> =
                engine.library(all)?.into_iter().map(|e| e.id).filter(|e| e.starts_with(id)).collect();
            match matches.as_slice() {
                [only] => engine.session(only.clone())?.context("the session disappeared")?,
                [] => bail!("no session matches {id:?}"),
                _ => bail!("{id:?} matches {} sessions; use more characters", matches.len()),
            }
        }
    };
    println!("{}", serde_json::to_string_pretty(&detail)?);
    Ok(())
}

/// Run an `mf index` subcommand (also callable from other command groups).
pub fn run_index(command: IndexCommand) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match command {
        IndexCommand::Install(args) => {
            let engine = open_engine(&args, None, None)?;
            let ok = follow(&engine, |engine, observer| engine.install_melody_index(observer))?;
            println!("{}", serde_json::to_string_pretty(&engine.melody_index_status())?);
            if !ok {
                std::process::exit(1);
            }
        }
        IndexCommand::Check(args) => {
            let engine = open_engine(&args, None, None)?;
            println!("{}", serde_json::to_string_pretty(&engine.check_melody_index()?)?);
        }
        IndexCommand::Status(args) => {
            let engine = open_engine(&args, None, None)?;
            println!("{}", serde_json::to_string_pretty(&engine.melody_index_status())?);
        }
        IndexCommand::Remove(args) => {
            let engine = open_engine(&args, None, None)?;
            engine.remove_melody_index()?;
            println!("{}", serde_json::to_string_pretty(&engine.melody_index_status())?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noise_matches_the_requested_snr() {
        let mut samples: Vec<f32> = (0..16_000).map(|n| (n as f32 * 0.05).sin() * 0.5).collect();
        let clean = samples.clone();
        add_noise(&mut samples, 10.0);
        let power = |x: &[f32]| x.iter().map(|s| s * s).sum::<f32>() / x.len() as f32;
        let noise: Vec<f32> = samples.iter().zip(&clean).map(|(a, b)| a - b).collect();
        let snr = 10.0 * (power(&clean) / power(&noise)).log10();
        assert!((snr - 10.0).abs() < 0.5, "{snr}");
    }
}
