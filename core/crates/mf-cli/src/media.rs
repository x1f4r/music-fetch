//! `mf probe`, `mf decode`, `mf download`: the media layer on the command line.
//!
//! Uses the system yt-dlp (and deno for YouTube) through the core's subprocess
//! backend. Ctrl-C cancels the running step and stops yt-dlp's process group.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use mfcore::MediaToolsConfig;
use mfcore::audio::SAMPLE_RATE;
use mfcore::audio::decode::decode_file;
use mfcore::media::{MediaInfo, YtDlpSubprocess, description_timestamps, parse_probe};
use mfcore::platform::MediaBackend;
use mfcore::util::cancel::{CancelToken, TaskHandle};

#[derive(Subcommand)]
pub enum Command {
    /// Show what yt-dlp reports for a link: title, duration, chapters, playlist entries.
    Probe {
        url: String,
        /// Print yt-dlp's raw JSON instead of a summary.
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        tools: Tools,
    },
    /// Decode an audio or video file to 16 kHz mono (.wav, or raw s16le .pcm).
    Decode {
        file: PathBuf,
        /// Output file; the extension picks the format.
        #[arg(long)]
        out: PathBuf,
    },
    /// Download the best audio stream of a link with yt-dlp.
    Download {
        url: String,
        /// Destination folder.
        #[arg(long, default_value = ".")]
        dir: PathBuf,
        #[command(flatten)]
        tools: Tools,
    },
}

#[derive(Args)]
pub struct Tools {
    /// yt-dlp executable (or zipapp when --python is set).
    #[arg(long, default_value = "yt-dlp")]
    yt_dlp: String,
    /// Python interpreter to run yt-dlp with.
    #[arg(long)]
    python: Option<String>,
    /// JavaScript runtime for yt-dlp, repeatable ("deno", "quickjs:/path/qjs").
    /// Without it yt-dlp uses its defaults (deno on PATH).
    #[arg(long = "js-runtime")]
    js_runtimes: Vec<String>,
}

impl Tools {
    fn backend(&self) -> YtDlpSubprocess {
        YtDlpSubprocess::new(MediaToolsConfig {
            python_path: self.python.clone(),
            yt_dlp_path: self.yt_dlp.clone(),
            js_runtimes: self.js_runtimes.clone(),
            env: HashMap::new(),
        })
    }
}

pub fn run(command: Command) -> Result<()> {
    let cancel = CancelToken::new();
    let on_interrupt = cancel.clone();
    // First Ctrl-C cancels cleanly; a second one exits at once.
    ctrlc::set_handler(move || {
        if on_interrupt.is_canceled() {
            std::process::exit(130);
        }
        on_interrupt.cancel();
    })
    .context("installing the Ctrl-C handler")?;
    match command {
        Command::Probe { url, json, tools } => probe(&url, json, &tools, &cancel),
        Command::Decode { file, out } => decode(&file, &out, &cancel),
        Command::Download { url, dir, tools } => download(&url, &dir, &tools, &cancel),
    }
}

fn probe(url: &str, raw: bool, tools: &Tools, cancel: &CancelToken) -> Result<()> {
    let task = TaskHandle::new(cancel.clone());
    let json = tools.backend().probe(url.to_string(), task)?;
    if raw {
        println!("{json}");
        return Ok(());
    }
    print_info(&parse_probe(&json)?);
    Ok(())
}

fn print_info(info: &MediaInfo) {
    let field = |name: &str, value: &Option<String>| {
        if let Some(v) = value {
            println!("{name:<11}{v}");
        }
    };
    field("title", &info.title);
    field("uploader", &info.uploader);
    field("track", &info.track);
    field("artist", &info.artist);
    field("album", &info.album);
    field("duration", &info.duration_ms.map(clock));
    field("site", &info.extractor);
    field("url", &info.webpage_url);
    field("thumbnail", &info.thumbnail);
    if info.is_live {
        println!("{:<11}yes", "live");
    }
    if !info.chapters.is_empty() {
        println!("chapters ({}):", info.chapters.len());
        for c in &info.chapters {
            println!("  {:>8}  {}", clock(c.start_ms), c.title);
        }
    }
    let stamps = info.description.as_deref().map(description_timestamps).unwrap_or_default();
    if !stamps.is_empty() {
        println!("description timestamps ({}):", stamps.len());
        for (ms, text) in &stamps {
            println!("  {:>8}  {text}", clock(*ms));
        }
    }
    if info.is_playlist {
        println!("entries ({}):", info.entries.len());
        for e in &info.entries {
            let duration = e.duration_ms.map(clock).unwrap_or_else(|| "-".into());
            println!("  {:>4}  {:>8}  {}  {}", e.index, duration, e.title.as_deref().unwrap_or("(untitled)"), e.url);
        }
    }
}

fn decode(file: &Path, out: &Path, cancel: &CancelToken) -> Result<()> {
    let wav = out.extension().is_some_and(|e| e.eq_ignore_ascii_case("wav"));
    let pcm_path = if wav { out.with_extension("pcm.tmp") } else { out.to_path_buf() };
    let started = Instant::now();
    let progress = |f: f32| eprint!("\rdecoding {:>5.1}%", f * 100.0);
    let result = decode_file(file, &pcm_path, None, cancel, &progress);
    eprintln!();
    let pcm = result?;
    let elapsed = started.elapsed().as_secs_f64();
    if wav {
        let written = pcm_to_wav(pcm.path(), out);
        let _ = std::fs::remove_file(&pcm_path);
        written?;
    }
    let secs = pcm.duration_ms() as f64 / 1000.0;
    eprintln!(
        "{}: {} of audio in {elapsed:.2} s ({:.0}x realtime)",
        out.display(),
        clock(pcm.duration_ms()),
        secs / elapsed.max(1e-6)
    );
    Ok(())
}

/// Wrap raw 16 kHz mono s16le samples in a WAV header.
fn pcm_to_wav(pcm: &Path, wav: &Path) -> Result<()> {
    let len = std::fs::metadata(pcm)?.len() & !1;
    if len > u32::MAX as u64 - 36 {
        bail!("audio too long for a WAV file; use .pcm");
    }
    let header = mfcore::audio::wav::encode_wav(&[], SAMPLE_RATE);
    let mut header = header[..44].to_vec();
    header[4..8].copy_from_slice(&(36 + len as u32).to_le_bytes());
    header[40..44].copy_from_slice(&(len as u32).to_le_bytes());
    let mut output = BufWriter::new(File::create(wav).with_context(|| format!("creating {}", wav.display()))?);
    output.write_all(&header)?;
    let mut input = BufReader::new(File::open(pcm)?).take(len);
    std::io::copy(&mut input, &mut output)?;
    output.flush()?;
    Ok(())
}

fn download(url: &str, dir: &Path, tools: &Tools, cancel: &CancelToken) -> Result<()> {
    let task = TaskHandle::with_progress(cancel.clone(), |f| eprint!("\rdownloading {:>5.1}%", f * 100.0));
    let started = Instant::now();
    let result = tools.backend().download_audio(url.to_string(), dir.to_string_lossy().into_owned(), Arc::clone(&task));
    eprintln!();
    let path = result?;
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    eprintln!("{:.1} MB in {:.1} s", size as f64 / 1e6, started.elapsed().as_secs_f64());
    println!("{path}");
    Ok(())
}

/// "h:mm:ss" or "m:ss".
fn clock(ms: u64) -> String {
    let s = ms / 1000;
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
    } else {
        format!("{}:{:02}", s / 60, s % 60)
    }
}
