//! `mf shazam` and `mf sig`: Shazam-compatible recognition of a WAV clip.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, bail};
use mfcore::shazam::signature::{self, Band, Options};
use mfcore::shazam::{Priority, ShazamClient};
use mfcore::util::cancel::CancelToken;

#[derive(clap::Subcommand)]
pub enum Command {
    /// Recognize a 16 kHz mono 16-bit WAV clip and print the match as JSON.
    Shazam {
        wav: PathBuf,
        #[command(flatten)]
        window: Window,
        /// Storefront for metadata and links.
        #[arg(long, default_value = "US")]
        country: String,
        /// Language for metadata.
        #[arg(long, default_value = "en-US")]
        language: String,
    },
    /// Print the signature size and peak counts per band for a WAV clip.
    Sig {
        wav: PathBuf,
        #[command(flatten)]
        window: Window,
        /// Use the plain reference algorithm (no tail padding).
        #[arg(long)]
        reference: bool,
        /// Also print the data URI.
        #[arg(long)]
        uri: bool,
    },
}

#[derive(clap::Args)]
pub struct Window {
    /// Clip start in seconds.
    #[arg(long, default_value_t = 0.0)]
    start: f64,
    /// Clip length in seconds (at most 12).
    #[arg(long, default_value_t = 12.0)]
    len: f64,
}

pub fn run(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Shazam { wav, window, country, language } => {
            let samples = load_window(&wav, &window)?;
            let sig = signature::generate(&samples);
            let _ = rustls::crypto::ring::default_provider().install_default();
            let http = mfcore::util::http::build_client(&mfcore::core_version())?;
            let client = ShazamClient::new(http, &country, &language);
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
            let started = Instant::now();
            let result = runtime.block_on(client.recognize(&sig, Priority::Interactive, &CancelToken::new()))?;
            eprintln!(
                "{:.1} s clip, {} byte signature, answered in {} ms",
                sig.sample_ms() as f64 / 1000.0,
                sig.bytes.len(),
                started.elapsed().as_millis()
            );
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::Sig { wav, window, reference, uri } => {
            let samples = load_window(&wav, &window)?;
            let sig = signature::generate_with(&samples, &Options { pad_tail: !reference });
            let decoded = sig.decode()?;
            let data_uri = sig.data_uri();
            println!(
                "{:.2} s, {} bytes, data URI {} chars, {} peaks",
                sig.sample_ms() as f64 / 1000.0,
                sig.bytes.len(),
                data_uri.len(),
                decoded.peak_count()
            );
            for band in Band::ALL {
                println!("  {:<13} {:>4}", band.label(), decoded.bands[band.index()].len());
            }
            if uri {
                println!("{data_uri}");
            }
        }
    }
    Ok(())
}

fn load_window(path: &Path, window: &Window) -> anyhow::Result<Vec<i16>> {
    let start_ok = window.start.is_finite() && window.start >= 0.0;
    let len_ok = window.len.is_finite() && window.len > 0.0 && window.len <= 12.0;
    if !start_ok || !len_ok {
        bail!("--start must be ≥ 0 and --len in (0, 12]");
    }
    let samples = read_wav_16k_mono(path)?;
    let rate = signature::SAMPLE_RATE as f64;
    let start = ((window.start * rate) as usize).min(samples.len());
    let end = (start + (window.len * rate) as usize).min(samples.len());
    if end - start < signature::HOP {
        bail!("the window is empty (file is {:.1} s long)", samples.len() as f64 / rate);
    }
    Ok(samples[start..end].to_vec())
}

/// Samples of a 16 kHz mono 16-bit PCM WAV file.
fn read_wav_16k_mono(path: &Path) -> anyhow::Result<Vec<i16>> {
    let bytes = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        bail!("{} is not a WAV file", path.display());
    }
    let hint = "convert with: ffmpeg -i <input> -ac 1 -ar 16000 -c:a pcm_s16le <output>.wav";
    let mut pos = 12;
    let mut format_ok = false;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let len = u32::from_le_bytes([bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7]]) as usize;
        let body = &bytes[pos + 8..pos.saturating_add(8).saturating_add(len).min(bytes.len())];
        match id {
            b"fmt " if body.len() >= 16 => {
                let format = u16::from_le_bytes([body[0], body[1]]);
                let channels = u16::from_le_bytes([body[2], body[3]]);
                let rate = u32::from_le_bytes([body[4], body[5], body[6], body[7]]);
                let bits = u16::from_le_bytes([body[14], body[15]]);
                if !matches!(format, 1 | 0xFFFE) || channels != 1 || rate != 16_000 || bits != 16 {
                    bail!("need 16 kHz mono 16-bit PCM, got {rate} Hz, {channels} ch, {bits} bit; {hint}");
                }
                format_ok = true;
            }
            b"data" => {
                if !format_ok {
                    bail!("WAV data before a valid fmt chunk; {hint}");
                }
                return Ok(body.as_chunks::<2>().0.iter().map(|c| i16::from_le_bytes(*c)).collect());
            }
            _ => {}
        }
        pos = pos.saturating_add(8).saturating_add(len).saturating_add(len & 1);
    }
    bail!("no audio data in {}", path.display())
}
