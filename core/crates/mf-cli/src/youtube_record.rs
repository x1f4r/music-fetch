//! `mf dev youtube-probe`: read YouTube links the way the browser engine does
//! (watch and playlist pages, InnerTube `next` and `browse`; no yt-dlp) and
//! optionally keep every raw answer for fixtures.
//!
//! Recording is budgeted: at most `--budget` requests in total for the folder
//! (counted across runs), at least `--gap` seconds apart, each only after
//! `--guard` (a shell command) exits 0, and never again after a bot check or
//! an HTTP 429 (a STOP file in the folder). Raw answers contain session data;
//! scrub them with tools/fixtures/scrub_youtube.py before committing anything.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Args;
use mfcore::links::youtube_api::{self, Exchange, ProbeHooks};
use mfcore::util::cancel::CancelToken;
use parking_lot::Mutex;
use serde_json::json;

#[derive(Args)]
pub struct ProbeArgs {
    /// YouTube video or playlist links.
    #[arg(required = true)]
    urls: Vec<String>,
    /// Also read the top comments of videos.
    #[arg(long)]
    comments: bool,
    /// Keep every raw answer in this folder (and enforce the budget there).
    #[arg(long, value_name = "DIR")]
    record: Option<PathBuf>,
    /// Most requests ever made for the recording folder.
    #[arg(long, default_value_t = 15)]
    budget: u32,
    /// Seconds between requests.
    #[arg(long, default_value_t = 60)]
    gap: u64,
    /// Shell command that must exit 0 before each request (e.g. a crawl guard check).
    #[arg(long, value_name = "CMD")]
    guard: Option<String>,
}

struct Recorder {
    dir: Option<PathBuf>,
    budget: u32,
    gap: Duration,
    guard: Option<String>,
    last: Mutex<Option<std::time::Instant>>,
}

impl Recorder {
    fn count(&self) -> u32 {
        let Some(dir) = &self.dir else { return 0 };
        std::fs::read_to_string(dir.join("requests.count")).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0)
    }

    fn stop(&self, why: &str) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::write(dir.join("STOP"), format!("{why}\n"));
        }
        eprintln!("youtube-probe: stopping for good: {why}");
    }
}

fn refused(message: String) -> mfcore::MfError {
    eprintln!("youtube-probe: {message}");
    mfcore::MfError::RateLimited { retry_in_ms: 24 * 3600 * 1000 }
}

impl ProbeHooks for Recorder {
    fn before(&self, kind: &str, url: &str) -> mfcore::error::Result<()> {
        if let Some(dir) = &self.dir {
            if let Ok(why) = std::fs::read_to_string(dir.join("STOP")) {
                return Err(refused(format!("STOP file present ({})", why.trim())));
            }
            let count = self.count();
            if count >= self.budget {
                return Err(refused(format!("request budget of {} spent", self.budget)));
            }
        }
        if let Some(last) = *self.last.lock() {
            let wait = self.gap.saturating_sub(last.elapsed());
            if !wait.is_zero() {
                eprintln!("youtube-probe: waiting {} s", wait.as_secs());
                std::thread::sleep(wait);
            }
        }
        if let Some(cmd) = &self.guard {
            let ok = std::process::Command::new("sh").arg("-c").arg(cmd).status().is_ok_and(|s| s.success());
            if !ok {
                return Err(refused("the guard says YouTube is paused".into()));
            }
        }
        if let Some(dir) = &self.dir {
            let count = self.count() + 1;
            let _ = std::fs::write(dir.join("requests.count"), format!("{count}\n"));
        }
        *self.last.lock() = Some(std::time::Instant::now());
        eprintln!("youtube-probe: {kind} {}", url.split('?').next().unwrap_or(url));
        Ok(())
    }

    fn after(&self, exchange: &Exchange<'_>) {
        let blocked = exchange.status == 429 || youtube_api::is_bot_check(exchange.body);
        if let Some(dir) = &self.dir {
            let n = self.count();
            let ext = if matches!(exchange.kind, "watch" | "playlist") { "html" } else { "json" };
            let _ = std::fs::write(dir.join(format!("{n:02}-{}.{ext}", exchange.kind)), exchange.body);
            if let Some(request) = exchange.request {
                let _ = std::fs::write(dir.join(format!("{n:02}-{}.request.json", exchange.kind)), request.to_string());
            }
            let at = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
            let line = json!({"n": n, "kind": exchange.kind, "url": exchange.url, "status": exchange.status,
                "bytes": exchange.body.len(), "at": at, "blocked": blocked});
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(dir.join("log.jsonl")) {
                let _ = writeln!(f, "{line}");
            }
        }
        if blocked {
            self.stop(&format!("{} answered {} with a block signal", exchange.kind, exchange.status));
        }
    }
}

pub fn run(args: ProbeArgs) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Some(dir) = &args.record {
        std::fs::create_dir_all(dir).with_context(|| format!("cannot create {}", dir.display()))?;
    }
    let recorder = Recorder {
        dir: args.record.clone(),
        budget: args.budget,
        gap: Duration::from_secs(args.gap),
        guard: args.guard.clone(),
        last: Mutex::new(None),
    };
    let http = mfcore::util::http::build_client(&mfcore::core_version())?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let cancel = CancelToken::new();
    for url in &args.urls {
        let result = runtime.block_on(youtube_api::probe_with(&http, url, args.comments, &cancel, &recorder));
        let summary = match result {
            Ok(info) => json!({
                "url": url,
                "id": info.id, "title": info.title, "uploader": info.uploader, "duration_ms": info.duration_ms,
                "is_playlist": info.is_playlist, "is_live": info.is_live,
                "track": info.track, "artist": info.artist, "album": info.album,
                "description_chars": info.description.as_ref().map(|d| d.chars().count()),
                "chapters": info.chapters.iter().map(|c| json!([c.start_ms, c.title])).collect::<Vec<_>>(),
                "comments": info.comments.iter().map(|c| json!({"pinned": c.is_pinned, "uploader": c.author_is_uploader,
                    "likes": c.like_count, "chars": c.text.chars().count(),
                    "first_line": c.text.lines().next().unwrap_or_default()})).collect::<Vec<_>>(),
                "entries": info.entries.len(),
                "first_entries": info.entries.iter().take(3).map(|e| json!([e.id, e.title, e.duration_ms])).collect::<Vec<_>>(),
            }),
            Err(e) => json!({"url": url, "error": e.to_string()}),
        };
        println!("{summary}");
        if summary.get("error").is_some() && recorder.dir.as_ref().is_some_and(|d| d.join("STOP").exists()) {
            break;
        }
    }
    Ok(())
}
