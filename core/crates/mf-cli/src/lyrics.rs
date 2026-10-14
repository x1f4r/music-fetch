//! `mf lyrics` and `mf eval lyrics`: lyric search from the command line.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Subcommand};
use mfcore::lyrics::eval::{self, CaseResult, EvalReport};
use mfcore::lyrics::{Backend, SearchOptions};
use mfcore::util::cancel::CancelToken;

#[derive(Subcommand)]
pub enum Command {
    /// Find songs from typed or transcribed lyrics.
    Lyrics(LyricsArgs),
    /// Measure recognition quality on built-in query sets.
    /// Dispatched through the shared `mf eval` command.
    #[command(skip)]
    Eval(EvalCommand),
}

#[derive(Subcommand)]
pub enum EvalCommand {
    /// Lyric search: top-1/top-3 of the fused search against each backend alone.
    Lyrics(EvalArgs),
    /// Links: mixes, single songs and playlists against a manifest with ground truth.
    Mixes(crate::eval_mixes::MixesArgs),
    /// Draft a mix's ground-truth tracklist from its description or comments.
    MixDraft(crate::eval_mixes::DraftArgs),
}

#[derive(Args)]
pub struct LyricsArgs {
    /// The lyrics. Several values are alternative transcriptions, best first.
    #[arg(required = true)]
    text: Vec<String>,
    /// Storefront country (ISO 3166-1 alpha-2).
    #[arg(long, default_value = "US")]
    country: String,
    /// Cache backend responses in this directory [default: $MF_LYRICS_CACHE].
    #[arg(long)]
    cache: Option<PathBuf>,
    /// Only query these backends: itunes, genius, ytmusic, deezer (repeatable).
    #[arg(long = "backend", value_parser = parse_backend)]
    backends: Vec<Backend>,
    /// Print the matches as JSON.
    #[arg(long)]
    json: bool,
    /// Also print the planned queries and failed requests.
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Args)]
pub struct EvalArgs {
    /// Storefront country (ISO 3166-1 alpha-2).
    #[arg(long, default_value = "US")]
    country: String,
    /// Cache backend responses in this directory [default: $MF_LYRICS_CACHE].
    #[arg(long)]
    cache: Option<PathBuf>,
    /// Print every fused result, not just the expected song's rank.
    #[arg(short, long)]
    verbose: bool,
}

pub fn run(command: Command) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async {
        match command {
            Command::Lyrics(args) => search(args).await,
            Command::Eval(EvalCommand::Lyrics(args)) => evaluate(args).await,
            Command::Eval(EvalCommand::Mixes(args)) => crate::eval_mixes::run(args).await,
            Command::Eval(EvalCommand::MixDraft(args)) => crate::eval_mixes::draft(args).await,
        }
    })
}

async fn search(args: LyricsArgs) -> anyhow::Result<()> {
    let http = eval::http_client().context("creating the HTTP client")?;
    let mut options = SearchOptions { cache_dir: cache_dir(args.cache), ..SearchOptions::default() };
    if !args.backends.is_empty() {
        options.backends = args.backends;
    }
    let cancel = CancelToken::new();
    let report = mfcore::lyrics::search_with(&http, &args.text, &args.country, &cancel, &options).await?;

    if args.json {
        let matches: Vec<_> = report
            .matches
            .iter()
            .map(|m| serde_json::json!({"score": m.score, "matched_line": m.matched_line, "track": m.track}))
            .collect();
        println!("{}", serde_json::to_string_pretty(&matches)?);
        return Ok(());
    }
    if args.verbose {
        for (backend, query) in &report.queries {
            println!("query    {:<8} {query}", backend.name());
        }
        for (backend, why) in &report.failures {
            println!("failed   {:<8} {why}", backend.name());
        }
        println!(
            "{} candidates, {} with lyrics checked, {} requests, {} cached",
            report.candidates, report.verified, report.requests, report.cache_hits
        );
        for r in &report.ranked {
            let backends: Vec<&str> = r.backends.iter().map(|b| b.name()).collect();
            println!(
                "  {:.2} prior {:>4.2} {:<12} cov {:.2} run {:>2} pop {:.2} [{}] {} – {}",
                r.score,
                r.prior,
                r.evidence,
                r.coverage,
                r.longest_run,
                r.popularity,
                backends.join(","),
                r.artist,
                r.title
            );
        }
        println!();
    }
    if report.matches.is_empty() {
        println!("No match.");
    }
    for (i, m) in report.matches.iter().enumerate() {
        let t = &m.track;
        let mut details = Vec::new();
        if let Some(album) = &t.album {
            details.push(album.clone());
        }
        if let Some(ms) = t.duration_ms {
            details.push(format!("{}:{:02}", ms / 60_000, ms / 1000 % 60));
        }
        let details = if details.is_empty() { String::new() } else { format!("  ({})", details.join(", ")) };
        println!("{:>2}. {:.2}  {} – {}{details}", i + 1, m.score, t.artist, t.title);
        if let Some(line) = &m.matched_line {
            println!("          “{line}”");
        }
        for link in &t.links {
            println!("          {}", link.url);
        }
    }
    Ok(())
}

async fn evaluate(args: EvalArgs) -> anyhow::Result<()> {
    let http = eval::http_client().context("creating the HTTP client")?;
    let options = SearchOptions {
        cache_dir: cache_dir(args.cache),
        // Wait out rate limits instead of skipping backends.
        max_rate_wait: Duration::from_secs(90),
        ..SearchOptions::default()
    };
    let backends = options.backends.clone();
    let header: String = backends.iter().map(|b| format!("{:>8}", b.name())).collect();
    println!(" #  {:<10} {:>5}{header} {:>6} {:>4}  query → fused top-1", "kind", "fused", "time", "req");
    let mut index = 0;
    let report = eval::run(&http, &args.country, &options, |result| {
        index += 1;
        print_case(index, result, &backends, args.verbose);
    })
    .await;
    print_summary(&report, &backends);
    Ok(())
}

fn print_case(index: usize, r: &CaseResult, backends: &[Backend], verbose: bool) {
    let rank = |rank: Option<usize>| rank.map_or("–".to_string(), |r| r.to_string());
    let baselines: String = backends
        .iter()
        .map(|b| format!("{:>8}", rank(r.baselines.iter().find(|(x, _)| x == b).and_then(|(_, r)| *r))))
        .collect();
    let top = r
        .top
        .first()
        .map(|(title, artist, score, _)| format!("{artist} – {title} ({score:.2})"))
        .or_else(|| r.error.clone())
        .unwrap_or_else(|| "no match".into());
    let query: String = r.case.hypotheses.first().copied().unwrap_or_default().chars().take(60).collect();
    println!(
        "{index:>2}  {:<10} {:>5}{baselines} {:>5.1}s {:>4}  {query} → {top}",
        format!("{:?}", r.case.kind),
        rank(r.rank),
        r.elapsed.as_secs_f32(),
        r.requests,
    );
    if verbose {
        for (i, (title, artist, score, line)) in r.top.iter().enumerate() {
            let line = line.as_deref().map(|l| format!("  “{l}”")).unwrap_or_default();
            println!("      {}. {score:.2} {artist} – {title}{line}", i + 1);
        }
        for (backend, why) in &r.failures {
            println!("      ! {} {why}", backend.name());
        }
    }
}

fn print_summary(report: &EvalReport, backends: &[Backend]) {
    for (sung, label) in [(false, "research queries"), (true, "sung clips")] {
        let fused = report.fused(sung);
        println!("\n{label} ({}): fused top-1 {}, top-3 {}", fused.cases, fused.top1, fused.top3);
        for &backend in backends {
            let alone = report.baseline(backend, sung);
            println!("  {:<8} alone: top-1 {}, top-3 {}", backend.name(), alone.top1, alone.top3);
        }
    }
    let requests: u32 = report.results.iter().map(|r| r.requests).sum();
    let cached: u32 = report.results.iter().map(|r| r.cache_hits).sum();
    println!("\nnetwork requests {requests}, cache hits {cached}");
}

fn cache_dir(flag: Option<PathBuf>) -> Option<PathBuf> {
    flag.or_else(|| std::env::var_os("MF_LYRICS_CACHE").filter(|v| !v.is_empty()).map(PathBuf::from))
}

fn parse_backend(name: &str) -> Result<Backend, String> {
    match name.to_ascii_lowercase().as_str() {
        "itunes" | "apple" => Ok(Backend::ITunes),
        "genius" => Ok(Backend::Genius),
        "ytmusic" | "youtube" | "youtubemusic" => Ok(Backend::YouTubeMusic),
        "deezer" => Ok(Backend::Deezer),
        other => Err(format!("unknown backend '{other}' (itunes, genius, ytmusic, deezer)")),
    }
}
