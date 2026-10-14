//! `mf library` and `mf export`: read the session library from the command line.

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use mfcore::store::Store;
use mfcore::{ExportFormat, LibraryEntry, LibraryQuery, SessionKind, SessionStatus};

#[derive(Subcommand)]
pub enum Command {
    /// List sessions in the library, newest first.
    Library(LibraryArgs),
    /// Export a session as a tracklist, CSV, JSON or CUE sheet.
    Export(ExportArgs),
}

#[derive(Args)]
pub struct DbArg {
    /// Library database [default: $MF_DB, else the platform data directory].
    #[arg(long, value_name = "PATH")]
    db: Option<PathBuf>,
}

#[derive(Args)]
pub struct LibraryArgs {
    #[command(flatten)]
    db: DbArg,
    /// Only sessions matching every word (titles, tracks, sources).
    #[arg(long, short)]
    search: Option<String>,
    /// Only sessions of this kind (repeatable).
    #[arg(long, value_enum)]
    kind: Vec<KindArg>,
    /// Only pinned sessions.
    #[arg(long)]
    pinned: bool,
    /// Maximum number of sessions (0 for all).
    #[arg(long, default_value_t = 50)]
    limit: u32,
    #[arg(long, default_value_t = 0)]
    offset: u32,
    /// Print JSON instead of a table.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct ExportArgs {
    /// Session id or a unique prefix of it.
    id: String,
    #[arg(long, short, value_enum, default_value_t = FormatArg::Text)]
    format: FormatArg,
    /// Write to this file instead of stdout.
    #[arg(long, short, value_name = "FILE")]
    output: Option<PathBuf>,
    #[command(flatten)]
    db: DbArg,
}

#[derive(Clone, Copy, ValueEnum)]
enum KindArg {
    Listen,
    SystemAudio,
    Hum,
    Link,
    File,
    Lyrics,
}

impl From<KindArg> for SessionKind {
    fn from(kind: KindArg) -> Self {
        match kind {
            KindArg::Listen => SessionKind::Listen,
            KindArg::SystemAudio => SessionKind::SystemAudio,
            KindArg::Hum => SessionKind::Hum,
            KindArg::Link => SessionKind::Link,
            KindArg::File => SessionKind::File,
            KindArg::Lyrics => SessionKind::Lyrics,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum FormatArg {
    Text,
    Csv,
    Json,
    Cue,
}

impl From<FormatArg> for ExportFormat {
    fn from(format: FormatArg) -> Self {
        match format {
            FormatArg::Text => ExportFormat::Text,
            FormatArg::Csv => ExportFormat::Csv,
            FormatArg::Json => ExportFormat::Json,
            FormatArg::Cue => ExportFormat::Cue,
        }
    }
}

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::Library(args) => library(args),
        Command::Export(args) => export(args),
    }
}

fn library(args: LibraryArgs) -> Result<()> {
    let store = open(args.db)?;
    let query = LibraryQuery {
        kinds: args.kind.into_iter().map(SessionKind::from).collect(),
        search: args.search,
        pinned_only: args.pinned,
        limit: args.limit,
        offset: args.offset,
    };
    let entries = store.library(&query)?;
    if args.json {
        return emit(&format!("{}\n", serde_json::to_string_pretty(&entries)?));
    }
    if entries.is_empty() {
        eprintln!("No sessions.");
        return Ok(());
    }
    let id_width = entries.iter().map(|e| e.id.chars().count()).fold(2, usize::max);
    let mut out = format!(
        "{:<id_width$}  {:<16}  {:<12}  {:<10}  {:>6}  TITLE\n",
        "ID", "CREATED (UTC)", "KIND", "STATUS", "TRACKS"
    );
    for entry in &entries {
        out.push_str(&row(entry, id_width));
        out.push('\n');
    }
    emit(&out)
}

fn row(entry: &LibraryEntry, id_width: usize) -> String {
    let mut title = truncate(&entry.title, 60);
    if let Some(track) = &entry.track {
        title.push_str(&format!(" — {} - {}", track.artist, track.title));
    }
    if entry.pinned {
        title.push_str(" [pinned]");
    }
    format!(
        "{:<id_width$}  {:<16}  {:<12}  {:<10}  {:>6}  {title}",
        entry.id,
        utc(entry.created_at_ms),
        kind_name(entry.kind),
        status_name(entry.status),
        entry.track_count
    )
}

fn export(args: ExportArgs) -> Result<()> {
    let store = open(args.db)?;
    let id = resolve_id(&store, &args.id)?;
    let text = store.export(&id, args.format.into())?;
    match args.output {
        Some(path) => std::fs::write(&path, text).with_context(|| format!("could not write {}", path.display())),
        None => emit(&text),
    }
}

/// Accepts a full id or a unique prefix, as shown by `mf library`.
fn resolve_id(store: &Store, id: &str) -> Result<String> {
    let id = id.trim();
    if id.is_empty() {
        bail!("session id is empty");
    }
    if store.session_detail(id)?.is_some() {
        return Ok(id.to_string());
    }
    let all = LibraryQuery { kinds: vec![], search: None, pinned_only: false, limit: 0, offset: 0 };
    let mut matches: Vec<String> =
        store.library(&all)?.into_iter().map(|e| e.id).filter(|e| e.starts_with(id)).collect();
    match matches.len() {
        0 => bail!("no session matches {id:?}"),
        1 => Ok(matches.remove(0)),
        n => bail!("{id:?} matches {n} sessions; use more characters"),
    }
}

fn open(db: DbArg) -> Result<Store> {
    let path = match db.db {
        Some(path) => path,
        None => default_db_path()?,
    };
    if !path.exists() {
        bail!("no library at {} (pass --db or set MF_DB)", path.display());
    }
    Ok(Store::open(&path)?)
}

/// `$MF_DB`, else `musicfetch.sqlite3` in the platform's data directory.
fn default_db_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("MF_DB") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME").map(PathBuf::from).context("HOME is not set; pass --db")?;
    let dir = if cfg!(target_os = "macos") {
        home.join("Library/Application Support/Music Fetch")
    } else {
        std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(&home).join(".local/share"))
            .join("music-fetch")
    };
    Ok(dir.join("musicfetch.sqlite3"))
}

/// Writes to stdout; a closed pipe (`mf library | head`) is not an error.
fn emit(text: &str) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    match stdout.write_all(text.as_bytes()).and_then(|()| stdout.flush()) {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        result => Ok(result?),
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn kind_name(kind: SessionKind) -> &'static str {
    match kind {
        SessionKind::Listen => "listen",
        SessionKind::SystemAudio => "system-audio",
        SessionKind::Hum => "hum",
        SessionKind::Link => "link",
        SessionKind::File => "file",
        SessionKind::Lyrics => "lyrics",
    }
}

fn status_name(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Running => "running",
        SessionStatus::Matched => "matched",
        SessionStatus::Candidates => "candidates",
        SessionStatus::Completed => "completed",
        SessionStatus::NoMatch => "no-match",
        SessionStatus::Failed => "failed",
        SessionStatus::Canceled => "canceled",
        SessionStatus::Waiting => "waiting",
    }
}

/// `YYYY-MM-DD HH:MM` in UTC.
fn utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let day_secs = secs.rem_euclid(86_400);
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}", day_secs / 3600, day_secs / 60 % 60)
}

/// Proleptic Gregorian date for days since 1970-01-01 (H. Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_dates() {
        assert_eq!(utc(0), "1970-01-01 00:00");
        assert_eq!(utc(951_782_400_000), "2000-02-29 00:00");
        assert_eq!(utc(1_791_929_100_000), "2026-10-13 22:05");
        assert_eq!(utc(-60_000), "1969-12-31 23:59");
    }

    #[test]
    fn truncates_by_chars() {
        assert_eq!(truncate("Beyoncé", 10), "Beyoncé");
        assert_eq!(truncate("Beyoncé Knowles", 8), "Beyoncé…");
    }
}
