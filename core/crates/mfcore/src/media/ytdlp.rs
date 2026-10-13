//! yt-dlp run as a subprocess (macOS app with the bundled runtime, and the CLI).
//!
//! Each call runs yt-dlp in its own process group with a private TMPDIR.
//! stdout and stderr are drained on their own threads (no pipe deadlock) while
//! the calling thread polls for cancellation every 100 ms. Cancellation and
//! timeouts send SIGTERM to the whole group, then SIGKILL after 3 s, so helper
//! processes (deno, ffmpeg) die with it.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant, SystemTime};

use crate::config::MediaToolsConfig;
use crate::error::PlatformError;
use crate::platform::MediaBackend;
use crate::util::cancel::TaskHandle;

/// Audio format selector: AAC in MP4 first (decodes everywhere), then any audio.
pub const AUDIO_FORMAT: &str = "bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best";
const PROGRESS_PREFIX: &str = "MFPROG";
const OUTPUT_TEMPLATE: &str = "%(id).80s.%(ext)s";
const POLL: Duration = Duration::from_millis(100);
const KILL_GRACE: Duration = Duration::from_secs(3);
/// Time to wait for output pipes after yt-dlp exited (children may hold them).
const PIPE_GRACE: Duration = Duration::from_secs(2);
const STDERR_TAIL: usize = 64;
const MAX_STDOUT_LINES: usize = 256;
/// Probe output cap; flat playlists with thousands of entries stay far below.
const MAX_PROBE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_MESSAGE_CHARS: usize = 200;

#[derive(Debug, Clone, Copy)]
struct Timeouts {
    probe: Duration,
    stall: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self { probe: Duration::from_secs(90), stall: Duration::from_secs(120) }
    }
}

/// yt-dlp subprocess implementing [`MediaBackend`].
pub struct YtDlpSubprocess {
    config: MediaToolsConfig,
    temp_root: PathBuf,
    timeouts: Timeouts,
}

impl YtDlpSubprocess {
    /// Backend for `config`. Private temp directories go under the system temp
    /// directory until [`with_temp_dir`](Self::with_temp_dir) points them elsewhere.
    pub fn new(config: MediaToolsConfig) -> Self {
        Self { config, temp_root: std::env::temp_dir().join("musicfetch-ytdlp"), timeouts: Timeouts::default() }
    }

    /// Put per-call temp directories (yt-dlp's TMPDIR and fragments) under `dir`,
    /// e.g. `<cache_dir>/tmp`.
    pub fn with_temp_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.temp_root = dir.into();
        self
    }

    #[cfg(test)]
    fn with_timeouts(mut self, probe: Duration, stall: Duration) -> Self {
        self.timeouts = Timeouts { probe, stall };
        self
    }

    fn program(&self) -> (OsString, Vec<OsString>) {
        match &self.config.python_path {
            Some(python) => (python.into(), vec![self.config.yt_dlp_path.clone().into()]),
            None => (self.config.yt_dlp_path.clone().into(), Vec::new()),
        }
    }

    /// Options shared by probe and download.
    fn common_args(&self) -> Vec<OsString> {
        let mut args: Vec<OsString> = vec!["--ignore-config".into(), "--no-warnings".into()];
        // Replace yt-dlp's default runtimes only when the config names its own.
        if !self.config.js_runtimes.is_empty() {
            args.push("--no-js-runtimes".into());
            for runtime in &self.config.js_runtimes {
                args.push("--js-runtimes".into());
                args.push(runtime.into());
            }
        }
        args.extend(["--socket-timeout", "20", "--retries", "3"].map(OsString::from));
        args
    }

    fn probe_args(&self, url: &str) -> Vec<OsString> {
        let mut args = self.common_args();
        args.extend(["-J", "--flat-playlist", "--skip-download", "--"].map(OsString::from));
        args.push(url.into());
        args
    }

    fn download_args(&self, url: &str, dest: &Path, temp: &Path) -> Vec<OsString> {
        let mut args = self.common_args();
        args.extend(
            [
                "--no-playlist",
                // A playlist URL that slipped through yields one file, not hundreds.
                "--playlist-items",
                "1",
                "--newline",
                "--progress",
                "--progress-template",
                "download:MFPROG %(progress)j",
                "-f",
                AUDIO_FORMAT,
            ]
            .map(OsString::from),
        );
        args.push("-P".into());
        args.push(dest.into());
        let mut temp_path = OsString::from("temp:");
        temp_path.push(temp);
        args.push("-P".into());
        args.push(temp_path);
        args.extend(
            [
                "-o",
                OUTPUT_TEMPLATE,
                "--print",
                "after_move:filepath",
                "--no-part",
                "--no-mtime",
                "--fixup",
                "never",
                "--",
            ]
            .map(OsString::from),
        );
        args.push(url.into());
        args
    }

    fn command(&self, args: Vec<OsString>, temp: &Path) -> Command {
        let (program, mut full) = self.program();
        full.extend(args);
        let mut cmd = Command::new(program);
        cmd.args(full).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
        cmd.env("TMPDIR", temp).env("PYTHONUNBUFFERED", "1").env("PYTHONIOENCODING", "utf-8");
        // The app's settings win (e.g. its own TMPDIR or SSL_CERT_FILE).
        cmd.envs(&self.config.env);
        cmd
    }

    /// Create a private temp directory for one call; removed on drop.
    fn private_temp(&self) -> Result<TempDir, PlatformError> {
        let dir = self.temp_root.join(crate::util::new_id());
        std::fs::create_dir_all(&dir).map_err(|e| PlatformError::Failed {
            message: format!("Cannot create a temporary folder for yt-dlp: {e}"),
        })?;
        Ok(TempDir(dir))
    }
}

impl MediaBackend for YtDlpSubprocess {
    fn probe(&self, url: String, task: Arc<TaskHandle>) -> Result<String, PlatformError> {
        let url = check_url(&url)?;
        let temp = self.private_temp()?;
        let cmd = self.command(self.probe_args(url), temp.path());
        let run = run(cmd, Mode::Probe, &task, &self.config.yt_dlp_path, self.timeouts)?;
        if !run.status.success() {
            return Err(map_failure(&run));
        }
        let json = String::from_utf8_lossy(&run.stdout).trim().to_string();
        if json.is_empty() {
            return Err(PlatformError::Failed { message: "yt-dlp returned no media information.".into() });
        }
        Ok(json)
    }

    fn download_audio(&self, url: String, dest_dir: String, task: Arc<TaskHandle>) -> Result<String, PlatformError> {
        let url = check_url(&url)?;
        let dest = PathBuf::from(&dest_dir);
        std::fs::create_dir_all(&dest)
            .map_err(|e| PlatformError::Failed { message: format!("Cannot create the download folder: {e}") })?;
        let temp = self.private_temp()?;
        let cmd = self.command(self.download_args(url, &dest, temp.path()), temp.path());
        let started = SystemTime::now();
        let run = run(cmd, Mode::Download, &task, &self.config.yt_dlp_path, self.timeouts)?;
        if !run.status.success() {
            return Err(map_failure(&run));
        }
        let path = downloaded_file(&run.stdout_lines, &dest, started).ok_or_else(|| PlatformError::Failed {
            message: "yt-dlp finished without producing an audio file.".into(),
        })?;
        task.report_progress(1.0);
        let path = path.canonicalize().unwrap_or(path);
        Ok(path.to_string_lossy().into_owned())
    }
}

/// The trimmed URL when it is an http(s) link.
fn check_url(url: &str) -> Result<&str, PlatformError> {
    let url = url.trim();
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("https://") || lower.starts_with("http://") {
        Ok(url)
    } else {
        Err(PlatformError::Unavailable { message: "Only web links can be fetched.".into() })
    }
}

/// The file yt-dlp reported with `--print after_move:filepath`, else the newest
/// finished file written to `dest` since `started`.
fn downloaded_file(stdout_lines: &[String], dest: &Path, started: SystemTime) -> Option<PathBuf> {
    let printed = stdout_lines.iter().rev().map(|l| PathBuf::from(l.trim())).find(|p| p.is_absolute() && p.is_file());
    if printed.is_some() {
        return printed;
    }
    // `--no-mtime` makes the modification time the download time.
    let since = started.checked_sub(Duration::from_secs(2)).unwrap_or(started);
    let partial = ["part", "ytdl", "temp", "tmp"];
    std::fs::read_dir(dest)
        .ok()?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            let meta = e.metadata().ok()?;
            let modified = meta.modified().ok()?;
            let ext = path.extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase();
            let finished = !partial.contains(&ext.as_str()) && !ext.starts_with("part-");
            (meta.is_file() && meta.len() > 0 && finished && modified >= since).then_some((modified, path))
        })
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path)
}

/// Removes a directory tree when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Collect stdout as one JSON document; 90 s overall limit.
    Probe,
    /// Parse stdout lines for progress and the final path; stall limit.
    Download,
}

/// What a finished yt-dlp run left behind.
struct RunOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    /// Non-progress stdout lines (the printed file path).
    stdout_lines: Vec<String>,
    stderr_tail: VecDeque<String>,
    /// Highest progress reported so far.
    progress: f32,
}

impl RunOutput {
    fn new() -> Self {
        Self {
            status: ExitStatus::default(),
            stdout: Vec::new(),
            stdout_lines: Vec::new(),
            stderr_tail: VecDeque::new(),
            progress: 0.0,
        }
    }

    /// Forward progress that moved forward (retries restart yt-dlp's counter).
    fn report(&mut self, task: &TaskHandle, fraction: Option<f32>) {
        if let Some(f) = fraction
            && f > self.progress
        {
            self.progress = f;
            task.report_progress(f);
        }
    }
}

enum Event {
    StdoutLine(String),
    StdoutAll(Vec<u8>),
    StderrLine(String),
    Closed,
}

fn spawn_error(program: &str, e: std::io::Error) -> PlatformError {
    let message = match e.kind() {
        std::io::ErrorKind::NotFound => format!("yt-dlp was not found ({program})."),
        std::io::ErrorKind::PermissionDenied => format!("yt-dlp cannot be run ({program}: permission denied)."),
        _ => format!("yt-dlp could not be started: {e}"),
    };
    PlatformError::Failed { message }
}

#[cfg(not(unix))]
fn run(_: Command, _: Mode, _: &TaskHandle, _: &str, _: Timeouts) -> Result<RunOutput, PlatformError> {
    Err(PlatformError::NotSupported)
}

#[cfg(unix)]
fn run(
    mut cmd: Command,
    mode: Mode,
    task: &TaskHandle,
    program: &str,
    timeouts: Timeouts,
) -> Result<RunOutput, PlatformError> {
    use std::os::unix::process::CommandExt;

    if task.is_canceled() {
        return Err(PlatformError::Canceled);
    }
    cmd.process_group(0);
    let mut child = cmd.spawn().map_err(|e| spawn_error(program, e))?;
    let mut group = ProcessGroup { pgid: child.id() as libc::pid_t, live: true };

    let (tx, rx) = mpsc::channel();
    let readers = start_readers(&mut child, mode, tx);
    if let Err(e) = readers {
        group.terminate(&mut child);
        return Err(PlatformError::Failed { message: format!("Cannot read yt-dlp output: {e}") });
    }

    let started = Instant::now();
    let mut last_activity = Instant::now();
    let mut exited_at: Option<Instant> = None;
    let mut status: Option<ExitStatus> = None;
    let mut open_streams = 2;
    let mut out = RunOutput::new();

    loop {
        match rx.recv_timeout(POLL) {
            Ok(event) => {
                last_activity = Instant::now();
                handle_event(event, task, &mut out, &mut open_streams);
                // Drain whatever else is queued before checking timers.
                while let Ok(event) = rx.try_recv() {
                    handle_event(event, task, &mut out, &mut open_streams);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => open_streams = 0,
        }

        if task.is_canceled() {
            group.terminate(&mut child);
            return Err(PlatformError::Canceled);
        }
        match mode {
            Mode::Probe if started.elapsed() > timeouts.probe => {
                group.terminate(&mut child);
                return Err(PlatformError::Failed { message: "Reading the media information timed out.".into() });
            }
            Mode::Download if last_activity.elapsed() > timeouts.stall => {
                group.terminate(&mut child);
                return Err(PlatformError::Failed { message: "The download stalled.".into() });
            }
            _ => {}
        }

        if status.is_none() {
            match child.try_wait() {
                Ok(Some(s)) => {
                    status = Some(s);
                    exited_at = Some(Instant::now());
                }
                Ok(None) => {}
                Err(e) => {
                    group.terminate(&mut child);
                    return Err(PlatformError::Failed { message: format!("Lost track of yt-dlp: {e}") });
                }
            }
        }
        if let Some(s) = status {
            if open_streams == 0 {
                out.status = s;
                group.live = false;
                return Ok(out);
            }
            // yt-dlp is gone but a helper still holds the pipes: stop the helpers.
            if exited_at.is_some_and(|t| t.elapsed() > PIPE_GRACE) {
                group.kill();
                group.live = false;
                out.status = s;
                drain_remaining(&rx, task, &mut out, &mut open_streams);
                return Ok(out);
            }
        }
    }
}

fn handle_event(event: Event, task: &TaskHandle, out: &mut RunOutput, open_streams: &mut usize) {
    match event {
        Event::StdoutLine(line) => {
            if line.starts_with(PROGRESS_PREFIX) {
                out.report(task, parse_progress(&line));
            } else if out.stdout_lines.len() < MAX_STDOUT_LINES {
                out.stdout_lines.push(line);
            }
        }
        Event::StdoutAll(bytes) => out.stdout = bytes,
        Event::StderrLine(line) => {
            if line.starts_with(PROGRESS_PREFIX) {
                out.report(task, parse_progress(&line));
            } else if !line.trim().is_empty() {
                if out.stderr_tail.len() == STDERR_TAIL {
                    out.stderr_tail.pop_front();
                }
                out.stderr_tail.push_back(line);
            }
        }
        Event::Closed => *open_streams = open_streams.saturating_sub(1),
    }
}

#[cfg(unix)]
fn drain_remaining(rx: &Receiver<Event>, task: &TaskHandle, out: &mut RunOutput, open_streams: &mut usize) {
    let deadline = Instant::now() + Duration::from_millis(500);
    while *open_streams > 0 && Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(event) => handle_event(event, task, out, open_streams),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn start_readers(child: &mut Child, mode: Mode, tx: Sender<Event>) -> std::io::Result<()> {
    let stdout = child.stdout.take().ok_or_else(|| std::io::Error::other("no stdout pipe"))?;
    let stderr = child.stderr.take().ok_or_else(|| std::io::Error::other("no stderr pipe"))?;
    let tx_err = tx.clone();
    std::thread::Builder::new().name("ytdlp-stdout".into()).spawn(move || {
        match mode {
            Mode::Probe => {
                let mut bytes = Vec::new();
                let _ = stdout.take(MAX_PROBE_BYTES).read_to_end(&mut bytes);
                let _ = tx.send(Event::StdoutAll(bytes));
            }
            Mode::Download => read_lines(stdout, |line| tx.send(Event::StdoutLine(line)).is_ok()),
        }
        let _ = tx.send(Event::Closed);
    })?;
    std::thread::Builder::new().name("ytdlp-stderr".into()).spawn(move || {
        read_lines(stderr, |line| tx_err.send(Event::StderrLine(line)).is_ok());
        let _ = tx_err.send(Event::Closed);
    })?;
    Ok(())
}

/// Read `\n`-separated lines (lossy UTF-8, CR stripped) until EOF or `f` returns false.
fn read_lines(input: impl Read, mut f: impl FnMut(String) -> bool) {
    let mut reader = BufReader::new(input);
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let line = String::from_utf8_lossy(&buf);
                let line = line.trim_end_matches(['\n', '\r']);
                // Progress bars without --newline separate updates with CR.
                let line = line.rsplit('\r').next().unwrap_or(line).to_string();
                if !f(line) {
                    break;
                }
            }
        }
    }
}

/// The yt-dlp child's process group.
#[cfg(unix)]
struct ProcessGroup {
    pgid: libc::pid_t,
    /// False once the group was reaped or killed; nothing left to clean up.
    live: bool,
}

#[cfg(unix)]
impl ProcessGroup {
    fn signal(&self, sig: libc::c_int) {
        if self.pgid > 0 {
            // SAFETY: plain syscall on a process group id we created.
            unsafe {
                libc::kill(-self.pgid, sig);
            }
        }
    }

    fn kill(&self) {
        self.signal(libc::SIGKILL);
    }

    /// SIGTERM the group, wait up to 3 s for yt-dlp, then SIGKILL the group.
    fn terminate(&mut self, child: &mut Child) {
        if !self.live {
            return;
        }
        self.signal(libc::SIGTERM);
        let deadline = Instant::now() + KILL_GRACE;
        loop {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) if Instant::now() >= deadline => {
                    self.kill();
                    let _ = child.wait();
                    break;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            }
        }
        // Helpers that ignored SIGTERM.
        self.kill();
        self.live = false;
    }
}

#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        // Early return or unwinding: never leave the group running.
        if self.live {
            self.kill();
        }
    }
}

/// Download progress from a `MFPROG {json}` line.
fn parse_progress(line: &str) -> Option<f32> {
    let json = line.strip_prefix(PROGRESS_PREFIX)?.trim();
    if json.is_empty() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    if value.get("status").and_then(|s| s.as_str()) == Some("finished") {
        return Some(1.0);
    }
    let number = |key: &str| value.get(key).and_then(|v| v.as_f64()).filter(|v| v.is_finite() && *v > 0.0);
    let fraction = match (number("downloaded_bytes"), number("total_bytes").or(number("total_bytes_estimate"))) {
        (Some(done), Some(total)) => done / total,
        _ => match (number("fragment_index"), number("fragment_count")) {
            (Some(index), Some(count)) => index / count,
            _ => return None,
        },
    };
    Some(fraction.clamp(0.0, 1.0) as f32)
}

/// Turn a failed run into a short, user-presentable error.
fn map_failure(run: &RunOutput) -> PlatformError {
    let errors: Vec<&str> = run
        .stderr_tail
        .iter()
        .chain(run.stdout_lines.iter())
        .map(|l| l.trim())
        .filter(|l| l.starts_with("ERROR:"))
        .collect();
    let haystack = errors.join("\n").to_lowercase();
    if let Some(error) = classify(&haystack) {
        return error;
    }
    let detail = errors
        .last()
        .copied()
        .or_else(|| run.stderr_tail.iter().rev().map(|l| l.trim()).find(|l| !l.is_empty()))
        .map(clean_error_line);
    let message = match (run.status.code(), detail) {
        (Some(2), Some(d)) => format!("yt-dlp rejected its options and may need an update: {d}"),
        (_, Some(d)) if !d.is_empty() => d,
        (Some(code), _) => format!("yt-dlp failed (exit code {code})."),
        (None, _) => "yt-dlp was stopped by a signal.".to_string(),
    };
    PlatformError::Failed { message }
}

/// Known yt-dlp error texts (lowercased) → friendly errors.
fn classify(text: &str) -> Option<PlatformError> {
    if text.is_empty() {
        return None;
    }
    let has = |needles: &[&str]| needles.iter().any(|n| text.contains(n));
    let unavailable = |m: &str| Some(PlatformError::Unavailable { message: m.into() });
    let failed = |m: &str| Some(PlatformError::Failed { message: m.into() });

    if has(&["rate-limit", "rate limit", "too many requests", "http error 429"]) {
        return failed("The site is limiting requests right now. Try again later.");
    }
    if has(&["not a bot", "confirm you’re not", "confirm you're not"]) {
        return failed("The site asked to confirm this is not a bot. Try again later.");
    }
    if has(&["private video", "video is private", "this track is private", "is private", "private playlist"]) {
        return unavailable("This media is private.");
    }
    if has(&["confirm your age", "age-restricted", "age restricted", "inappropriate for some users", "age limit"]) {
        return unavailable("This media is age-restricted and needs a signed-in account.");
    }
    if has(&[
        "not available in your country",
        "geo restrict",
        "geo-restrict",
        "in your country",
        "in your region",
        "from your location",
        "geoblock",
    ]) {
        return unavailable("This media is not available in your country.");
    }
    if has(&[
        "members-only",
        "members only",
        "join this channel",
        "login required",
        "log in",
        "login to",
        "sign in to",
        "requires authentication",
        "account is required",
        "use --cookies",
        "premium",
        "subscription",
    ]) {
        return unavailable("This media requires a signed-in account.");
    }
    if has(&["live event will begin", "premieres in", "is upcoming", "not started yet", "will begin in"]) {
        return unavailable("This live stream or premiere has not started yet.");
    }
    if has(&["drm"]) {
        return unavailable("This media is DRM-protected.");
    }
    if has(&["unsupported url", "no suitable extractor"]) {
        return unavailable("This link is not supported.");
    }
    if has(&["requested format is not available", "no video formats found", "no formats found"]) {
        return unavailable("No downloadable audio was found for this link.");
    }
    if has(&[
        "video unavailable",
        "is unavailable",
        "has been removed",
        "no longer available",
        "has been terminated",
        "does not exist",
        "http error 404",
        "not found",
        "is not available",
        "content isn't available",
        "content is not available",
        "deleted",
    ]) {
        return unavailable("This media is no longer available.");
    }
    if has(&[
        "unable to download",
        "timed out",
        "connection",
        "name resolution",
        "nodename nor servname",
        "name or service not known",
        "network is unreachable",
        "ssl",
        "certificate",
    ]) {
        return failed("Could not reach the site. Check the connection and try again.");
    }
    None
}

/// "ERROR: [youtube] abc: Something" → "Something", shortened.
fn clean_error_line(line: &str) -> String {
    let mut text = line.trim().strip_prefix("ERROR:").unwrap_or(line).trim();
    if text.starts_with('[')
        && let Some(end) = text.find("] ")
    {
        text = &text[end + 2..];
        // "id: message"
        if let Some((head, rest)) = text.split_once(": ")
            && !head.contains(' ')
        {
            text = rest;
        }
    }
    let mut out: String = text.chars().take(MAX_MESSAGE_CHARS).collect();
    if text.chars().count() > MAX_MESSAGE_CHARS {
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests;
