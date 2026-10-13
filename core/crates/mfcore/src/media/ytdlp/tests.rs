use std::collections::HashMap;
use std::sync::Mutex;

use super::*;
use crate::audio::testutil::TempDir as TestDir;
use crate::util::cancel::CancelToken;

fn config(python: Option<&str>, yt_dlp: &str, runtimes: &[&str]) -> MediaToolsConfig {
    MediaToolsConfig {
        python_path: python.map(str::to_string),
        yt_dlp_path: yt_dlp.to_string(),
        js_runtimes: runtimes.iter().map(|s| s.to_string()).collect(),
        env: HashMap::new(),
    }
}

fn strings(args: &[OsString]) -> Vec<String> {
    args.iter().map(|a| a.to_string_lossy().into_owned()).collect()
}

fn task() -> (CancelToken, Arc<TaskHandle>, Arc<Mutex<Vec<f32>>>) {
    let token = CancelToken::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let handle = TaskHandle::with_progress(token.clone(), move |f| sink.lock().unwrap().push(f));
    (token, handle, seen)
}

#[test]
fn probe_args_keep_default_runtimes_without_config() {
    let backend = YtDlpSubprocess::new(config(None, "/usr/bin/yt-dlp", &[]));
    let (program, prefix) = backend.program();
    assert_eq!(program, "/usr/bin/yt-dlp");
    assert!(prefix.is_empty());
    let args = strings(&backend.probe_args("https://www.youtube.com/watch?v=x", false));
    assert_eq!(
        args,
        [
            "--ignore-config",
            "--no-warnings",
            "--socket-timeout",
            "20",
            "--retries",
            "3",
            "-J",
            "--flat-playlist",
            "--skip-download",
            "--",
            "https://www.youtube.com/watch?v=x"
        ]
    );
}

#[test]
fn args_with_python_and_runtimes() {
    let backend =
        YtDlpSubprocess::new(config(Some("/app/python3"), "/app/yt-dlp.zip", &["quickjs:/app/qjs", "deno:/opt/deno"]));
    let (program, prefix) = backend.program();
    assert_eq!(program, "/app/python3");
    assert_eq!(strings(&prefix), ["/app/yt-dlp.zip"]);
    let args = strings(&backend.probe_args("https://example.com/a", false));
    let pos = args.iter().position(|a| a == "--no-js-runtimes").unwrap();
    assert_eq!(args[pos + 1..pos + 5], ["--js-runtimes", "quickjs:/app/qjs", "--js-runtimes", "deno:/opt/deno"]);
}

#[test]
fn download_args() {
    let backend = YtDlpSubprocess::new(config(None, "yt-dlp", &["deno"]));
    let args = strings(&backend.download_args("https://x.test/v", Path::new("/d/s"), Path::new("/t/1")));
    let joined = args.join(" ");
    for expected in [
        "--no-playlist --playlist-items 1",
        "--newline --progress --progress-template download:MFPROG %(progress)j",
        "-f bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best",
        "-P /d/s -P temp:/t/1",
        "-o %(id).80s.%(ext)s",
        "--print after_move:filepath",
        "--no-part",
        "--fixup never",
        "--socket-timeout 20 --retries 3",
        "--no-js-runtimes --js-runtimes deno",
    ] {
        assert!(joined.contains(expected), "missing {expected:?} in {joined}");
    }
    assert_eq!(args[args.len() - 2..], ["--", "https://x.test/v"]);
}

#[test]
fn progress_lines() {
    let line = r#"MFPROG {"status": "downloading", "downloaded_bytes": 1024, "total_bytes": 4096, "eta": 4}"#;
    assert_eq!(parse_progress(line), Some(0.25));
    let estimate = r#"MFPROG {"status": "downloading", "downloaded_bytes": 50, "total_bytes": null, "total_bytes_estimate": 200.0}"#;
    assert_eq!(parse_progress(estimate), Some(0.25));
    let fragments =
        r#"MFPROG {"status": "downloading", "downloaded_bytes": 10, "fragment_index": 3, "fragment_count": 4}"#;
    assert_eq!(parse_progress(fragments), Some(0.75));
    assert_eq!(parse_progress(r#"MFPROG {"status": "finished", "downloaded_bytes": 9}"#), Some(1.0));
    assert_eq!(parse_progress(r#"MFPROG {"status": "downloading", "downloaded_bytes": 9}"#), None);
    assert_eq!(parse_progress(r#"MFPROG {"downloaded_bytes": 9000, "total_bytes": 10}"#), Some(1.0));
    assert_eq!(parse_progress("MFPROG"), None);
    assert_eq!(parse_progress("MFPROG {broken"), None);
    assert_eq!(parse_progress("/tmp/file.m4a"), None);
}

fn classify_line(line: &str) -> Option<PlatformError> {
    classify(&line.to_lowercase())
}

#[test]
fn classifies_real_yt_dlp_errors() {
    use PlatformError::{Failed, Unavailable};
    let cases: &[(&str, bool, &str)] = &[
        ("ERROR: [youtube] abc: Private video. Sign in if you've been granted access to this video", true, "private"),
        ("ERROR: [youtube] abc: Video unavailable. This video has been removed by the uploader", true, "no longer"),
        ("ERROR: [youtube] aaaaaaaaaaa: This video is unavailable", true, "no longer"),
        (
            "ERROR: [soundcloud] forss/x: Unable to download JSON metadata: HTTP Error 404: Not Found (caused by <HTTPError 404: Not Found>)",
            true,
            "no longer",
        ),
        (
            "ERROR: [youtube] abc: Video unavailable. The uploader has not made this video available in your country",
            true,
            "country",
        ),
        (
            "ERROR: [youtube] abc: Sign in to confirm your age. This video may be inappropriate for some users.",
            true,
            "age-restricted",
        ),
        (
            "ERROR: [youtube] abc: Sign in to confirm you’re not a bot. Use --cookies-from-browser or --cookies for the authentication.",
            false,
            "not a bot",
        ),
        ("ERROR: Unsupported URL: https://example.com/page", true, "not supported"),
        ("ERROR: [youtube] abc: This live event will begin in 3 hours.", true, "not started"),
        (
            "ERROR: [youtube] abc: Join this channel to get access to members-only content like this video, and other exclusive perks.",
            true,
            "signed-in",
        ),
        ("ERROR: [soundcloud] 123: Unable to download JSON metadata: HTTP Error 404: Not Found", true, "no longer"),
        (
            "ERROR: [generic] Unable to download webpage: <urlopen error [Errno 8] nodename nor servname provided, or not known>",
            false,
            "reach the site",
        ),
        (
            "ERROR: [youtube] abc: Requested format is not available. Use --list-formats for a list of available formats",
            true,
            "No downloadable audio",
        ),
        ("ERROR: [vimeo] 123: This video is DRM protected", true, "DRM"),
        (
            "ERROR: [youtube] abc: This content isn't available, try again later. Your account has been rate-limited by YouTube for up to an hour.",
            false,
            "limiting requests",
        ),
    ];
    for (line, is_unavailable, needle) in cases {
        let got = classify_line(line).unwrap_or_else(|| panic!("unclassified: {line}"));
        let message = match (&got, is_unavailable) {
            (Unavailable { message }, true) | (Failed { message }, false) => message,
            _ => panic!("wrong kind for {line}: {got:?}"),
        };
        assert!(message.contains(needle), "{line} → {message}");
    }
    assert!(classify_line("ERROR: [youtube] abc: Something odd happened").is_none());
}

#[cfg(unix)]
#[test]
fn unknown_errors_keep_the_last_error_line() {
    let failed = |code: i32, stderr: &[&str]| RunOutput {
        status: exit_status(code),
        stderr_tail: stderr.iter().map(|s| s.to_string()).collect(),
        ..RunOutput::new()
    };
    let run =
        failed(1, &["noise", "ERROR: [youtube] abc: First thing", "ERROR: [youtube] abc: Weird failure here", ""]);
    assert_eq!(map_failure(&run), PlatformError::Failed { message: "Weird failure here".into() });

    let run = failed(2, &["yt-dlp: error: no such option: --bogus"]);
    assert!(matches!(map_failure(&run), PlatformError::Failed { message } if message.contains("may need an update")));

    let run = failed(1, &[]);
    assert_eq!(map_failure(&run), PlatformError::Failed { message: "yt-dlp failed (exit code 1).".into() });
}

#[test]
fn error_lines_are_cleaned_and_shortened() {
    assert_eq!(clean_error_line("ERROR: [youtube] dQw4w9WgXcQ: Video unavailable"), "Video unavailable");
    assert_eq!(
        clean_error_line("ERROR: [generic] Unable to download webpage: boom"),
        "Unable to download webpage: boom"
    );
    assert_eq!(clean_error_line("ERROR: plain"), "plain");
    let long = format!("ERROR: {}", "é".repeat(500));
    let cleaned = clean_error_line(&long);
    assert_eq!(cleaned.chars().count(), MAX_MESSAGE_CHARS + 1);
}

#[cfg(unix)]
fn exit_status(code: i32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    ExitStatus::from_raw(code << 8)
}

#[test]
fn rejects_non_web_urls() {
    let backend = YtDlpSubprocess::new(config(None, "yt-dlp", &[]));
    let (_, handle, _) = task();
    for url in ["/etc/passwd", "file:///etc/passwd", "ytsearch:abc", "", "-o"] {
        assert!(matches!(backend.probe(url.into(), false, handle.clone()), Err(PlatformError::Unavailable { .. })), "{url}");
    }
}

#[test]
fn missing_binary_is_reported() {
    let backend = YtDlpSubprocess::new(config(None, "/nonexistent/yt-dlp", &[]));
    let (_, handle, _) = task();
    match backend.probe("https://example.com".into(), false, handle) {
        Err(PlatformError::Failed { message }) => assert!(message.contains("not found"), "{message}"),
        other => panic!("{other:?}"),
    }
}

/// Fake yt-dlp: a shell script run through `/bin/sh` as the "python" interpreter.
#[cfg(unix)]
mod fake {
    use super::*;

    pub struct Fake {
        pub dir: TestDir,
        pub backend: YtDlpSubprocess,
    }

    pub fn fake(body: &str) -> Fake {
        fake_with(body, |c| c, |b| b)
    }

    pub fn fake_with(
        body: &str,
        cfg: impl FnOnce(MediaToolsConfig) -> MediaToolsConfig,
        backend: impl FnOnce(YtDlpSubprocess) -> YtDlpSubprocess,
    ) -> Fake {
        let dir = TestDir::new("ytdlp");
        let script = dir.path().join("yt-dlp.sh");
        // `dest` is the first -P value, as in the real download arguments.
        let prelude = r#"
dest=""; prev=""
for a in "$@"; do
  if [ "$prev" = "-P" ] && [ -z "$dest" ]; then dest="$a"; fi
  prev="$a"
done
"#;
        std::fs::write(&script, format!("{prelude}\n{body}\n")).unwrap();
        let cfg = cfg(config(Some("/bin/sh"), script.to_str().unwrap(), &[]));
        let backend = backend(YtDlpSubprocess::new(cfg).with_temp_dir(dir.path().join("tmp")));
        Fake { dir, backend }
    }

    /// True once no process with `pid` exists (zombies are reaped by init).
    pub fn gone(pid: i32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            // SAFETY: signal 0 only checks for existence.
            let alive = unsafe { libc::kill(pid, 0) } == 0;
            if !alive {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    pub fn read_pids(path: &Path) -> Vec<i32> {
        std::fs::read_to_string(path).unwrap_or_default().split_whitespace().filter_map(|p| p.parse().ok()).collect()
    }
}

#[cfg(unix)]
use fake::{fake, fake_with, gone, read_pids};

#[cfg(unix)]
#[test]
fn probe_returns_stdout_json() {
    let f = fake(r#"printf '{"id": "abc", "title": "T"}\n'"#);
    let (_, handle, _) = task();
    let json = f.backend.probe("https://example.com/v".into(), false, handle).unwrap();
    assert_eq!(json, r#"{"id": "abc", "title": "T"}"#);
}

#[cfg(unix)]
#[test]
fn probe_failure_maps_stderr() {
    let f = fake("echo 'ERROR: [youtube] abc: Private video. Sign in if granted access' >&2; exit 1");
    let (_, handle, _) = task();
    assert_eq!(
        f.backend.probe("https://example.com/v".into(), false, handle),
        Err(PlatformError::Unavailable { message: "This media is private.".into() })
    );
}

#[cfg(unix)]
#[test]
fn download_reports_progress_and_path() {
    let f = fake(
        r#"
for n in 1 2 3; do
  echo "MFPROG {\"status\": \"downloading\", \"downloaded_bytes\": $((n * 100)), \"total_bytes\": 400}"
done
printf 'audio' > "$dest/abc.m4a"
echo 'MFPROG {"status": "finished", "downloaded_bytes": 400, "total_bytes": 400}'
echo "$dest/abc.m4a"
"#,
    );
    let dest = f.dir.path().join("out");
    let (_, handle, seen) = task();
    let path = f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle).unwrap();
    assert!(path.ends_with("abc.m4a"), "{path}");
    assert!(Path::new(&path).is_absolute());
    assert_eq!(std::fs::read(&path).unwrap(), b"audio");
    assert_eq!(*seen.lock().unwrap(), vec![0.25, 0.5, 0.75, 1.0, 1.0]);
}

#[cfg(unix)]
#[test]
fn download_without_printed_path_finds_the_file() {
    let f = fake(r#"printf 'audio' > "$dest/zzz.webm"; printf 'x' > "$dest/zzz.webm.part""#);
    let dest = f.dir.path().join("out2");
    let (_, handle, _) = task();
    let path = f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle).unwrap();
    assert!(path.ends_with("zzz.webm"), "{path}");
}

#[cfg(unix)]
#[test]
fn download_without_file_fails() {
    let f = fake("exit 0");
    let dest = f.dir.path().join("out3");
    // A file left in the folder by an earlier download is not mistaken for this one.
    std::fs::create_dir_all(&dest).unwrap();
    let old = std::fs::File::create(dest.join("earlier.m4a")).unwrap();
    old.set_len(10).unwrap();
    old.set_modified(SystemTime::now() - Duration::from_secs(3600)).unwrap();
    let (_, handle, _) = task();
    assert!(matches!(
        f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle),
        Err(PlatformError::Failed { .. })
    ));
}

#[cfg(unix)]
#[test]
fn progress_only_moves_forward() {
    let f = fake(
        r#"
for n in 50 25 75 10; do
  echo "MFPROG {\"downloaded_bytes\": $n, \"total_bytes\": 100}"
done
printf 'audio' > "$dest/p.m4a"
echo "$dest/p.m4a"
"#,
    );
    let dest = f.dir.path().join("mono");
    let (_, handle, seen) = task();
    f.backend.download_audio("  https://example.com/v  ".into(), dest.to_string_lossy().into(), handle).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![0.5, 0.75, 1.0]);
}

#[cfg(unix)]
#[test]
fn environment_and_private_temp_dir() {
    let f = fake_with(
        r#"echo "{\"tmp\": \"$TMPDIR\", \"foo\": \"$MF_TEST_FOO\"}""#,
        |mut c| {
            c.env.insert("MF_TEST_FOO".into(), "bar".into());
            c
        },
        |b| b,
    );
    let (_, handle, _) = task();
    let json = f.backend.probe("https://example.com/v".into(), false, handle).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["foo"], "bar");
    let tmp = value["tmp"].as_str().unwrap();
    assert!(tmp.starts_with(f.dir.path().join("tmp").to_str().unwrap()), "{tmp}");
    // Removed after the run.
    assert!(!Path::new(tmp).exists());
}

#[cfg(unix)]
#[test]
fn large_output_on_both_pipes_does_not_deadlock() {
    // ~4 MB on stderr while stdout carries ~4 MB of JSON.
    let f = fake(
        r#"
i=0
while [ $i -lt 2000 ]; do
  printf '%02000d\n' 0 >&2
  i=$((i + 1))
done &
printf '{"pad": "'
i=0
while [ $i -lt 2000 ]; do printf '%02000d' 0; i=$((i + 1)); done
printf '"}\n'
wait
"#,
    );
    let (_, handle, _) = task();
    let started = Instant::now();
    let json = f.backend.probe("https://example.com/v".into(), false, handle).unwrap();
    assert_eq!(json.len(), 4_000_000 + 11);
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[cfg(unix)]
#[test]
fn cancel_kills_the_process_tree() {
    let f = fake(
        r#"
sleep 30 &
echo $! > "$dest/pids"
sh -c 'sleep 30 & echo $! >> "$1"; wait' _ "$dest/pids" &
echo $! >> "$dest/pids"
echo $$ >> "$dest/pids"
echo 'MFPROG {"downloaded_bytes": 1, "total_bytes": 10}'
wait
"#,
    );
    let dest = f.dir.path().join("cancel");
    let (token, handle, _) = task();
    let pids_file = dest.join("pids");
    let canceler = {
        let pids_file = pids_file.clone();
        std::thread::spawn(move || {
            // Wait for the whole tree to exist.
            let deadline = Instant::now() + Duration::from_secs(5);
            while read_pids(&pids_file).len() < 4 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            token.cancel();
        })
    };
    let started = Instant::now();
    let result = f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle);
    canceler.join().unwrap();
    assert_eq!(result, Err(PlatformError::Canceled));
    assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
    let pids = read_pids(&pids_file);
    assert_eq!(pids.len(), 4, "{pids:?}");
    for pid in pids {
        assert!(gone(pid), "process {pid} survived cancellation");
    }
}

#[cfg(unix)]
#[test]
fn cancel_escalates_to_sigkill() {
    let f = fake(
        r#"
trap '' TERM
echo $$ > "$dest/pids"
while true; do sleep 1; done
"#,
    );
    let dest = f.dir.path().join("stubborn");
    let (token, handle, _) = task();
    let pids_file = dest.join("pids");
    let watcher = {
        let pids_file = pids_file.clone();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while read_pids(&pids_file).is_empty() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            token.cancel();
        })
    };
    let started = Instant::now();
    let result = f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle);
    watcher.join().unwrap();
    assert_eq!(result, Err(PlatformError::Canceled));
    let took = started.elapsed();
    assert!(took >= KILL_GRACE && took < KILL_GRACE + Duration::from_secs(3), "took {took:?}");
    for pid in read_pids(&pids_file) {
        assert!(gone(pid), "process {pid} survived SIGKILL");
    }
}

#[cfg(unix)]
#[test]
fn canceled_before_start() {
    let f = fake("echo should-not-run > \"$dest/ran\"");
    let (token, handle, _) = task();
    token.cancel();
    let dest = f.dir.path().join("pre");
    assert_eq!(
        f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle),
        Err(PlatformError::Canceled)
    );
    assert!(!dest.join("ran").exists());
}

#[cfg(unix)]
#[test]
fn probe_times_out() {
    let f = fake_with(
        "echo $$ > \"$TMPDIR/../pid\"; sleep 30",
        |c| c,
        |b| b.with_timeouts(Duration::from_millis(700), Duration::from_secs(120)),
    );
    let (_, handle, _) = task();
    let started = Instant::now();
    match f.backend.probe("https://example.com/v".into(), false, handle) {
        Err(PlatformError::Failed { message }) => assert!(message.contains("timed out"), "{message}"),
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(4));
    for pid in read_pids(&f.dir.path().join("tmp/pid")) {
        assert!(gone(pid));
    }
}

#[cfg(unix)]
#[test]
fn download_stall_is_detected() {
    let f = fake_with(
        r#"echo 'MFPROG {"downloaded_bytes": 1, "total_bytes": 10}'; sleep 30"#,
        |c| c,
        |b| b.with_timeouts(Duration::from_secs(90), Duration::from_millis(600)),
    );
    let dest = f.dir.path().join("stall");
    let (_, handle, seen) = task();
    match f.backend.download_audio("https://example.com/v".into(), dest.to_string_lossy().into(), handle) {
        Err(PlatformError::Failed { message }) => assert!(message.contains("stalled"), "{message}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(*seen.lock().unwrap(), vec![0.1]);
}

#[cfg(unix)]
#[test]
fn helper_holding_the_pipes_does_not_hang() {
    // A background child inherits stdout and outlives yt-dlp.
    let f = fake(r#"sleep 30 & echo $! > "$TMPDIR/../helper"; echo '{"ok": true}'; exit 0"#);
    let (_, handle, _) = task();
    let started = Instant::now();
    let json = f.backend.probe("https://example.com/v".into(), false, handle).unwrap();
    assert_eq!(json, r#"{"ok": true}"#);
    assert!(started.elapsed() < PIPE_GRACE + Duration::from_secs(2));
    for pid in read_pids(&f.dir.path().join("tmp/helper")) {
        assert!(gone(pid), "helper {pid} survived");
    }
}

/// Live checks against the system yt-dlp (+ deno for YouTube).
/// `cargo test -p mfcore ytdlp -- --ignored --test-threads=1`
mod live {
    use super::*;
    use crate::audio::decode::decode_file;
    use crate::media::parse_probe;

    fn system_backend(dir: &TestDir) -> YtDlpSubprocess {
        YtDlpSubprocess::new(config(None, "yt-dlp", &["deno"])).with_temp_dir(dir.path().join("tmp"))
    }

    fn download_and_decode(url: &str, expected_secs: f64) {
        let dir = TestDir::new("live");
        let backend = system_backend(&dir);
        let (_, handle, seen) = task();
        let json = backend.probe(url.into(), false, handle.clone()).expect("probe");
        let info = parse_probe(&json).expect("parse");
        let duration = info.duration_ms.expect("duration") as f64 / 1000.0;
        assert!((duration - expected_secs).abs() < 2.0, "probe duration {duration}");

        let dest = dir.path().join("dl");
        let path = backend.download_audio(url.into(), dest.to_string_lossy().into(), handle).expect("download");
        let progress = seen.lock().unwrap().clone();
        assert!(progress.len() >= 2 && progress.last() == Some(&1.0), "{progress:?}");
        assert!(progress.windows(2).all(|w| w[0] <= w[1]) || progress.len() > 2);
        let cancel = CancelToken::new();
        let pcm = decode_file(Path::new(&path), &dir.path().join("a.pcm"), None, &cancel, &|_| {}).expect("decode");
        let decoded = pcm.duration_ms() as f64 / 1000.0;
        assert!((decoded - expected_secs).abs() < 1.5, "decoded {decoded}s from {path}");
        // No leftovers in the private temp root.
        let leftovers = std::fs::read_dir(dir.path().join("tmp")).map(|d| d.count()).unwrap_or(0);
        assert_eq!(leftovers, 0);
    }

    #[test]
    #[ignore = "live network + system yt-dlp/deno"]
    fn youtube_probe_and_audio_download() {
        download_and_decode("https://www.youtube.com/watch?v=dQw4w9WgXcQ", 213.0);
    }

    #[test]
    #[ignore = "live network + system yt-dlp"]
    fn soundcloud_probe_and_audio_download() {
        download_and_decode("https://soundcloud.com/forss/flickermood", 213.9);
    }

    #[test]
    #[ignore = "live network + system yt-dlp"]
    fn unavailable_video_is_friendly() {
        let dir = TestDir::new("live-missing");
        let backend = system_backend(&dir);
        let (_, handle, _) = task();
        // A well-formed id that does not exist.
        let err = backend.probe("https://www.youtube.com/watch?v=aaaaaaaaaaa".into(), false, handle).unwrap_err();
        assert!(matches!(err, PlatformError::Unavailable { .. }), "{err:?}");
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "live network + system yt-dlp/deno"]
    fn cancel_kills_real_download_tree() {
        let dir = TestDir::new("live-cancel");
        let backend = system_backend(&dir);
        let (token, handle, seen) = task();
        let watcher = {
            let seen = seen.clone();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(60);
                while seen.lock().unwrap().is_empty() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(20));
                }
                token.cancel();
            })
        };
        let dest = dir.path().join("dl");
        let started = Instant::now();
        // 45-minute video: still downloading when canceled.
        let result = backend.download_audio(
            "https://www.youtube.com/watch?v=Ss7VzIEoKtk".into(),
            dest.to_string_lossy().into(),
            handle,
        );
        watcher.join().unwrap();
        assert_eq!(result, Err(PlatformError::Canceled));
        let canceled_after = started.elapsed();
        // Nothing launched with our private paths is left.
        let marker = dir.path().to_string_lossy().into_owned();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let out = Command::new("pgrep").args(["-f", &marker]).output().expect("pgrep");
            if out.stdout.is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "survivors: {}", String::from_utf8_lossy(&out.stdout));
            std::thread::sleep(Duration::from_millis(50));
        }
        eprintln!("canceled after {canceled_after:?}, progress events {}", seen.lock().unwrap().len());
    }
}
