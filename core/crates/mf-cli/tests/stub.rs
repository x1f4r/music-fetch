//! The engine pointed at the stub recognizer identifies the synthetic fixtures:
//! a song heard through a live session, a hummed melody through the stub's
//! melody index, and the tracks of a mix. Nothing leaves the machine (the stub
//! counts requests that would have).
//!
//! Needs python3 with numpy for `tools/fixtures/synth_songs.py`; skipped (with a
//! note) without them. `cargo test -p mf-cli --test stub`

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const MF: &str = env!("CARGO_BIN_EXE_mf");

struct Stub {
    child: Child,
    url: String,
}

impl Drop for Stub {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..").canonicalize().unwrap()
}

fn fixtures(dir: &Path) -> bool {
    let script = repo().join("tools/fixtures/synth_songs.py");
    let status =
        Command::new("python3").arg(script).arg("--out").arg(dir).arg("--quick").stdout(Stdio::null()).status();
    match status {
        Ok(s) if s.success() => true,
        _ => {
            eprintln!("skipped: tools/fixtures/synth_songs.py needs python3 with numpy");
            false
        }
    }
}

fn start_stub(refs: &Path, ready: &Path) -> Stub {
    let child = Command::new(MF)
        .args(["dev", "stub-recognizer", "--refs"])
        .arg(refs)
        .arg("--ready-file")
        .arg(ready)
        .stdout(Stdio::null())
        .spawn()
        .expect("start the stub");
    // Dropping the guard (also on a failed assertion) stops the stub.
    let mut stub = Stub { child, url: String::new() };
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(text) = std::fs::read_to_string(ready)
            && let Ok(value) = serde_json::from_str::<Value>(text.trim())
        {
            stub.url = value["url"].as_str().unwrap().to_string();
            return stub;
        }
        assert!(Instant::now() < deadline, "the stub did not start");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Run an engine command against the stub; its JSON-line events.
fn mf(stub: &Stub, data: &Path, args: &[&str]) -> Vec<Value> {
    let out = Command::new(MF)
        .args(args)
        .args(["--stub", &stub.url, "--data-dir"])
        .arg(data.join("data"))
        .arg("--cache-dir")
        .arg(data.join("cache"))
        .output()
        .expect("run mf");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let tail: Vec<&str> = stdout.lines().rev().take(3).collect();
    assert!(
        out.status.success(),
        "mf {args:?} failed ({}): {}\n{tail:?}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
}

fn outcome(events: &[Value]) -> Value {
    events.iter().find_map(|e| e["event"].get("Finished").map(|f| f["outcome"].clone())).expect("finished")
}

fn get(url: &str) -> Value {
    // A tiny HTTP/1.1 GET without another HTTP client dependency.
    use std::io::{Read, Write};
    let rest = url.strip_prefix("http://").unwrap();
    let (host, path) = rest.split_once('/').map(|(h, p)| (h, format!("/{p}"))).unwrap();
    let mut stream = std::net::TcpStream::connect(host).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap()
}

#[test]
fn the_engine_identifies_the_synthetic_fixtures_through_the_stub() {
    let dir = std::env::temp_dir().join(format!("mf-stub-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let refs = dir.join("fixtures");
    if !fixtures(&refs) {
        return;
    }
    let stub = start_stub(&refs, &dir.join("ready.json"));

    // A song, heard from 20 s in.
    let song = refs.join("songs/song-02.wav");
    let events = mf(&stub, &dir, &["listen", song.to_str().unwrap(), "--start", "20", "--duration", "20"]);
    let matched = &outcome(&events)["Matched"]["track"];
    assert_eq!(matched["title"], "Velvet Arcade", "{matched}");
    assert_eq!(matched["isrc"], "ZZMF02000002");
    assert_eq!(matched["ids"]["deezer"], "900000002", "enriched from the Deezer stub");

    // An unreleased track is not in the catalog.
    let unknown = refs.join("songs/id-01.wav");
    let events = mf(&stub, &dir, &["listen", unknown.to_str().unwrap(), "--start", "20", "--duration", "20"]);
    assert!(outcome(&events).get("NoMatch").is_some(), "{}", outcome(&events));

    // A hum, through the melody index the stub builds and serves.
    mf(&stub, &dir, &["index", "install"]);
    let hum = refs.join("hums/hum-01.wav");
    let events = mf(&stub, &dir, &["listen", hum.to_str().unwrap(), "--mode", "hum"]);
    let candidates = events
        .iter()
        .filter_map(|e| e["event"].get("Candidates").map(|c| c["candidates"].clone()))
        .next_back()
        .expect("candidates");
    assert_eq!(candidates[0]["track"]["title"], "Copper Orchard", "{candidates}");

    // A mix: its catalog tracks come back (the ID track does not).
    let mix = refs.join("mixes/synth-mix-q.wav");
    let events = mf(&stub, &dir, &["analyze", mix.to_str().unwrap(), "--mode", "mix"]);
    assert!(outcome(&events).get("Completed").is_some(), "{}", outcome(&events));
    let session = events.iter().find_map(|e| e["event"]["Started"]["session_id"].as_str()).unwrap().to_string();
    let detail = Command::new(MF)
        .args(["session", &session, "--data-dir"])
        .arg(dir.join("data"))
        .arg("--cache-dir")
        .arg(dir.join("cache"))
        .output()
        .unwrap();
    let detail: Value = serde_json::from_slice(&detail.stdout).expect("session json");
    let titles: Vec<&str> =
        detail["segments"].as_array().unwrap().iter().filter_map(|s| s["track"]["title"].as_str()).collect();
    assert!(titles.contains(&"Copper Orchard") && titles.contains(&"Paper Monsoon"), "{titles:?}");
    assert!(!titles.contains(&"Marble Meadow"), "the ID track is unknown to the catalog");

    // Every request went to the stub's services, none to its sink.
    let requests = get(&format!("{}/__requests", stub.url));
    let paths: Vec<&str> = requests.as_array().unwrap().iter().map(|r| r["path"].as_str().unwrap()).collect();
    assert!(paths.iter().any(|p| p.starts_with("/shazam/discovery/v5/")));
    assert!(paths.iter().any(|p| p.starts_with("/deezer/")));
    assert!(paths.contains(&"/melody/index.mfi"));
    assert!(!paths.contains(&"/blocked"), "a request left the stub's services: {paths:?}");
    // The Shazam rule's agent reached the recognizer.
    let shazam = requests
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["path"].as_str().unwrap().starts_with("/shazam/discovery"))
        .unwrap();
    assert!(shazam["headers"]["user-agent"].as_str().unwrap().starts_with("Dalvik/2.1.0"));

    drop(stub);
    let _ = std::fs::remove_dir_all(&dir);
}
