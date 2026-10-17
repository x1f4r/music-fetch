//! End-to-end tests of the engine through its public API, with a local HTTP
//! server standing in for the recognizer and the melody index host. No network.
//!
//! `cargo test -p mfcore --test engine`

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use mfcore::engine::{Engine, EngineOptions, LiveSession};
use mfcore::model::*;
use mfcore::pipeline::recognize::CallBudget;
use mfcore::platform::SessionObserver;
use mfcore::shazam::QueuePolicy;
use mfcore::store::SessionUpdate;
use mfcore::{EngineConfig, MfError};
use sha2::{Digest, Sha256};

const MATCH_JSON: &str = include_str!("../src/shazam/testdata/match_get_lucky.json");
const NO_MATCH_JSON: &str = r#"{"matches": [], "timestamp": 1, "timezone": "Europe/Paris", "tagid": "x"}"#;
const WAIT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------- server

#[derive(Clone)]
enum Body {
    Bytes(Vec<u8>),
    /// Sent in pieces with a pause before each (slow downloads).
    Slow {
        data: Vec<u8>,
        piece: usize,
        pause: Duration,
    },
}

#[derive(Clone)]
struct Route {
    prefix: String,
    status: u16,
    content_type: &'static str,
    body: Body,
}

/// Minimal HTTP/1.1 server: routes by path prefix, counts requests per route.
struct Server {
    url: String,
    hits: Arc<Mutex<HashMap<String, usize>>>,
}

impl Server {
    fn start(routes: Vec<Route>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(Mutex::new(HashMap::new()));
        let counter = hits.clone();
        let routes = Arc::new(routes);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (routes, counter) = (routes.clone(), counter.clone());
                std::thread::spawn(move || serve(stream, &routes, &counter));
            }
        });
        Self { url, hits }
    }

    fn hits(&self, prefix: &str) -> usize {
        self.hits.lock().unwrap().get(prefix).copied().unwrap_or(0)
    }
}

fn serve(stream: TcpStream, routes: &[Route], hits: &Mutex<HashMap<String, usize>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    let path = request_line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; length];
    let _ = reader.read_exact(&mut body);
    let mut stream = stream;
    let Some(route) = routes.iter().find(|r| path.starts_with(&r.prefix)) else {
        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
    };
    *hits.lock().unwrap().entry(route.prefix.clone()).or_default() += 1;
    let data = match &route.body {
        Body::Bytes(b) | Body::Slow { data: b, .. } => b,
    };
    let head = format!(
        "HTTP/1.1 {} X\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        route.status,
        route.content_type,
        data.len()
    );
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    match &route.body {
        Body::Bytes(b) => {
            let _ = stream.write_all(b);
        }
        Body::Slow { data, piece, pause } => {
            for chunk in data.chunks(*piece) {
                std::thread::sleep(*pause);
                if stream.write_all(chunk).and_then(|_| stream.flush()).is_err() {
                    return;
                }
            }
        }
    }
}

fn json_route(prefix: &str, body: &str) -> Route {
    Route { prefix: prefix.into(), status: 200, content_type: "application/json", body: Body::Bytes(body.into()) }
}

// ---------------------------------------------------------------- engine

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    engine: Arc<Engine>,
    dir: TempDir,
}

fn engine_with(server: &Server, manifest_path: &str) -> Fixture {
    engine_with_wait(server, manifest_path, Duration::from_secs(5))
}

/// `max_wait`: the longest breaker cool-down a query waits out.
fn engine_with_wait(server: &Server, manifest_path: &str, max_wait: Duration) -> Fixture {
    let dir = std::env::temp_dir().join(format!("mf-engine-test-{}", mfcore::util::new_id()));
    let config = EngineConfig {
        data_dir: dir.join("data").to_string_lossy().into_owned(),
        cache_dir: dir.join("cache").to_string_lossy().into_owned(),
        country: "US".into(),
        language: "en-US".into(),
        app_version: "1.0.0-test".into(),
        media_tools: None,
        melody_index_manifest_url: format!("{}{manifest_path}", server.url),
    };
    let policy = QueuePolicy {
        interactive_spacing: Duration::ZERO,
        batch_spacing: Duration::ZERO,
        jitter: Duration::ZERO,
        interactive_max_wait: max_wait,
        batch_max_wait: max_wait,
        breaker_base: Duration::from_millis(200),
        breaker_max: Duration::from_secs(1),
        retry_delays: Vec::new(),
        request_timeout: Duration::from_secs(5),
    };
    let options = EngineOptions {
        shazam_base_url: Some(server.url.clone()),
        shazam_policy: Some(policy),
        call_budget: Some(CallBudget::new(1000.0, Duration::from_millis(1))),
        offline_catalog: true,
    };
    let engine = Engine::with_options(config, None, None, None, options).expect("engine");
    Fixture { engine, dir: TempDir(dir) }
}

fn shazam_server(body: &str) -> Server {
    Server::start(vec![json_route("/discovery/", body)])
}

/// Records events and wakes waiters on the terminal one.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<SessionEvent>>,
    done: Condvar,
}

impl SessionObserver for Recorder {
    fn on_event(&self, event: SessionEvent) {
        self.events.lock().unwrap().push(event);
        self.done.notify_all();
    }
}

fn is_terminal(e: &SessionEvent) -> bool {
    matches!(e, SessionEvent::Finished { .. } | SessionEvent::Failed { .. } | SessionEvent::Canceled)
}

impl Recorder {
    fn wait_terminal(&self) -> Vec<SessionEvent> {
        let deadline = Instant::now() + WAIT;
        let mut events = self.events.lock().unwrap();
        while !events.iter().any(is_terminal) {
            let left = deadline.checked_duration_since(Instant::now()).expect("no terminal event in time");
            events = self.done.wait_timeout(events, left).unwrap().0;
        }
        drop(events);
        // Anything sent after the terminal event would show up now.
        std::thread::sleep(Duration::from_millis(50));
        self.events.lock().unwrap().clone()
    }

    fn wait_for(&self, pred: impl Fn(&SessionEvent) -> bool) {
        let deadline = Instant::now() + WAIT;
        let mut events = self.events.lock().unwrap();
        while !events.iter().any(&pred) {
            let left = deadline.checked_duration_since(Instant::now()).expect("event did not arrive in time");
            events = self.done.wait_timeout(events, left).unwrap().0;
        }
    }
}

/// Checks the event contract and returns the terminal event.
fn check_contract(events: &[SessionEvent]) -> SessionEvent {
    assert!(matches!(events.first(), Some(SessionEvent::Started { .. })), "Started first: {events:?}");
    assert_eq!(events.iter().filter(|e| matches!(e, SessionEvent::Started { .. })).count(), 1);
    assert_eq!(events.iter().filter(|e| is_terminal(e)).count(), 1, "one terminal event: {events:?}");
    let last = events.last().unwrap().clone();
    assert!(is_terminal(&last), "terminal event last: {events:?}");
    last
}

fn notices(events: &[SessionEvent]) -> Vec<Notice> {
    events
        .iter()
        .filter_map(|e| if let SessionEvent::Notice { notice } = e { Some(notice.clone()) } else { None })
        .collect()
}

// ---------------------------------------------------------------- audio

/// Synthetic "music": a chord progression with harmonics and a little noise,
/// interleaved stereo at 44.1 kHz.
fn music(seconds: f32, amplitude: f32) -> Vec<f32> {
    let rate = 44_100.0;
    let n = (seconds * rate) as usize;
    let mut noise: u32 = 0x1234_5678;
    let mut out = Vec::with_capacity(n * 2);
    for i in 0..n {
        let t = i as f32 / rate;
        let chord = [[220.0, 277.2, 329.6], [196.0, 246.9, 293.7], [174.6, 220.0, 261.6]][(t * 2.0) as usize % 3];
        let mut s = 0.0;
        for (k, f) in chord.iter().enumerate() {
            s += (2.0 * std::f32::consts::PI * f * t).sin() / (k as f32 + 1.0);
            s += 0.3 * (2.0 * std::f32::consts::PI * 2.0 * f * t).sin();
        }
        noise ^= noise << 13;
        noise ^= noise >> 17;
        noise ^= noise << 5;
        s += (noise as f32 / u32::MAX as f32 - 0.5) * 0.2;
        let s = s * amplitude / 2.5;
        out.push(s);
        out.push(s * 0.9);
    }
    out
}

/// A hummed melody: one voice with harmonics, mono at 16 kHz.
fn hum(seconds: f32) -> Vec<f32> {
    let rate = 16_000.0;
    let notes = [220.0, 246.9, 261.6, 293.7, 329.6, 293.7, 261.6, 246.9];
    (0..(seconds * rate) as usize)
        .map(|i| {
            let t = i as f32 / rate;
            let f = notes[(t * 2.5) as usize % notes.len()];
            let phase = 2.0 * std::f32::consts::PI * f * t;
            0.25 * (phase.sin() + 0.4 * (2.0 * phase).sin() + 0.2 * (3.0 * phase).sin())
        })
        .collect()
}

fn bytes(samples: &[f32]) -> Vec<u8> {
    samples.iter().flat_map(|s| s.to_le_bytes()).collect()
}

/// Push `samples` in 20 ms chunks as fast as the engine settles.
fn feed(session: &LiveSession, samples: &[f32], rate: u32, channels: u32) {
    let chunk = (rate as usize / 50) * channels as usize;
    for piece in samples.chunks(chunk) {
        if !session.is_capturing() {
            return;
        }
        session.push_pcm(&bytes(piece), rate, channels);
        let started = Instant::now();
        while !session.is_settled() && started.elapsed() < WAIT {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

// ---------------------------------------------------------------- live sessions

#[test]
fn live_session_matches_on_the_first_query() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session =
        fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Recording, recorder.clone()).unwrap();
    feed(&session, &music(12.0, 0.5), 44_100, 2);
    session.finish();
    let events = recorder.wait_terminal();
    let SessionEvent::Finished { outcome: Outcome::Matched { track } } = check_contract(&events) else {
        panic!("expected a match: {events:?}")
    };
    assert_eq!(track.title, "Get Lucky");
    assert!(!track.identity_key.is_empty());
    assert_eq!(server.hits("/discovery/"), 1, "the first query at 4 s matched");
    let segment = events
        .iter()
        .find_map(|e| if let SessionEvent::Segment { segment } = e { Some(segment.clone()) } else { None })
        .expect("a segment with the evidence");
    assert_eq!(segment.evidence[0].source, EvidenceSource::Shazam);
    assert!((4_000..4_100).contains(&segment.evidence[0].at_ms), "{}", segment.evidence[0].at_ms);
    assert!(events.iter().any(|e| matches!(e, SessionEvent::Level { .. })));
    let detail = fx.engine.session(session.id()).unwrap().unwrap();
    assert_eq!(detail.entry.status, SessionStatus::Matched);
    assert_eq!(detail.entry.title, "Get Lucky");
    assert_eq!(detail.segments.len(), 1);
    assert!(!session.is_capturing());
}

#[test]
fn live_session_without_a_match_queries_on_schedule() {
    let server = shazam_server(NO_MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.start_listening(CaptureSource::SystemAudio, ListenMode::Hum, recorder.clone()).unwrap();
    feed(&session, &music(31.0, 0.5), 44_100, 2);
    let events = recorder.wait_terminal();
    assert_eq!(events[0], SessionEvent::Started { session_id: session.id(), kind: SessionKind::SystemAudio });
    assert_eq!(
        check_contract(&events),
        SessionEvent::Finished { outcome: Outcome::NoMatch { reason: NoMatchReason::NotFound } }
    );
    // System audio always uses recording rules: queries at 4, 8, 13, 20 and 28 s.
    assert_eq!(server.hits("/discovery/"), 5);
    let detail = fx.engine.session(session.id()).unwrap().unwrap();
    assert_eq!(detail.entry.status, SessionStatus::NoMatch);
}

#[test]
fn format_changes_mid_session_keep_the_timeline() {
    let server = shazam_server(NO_MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session =
        fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Recording, recorder.clone()).unwrap();
    feed(&session, &music(3.0, 0.5), 44_100, 2);
    let mono: Vec<f32> = music(6.0, 0.5).chunks(2).map(|f| f[0]).collect();
    feed(&session, &mono[..44_100 * 3], 44_100, 1);
    let at_48k: Vec<f32> = (0..48_000 * 3).map(|i| mono[i * 44_100 / 48_000]).collect();
    feed(&session, &at_48k, 48_000, 1);
    session.finish();
    let events = recorder.wait_terminal();
    assert!(matches!(check_contract(&events), SessionEvent::Finished { outcome: Outcome::NoMatch { .. } }));
    // 9 s of audio: queries at 4 s and 8 s, none at the end (the last one is recent).
    assert_eq!(server.hits("/discovery/"), 2);
}

#[test]
fn rate_limits_are_announced_and_the_session_keeps_listening() {
    let server = Server::start(vec![Route {
        prefix: "/discovery/".into(),
        status: 429,
        content_type: "text/plain",
        body: Body::Bytes(b"slow down".to_vec()),
    }]);
    let fx = engine_with_wait(&server, "/manifest.json", Duration::ZERO);
    let recorder = Arc::new(Recorder::default());
    let session =
        fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Recording, recorder.clone()).unwrap();
    feed(&session, &music(14.0, 0.5), 44_100, 2);
    session.finish();
    let events = recorder.wait_terminal();
    assert!(notices(&events).iter().any(|n| matches!(n, Notice::RateLimited { .. })), "{events:?}");
    assert!(matches!(check_contract(&events), SessionEvent::Finished { outcome: Outcome::NoMatch { .. } }));
}

#[test]
fn digital_silence_reports_capture_silent() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.start_listening(CaptureSource::SystemAudio, ListenMode::Auto, recorder.clone()).unwrap();
    feed(&session, &vec![0.0; 48_000 * 5], 48_000, 1);
    session.finish();
    let events = recorder.wait_terminal();
    assert!(notices(&events).contains(&Notice::CaptureSilent));
    assert_eq!(
        check_contract(&events),
        SessionEvent::Finished { outcome: Outcome::NoMatch { reason: NoMatchReason::CaptureSilent } }
    );
    assert_eq!(server.hits("/discovery/"), 0, "silence is never sent");
}

#[test]
fn quiet_input_reports_too_quiet() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session =
        fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Recording, recorder.clone()).unwrap();
    // About −60 dBFS.
    feed(&session, &music(8.0, 0.0015), 44_100, 2);
    session.finish();
    let events = recorder.wait_terminal();
    assert!(notices(&events).contains(&Notice::MoveCloser));
    assert_eq!(
        check_contract(&events),
        SessionEvent::Finished { outcome: Outcome::NoMatch { reason: NoMatchReason::TooQuiet } }
    );
    assert_eq!(server.hits("/discovery/"), 0, "clips below −50 dBFS are not sent");
}

#[test]
fn finishing_early_reports_too_short() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session =
        fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Recording, recorder.clone()).unwrap();
    feed(&session, &music(2.0, 0.5), 44_100, 2);
    session.finish();
    session.push_pcm(&bytes(&music(1.0, 0.5)), 44_100, 2); // ignored after finish
    let events = recorder.wait_terminal();
    assert_eq!(
        check_contract(&events),
        SessionEvent::Finished { outcome: Outcome::NoMatch { reason: NoMatchReason::TooShort } }
    );
}

#[test]
fn cancel_emits_canceled_exactly_once() {
    let server = shazam_server(NO_MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Auto, recorder.clone()).unwrap();
    feed(&session, &music(5.0, 0.5), 44_100, 2);
    session.cancel();
    session.cancel();
    session.push_pcm(&bytes(&music(1.0, 0.5)), 44_100, 2);
    session.finish();
    let events = recorder.wait_terminal();
    assert_eq!(check_contract(&events), SessionEvent::Canceled);
    let detail = fx.engine.session(session.id()).unwrap().unwrap();
    assert_eq!(detail.entry.status, SessionStatus::Canceled);
}

#[test]
fn three_concurrent_sessions() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let workers: Vec<_> = (0..3)
        .map(|i| {
            let engine = fx.engine.clone();
            std::thread::spawn(move || {
                let recorder = Arc::new(Recorder::default());
                let source = if i == 0 { CaptureSource::SystemAudio } else { CaptureSource::Microphone };
                let session = engine.start_listening(source, ListenMode::Auto, recorder.clone()).unwrap();
                // Different audio per session (different amplitude and length).
                feed(&session, &music(10.0 + i as f32, 0.3 + 0.1 * i as f32), 44_100, 2);
                session.finish();
                check_contract(&recorder.wait_terminal())
            })
        })
        .collect();
    for worker in workers {
        let terminal = worker.join().unwrap();
        assert!(matches!(terminal, SessionEvent::Finished { outcome: Outcome::Matched { .. } }), "{terminal:?}");
    }
    let all = LibraryQuery { kinds: vec![], search: None, pinned_only: false, limit: 0, offset: 0 };
    assert_eq!(fx.engine.library(all).unwrap().len(), 3);
}

#[test]
fn hum_mode_keeps_the_recording_and_reports_a_missing_index() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Hum, recorder.clone()).unwrap();
    feed(&session, &hum(8.0), 16_000, 1);
    session.finish();
    let events = recorder.wait_terminal();
    assert_eq!(events[0], SessionEvent::Started { session_id: session.id(), kind: SessionKind::Hum });
    let missing = notices(&events).iter().filter(|n| **n == Notice::MelodyIndexMissing).count();
    assert_eq!(missing, 1, "{events:?}");
    assert_eq!(
        check_contract(&events),
        SessionEvent::Finished { outcome: Outcome::NoMatch { reason: NoMatchReason::NotFound } }
    );
    assert_eq!(server.hits("/discovery/"), 0, "humming is not fingerprinted");
    let recording = fx.dir.0.join("data/recordings").join(format!("{}.wav", session.id()));
    assert!(recording.metadata().unwrap().len() > 100_000, "the capture is kept");
    let detail = fx.engine.session(session.id()).unwrap().unwrap();
    assert_eq!(detail.entry.kind, SessionKind::Hum);
    // Deleting the session deletes the recording too.
    fx.engine.delete_sessions(vec![session.id()]).unwrap();
    assert!(!recording.exists());
    assert!(fx.engine.session(session.id()).unwrap().is_none());
}

#[test]
fn delete_cancels_a_running_session() {
    let server = shazam_server(NO_MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Auto, recorder.clone()).unwrap();
    feed(&session, &music(1.0, 0.5), 44_100, 2);
    fx.engine.delete_sessions(vec![session.id()]).unwrap();
    let events = recorder.wait_terminal();
    assert_eq!(check_contract(&events), SessionEvent::Canceled);
    assert!(fx.engine.session(session.id()).unwrap().is_none());
}

#[test]
fn a_panicking_pipeline_fails_cleanly_and_the_engine_keeps_working() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx
        .engine
        .start_custom_session(SessionKind::Link, recorder.clone(), |_ctx| async move {
            panic!("pipeline bug");
        })
        .unwrap();
    let events = recorder.wait_terminal();
    let SessionEvent::Failed { code, .. } = check_contract(&events) else { panic!("{events:?}") };
    assert_eq!(code, ErrorCode::Internal);
    let detail = fx.engine.session(session.id()).unwrap().unwrap();
    assert_eq!(detail.entry.status, SessionStatus::Failed);

    // Analysis entry points that are not built yet must not take the engine down.
    let observer = Arc::new(Recorder::default());
    assert!(fx.engine.analyze(String::new(), AnalyzeMode::Auto, observer).is_err());

    let recorder = Arc::new(Recorder::default());
    let live = fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Recording, recorder.clone()).unwrap();
    feed(&live, &music(6.0, 0.5), 44_100, 2);
    live.finish();
    assert!(matches!(
        check_contract(&recorder.wait_terminal()),
        SessionEvent::Finished { outcome: Outcome::Matched { .. } }
    ));
}

#[test]
fn custom_session_cancel_is_prompt_even_when_the_body_ignores_it() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx
        .engine
        .start_custom_session(SessionKind::Link, recorder.clone(), |ctx| async move {
            ctx.emitter.phase(Phase::Resolving, None);
            tokio::time::sleep(Duration::from_secs(60)).await;
        })
        .unwrap();
    recorder.wait_for(|e| matches!(e, SessionEvent::Phase { .. }));
    let started = Instant::now();
    session.cancel();
    let events = recorder.wait_terminal();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(check_contract(&events), SessionEvent::Canceled);
}

#[test]
fn invalid_calls_return_errors() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let observer = Arc::new(Recorder::default());
    assert!(matches!(fx.engine.search_lyrics("   ".into(), observer), Err(MfError::InvalidInput { .. })));
    assert!(fx.engine.correct_segment("nope".into(), "x".into(), Track::default()).is_err());
    assert!(fx.engine.confirm_candidate("nope".into(), Track::default()).is_err());
    assert!(fx.engine.export_session("nope".into(), ExportFormat::Text).is_err());
    let bad = EngineConfig {
        data_dir: String::new(),
        cache_dir: String::new(),
        country: "US".into(),
        language: "en-US".into(),
        app_version: "1".into(),
        media_tools: None,
        melody_index_manifest_url: String::new(),
    };
    assert!(Engine::new(bad, None, None, None).is_err());
}

#[test]
fn low_power_follows_the_app() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let detected = mfcore::util::system::SystemInfo::current().is_low_end();
    assert_eq!(fx.engine.is_low_power(), detected, "defaults to what the hardware suggests");
    fx.engine.set_low_power(true);
    assert!(fx.engine.is_low_power());
    fx.engine.set_low_power(false);
    assert!(!fx.engine.is_low_power());
}

// ---------------------------------------------------------------- library

#[test]
fn library_operations() {
    let server = shazam_server(MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.start_listening(CaptureSource::Microphone, ListenMode::Auto, recorder.clone()).unwrap();
    feed(&session, &music(6.0, 0.5), 44_100, 2);
    recorder.wait_terminal();
    let id = session.id();

    // A user correction keeps the old answer as an alternate.
    let mine = Track { title: "Instant Crush".into(), artist: "Daft Punk".into(), ..Track::default() };
    let corrected = fx.engine.correct_segment(id.clone(), "match".into(), mine.clone()).unwrap();
    assert_eq!(corrected.track.as_ref().unwrap().title, "Instant Crush");
    assert_eq!(corrected.tier, Tier::Confirmed);
    assert_eq!(corrected.confidence, 1.0);
    assert_eq!(corrected.alternates[0].title, "Get Lucky");
    assert!(corrected.evidence.iter().any(|e| e.source == EvidenceSource::User));
    let stored = fx.engine.session(id.clone()).unwrap().unwrap();
    assert_eq!(stored.segments[0], corrected);

    // Confirming a candidate makes it the primary track at rank 0.
    fx.engine.confirm_candidate(id.clone(), mine.clone()).unwrap();
    let stored = fx.engine.session(id.clone()).unwrap().unwrap();
    assert_eq!(stored.candidates[0].track.title, "Instant Crush");
    assert_eq!(stored.candidates[0].tier, CandidateTier::Strong);
    assert_eq!(stored.entry.track.unwrap().title, "Instant Crush");

    let text = fx.engine.export_session(id.clone(), ExportFormat::Text).unwrap();
    assert!(text.contains("Instant Crush"), "{text}");

    fx.engine.set_pinned(id.clone(), true).unwrap();
    let pinned_dir = fx.dir.0.join("cache/sessions").join(&id);
    let stale_dir = fx.dir.0.join("cache/sessions/old-session");
    std::fs::create_dir_all(&pinned_dir).unwrap();
    std::fs::create_dir_all(&stale_dir).unwrap();
    std::fs::write(stale_dir.join("audio.pcm"), vec![0u8; 4096]).unwrap();
    let before = fx.engine.storage_usage();
    assert!(before.database_bytes > 0);
    assert!(before.cache_bytes >= 4096);
    fx.engine.clear_cache().unwrap();
    assert!(pinned_dir.exists(), "pinned sessions keep their cache");
    assert!(!stale_dir.exists());
    assert!(fx.engine.storage_usage().cache_bytes < before.cache_bytes);
}

#[test]
fn credentials_are_snapshotted_per_session() {
    let server = shazam_server(NO_MATCH_JSON);
    let fx = engine_with(&server, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx
        .engine
        .start_custom_session(SessionKind::Link, recorder.clone(), |ctx| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let seen = ctx.credentials.audd_token.clone();
            let update = SessionUpdate { title: Some(format!("{seen:?}")), ..Default::default() };
            ctx.emitter.finished(Outcome::Completed { identified: 0, total: 0 }, update);
        })
        .unwrap();
    fx.engine.set_credentials(mfcore::Credentials { audd_token: Some("later".into()), ..Default::default() });
    recorder.wait_terminal();
    assert_eq!(fx.engine.session(session.id()).unwrap().unwrap().entry.title, "None");
}

// ---------------------------------------------------------------- melody index

fn manifest(url: &str, data: &[u8], sha: Option<String>) -> String {
    let sha = sha.unwrap_or_else(|| Sha256::digest(data).iter().map(|b| format!("{b:02x}")).collect());
    format!(
        r#"{{"version": "2026.10.1", "url": "{url}/melody-index.mfi", "sha256": "{sha}", "bytes": {}, "songs": 2, "min_core": "1.1.0", "format": 2}}"#,
        data.len()
    )
}

/// The manifest route needs the server's own URL, so start a data server first.
fn index_servers(data: Vec<u8>, slow: bool, sha: Option<String>) -> (Server, Server) {
    let body = if slow {
        Body::Slow { data: data.clone(), piece: 4096, pause: Duration::from_millis(30) }
    } else {
        Body::Bytes(data.clone())
    };
    let files = Server::start(vec![Route {
        prefix: "/melody-index.mfi".into(),
        status: 200,
        content_type: "application/octet-stream",
        body,
    }]);
    let manifest = manifest(&files.url, &data, sha);
    let api = Server::start(vec![json_route("/manifest.json", &manifest), json_route("/discovery/", NO_MATCH_JSON)]);
    (api, files)
}

fn melody_dir(fx: &Fixture) -> PathBuf {
    fx.dir.0.join("data/melody")
}

#[test]
fn melody_index_check_reports_the_published_version() {
    let (api, _files) = index_servers(vec![7u8; 1000], false, None);
    let fx = engine_with(&api, "/manifest.json");
    let status = fx.engine.check_melody_index().unwrap();
    assert!(!status.installed);
    assert_eq!(status.available_version.as_deref(), Some("2026.10.1"));
    assert_eq!(status.download_bytes, Some(1000));
    assert_eq!(fx.engine.melody_index_status().available_version.as_deref(), Some("2026.10.1"));
}

#[test]
fn melody_index_corrupt_download_fails_and_leaves_nothing() {
    let (api, files) = index_servers(vec![7u8; 50_000], false, Some("0".repeat(64)));
    let fx = engine_with(&api, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    fx.engine.install_melody_index(recorder.clone()).unwrap();
    let events = recorder.wait_terminal();
    let SessionEvent::Failed { code, .. } = check_contract(&events) else { panic!("{events:?}") };
    assert_eq!(code, ErrorCode::Network);
    assert_eq!(files.hits("/melody-index.mfi"), 1);
    assert!(events.iter().any(|e| matches!(e, SessionEvent::Phase { phase: Phase::Downloading, .. })));
    let dir = melody_dir(&fx);
    assert!(!dir.join(".download").exists());
    assert!(!dir.join("melody-index.mfi").exists());
    assert!(!fx.engine.melody_index_status().installed);
    // The download is not a library session.
    let all = LibraryQuery { kinds: vec![], search: None, pinned_only: false, limit: 0, offset: 0 };
    assert!(fx.engine.library(all).unwrap().is_empty());
}

#[test]
fn melody_index_cancel_removes_the_partial_download() {
    let (api, _files) = index_servers(vec![7u8; 400_000], true, None);
    let fx = engine_with(&api, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    let session = fx.engine.install_melody_index(recorder.clone()).unwrap();
    assert!(fx.engine.install_melody_index(Arc::new(Recorder::default())).is_err(), "one download at a time");
    recorder.wait_for(|e| matches!(e, SessionEvent::Phase { fraction: Some(f), .. } if *f > 0.0));
    assert!(fx.engine.remove_melody_index().is_err(), "busy while downloading");
    session.cancel();
    let events = recorder.wait_terminal();
    assert_eq!(check_contract(&events), SessionEvent::Canceled);
    std::thread::sleep(Duration::from_millis(200));
    assert!(!melody_dir(&fx).join(".download").exists());
    // A new download may start once the first one ended.
    let started = Instant::now();
    while fx.engine.remove_melody_index().is_err() && started.elapsed() < WAIT {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(fx.engine.remove_melody_index().is_ok());
}

#[test]
fn melody_index_with_a_bad_manifest_fails() {
    let api = Server::start(vec![json_route("/manifest.json", r#"{"version": ""}"#)]);
    let fx = engine_with(&api, "/manifest.json");
    assert!(fx.engine.check_melody_index().is_err());
    let recorder = Arc::new(Recorder::default());
    fx.engine.install_melody_index(recorder.clone()).unwrap();
    assert!(matches!(check_contract(&recorder.wait_terminal()), SessionEvent::Failed { .. }));
}

/// The full install path, with an index built from JSONL by the melody builder.
#[test]
fn melody_index_installs_and_swaps_in() {
    let dir = std::env::temp_dir().join(format!("mf-index-build-{}", mfcore::util::new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let _cleanup = TempDir(dir.clone());
    let jsonl = dir.join("songs.jsonl");
    let notes = |base: f32| -> String {
        (0..24)
            .map(|i| format!("[{}, {}, 300]", base + [0.0, 2.0, 4.0, 5.0, 7.0, 5.0, 4.0, 2.0][i % 8], i * 320))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let lines = [
        format!(
            r#"{{"title": "Song A", "artist": "Artist A", "isrc": null, "deezer_id": 1, "year": 2001, "popularity": 0.5, "phrases": [{{"source": "midi", "start_ms": 0, "notes": [{}]}}]}}"#,
            notes(60.0)
        ),
        format!(
            r#"{{"title": "Song B", "artist": "Artist B", "isrc": null, "deezer_id": 2, "year": 2002, "popularity": 0.4, "phrases": [{{"source": "midi", "start_ms": 0, "notes": [{}]}}]}}"#,
            notes(64.0)
        ),
    ];
    std::fs::write(&jsonl, lines.join("\n")).unwrap();
    let mfi = dir.join("melody-index.mfi");
    mfcore::melody::builder::build_index(&jsonl, &mfi, "2026.10.1").expect("build index");
    let data = std::fs::read(&mfi).unwrap();

    let (api, _files) = index_servers(data.clone(), false, None);
    let fx = engine_with(&api, "/manifest.json");
    let recorder = Arc::new(Recorder::default());
    fx.engine.install_melody_index(recorder.clone()).unwrap();
    let events = recorder.wait_terminal();
    assert_eq!(
        check_contract(&events),
        SessionEvent::Finished { outcome: Outcome::Completed { identified: 1, total: 1 } }
    );
    let status = fx.engine.melody_index_status();
    assert!(status.installed);
    assert_eq!(status.version.as_deref(), Some("2026.10.1"));
    assert_eq!(status.song_count, 2);
    assert_eq!(status.size_bytes, data.len() as u64);
    assert_eq!(status.available_version, None);
    assert!(fx.engine.storage_usage().melody_index_bytes >= data.len() as u64);
    fx.engine.remove_melody_index().unwrap();
    assert!(!fx.engine.melody_index_status().installed);
    assert!(!melody_dir(&fx).join("melody-index.mfi").exists());
}

#[test]
fn concurrent_index_download_counter() {
    // Guard against a stuck "installing" flag after failures.
    let api = Server::start(vec![Route {
        prefix: "/manifest.json".into(),
        status: 500,
        content_type: "text/plain",
        body: Body::Bytes(b"down".to_vec()),
    }]);
    let fx = engine_with(&api, "/manifest.json");
    let count = AtomicUsize::new(0);
    for _ in 0..3 {
        let recorder = Arc::new(Recorder::default());
        fx.engine.install_melody_index(recorder.clone()).unwrap();
        if matches!(check_contract(&recorder.wait_terminal()), SessionEvent::Failed { code: ErrorCode::Network, .. }) {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }
    assert_eq!(count.load(Ordering::SeqCst), 3);
}
