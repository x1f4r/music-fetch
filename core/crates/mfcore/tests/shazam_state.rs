//! The recognizer's back-off outlives the engine: once Shazam has throttled
//! us, a restarted engine (an app relaunch, a recycled browser Worker) keeps
//! cooling down instead of asking again.
//!
//! `cargo test -p mfcore --test shazam_state`

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mfcore::engine::{Engine, EngineOptions};
use mfcore::model::{CaptureSource, ListenMode, ListenOptions, Notice, SessionEvent};
use mfcore::platform::SessionObserver;
use mfcore::shazam::{Priority, QueuePolicy, signature};
use mfcore::util::cancel::CancelToken;
use mfcore::{EngineConfig, MfError};

/// Answers every request with 429 and `Retry-After: 120`; counts them.
fn throttling_server() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut length = 0;
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) > 0 && line.trim() != "" {
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap_or(0);
                }
                line.clear();
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            counter.fetch_add(1, Ordering::SeqCst);
            let mut stream = stream;
            let _ = stream.write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    (url, hits)
}

fn open(dir: &Path, url: &str) -> Arc<Engine> {
    let config = EngineConfig {
        data_dir: dir.join("data").to_string_lossy().into_owned(),
        cache_dir: dir.join("cache").to_string_lossy().into_owned(),
        country: "US".into(),
        language: "en-US".into(),
        app_version: "1.0.0-test".into(),
        media_tools: None,
        melody_index_manifest_url: String::new(),
    };
    let policy = QueuePolicy {
        interactive_max_wait: Duration::from_millis(200),
        batch_max_wait: Duration::from_millis(200),
        retry_delays: Vec::new(),
        ..QueuePolicy::default()
    };
    let options = EngineOptions {
        shazam_base_url: Some(url.to_string()),
        shazam_policy: Some(policy),
        offline_catalog: true,
        ..EngineOptions::default()
    };
    Engine::with_options(config, None, None, None, options).expect("engine")
}

/// Three seconds of something to sign.
fn clip() -> signature::Signature {
    let samples: Vec<i16> = (0..4 * 16_000)
        .map(|i| {
            let t = i as f32 / 16_000.0;
            let f = [440.0, 554.4, 659.3][(t * 3.0) as usize % 3];
            ((2.0 * std::f32::consts::PI * f * t).sin() * 9000.0) as i16
        })
        .collect();
    signature::generate(&samples)
}

#[derive(Default)]
struct Events(Mutex<Vec<SessionEvent>>);

impl SessionObserver for Events {
    fn on_event(&self, event: SessionEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[test]
fn a_restarted_engine_keeps_cooling_down() {
    let dir = std::env::temp_dir().join(format!("mf-shazam-state-{}", mfcore::util::new_id()));
    let (url, hits) = throttling_server();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let cancel = CancelToken::new();

    let first = open(&dir, &url);
    let err = runtime.block_on(first.context().shazam.recognize(&clip(), Priority::Interactive, &cancel)).unwrap_err();
    assert!(matches!(err, MfError::RateLimited { .. }), "{err:?}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    drop(first);

    // The restarted engine is still cooling down and does not ask again.
    let second = open(&dir, &url);
    let cooldown = second.context().shazam.cooldown().expect("the breaker survived the restart");
    assert!(cooldown > Duration::from_secs(100) && cooldown <= Duration::from_secs(135), "{cooldown:?}");
    let err = runtime.block_on(second.context().shazam.recognize(&clip(), Priority::Batch, &cancel)).unwrap_err();
    let MfError::RateLimited { retry_in_ms } = err else { panic!("expected RateLimited, got {err:?}") };
    assert!(retry_in_ms > 100_000, "{retry_in_ms}");

    // A live session announces the wait at once.
    let events = Arc::new(Events::default());
    let session = second
        .start_listening_with(
            CaptureSource::Microphone,
            ListenMode::Recording,
            ListenOptions::default(),
            events.clone(),
        )
        .expect("session");
    let tone: Vec<u8> = (0..16_000 * 6)
        .map(|i| (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 16_000.0).sin() * 0.3)
        .flat_map(|s: f32| s.to_le_bytes())
        .collect();
    for piece in tone.chunks(16_000 / 50 * 4) {
        session.push_pcm(piece, 16_000, 1);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let announced = loop {
        let seen = events.0.lock().unwrap().iter().any(|e| {
            matches!(e, SessionEvent::Notice { notice: Notice::RateLimited { retry_in_ms } } if *retry_in_ms > 100_000)
        });
        if seen || std::time::Instant::now() > deadline {
            break seen;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    session.cancel();
    assert!(announced, "no RateLimited notice: {:?}", events.0.lock().unwrap());
    assert_eq!(hits.load(Ordering::SeqCst), 1, "nothing was sent after the restart");
    drop(second);
    let _ = std::fs::remove_dir_all(&dir);
}
