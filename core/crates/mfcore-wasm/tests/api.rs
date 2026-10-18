//! The facade's API in a real JS environment: Node, and dedicated Workers in Chrome
//! and Firefox (where the OPFS storage is tested too). No network: the recognizer
//! points at an address where nothing listens, and nothing here needs it.
//!
//!   cargo test -p mfcore-wasm --target wasm32-unknown-unknown --test api            (Node)
//!   WASM_BINDGEN_USE_DEDICATED_WORKER=1 CHROMEDRIVER=… cargo test … --test api    (browsers)

#![cfg(target_family = "wasm")]

use std::cell::RefCell;
use std::rc::Rc;

use mfcore::model::{NoMatchReason, Outcome, SessionEvent};
use mfcore_wasm::WasmEngine;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;

type Events = Rc<RefCell<Vec<(String, SessionEvent)>>>;

fn object(pairs: &[(&str, JsValue)]) -> JsValue {
    let o = js_sys::Object::new();
    for (k, v) in pairs {
        js_sys::Reflect::set(&o, &(*k).into(), v).unwrap();
    }
    o.into()
}

/// An engine on in-memory storage (or OPFS in `directory`) and the events it sends.
async fn engine(storage: &str, directory: &str) -> (WasmEngine, Events) {
    let events: Events = Rc::default();
    let sink = events.clone();
    let on_event = Closure::<dyn Fn(String, JsValue)>::new(move |id: String, event: JsValue| {
        let event: SessionEvent = serde_wasm_bindgen::from_value(event).expect("a SessionEvent");
        sink.borrow_mut().push((id, event));
    });
    let host = object(&[("onEvent", on_event.as_ref().clone())]);
    on_event.forget();
    let config = object(&[
        ("storage", storage.into()),
        ("directory", directory.into()),
        ("shazam_base_url", "http://127.0.0.1:9".into()),
        ("offline_catalog", true.into()),
    ]);
    (WasmEngine::open_with(config, host).await.expect("engine"), events)
}

async fn settle(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        let set_timeout: js_sys::Function =
            js_sys::Reflect::get(&global, &"setTimeout".into()).unwrap().unchecked_into();
        set_timeout.call2(&global, &resolve, &ms.into()).unwrap();
    });
    JsFuture::from(promise).await.unwrap();
}

async fn terminal(events: &Events, id: &str) -> Vec<SessionEvent> {
    for _ in 0..400 {
        let of: Vec<SessionEvent> = events.borrow().iter().filter(|(i, _)| i == id).map(|(_, e)| e.clone()).collect();
        if of
            .iter()
            .any(|e| matches!(e, SessionEvent::Finished { .. } | SessionEvent::Failed { .. } | SessionEvent::Canceled))
        {
            return of;
        }
        settle(25).await;
    }
    panic!("no terminal event for {id}: {:?}", events.borrow());
}

fn code(error: JsValue) -> String {
    js_sys::Reflect::get(&error, &"code".into()).ok().and_then(|c| c.as_string()).unwrap_or_default()
}

fn mode(name: &str) -> JsValue {
    JsValue::from_str(name)
}

#[wasm_bindgen_test]
async fn statics() {
    assert!(!WasmEngine::core_version().is_empty());
    assert!(WasmEngine::memory_bytes() > 1e6);
    assert!(WasmEngine::is_link("see https://open.spotify.com/track/abc".into()));
    let track = mfcore::Track { title: "Song".into(), artist: "Artist".into(), ..Default::default() };
    let serializer =
        serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true).serialize_maps_as_objects(true);
    let track = serde::Serialize::serialize(&track, &serializer).unwrap();
    // A partial track is refused: JS passes complete records (`emptyTrack()`).
    let partial = object(&[("title", "Song".into()), ("artist", "Artist".into())]);
    assert_eq!(code(WasmEngine::provider_links(partial).err().unwrap()), "InvalidInput");
    let links: Vec<JsValue> = js_sys::Array::from(&WasmEngine::provider_links(track.clone()).unwrap()).to_vec();
    assert!(links.len() > 3, "{links:?}");
    let custom = WasmEngine::custom_provider_link(track, "https://example.com/?q={query}".into()).unwrap();
    assert_eq!(custom.as_deref(), Some("https://example.com/?q=Artist%20Song"));
}

#[wasm_bindgen_test]
async fn a_silent_live_session_ends_capture_silent_and_lands_in_the_library() {
    let (engine, events) = engine("memory", "").await;
    let live = engine.start_listening(mode("Microphone"), mode("Recording"), JsValue::UNDEFINED).expect("live");
    let id = live.id();
    assert_eq!(engine.running_sessions(), vec![id.clone()]);
    let silence = vec![0f32; 960];
    for _ in 0..250 {
        live.push_pcm(&silence, 48_000, 1);
        settle(0).await;
    }
    assert!(engine.finish_session(id.clone()));
    let stream = terminal(&events, &id).await;
    assert!(matches!(stream.first(), Some(SessionEvent::Started { .. })), "{stream:?}");
    assert!(stream.iter().any(|e| matches!(e, SessionEvent::Notice { .. })), "CaptureSilent notice: {stream:?}");
    assert!(
        matches!(
            stream.last(),
            Some(SessionEvent::Finished { outcome: Outcome::NoMatch { reason: NoMatchReason::CaptureSilent } })
        ),
        "{stream:?}"
    );
    let query = object(&[
        ("kinds", js_sys::Array::new().into()),
        ("search", JsValue::NULL),
        ("pinned_only", false.into()),
        ("limit", 10.into()),
        ("offset", 0.into()),
    ]);
    let library = js_sys::Array::from(&engine.library(query.clone()).unwrap());
    assert_eq!(library.length(), 1);
    engine.set_pinned(id.clone(), true).unwrap();
    let detail = engine.session(id.clone()).unwrap();
    let entry = js_sys::Reflect::get(&detail, &"entry".into()).unwrap();
    assert_eq!(js_sys::Reflect::get(&entry, &"pinned".into()).unwrap(), JsValue::TRUE);
    assert_eq!(js_sys::Reflect::get(&entry, &"status".into()).unwrap(), JsValue::from_str("NoMatch"));
    JsFuture::from(engine.delete_sessions(vec![id.clone()]).unwrap()).await.unwrap();
    assert_eq!(js_sys::Array::from(&engine.library(query).unwrap()).length(), 0);
    assert!(engine.running_sessions().is_empty());
    JsFuture::from(engine.close()).await.unwrap();
}

#[wasm_bindgen_test]
async fn bad_input_is_refused_with_the_core_error_codes() {
    let (engine, _) = engine("memory", "").await;
    assert_eq!(code(engine.analyze("not a link".into(), mode("Auto")).err().unwrap()), "InvalidInput");
    assert_eq!(code(engine.search_lyrics("  ".into()).err().unwrap()), "InvalidInput");
    assert_eq!(code(engine.analyze_file("empty.wav".into(), &[], mode("Auto")).err().unwrap()), "InvalidInput");
    assert_eq!(
        code(engine.start_listening(mode("Tab"), mode("Auto"), JsValue::UNDEFINED).err().unwrap()),
        "InvalidInput"
    );
    let status = engine.melody_index_status().unwrap();
    assert_eq!(js_sys::Reflect::get(&status, &"installed".into()).unwrap(), JsValue::FALSE);
    assert_eq!(JsFuture::from(engine.load_melody_index().unwrap()).await.unwrap(), JsValue::FALSE);
    JsFuture::from(engine.close()).await.unwrap();
    assert_eq!(code(engine.analyze("https://example.com".into(), mode("Auto")).err().unwrap()), "Internal", "closed");
}

#[wasm_bindgen_test]
async fn a_file_is_decoded_from_its_bytes_and_cancel_ends_it() {
    let (engine, events) = engine("memory", "").await;
    // Two seconds of a 440 Hz tone as a 16-bit WAV: decoded in the engine; the survey
    // cannot reach the recognizer here, so the session ends early, or the cancel ends it.
    let rate = 16_000u32;
    let mut wav = Vec::new();
    let samples: Vec<i16> = (0..rate * 2)
        .map(|i| ((i as f32 * 440.0 * std::f32::consts::TAU / rate as f32).sin() * 8000.0) as i16)
        .collect();
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + samples.len() as u32 * 2).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&rate.to_le_bytes());
    wav.extend_from_slice(&(rate * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&(samples.len() as u32 * 2).to_le_bytes());
    for s in &samples {
        wav.extend_from_slice(&s.to_le_bytes());
    }
    let id = engine.analyze_file("../tone.wav".into(), &wav, mode("Auto")).expect("file");
    for _ in 0..40 {
        if events.borrow().iter().any(|(i, e)| i == &id && matches!(e, SessionEvent::Source { .. })) {
            break;
        }
        settle(25).await;
    }
    let _ = engine.cancel_session(id.clone());
    let stream = terminal(&events, &id).await;
    assert!(matches!(stream.first(), Some(SessionEvent::Started { .. })), "{stream:?}");
    let source =
        stream.iter().find_map(|e| if let SessionEvent::Source { info } = e { info.title.clone() } else { None });
    assert_eq!(source.as_deref(), Some("tone"));
    assert!(matches!(
        stream.last(),
        Some(SessionEvent::Canceled | SessionEvent::Finished { .. } | SessionEvent::Failed { .. })
    ));
    // The work file is gone with the session.
    let usage = engine.storage_usage().unwrap();
    assert_eq!(js_sys::Reflect::get(&usage, &"cache_bytes".into()).unwrap().as_f64(), Some(0.0));
    JsFuture::from(engine.close()).await.unwrap();
}

/// OPFS: only dedicated Workers have sync access handles.
fn in_worker() -> bool {
    js_sys::Reflect::has(&js_sys::global(), &"WorkerGlobalScope".into()).unwrap_or(false)
        && js_sys::Reflect::get(&js_sys::global(), &"navigator".into())
            .ok()
            .and_then(|n| js_sys::Reflect::get(&n, &"storage".into()).ok())
            .is_some_and(|s| !s.is_undefined())
}

#[wasm_bindgen_test]
async fn the_library_survives_a_new_engine_on_opfs() {
    if !in_worker() {
        web_sys::console::log_1(&"OPFS test skipped outside a dedicated Worker".into());
        return;
    }
    let directory = format!(".mf-api-{}", mfcore::util::new_id());
    let (first, events) = engine("opfs", &directory).await;
    let live = first.start_listening(mode("Microphone"), mode("Recording"), JsValue::UNDEFINED).unwrap();
    let id = live.id();
    live.cancel();
    terminal(&events, &id).await;
    first.set_pinned(id.clone(), true).unwrap();
    JsFuture::from(first.close()).await.expect("close releases the pool");
    let (second, _) = engine("opfs", &directory).await;
    let detail = second.session(id.clone()).unwrap();
    let entry = js_sys::Reflect::get(&detail, &"entry".into()).unwrap();
    assert_eq!(js_sys::Reflect::get(&entry, &"pinned".into()).unwrap(), JsValue::TRUE);
    assert_eq!(js_sys::Reflect::get(&entry, &"status".into()).unwrap(), JsValue::from_str("Canceled"));
    JsFuture::from(second.close()).await.unwrap();
}
