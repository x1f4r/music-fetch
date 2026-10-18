//! Hum parity: the engine's melody path (pitch tracking of the whole recording, the
//! query, the index search, `pipeline::hum::search_frames`) answers the same in the
//! browser as natively. The same code runs on both targets and prints one line per
//! query; `smoke/hum-parity.py` compares the runs. (The encoder companion is not used:
//! the browser installs the index alone.)
//!
//! Native (reads files):
//!   MF_PARITY_INDEX=<index.mfi> MF_PARITY_QUERIES=<queries.tsv> \
//!     cargo test -p mfcore-wasm --profile release-fast --test hum_parity -- --ignored --nocapture
//! Browser (fetches `<url>/index.mfi`, `<url>/queries.tsv` and the WAVs it names from a
//! server that allows CORS; the URL is fixed at build time):
//!   MF_PARITY_URL=http://127.0.0.1:<port> WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     cargo test -p mfcore-wasm --target wasm32-unknown-unknown --profile wasm-dev --test hum_parity
//!
//! The queries file is TSV: WAV path (relative to the queries file), expected title,
//! expected artist. Lines: `PARITY\t<wav>\t<expected>\t<top title>\t[[song, score], …]`.
//!
//! The browser build also installs the index through the engine from `<url>/manifest.json`
//! (as the extension does: streamed into the work-file database with SHA-256 on the
//! way), reads it back in pieces, and hums the first query through a live session.

use mfcore::audio::decode::decode_bytes;
use mfcore::audio::resample::i16_to_f32;
use mfcore::melody::index::IndexLoader;
use mfcore::melody::{MelodyIndex, PitchTracker};
use mfcore::pipeline::hum::MelodySearcher;
use mfcore::util::cancel::CancelToken;
use mfcore::util::rt::Instant as Clock;

/// One query's line, or why it has none.
fn query_line(searcher: &MelodySearcher, tracker: &PitchTracker, wav: &str, expected: &str, bytes: &[u8]) -> String {
    let samples = match decode_bytes(bytes, Some("wav"), &CancelToken::new()) {
        Ok(s) => i16_to_f32(&s),
        Err(e) => return format!("PARITY\t{wav}\t{expected}\t!decode {e}\t[]"),
    };
    let frames = match tracker.analyze_detailed(&samples) {
        Ok(analysis) => analysis.frames,
        Err(e) => return format!("PARITY\t{wav}\t{expected}\t!pitch {e}\t[]"),
    };
    let Some(search) = searcher.search_frames(&frames) else {
        return format!("PARITY\t{wav}\t{expected}\t-\t[]");
    };
    let top: Vec<String> = search.matches.iter().map(|m| format!("[{}, {:.6}]", m.song.id, m.score)).collect();
    let title =
        search.matches.first().map(|m| format!("{} – {}", m.song.artist, m.song.title)).unwrap_or_else(|| "-".into());
    format!("PARITY\t{wav}\t{expected}\t{title}\t[{}]", top.join(", "))
}

/// The queries of a TSV file: (wav, "artist – title").
fn queries(tsv: &str) -> Vec<(String, String)> {
    tsv.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let mut parts = l.split('\t');
            let wav = parts.next()?.trim().to_string();
            let title = parts.next().unwrap_or("").trim();
            let artist = parts.next().unwrap_or("").trim();
            Some((wav, format!("{artist} – {title}")))
        })
        .collect()
}

/// The index read in 1 MiB pieces, as the browser installs and loads it.
fn load_index(bytes: &[u8]) -> MelodyIndex {
    let mut loader = IndexLoader::new(bytes.len() as u64).expect("loader");
    for piece in bytes.chunks(1 << 20) {
        loader.push(piece).expect("index piece");
    }
    loader.finish().expect("index")
}

#[cfg(not(target_family = "wasm"))]
#[test]
#[ignore = "needs MF_PARITY_INDEX and MF_PARITY_QUERIES"]
fn hum_parity_native() {
    use std::path::Path;
    let index_path = std::env::var("MF_PARITY_INDEX").expect("MF_PARITY_INDEX");
    let queries_path = std::env::var("MF_PARITY_QUERIES").expect("MF_PARITY_QUERIES");
    let started = Clock::now();
    let index = load_index(&std::fs::read(&index_path).expect("index file"));
    println!("PARITY-INDEX\t{}\t{} songs\t{} ms", index.version(), index.song_count(), started.elapsed().as_millis());
    let searcher = MelodySearcher::index_only(std::sync::Arc::new(index));
    // One thread, as in the browser (the batch tracker's result does not depend on it).
    let tracker = PitchTracker::with_threads(1).expect("pitch tracker");
    let tsv = std::fs::read_to_string(&queries_path).expect("queries");
    let base = Path::new(&queries_path).parent().unwrap_or(Path::new("."));
    let started = Clock::now();
    let list = queries(&tsv);
    for (wav, expected) in &list {
        let bytes = std::fs::read(base.join(wav)).expect("wav");
        println!("{}", query_line(&searcher, &tracker, wav, expected, &bytes));
    }
    println!("PARITY-DONE\t{}\t{} ms", list.len(), started.elapsed().as_millis());
}

#[cfg(target_family = "wasm")]
mod browser {
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::*;

    use super::*;

    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    async fn fetch(url: &str) -> Vec<u8> {
        let global = js_sys::global();
        let fetch: js_sys::Function = js_sys::Reflect::get(&global, &"fetch".into()).expect("fetch").unchecked_into();
        let response = JsFuture::from(js_sys::Promise::from(fetch.call1(&global, &url.into()).expect("fetch call")))
            .await
            .unwrap_or_else(|e| panic!("{url}: {e:?}"));
        let status = js_sys::Reflect::get(&response, &"status".into()).ok().and_then(|s| s.as_f64()).unwrap_or(0.0);
        assert_eq!(status, 200.0, "{url}");
        let read: js_sys::Function =
            js_sys::Reflect::get(&response, &"arrayBuffer".into()).expect("arrayBuffer").unchecked_into();
        let buffer = JsFuture::from(js_sys::Promise::from(read.call0(&response).expect("arrayBuffer call")))
            .await
            .expect("body");
        js_sys::Uint8Array::new(&buffer).to_vec()
    }

    fn log(line: &str) {
        web_sys::console::log_1(&line.into());
    }

    fn object(pairs: &[(&str, wasm_bindgen::JsValue)]) -> wasm_bindgen::JsValue {
        let o = js_sys::Object::new();
        for (k, v) in pairs {
            js_sys::Reflect::set(&o, &(*k).into(), v).unwrap();
        }
        o.into()
    }

    async fn sleep(ms: i32) {
        let promise = js_sys::Promise::new(&mut |resolve, _| {
            let global = js_sys::global();
            let set_timeout: js_sys::Function =
                js_sys::Reflect::get(&global, &"setTimeout".into()).unwrap().unchecked_into();
            set_timeout.call2(&global, &resolve, &ms.into()).unwrap();
        });
        JsFuture::from(promise).await.unwrap();
    }

    fn field(value: &wasm_bindgen::JsValue, name: &str) -> wasm_bindgen::JsValue {
        js_sys::Reflect::get(value, &name.into()).unwrap()
    }

    /// The index installed through the engine (manifest, streamed download, SHA-256),
    /// loaded back in pieces, then one query hummed through a live session.
    #[wasm_bindgen_test]
    async fn index_install_browser() {
        use std::cell::RefCell;
        use std::rc::Rc;

        use mfcore::model::{Outcome, SessionEvent};
        use wasm_bindgen::prelude::*;

        let Some(base) = option_env!("MF_PARITY_URL") else { return };
        let events: Rc<RefCell<Vec<(String, SessionEvent)>>> = Rc::default();
        let sink = events.clone();
        let on_event = Closure::<dyn Fn(String, JsValue)>::new(move |id: String, event: JsValue| {
            sink.borrow_mut().push((id, serde_wasm_bindgen::from_value(event).expect("event")));
        });
        let host = object(&[("onEvent", on_event.as_ref().clone())]);
        on_event.forget();
        let config = object(&[
            ("storage", "memory".into()),
            ("melody_index_manifest_url", format!("{base}/manifest.json").into()),
            ("shazam_base_url", "http://127.0.0.1:9".into()),
            ("offline_catalog", true.into()),
        ]);
        let engine = mfcore_wasm::WasmEngine::open_with(config, host).await.expect("engine");
        let status = JsFuture::from(engine.check_melody_index().unwrap()).await.expect("check");
        assert!(field(&status, "available_version").is_string(), "a version to install");
        let started = Clock::now();
        let id = engine.install_melody_index().expect("install");
        let ended = loop {
            if let Some(e) = events.borrow().iter().rev().find(|(i, e)| {
                i == &id
                    && matches!(e, SessionEvent::Finished { .. } | SessionEvent::Failed { .. } | SessionEvent::Canceled)
            }) {
                break e.1.clone();
            }
            sleep(50).await;
        };
        let install_ms = started.elapsed().as_millis();
        assert!(matches!(ended, SessionEvent::Finished { .. }), "{ended:?}");
        let phases =
            events.borrow().iter().filter(|(i, e)| i == &id && matches!(e, SessionEvent::Phase { .. })).count();
        let status = engine.melody_index_status().unwrap();
        assert_eq!(field(&status, "installed"), JsValue::TRUE);
        assert!(engine.melody_index_loaded().unwrap(), "the download is the loaded index");
        let memory_installed = core::arch::wasm32::memory_size(0) * 64 / 1024;
        engine.unload_melody_index().unwrap();
        assert_eq!(field(&engine.melody_index_status().unwrap(), "installed"), JsValue::TRUE, "still installed");
        let started = Clock::now();
        assert_eq!(JsFuture::from(engine.load_melody_index().unwrap()).await.unwrap(), JsValue::TRUE);
        let load_ms = started.elapsed().as_millis();
        log(&format!(
            "PARITY-INSTALL\tinstall {install_ms} ms ({phases} progress events)\tload {load_ms} ms\tmemory {memory_installed} MB, {} MB after reload\tsize {} bytes",
            core::arch::wasm32::memory_size(0) * 64 / 1024,
            field(&status, "size_bytes").as_f64().unwrap_or(0.0)
        ));

        // The first query hummed live, 20 ms at a time.
        let tsv = String::from_utf8(fetch(&format!("{base}/queries.tsv")).await).expect("tsv");
        let (wav, expected) = queries(&tsv).into_iter().next().expect("a query");
        let samples = i16_to_f32(
            &decode_bytes(&fetch(&format!("{base}/{wav}")).await, Some("wav"), &CancelToken::new()).unwrap(),
        );
        let options = object(&[("auto_stop", true.into())]);
        let live = engine.start_listening("Microphone".into(), "Hum".into(), options).expect("hum");
        let hum = live.id();
        for piece in samples.chunks(320) {
            if !live.is_capturing() {
                break;
            }
            live.push_pcm(piece, 16_000, 1);
            if live.backlog_ms() > 1000.0 {
                sleep(0).await;
            }
        }
        live.finish();
        let outcome =
            loop {
                if let Some((_, e)) = events.borrow().iter().rev().find(|(i, e)| {
                    i == &hum && matches!(e, SessionEvent::Finished { .. } | SessionEvent::Failed { .. })
                }) {
                    break e.clone();
                }
                sleep(50).await;
            };
        let top = events.borrow().iter().rev().find_map(|(i, e)| match e {
            SessionEvent::Candidates { candidates, is_final: true } if i == &hum => {
                candidates.first().map(|c| format!("{} – {}", c.track.artist, c.track.title))
            }
            _ => None,
        });
        log(&format!("PARITY-HUM\t{wav}\t{expected}\t{}\t{outcome:?}", top.unwrap_or_else(|| "-".into())));
        assert!(matches!(
            outcome,
            SessionEvent::Finished { outcome: Outcome::Candidates { .. } | Outcome::NoMatch { .. } }
        ));
        engine.remove_melody_index().unwrap();
            .await
            .unwrap();
        assert_eq!(field(&engine.melody_index_status().unwrap(), "installed"), JsValue::FALSE);
        JsFuture::from(engine.close()).await.unwrap();
    }

    #[wasm_bindgen_test]
    async fn hum_parity_browser() {
        let Some(base) = option_env!("MF_PARITY_URL") else {
            log("PARITY-SKIPPED\tbuild with MF_PARITY_URL");
            return;
        };
        let started = Clock::now();
        let bytes = fetch(&format!("{base}/index.mfi")).await;
        let fetched = started.elapsed().as_millis();
        let started = Clock::now();
        let index = load_index(&bytes);
        drop(bytes);
        log(&format!(
            "PARITY-INDEX\t{}\t{} songs\t{} ms (fetch {} ms)\tmemory {} MB",
            index.version(),
            index.song_count(),
            started.elapsed().as_millis(),
            fetched,
            core::arch::wasm32::memory_size(0) * 64 / 1024
        ));
        let searcher = MelodySearcher::index_only(std::sync::Arc::new(index));
        let tracker = PitchTracker::with_threads(1).expect("pitch tracker");
        let tsv = String::from_utf8(fetch(&format!("{base}/queries.tsv")).await).expect("tsv");
        let list = queries(&tsv);
        let started = Clock::now();
        for (wav, expected) in &list {
            let bytes = fetch(&format!("{base}/{wav}")).await;
            log(&query_line(&searcher, &tracker, wav, expected, &bytes));
        }
        log(&format!(
            "PARITY-DONE\t{}\t{} ms\tmemory {} MB",
            list.len(),
            started.elapsed().as_millis(),
            core::arch::wasm32::memory_size(0) * 64 / 1024
        ));
    }
}
