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
