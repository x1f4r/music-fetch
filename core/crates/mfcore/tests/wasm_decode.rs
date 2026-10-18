//! Decoding in the browser build: the codec fixtures must decode to exactly
//! the PCM the native build produces (`fixtures/codecs/expected.json`). Runs in
//! Node by default; `WASM_BINDGEN_USE_DEDICATED_WORKER=1` with `CHROMEDRIVER` or
//! `GECKODRIVER` runs it in a browser Worker (see tests/wasm_store.rs).
#![cfg(all(target_family = "wasm", feature = "decode"))]

use mfcore::audio::decode::decode_bytes;
use mfcore::util::cancel::CancelToken;
use wasm_bindgen_test::wasm_bindgen_test;

include!("fixtures/codecs/check.rs");

#[wasm_bindgen_test]
fn decoded_pcm_equals_native() {
    for (name, differing, max) in codec_window_diffs() {
        wasm_bindgen_test::console_log!("parity {name}: {differing} of {CODEC_WINDOW} samples differ, by up to {max}");
    }
    let bad = codec_mismatches();
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// Long media goes into a work file instead of memory.
#[cfg(feature = "store")]
#[wasm_bindgen_test]
fn decodes_into_a_work_file() {
    use std::sync::Arc;

    use mfcore::audio::decode::decode_bytes_into;
    use mfcore::audio::{PcmFile, PcmWriter};
    use mfcore::util::files::FileStore;
    use mfcore::util::files_sqlite::SqliteFiles;

    let files: Arc<dyn FileStore> = Arc::new(SqliteFiles::open_in_memory().unwrap());
    let (name, hint, bytes, _) = CODEC_FIXTURES[2];
    let writer = PcmWriter::create_in(files.clone(), "sessions/x/audio.pcm").unwrap();
    let pcm = decode_bytes_into(Arc::from(bytes), Some(hint), writer, &CancelToken::new(), &|_| {}).unwrap();
    let in_memory = decode_bytes(bytes, Some(hint), &CancelToken::new()).unwrap();
    assert_eq!(pcm.read_samples(0, usize::MAX).unwrap(), in_memory, "{name}");
    let reopened = PcmFile::open_in(files, "sessions/x/audio.pcm").unwrap();
    assert_eq!(reopened.len_samples(), in_memory.len() as u64);
    assert!(reopened.coverage().is_none());
}
