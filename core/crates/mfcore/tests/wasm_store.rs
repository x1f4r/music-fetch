//! The store and the work files on the browser's OPFS pool, in a dedicated
//! Worker of headless Chrome and Firefox:
//!
//! ```sh
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//! CHROMEDRIVER=chromedriver cargo test -p mfcore --target wasm32-unknown-unknown \
//!     --no-default-features --features store --test wasm_store
//! ```
//!
//! (`GECKODRIVER=geckodriver` for Firefox; clang must target wasm32 for SQLite.)
#![cfg(all(target_family = "wasm", feature = "store"))]

use mfcore::model::{LibraryQuery, SessionKind, SessionStatus};
use mfcore::store::opfs::{BrowserStorage, FILES_DB, LIBRARY_DB, OpfsPool};
use mfcore::store::{SessionUpdate, Store};
use mfcore::util::files::FileStore;
use mfcore::util::files_sqlite::SqliteFiles;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

include!("../src/util/files_conformance.rs");

fn all() -> LibraryQuery {
    LibraryQuery { kinds: vec![], search: None, pinned_only: false, limit: 0, offset: 0 }
}

/// Each test gets its own pool (VFS name and OPFS directory).
async fn pool(name: &str) -> OpfsPool {
    OpfsPool::install_as(&format!("mf-{name}"), &format!(".mf-test-{name}")).await.expect("pool")
}

#[wasm_bindgen_test]
async fn library_survives_reopening() {
    let storage = BrowserStorage::open_in(pool("reopen").await).await.expect("open");
    let store = &storage.store;
    store.create_session("a", SessionKind::Listen, "First", None, 1).unwrap();
    store.create_session("b", SessionKind::Hum, "Second", Some("https://example.com/x"), 2).unwrap();
    store.update_session("a", SessionUpdate { status: Some(SessionStatus::Completed), ..Default::default() }).unwrap();
    store.update_session("b", SessionUpdate { status: Some(SessionStatus::Failed), ..Default::default() }).unwrap();
    assert_eq!(store.set_pinned_many(&["a".into(), "b".into(), "zz".into()], true).unwrap(), 2);
    store.kv_put("shazam.breaker", "{\"open_until_ms\":5}").unwrap();
    store.cache_put("shazam", "sig", "{}", 60_000).unwrap();
    assert!(storage.bytes() > 0);
    // Each database keeps a journal file next to it.
    let files = storage.pool.files();
    assert!(files.contains(&LIBRARY_DB.to_string()) && files.contains(&FILES_DB.to_string()), "{files:?}");
    let pool = storage.close().map_err(|(_, e)| e).expect("close");
    assert!(pool.is_paused());
    assert!(pool.files().is_empty(), "a paused pool holds no files");

    let storage = BrowserStorage::open_in(pool).await.expect("reopen");
    let library = storage.store.library(&all()).unwrap();
    assert_eq!(library.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(), vec!["b", "a"]);
    assert!(library.iter().all(|e| e.pinned));
    assert_eq!(storage.store.kv_get("shazam.breaker").unwrap().as_deref(), Some("{\"open_until_ms\":5}"));
    assert_eq!(storage.store.cache_get("shazam", "sig").unwrap().as_deref(), Some("{}"));
    assert!(storage.interrupted.is_empty());
    storage.close().map_err(|(_, e)| e).expect("close");
}

#[wasm_bindgen_test]
async fn a_new_worker_recovers_interrupted_sessions() {
    let storage = BrowserStorage::open_in(pool("recover").await).await.expect("open");
    storage.store.create_session("live", SessionKind::Listen, "", None, 1).unwrap();
    storage.store.create_session("kept", SessionKind::Listen, "", None, 2).unwrap();
    storage
        .store
        .update_session("kept", SessionUpdate { status: Some(SessionStatus::Completed), ..Default::default() })
        .unwrap();
    storage.files.write("sessions/live/audio.pcm", &[1u8; 70_000]).unwrap();
    storage.files.write("sessions/kept/audio.pcm", &[2u8; 10]).unwrap();
    // The Worker goes away with a session running; the next one takes over.
    let pool = storage.close().map_err(|(_, e)| e).expect("close");
    let storage = BrowserStorage::open_in(pool).await.expect("reopen");
    assert_eq!(storage.interrupted, vec!["live".to_string()]);
    let detail = storage.store.session_detail("live").unwrap().unwrap();
    assert_eq!(detail.entry.status, SessionStatus::Failed);
    assert_eq!(storage.files.list("sessions/").unwrap(), vec![("sessions/kept/audio.pcm".to_string(), 10)]);
    storage.close().map_err(|(_, e)| e).expect("close");
}

#[wasm_bindgen_test]
async fn the_pool_is_released_only_when_nothing_is_open() {
    let storage = BrowserStorage::open_in(pool("pause").await).await.expect("open");
    assert!(storage.pool.pause().is_err(), "open databases keep the pool");
    let extra = storage.store.clone();
    let (storage, err) = storage.close().err().expect("still shared");
    assert!(err.to_string().contains("in use"), "{err}");
    drop(extra);
    let pool = storage.close().map_err(|(_, e)| e).expect("close");
    assert!(pool.is_paused());
    pool.resume().await.unwrap();
    assert!(!pool.is_paused());
    let storage = BrowserStorage::open_in(pool).await.expect("reopen");
    storage.close().map_err(|(_, e)| e).expect("close");
}

#[wasm_bindgen_test]
async fn a_damaged_library_is_moved_aside() {
    let storage = BrowserStorage::open_in(pool("damaged").await).await.expect("open");
    storage.store.create_session("x", SessionKind::Listen, "Lost", None, 1).unwrap();
    let pool = storage.close().map_err(|(_, e)| e).expect("close");
    pool.resume().await.unwrap();
    // Keep the header (so the import is accepted), break the schema page.
    let mut bytes = pool.export(LIBRARY_DB).unwrap();
    for b in &mut bytes[100..4096] {
        *b = 0xA5;
    }
    pool.import(LIBRARY_DB, &bytes).unwrap();
    let storage = BrowserStorage::open_in(pool).await.expect("reopen");
    assert!(storage.store.library(&all()).unwrap().is_empty(), "an empty library replaces the damaged one");
    assert!(
        storage.pool.files().iter().any(|f| f.starts_with(&format!("{LIBRARY_DB}.corrupt-"))),
        "{:?}",
        storage.pool.files()
    );
    storage.store.create_session("y", SessionKind::Listen, "", None, 2).unwrap();
    storage.close().map_err(|(_, e)| e).expect("close");
}

#[wasm_bindgen_test]
async fn work_files_on_opfs() {
    let pool = pool("files").await;
    let files = SqliteFiles::open_vfs(pool.vfs(), "conformance.sqlite3").expect("open");
    exercise_store(&files);
}

#[wasm_bindgen_test]
fn in_memory_snapshot_fallback() {
    let store = Store::open_in_memory().unwrap();
    store.create_session("m", SessionKind::Listen, "Snapshot", None, 1).unwrap();
    let bytes = store.serialize().unwrap();
    let restored = Store::open_serialized(&bytes).unwrap();
    assert_eq!(restored.library(&all()).unwrap()[0].title, "Snapshot");

    let files = SqliteFiles::open_in_memory().unwrap();
    files.write("rec/1.wav", b"RIFF").unwrap();
    let restored = SqliteFiles::open_serialized(&files.serialize().unwrap()).unwrap();
    assert_eq!(restored.read("rec/1.wav").unwrap(), b"RIFF");
}
