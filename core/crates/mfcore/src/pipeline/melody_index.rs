//! Melody index installation: manifest, streamed download with progress,
//! SHA-256 verification and an atomic swap, then the index's optional encoder
//! file the same way. See docs/PIPELINES.md ("Melody index installation") and
//! docs/CORE_API.md ("Melody index").
//!
//! In the browser the index is a work file (`melody/melody-index.mfi` in
//! `EngineCtx::files`) instead of a memory-mapped file. It is read into memory
//! only when humming needs it ([`load_installed`], in 1 MiB pieces with the
//! event loop running in between) and dropped with the Worker; a download
//! builds the index in memory as it arrives, so installing needs no second read.
//! The browser does not install the encoder file (the index works without it).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(not(target_family = "wasm"))]
use tokio::io::AsyncWriteExt;

use crate::error::{MfError, Result};
#[cfg(not(target_family = "wasm"))]
use crate::melody::encoder::EncoderIndex;
use crate::melody::{MelodyIndex, index};
use crate::model::{MelodyIndexStatus, Phase};
use crate::pipeline::ctx::{EngineCtx, SessionCtx};
use crate::util::cancel::CancelToken;
use crate::util::rt::Instant;

/// The installed index, in `data_dir/melody/`.
pub const INDEX_FILE: &str = "melody-index.mfi";
/// The manifest the installed index came with (its `encoder` entry only when
/// that file is installed too).
pub const MANIFEST_FILE: &str = "manifest.json";
/// The installed index's learned encoder (`melody-encoder-<tier>.mfe` on the server).
pub const ENCODER_FILE: &str = "melody-encoder.mfe";
const DOWNLOAD_FILE: &str = ".download";
#[cfg_attr(target_family = "wasm", allow(dead_code))]
const ENCODER_DOWNLOAD_FILE: &str = ".download-encoder";
const MANIFEST_TIMEOUT: Duration = Duration::from_secs(15);
/// Longest wait for the next piece of the download before giving up.
const STALL_TIMEOUT: Duration = Duration::from_secs(60);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const MAX_MANIFEST_BYTES: usize = 64 * 1024;
/// Upper bound for an index file (sanity check of the manifest).
const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const PROGRESS_STEP: f32 = 0.01;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

/// The published manifest (`EngineConfig.melody_index_manifest_url`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: String,
    pub url: String,
    pub sha256: String,
    pub bytes: u64,
    #[serde(default)]
    pub songs: u32,
    #[serde(default)]
    pub min_core: Option<String>,
    /// Index file format the manifest points at. Manifests published before
    /// format 2 have none and point at format 1.
    #[serde(default = "first_format")]
    pub format: u32,
    /// The index's learned-encoder file (`melody-encoder-<tier>.mfe`, see
    /// `melody/encoder.rs`). Optional: the index works without it, and cores
    /// ignore entries they cannot use ([`Manifest::usable_encoder`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoder: Option<CompanionFile>,
}

/// A file published next to the index that belongs to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompanionFile {
    pub url: String,
    pub sha256: String,
    pub bytes: u64,
    /// The file's own format number.
    pub format: u32,
    #[serde(default)]
    pub min_core: Option<String>,
}

fn first_format() -> u32 {
    1
}

/// Upper bound for an encoder file (sanity check of the manifest).
const MAX_ENCODER_BYTES: u64 = 512 * 1024 * 1024;

impl Manifest {
    fn validate(&self) -> Result<()> {
        let bad = |why: &str| MfError::network(format!("The melody index manifest is invalid ({why})."));
        if self.version.trim().is_empty() {
            return Err(bad("no version"));
        }
        if !(self.url.starts_with("https://") || self.url.starts_with("http://")) {
            return Err(bad("bad download link"));
        }
        if self.sha256.len() != 64 || !self.sha256.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(bad("bad checksum"));
        }
        if self.bytes == 0 || self.bytes > MAX_INDEX_BYTES {
            return Err(bad("bad size"));
        }
        if let Some(min) = self.min_core.as_deref()
            && compare_versions(env!("CARGO_PKG_VERSION"), min).is_lt()
        {
            return Err(MfError::IndexUnavailable);
        }
        // Another format would be downloaded only to be refused when opened.
        if self.format != index::FORMAT {
            return Err(MfError::IndexUnavailable);
        }
        Ok(())
    }

    /// The encoder file, when this core can use it. A bad or unusable entry is
    /// skipped (the index installs without it), never an error.
    pub fn usable_encoder(&self) -> Option<&CompanionFile> {
        let e = self.encoder.as_ref()?;
        let ok = (e.url.starts_with("https://") || e.url.starts_with("http://"))
            && e.sha256.len() == 64
            && e.sha256.chars().all(|c| c.is_ascii_hexdigit())
            && e.bytes > 0
            && e.bytes <= MAX_ENCODER_BYTES
            && e.format == crate::melody::encoder::FORMAT
            && e.min_core.as_deref().is_none_or(|min| compare_versions(env!("CARGO_PKG_VERSION"), min).is_ge());
        ok.then_some(e)
    }
}

/// Numeric comparison of dotted versions ("2026.10.1" < "2026.10.12").
pub fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    let parts = |v: &str| -> Vec<u64> {
        v.trim()
            .trim_start_matches('v')
            .split(['.', '-', '+'])
            .map(|p| p.chars().take_while(char::is_ascii_digit).collect::<String>().parse().unwrap_or(0))
            .collect()
    };
    let (a, b) = (parts(a), parts(b));
    for i in 0..a.len().max(b.len()) {
        let ord = a.get(i).copied().unwrap_or(0).cmp(&b.get(i).copied().unwrap_or(0));
        if ord.is_ne() {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// `data_dir/melody` (in the browser the name prefix `melody` in `files`).
pub fn melody_dir(engine: &EngineCtx) -> PathBuf {
    #[cfg(not(target_family = "wasm"))]
    return PathBuf::from(&engine.config.data_dir).join("melody");
    #[cfg(target_family = "wasm")]
    {
        let _ = engine;
        PathBuf::from("melody")
    }
}

/// `data_dir/melody/melody-index.mfi`.
pub fn index_path(engine: &EngineCtx) -> PathBuf {
    melody_dir(engine).join(INDEX_FILE)
}

/// The manifest of the installed index, if any.
pub fn installed_manifest(engine: &EngineCtx) -> Option<Manifest> {
    #[cfg(not(target_family = "wasm"))]
    let text = std::fs::read_to_string(melody_dir(engine).join(MANIFEST_FILE)).ok()?;
    #[cfg(target_family = "wasm")]
    let text = String::from_utf8(engine.files.read(&name(&melody_dir(engine).join(MANIFEST_FILE))).ok()?).ok()?;
    serde_json::from_str(&text).ok()
}

/// A path below `melody_dir` as a work-file name (browser).
#[cfg(target_family = "wasm")]
fn name(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Run melody-index code that may panic (the index is untrusted data).
fn guarded<T>(f: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(f)).ok()
}

/// Open the installed index at startup. An index of another format (installed
/// by an older or newer app version) is deleted, a damaged file moved aside
/// (`melody-index.mfi.corrupt-<ms>`), so a fresh download can replace it.
#[cfg(not(target_family = "wasm"))]
pub fn open_installed(engine: &EngineCtx) -> Option<Arc<MelodyIndex>> {
    let path = index_path(engine);
    if !path.exists() {
        return None;
    }
    if let Some(format) = index::file_format(&path)
        && format != index::FORMAT
    {
        tracing::info!("removing the format {format} melody index (this version reads format {})", index::FORMAT);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(melody_dir(engine).join(MANIFEST_FILE));
        let _ = std::fs::remove_file(melody_dir(engine).join(ENCODER_FILE));
        return None;
    }
    match catch_unwind(AssertUnwindSafe(|| MelodyIndex::open(&path))) {
        Ok(Ok(index)) => Some(Arc::new(index)),
        Ok(Err(e)) => {
            tracing::error!("melody index unreadable ({e}); moving it aside");
            let aside = path.with_file_name(format!("{INDEX_FILE}.corrupt-{}", crate::util::now_ms()));
            if let Err(e) = std::fs::rename(&path, &aside) {
                tracing::warn!("could not move the melody index aside: {e}");
            }
            let _ = std::fs::remove_file(melody_dir(engine).join(MANIFEST_FILE));
            let _ = std::fs::remove_file(melody_dir(engine).join(ENCODER_FILE));
            None
        }
        Err(_) => {
            tracing::error!("opening the melody index panicked; leaving it in place");
            None
        }
    }
}

/// Open the installed index's encoder file. A file that does not belong to
/// `index` (or is damaged) is deleted: it can never be used.
#[cfg(not(target_family = "wasm"))]
pub fn open_encoder(engine: &EngineCtx, index: &Arc<MelodyIndex>) -> Option<(Arc<MelodyIndex>, Arc<EncoderIndex>)> {
    let path = melody_dir(engine).join(ENCODER_FILE);
    if !path.exists() {
        return None;
    }
    match catch_unwind(AssertUnwindSafe(|| EncoderIndex::open(&path, index))) {
        Ok(Ok(encoder)) => Some((index.clone(), Arc::new(encoder))),
        Ok(Err(e)) => {
            tracing::warn!("removing the melody encoder file ({e})");
            let _ = std::fs::remove_file(&path);
            None
        }
        Err(_) => {
            tracing::error!("opening the melody encoder panicked; searching without it");
            None
        }
    }
}

/// The installed encoder's manifest entry, when the engine uses that file.
fn installed_encoder(
    engine: &EngineCtx,
    index: &Arc<MelodyIndex>,
    manifest: Option<&Manifest>,
) -> Option<CompanionFile> {
    engine.melody_encoder(index)?;
    manifest?.encoder.clone()
}

/// What installing `available` would still download: the index when it is
/// newer than the installed one (or none is installed), and its encoder file
/// when this core uses one and the installed index lacks that exact file.
fn pending(engine: &EngineCtx, available: &Manifest, installed: Option<&Manifest>) -> (bool, Option<CompanionFile>) {
    let index = engine.melody_index();
    let version = index.as_ref().and_then(|i| guarded(|| i.version().to_string()));
    let index_newer = match (&index, &version) {
        (Some(_), Some(v)) => compare_versions(&available.version, v).is_gt(),
        _ => true,
    };
    let current = index.as_ref().and_then(|i| installed_encoder(engine, i, installed));
    let encoder = available
        .usable_encoder()
        .filter(|e| index_newer || current.as_ref().is_none_or(|c| !c.sha256.eq_ignore_ascii_case(&e.sha256)))
        .cloned();
    // The browser installs the index alone.
    #[cfg(target_family = "wasm")]
    let encoder = encoder.filter(|_| false);
    (index_newer, encoder)
}

/// Status of the installed index plus what the last manifest check found.
pub fn status(engine: &EngineCtx, available: Option<&Manifest>) -> MelodyIndexStatus {
    let index = engine.melody_index();
    let manifest = installed_manifest(engine);
    let version = index
        .as_ref()
        .and_then(|i| guarded(|| i.version().to_string()))
        .or_else(|| manifest.as_ref().map(|m| m.version.clone()));
    let song_count =
        index.as_ref().and_then(|i| guarded(|| i.song_count())).or(manifest.as_ref().map(|m| m.songs)).unwrap_or(0);
    let encoder_installed = index.as_ref().is_some_and(|i| engine.melody_encoder(i).is_some());
    #[cfg(not(target_family = "wasm"))]
    let (size_bytes, installed) = {
        let size = |name: &str| std::fs::metadata(melody_dir(engine).join(name)).map(|m| m.len()).unwrap_or(0);
        (size(INDEX_FILE) + if encoder_installed { size(ENCODER_FILE) } else { 0 }, index.is_some())
    };
    // In the browser the index is loaded only for humming: a stored file with
    // its manifest counts as installed.
    #[cfg(target_family = "wasm")]
    let (size_bytes, installed) = {
        let size = engine.files.len(&name(&index_path(engine))).ok().flatten().unwrap_or(0);
        (size, index.is_some() || (size > 0 && manifest.as_ref().is_some_and(|m| m.bytes == size)))
    };
    let pending = available.map(|m| (m, pending(engine, m, manifest.as_ref())));
    let wanted = pending.filter(|(m, (index_newer, encoder))| {
        // Browser: an index that is stored but not loaded yet is as new as its manifest.
        let stored_current = installed
            && index.is_none()
            && manifest.as_ref().is_some_and(|stored| compare_versions(&m.version, &stored.version).is_le());
        (*index_newer && !stored_current) || encoder.is_some()
    });
    MelodyIndexStatus {
        installed,
        version: if installed { version } else { None },
        song_count: if installed { song_count } else { 0 },
        size_bytes: if installed { size_bytes } else { 0 },
        available_version: wanted.as_ref().map(|(m, _)| m.version.clone()),
        download_bytes: wanted.map(|(m, (index_newer, encoder))| {
            (if index_newer { m.bytes } else { 0 }) + encoder.map_or(0, |e| e.bytes)
        }),
        encoder_installed,
    }
}

/// Browser: reads the installed index into memory (1 MiB at a time, letting
/// the event loop run in between) and swaps it in. `None` when none is
/// installed; an index of another format is deleted and a damaged one moved
/// aside, as natively at startup.
#[cfg(target_family = "wasm")]
pub async fn load_installed(engine: &EngineCtx) -> Option<Arc<MelodyIndex>> {
    use crate::melody::index::IndexLoader;
    if let Some(index) = engine.melody_index() {
        return Some(index);
    }
    let file = name(&index_path(engine));
    let manifest = name(&melody_dir(engine).join(MANIFEST_FILE));
    let size = engine.files.len(&file).ok().flatten()?;
    let started = Instant::now();
    let mut head = [0u8; 12];
    if engine.files.read_at(&file, 0, &mut head).ok()? == head.len()
        && let Some(format) = index::header_format(&head)
        && format != index::FORMAT
    {
        tracing::info!("removing the format {format} melody index (this version reads format {})", index::FORMAT);
        let _ = engine.files.remove(&file);
        let _ = engine.files.remove(&manifest);
        return None;
    }
    let read = async {
        let mut loader = IndexLoader::new(size)?;
        let mut piece = vec![0u8; LOAD_PIECE];
        while loader.len() < size {
            let n = engine.files.read_at(&file, loader.len(), &mut piece)?;
            if n == 0 {
                return Err(MfError::Internal { message: "the melody index is incomplete".into() });
            }
            loader.push(&piece[..n])?;
            crate::util::rt::yield_now().await;
        }
        loader.finish()
    };
    match read.await {
        Ok(index) => {
            tracing::info!(bytes = size, ms = started.elapsed().as_millis() as u64, "melody index loaded");
            let index = Arc::new(index);
            *engine.melody.write() = Some(index.clone());
            Some(index)
        }
        Err(e) => {
            tracing::error!("melody index unreadable ({e}); moving it aside");
            let aside = format!("{file}.corrupt-{}", crate::util::now_ms());
            if let Err(e) = engine.files.rename(&file, &aside) {
                tracing::warn!("could not move the melody index aside: {e}");
            }
            let _ = engine.files.remove(&manifest);
            None
        }
    }
}

/// Browser: piece size of [`load_installed`].
#[cfg(target_family = "wasm")]
const LOAD_PIECE: usize = 1024 * 1024;

/// Fetch and validate the manifest.
pub async fn fetch_manifest(http: &reqwest::Client, url: &str, cancel: &CancelToken) -> Result<Manifest> {
    if url.trim().is_empty() {
        return Err(MfError::invalid("No melody index source is configured."));
    }
    let request = crate::util::http::prepare(http.get(url.trim()).timeout(MANIFEST_TIMEOUT)).send();
    let response = tokio::select! {
        r = request => r.map_err(|e| network(&e))?,
        _ = cancel.canceled() => return Err(MfError::Canceled),
    };
    if !response.status().is_success() {
        return Err(MfError::network(format!(
            "The melody index is not available (HTTP {}).",
            response.status().as_u16()
        )));
    }
    let body = tokio::select! {
        b = response.bytes() => b.map_err(|e| network(&e))?,
        _ = cancel.canceled() => return Err(MfError::Canceled),
    };
    if body.len() > MAX_MANIFEST_BYTES {
        return Err(MfError::network("The melody index manifest is too large."));
    }
    let manifest: Manifest =
        serde_json::from_slice(&body).map_err(|_| MfError::network("The melody index manifest could not be read."))?;
    manifest.validate()?;
    Ok(manifest)
}

fn network(e: &reqwest::Error) -> MfError {
    if e.is_timeout() {
        MfError::network("The download timed out.")
    } else {
        MfError::network("Couldn't reach the server.")
    }
}

/// Deletes the partial download unless disarmed.
#[cfg(not(target_family = "wasm"))]
struct Partial(Option<PathBuf>);

#[cfg(not(target_family = "wasm"))]
impl Drop for Partial {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Download, verify and install the index from `manifest_url`, then swap it
/// into the engine; then the same for the index's encoder file, when the
/// manifest names one this core can use. An index that is already current only
/// gets its missing encoder file. Emits `Phase(Downloading, fraction)` over
/// both files; the caller emits the terminal event.
#[cfg(not(target_family = "wasm"))]
pub async fn install(ctx: &SessionCtx, manifest_url: &str) -> Result<Manifest> {
    let engine = &ctx.engine;
    ctx.emitter.phase(Phase::Downloading, Some(0.0));
    let manifest = fetch_manifest(&engine.http, manifest_url, &ctx.cancel).await?;
    let dir = melody_dir(engine);
    std::fs::create_dir_all(&dir)
        .map_err(|_| MfError::Storage { message: "The app folder is not writable.".into() })?;

    let current = installed_manifest(engine);
    let same_index = engine.melody_index().is_some()
        && current
            .as_ref()
            .is_some_and(|m| m.version == manifest.version && m.sha256.eq_ignore_ascii_case(&manifest.sha256));
    let (_, wanted_encoder) = pending(engine, &manifest, current.as_ref());
    let encoder_bytes = wanted_encoder.as_ref().map_or(0, |e| e.bytes);

    if same_index {
        if let (Some(file), Some(index)) = (wanted_encoder, engine.melody_index()) {
            // Only the encoder is missing (or outdated): fetching it is the whole job.
            let progress = Progress { done: 0, total: file.bytes };
            install_encoder(ctx, &file, &index, progress).await?;
            let kept = Manifest { encoder: Some(file), ..current.clone().unwrap_or_else(|| manifest.clone()) };
            write_manifest(&dir, &kept)?;
        }
        ctx.emitter.phase(Phase::Downloading, Some(1.0));
        return Ok(manifest);
    }

    let partial_path = dir.join(DOWNLOAD_FILE);
    let mut partial = Partial(Some(partial_path.clone()));
    let progress = Progress { done: 0, total: manifest.bytes + encoder_bytes };
    download(ctx, &manifest.url, &manifest.sha256, manifest.bytes, &partial_path, progress, "melody index").await?;
    ctx.cancel.check()?;

    let open_path = partial_path.clone();
    let opened =
        crate::util::rt::spawn_blocking(move || guarded(|| MelodyIndex::open(&open_path))).await.ok().flatten();
    let index = match opened {
        Some(Ok(index)) => Arc::new(index),
        _ => return Err(MfError::internal("the downloaded melody index could not be opened")),
    };
    ctx.cancel.check()?;

    // The old encoder file belongs to the old index.
    *engine.encoder.write() = None;
    let _ = std::fs::remove_file(dir.join(ENCODER_FILE));
    let final_path = dir.join(INDEX_FILE);
    std::fs::rename(&partial_path, &final_path)
        .map_err(|_| MfError::Storage { message: "The melody index could not be saved.".into() })?;
    partial.0 = None;
    write_manifest(&dir, &Manifest { encoder: None, ..manifest.clone() })?;
    *engine.melody.write() = Some(index.clone());

    if let Some(file) = wanted_encoder {
        let progress = Progress { done: manifest.bytes, total: manifest.bytes + file.bytes };
        match install_encoder(ctx, &file, &index, progress).await {
            Ok(()) => write_manifest(&dir, &Manifest { encoder: Some(file), ..manifest.clone() })?,
            // The index is installed and works alone; the status offers the encoder again.
            Err(MfError::Canceled) => return Err(MfError::Canceled),
            Err(e) => tracing::warn!("melody encoder not installed: {e}"),
        }
    }
    ctx.emitter.phase(Phase::Downloading, Some(1.0));
    Ok(manifest)
}

/// Download, verify and open the encoder file for `index`, then swap it in.
#[cfg(not(target_family = "wasm"))]
async fn install_encoder(
    ctx: &SessionCtx,
    file: &CompanionFile,
    index: &Arc<MelodyIndex>,
    progress: Progress,
) -> Result<()> {
    let engine = &ctx.engine;
    let dir = melody_dir(engine);
    let partial_path = dir.join(ENCODER_DOWNLOAD_FILE);
    let mut partial = Partial(Some(partial_path.clone()));
    download(ctx, &file.url, &file.sha256, file.bytes, &partial_path, progress, "melody encoder").await?;
    ctx.cancel.check()?;
    let open_path = partial_path.clone();
    let for_index = index.clone();
    let opened = crate::util::rt::spawn_blocking(move || guarded(|| EncoderIndex::open(&open_path, &for_index)))
        .await
        .ok()
        .flatten();
    let encoder = match opened {
        Some(Ok(encoder)) => encoder,
        // Another index's file, or damaged in a way the checksum cannot see.
        Some(Err(e)) => return Err(MfError::internal(format!("the downloaded melody encoder is unusable ({e})"))),
        None => return Err(MfError::internal("the downloaded melody encoder could not be opened")),
    };
    ctx.cancel.check()?;
    *engine.encoder.write() = None;
    std::fs::rename(&partial_path, dir.join(ENCODER_FILE))
        .map_err(|_| MfError::Storage { message: "The melody encoder could not be saved.".into() })?;
    partial.0 = None;
    // Only for the index the engine still has (a removal may have raced us).
    if engine.melody_index().is_some_and(|i| Arc::ptr_eq(&i, index)) {
        *engine.encoder.write() = Some((index.clone(), Arc::new(encoder)));
    }
    Ok(())
}

/// Download progress over several files: `done` bytes before this one, `total` overall.
#[cfg(not(target_family = "wasm"))]
#[derive(Debug, Clone, Copy)]
struct Progress {
    done: u64,
    total: u64,
}

#[cfg(not(target_family = "wasm"))]
async fn download(
    ctx: &SessionCtx,
    url: &str,
    sha256: &str,
    bytes: u64,
    path: &Path,
    progress: Progress,
    what: &str,
) -> Result<()> {
    let request = crate::util::http::prepare(ctx.engine.http.get(url).timeout(DOWNLOAD_TIMEOUT)).send();
    let mut response = tokio::select! {
        r = request => r.map_err(|e| network(&e))?,
        _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
    };
    if !response.status().is_success() {
        return Err(MfError::network(format!("The {what} download failed (HTTP {}).", response.status().as_u16())));
    }
    let storage = |_| MfError::Storage { message: format!("The {what} could not be saved.") };
    let mut file = tokio::fs::File::create(path).await.map_err(storage)?;
    let mut hasher = Sha256::new();
    let mut written: u64 = 0;
    let mut reported = (0f32, Instant::now());
    loop {
        let next = tokio::select! {
            r = crate::util::rt::timeout(STALL_TIMEOUT, response.chunk()) => r,
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
        };
        let chunk = match next {
            Ok(Ok(Some(chunk))) => chunk,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => return Err(network(&e)),
            Err(_) => return Err(MfError::network("The download stalled.")),
        };
        written += chunk.len() as u64;
        if written > bytes {
            return Err(MfError::network("The download was damaged. Try again."));
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(storage)?;
        let fraction = (progress.done + written) as f32 / progress.total.max(1) as f32;
        if fraction - reported.0 >= PROGRESS_STEP && reported.1.elapsed() >= PROGRESS_INTERVAL {
            reported = (fraction, Instant::now());
            ctx.emitter.phase(Phase::Downloading, Some(fraction.min(0.99)));
        }
    }
    file.flush().await.map_err(storage)?;
    file.sync_all().await.map_err(storage)?;
    drop(file);
    let digest: String = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if written != bytes || !digest.eq_ignore_ascii_case(sha256) {
        tracing::warn!(written, expected = bytes, "{what} download failed verification");
        return Err(MfError::network("The download was damaged. Try again."));
    }
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let tmp = dir.join(".manifest.json.tmp");
    let json = serde_json::to_vec_pretty(manifest)?;
    let save = || -> std::io::Result<()> {
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, dir.join(MANIFEST_FILE))
    };
    save().map_err(|_| MfError::Storage { message: "The melody index could not be saved.".into() })
}

/// Remove the installed index, its encoder and its manifest.
pub fn remove(engine: &EngineCtx) -> Result<()> {
    *engine.encoder.write() = None;
    *engine.melody.write() = None;
    let dir = melody_dir(engine);
    for file in [INDEX_FILE, MANIFEST_FILE, DOWNLOAD_FILE, ENCODER_FILE, ENCODER_DOWNLOAD_FILE] {
        #[cfg(not(target_family = "wasm"))]
        match std::fs::remove_file(dir.join(file)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(MfError::Storage { message: "The melody index could not be removed.".into() }),
        }
        #[cfg(target_family = "wasm")]
        if engine.files.remove(&name(&dir.join(file))).is_err() {
            return Err(MfError::Storage { message: "The melody index could not be removed.".into() });
        }
    }
    Ok(())
}

/// Browser: the index downloads into a work file and into memory at once
/// (SHA-256 on the way), so it is ready without reading the file back.
#[cfg(target_family = "wasm")]
pub async fn install(ctx: &SessionCtx, manifest_url: &str) -> Result<Manifest> {
    let engine = &ctx.engine;
    ctx.emitter.phase(Phase::Downloading, Some(0.0));
    let manifest = fetch_manifest(&engine.http, manifest_url, &ctx.cancel).await?;
    let dir = melody_dir(engine);
    let current = installed_manifest(engine);
    let same = |m: &Manifest| m.version == manifest.version && m.sha256.eq_ignore_ascii_case(&manifest.sha256);
    if current.as_ref().is_some_and(same) && status(engine, None).installed {
        ctx.emitter.phase(Phase::Downloading, Some(1.0));
        return Ok(manifest);
    }
    let partial = name(&dir.join(DOWNLOAD_FILE));
    let downloaded = download_into_memory(ctx, &manifest, &partial).await;
    let index = match downloaded.and_then(|loader| {
        ctx.cancel.check()?;
        guarded(|| loader.finish()).unwrap_or_else(|| Err(MfError::internal("melody index")))
    }) {
        Ok(index) => index,
        Err(e) => {
            let _ = engine.files.remove(&partial);
            return Err(match e {
                MfError::Canceled | MfError::Network { .. } | MfError::Storage { .. } | MfError::IndexUnavailable => e,
                _ => MfError::internal("the downloaded melody index could not be opened"),
            });
        }
    };
    let saved = engine.files.rename(&partial, &name(&dir.join(INDEX_FILE))).and_then(|()| {
        // Without the encoder entry: the browser installs the index alone.
        let json = serde_json::to_vec_pretty(&Manifest { encoder: None, ..manifest.clone() })?;
        engine.files.write(&name(&dir.join(MANIFEST_FILE)), &json)
    });
    if saved.is_err() {
        let _ = engine.files.remove(&partial);
        return Err(MfError::Storage { message: "The melody index could not be saved.".into() });
    }
    *engine.melody.write() = Some(Arc::new(index));
    ctx.emitter.phase(Phase::Downloading, Some(1.0));
    Ok(manifest)
}

#[cfg(target_family = "wasm")]
async fn download_into_memory(
    ctx: &SessionCtx,
    manifest: &Manifest,
    partial: &str,
) -> Result<crate::melody::index::IndexLoader> {
    use futures_util::StreamExt;
    let request = crate::util::http::prepare(ctx.engine.http.get(&manifest.url).timeout(DOWNLOAD_TIMEOUT)).send();
    let response = tokio::select! {
        r = request => r.map_err(|e| network(&e))?,
        _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
    };
    if !response.status().is_success() {
        return Err(MfError::network(format!(
            "The melody index download failed (HTTP {}).",
            response.status().as_u16()
        )));
    }
    let storage = |_| MfError::Storage { message: "The melody index could not be saved.".into() };
    let damaged = || MfError::network("The download was damaged. Try again.");
    let mut loader = crate::melody::index::IndexLoader::new(manifest.bytes).map_err(|e| match e {
        MfError::IndexUnavailable => damaged(),
        other => other,
    })?;
    let mut file = ctx.engine.files.create(partial).map_err(storage)?;
    let mut hasher = Sha256::new();
    let mut reported = (0f32, Instant::now());
    let mut stream = response.bytes_stream();
    loop {
        let next = tokio::select! {
            r = crate::util::rt::timeout(STALL_TIMEOUT, stream.next()) => r,
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
        };
        let chunk = match next {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(None) => break,
            Ok(Some(Err(e))) => return Err(network(&e)),
            Err(_) => return Err(MfError::network("The download stalled.")),
        };
        if loader.len() + chunk.len() as u64 > manifest.bytes {
            return Err(damaged());
        }
        hasher.update(&chunk);
        loader.push(&chunk).map_err(|e| match e {
            MfError::IndexUnavailable => MfError::IndexUnavailable,
            _ => damaged(),
        })?;
        file.append(&chunk).map_err(storage)?;
        let fraction = loader.len() as f32 / manifest.bytes as f32;
        if fraction - reported.0 >= PROGRESS_STEP && reported.1.elapsed() >= PROGRESS_INTERVAL {
            reported = (fraction, Instant::now());
            ctx.emitter.phase(Phase::Downloading, Some(fraction.min(0.99)));
        }
    }
    file.finish().map_err(storage)?;
    let digest: String = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if loader.len() != manifest.bytes || !digest.eq_ignore_ascii_case(&manifest.sha256) {
        tracing::warn!(written = loader.len(), expected = manifest.bytes, "melody index download failed verification");
        return Err(damaged());
    }
    Ok(loader)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_numerically() {
        use std::cmp::Ordering::*;
        assert_eq!(compare_versions("2026.10.1", "2026.10.12"), Less);
        assert_eq!(compare_versions("2026.10", "2026.10.0"), Equal);
        assert_eq!(compare_versions("v1.2.0", "1.1.9"), Greater);
        assert_eq!(compare_versions("1.0.0", "1.0.0-beta"), Equal);
    }

    #[test]
    fn manifest_validation() {
        let good = Manifest {
            version: "2026.10.1".into(),
            url: "https://example.com/melody-index.mfi".into(),
            sha256: "a".repeat(64),
            bytes: 10,
            songs: 3,
            min_core: Some("1.1.0".into()),
            format: index::FORMAT,
            encoder: None,
        };
        assert!(good.validate().is_ok());
        // A manifest of another index format, or one from before formats were named (format 1).
        assert_eq!(Manifest { format: index::FORMAT + 1, ..good.clone() }.validate(), Err(MfError::IndexUnavailable));
        assert!(Manifest { sha256: "xyz".into(), ..good.clone() }.validate().is_err());
        assert!(Manifest { url: "file:///etc/passwd".into(), ..good.clone() }.validate().is_err());
        assert!(Manifest { bytes: 0, ..good.clone() }.validate().is_err());
        assert_eq!(
            Manifest { min_core: Some("99.0".into()), ..good.clone() }.validate(),
            Err(MfError::IndexUnavailable)
        );
        let parsed: Manifest =
            serde_json::from_str(r#"{"version": "2026.10.1", "url": "https://x/y.mfi", "sha256": "00", "bytes": 1}"#)
                .unwrap();
        assert_eq!(parsed.songs, 0);
        assert_eq!(parsed.min_core, None);
        assert_eq!(parsed.format, 1);
        assert_eq!(Manifest { sha256: "a".repeat(64), ..parsed }.validate(), Err(MfError::IndexUnavailable));
    }

    #[test]
    fn the_encoder_entry_is_optional() {
        let text = r#"{"version": "2026.10.1", "url": "https://x/melody-index.mfi", "sha256": "SHA", "bytes": 10,
            "songs": 3, "min_core": "1.1.0", "format": 2,
            "encoder": {"url": "https://x/melody-encoder-android.mfe", "sha256": "SHA", "bytes": 13000000,
                        "format": 1, "min_core": "1.1.0"}}"#
            .replace("SHA", &"b".repeat(64));
        let m: Manifest = serde_json::from_str(&text).unwrap();
        assert!(m.validate().is_ok());
        let e = m.usable_encoder().expect("encoder");
        assert_eq!((e.bytes, e.format), (13_000_000, 1));
        // A newer encoder format, a newer core or a bad checksum: the index alone.
        let with = |f: &dyn Fn(&mut CompanionFile)| {
            let mut m = m.clone();
            f(m.encoder.as_mut().unwrap());
            m
        };
        assert!(with(&|e| e.format = 2).usable_encoder().is_none());
        assert!(with(&|e| e.min_core = Some("99.0".into())).usable_encoder().is_none());
        assert!(with(&|e| e.sha256 = "00".into()).usable_encoder().is_none());
        assert!(with(&|e| e.format = 2).validate().is_ok());
        // Without one, and written back without one.
        let plain = Manifest { encoder: None, ..m };
        assert!(plain.usable_encoder().is_none());
        assert!(!serde_json::to_string(&plain).unwrap().contains("encoder"));
    }

    #[tokio::test]
    async fn an_index_of_another_format_is_removed_at_startup() {
        let dir = std::env::temp_dir().join(format!("mf-melody-format-{}", crate::util::new_id()));
        let (ctx, _) = crate::pipeline::testutil::session(&dir, crate::model::SessionKind::Hum);
        let melody = melody_dir(&ctx.engine);
        std::fs::create_dir_all(&melody).unwrap();
        let mut old = b"MFMELODY".to_vec();
        old.extend(1u32.to_le_bytes());
        old.resize(4096, 0);
        std::fs::write(index_path(&ctx.engine), &old).unwrap();
        std::fs::write(melody.join(MANIFEST_FILE), "{}").unwrap();
        assert!(open_installed(&ctx.engine).is_none());
        assert!(!index_path(&ctx.engine).exists());
        assert!(!melody.join(MANIFEST_FILE).exists());
        assert!(std::fs::read_dir(&melody).unwrap().next().is_none(), "nothing is kept aside");
        ctx.cleanup();
        let _ = std::fs::remove_dir_all(dir);
    }
}
