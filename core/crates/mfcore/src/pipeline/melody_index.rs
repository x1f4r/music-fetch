//! Melody index installation: manifest, streamed download with progress,
//! SHA-256 verification and an atomic swap. See docs/PIPELINES.md ("Melody
//! index installation") and docs/CORE_API.md ("Melody index").

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::error::{MfError, Result};
use crate::melody::{MelodyIndex, index};
use crate::model::{MelodyIndexStatus, Phase};
use crate::pipeline::ctx::{EngineCtx, SessionCtx};
use crate::util::cancel::CancelToken;

/// The installed index, in `data_dir/melody/`.
pub const INDEX_FILE: &str = "melody-index.mfi";
/// The manifest the installed index came with.
pub const MANIFEST_FILE: &str = "manifest.json";
const DOWNLOAD_FILE: &str = ".download";
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
}

fn first_format() -> u32 {
    1
}

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

/// `data_dir/melody`.
pub fn melody_dir(engine: &EngineCtx) -> PathBuf {
    PathBuf::from(&engine.config.data_dir).join("melody")
}

/// `data_dir/melody/melody-index.mfi`.
pub fn index_path(engine: &EngineCtx) -> PathBuf {
    melody_dir(engine).join(INDEX_FILE)
}

/// The manifest of the installed index, if any.
pub fn installed_manifest(engine: &EngineCtx) -> Option<Manifest> {
    let text = std::fs::read_to_string(melody_dir(engine).join(MANIFEST_FILE)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Run melody-index code that may panic (the index is untrusted data).
fn guarded<T>(f: impl FnOnce() -> T) -> Option<T> {
    catch_unwind(AssertUnwindSafe(f)).ok()
}

/// Open the installed index at startup. An index of another format (installed
/// by an older or newer app version) is deleted, a damaged file moved aside
/// (`melody-index.mfi.corrupt-<ms>`), so a fresh download can replace it.
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
            None
        }
        Err(_) => {
            tracing::error!("opening the melody index panicked; leaving it in place");
            None
        }
    }
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
    let size_bytes = std::fs::metadata(index_path(engine)).map(|m| m.len()).unwrap_or(0);
    let newer = available.filter(|m| match (&version, index.is_some()) {
        (Some(v), true) => compare_versions(&m.version, v).is_gt(),
        _ => true,
    });
    MelodyIndexStatus {
        installed: index.is_some(),
        version: if index.is_some() { version } else { None },
        song_count: if index.is_some() { song_count } else { 0 },
        size_bytes: if index.is_some() { size_bytes } else { 0 },
        available_version: newer.map(|m| m.version.clone()),
        download_bytes: newer.map(|m| m.bytes),
    }
}

/// Fetch and validate the manifest.
pub async fn fetch_manifest(http: &reqwest::Client, url: &str, cancel: &CancelToken) -> Result<Manifest> {
    if url.trim().is_empty() {
        return Err(MfError::invalid("No melody index source is configured."));
    }
    let request = http.get(url.trim()).timeout(MANIFEST_TIMEOUT).send();
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
struct Partial(Option<PathBuf>);

impl Drop for Partial {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Download, verify and install the index from `manifest_url`, then swap it
/// into the engine. Emits `Phase(Downloading, fraction)`; the caller emits the
/// terminal event.
pub async fn install(ctx: &SessionCtx, manifest_url: &str) -> Result<Manifest> {
    let engine = &ctx.engine;
    ctx.emitter.phase(Phase::Downloading, Some(0.0));
    let manifest = fetch_manifest(&engine.http, manifest_url, &ctx.cancel).await?;
    let dir = melody_dir(engine);
    std::fs::create_dir_all(&dir)
        .map_err(|_| MfError::Storage { message: "The app folder is not writable.".into() })?;

    let current = installed_manifest(engine);
    if engine.melody_index().is_some()
        && current
            .as_ref()
            .is_some_and(|m| m.version == manifest.version && m.sha256.eq_ignore_ascii_case(&manifest.sha256))
    {
        ctx.emitter.phase(Phase::Downloading, Some(1.0));
        return Ok(manifest);
    }

    let partial_path = dir.join(DOWNLOAD_FILE);
    let mut partial = Partial(Some(partial_path.clone()));
    download(ctx, &manifest, &partial_path).await?;
    ctx.cancel.check()?;

    let open_path = partial_path.clone();
    let opened = tokio::task::spawn_blocking(move || guarded(|| MelodyIndex::open(&open_path))).await.ok().flatten();
    let index = match opened {
        Some(Ok(index)) => index,
        _ => return Err(MfError::internal("the downloaded melody index could not be opened")),
    };
    ctx.cancel.check()?;

    let final_path = dir.join(INDEX_FILE);
    std::fs::rename(&partial_path, &final_path)
        .map_err(|_| MfError::Storage { message: "The melody index could not be saved.".into() })?;
    partial.0 = None;
    write_manifest(&dir, &manifest)?;
    *engine.melody.write() = Some(Arc::new(index));
    ctx.emitter.phase(Phase::Downloading, Some(1.0));
    Ok(manifest)
}

async fn download(ctx: &SessionCtx, manifest: &Manifest, path: &Path) -> Result<()> {
    let request = ctx.engine.http.get(&manifest.url).timeout(DOWNLOAD_TIMEOUT).send();
    let mut response = tokio::select! {
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
    let mut file = tokio::fs::File::create(path).await.map_err(storage)?;
    let mut hasher = Sha256::new();
    let mut written: u64 = 0;
    let mut reported = (0f32, Instant::now());
    loop {
        let next = tokio::select! {
            r = tokio::time::timeout(STALL_TIMEOUT, response.chunk()) => r,
            _ = ctx.cancel.canceled() => return Err(MfError::Canceled),
        };
        let chunk = match next {
            Ok(Ok(Some(chunk))) => chunk,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => return Err(network(&e)),
            Err(_) => return Err(MfError::network("The download stalled.")),
        };
        written += chunk.len() as u64;
        if written > manifest.bytes {
            return Err(MfError::network("The download was damaged. Try again."));
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(storage)?;
        let fraction = written as f32 / manifest.bytes as f32;
        if fraction - reported.0 >= PROGRESS_STEP && reported.1.elapsed() >= PROGRESS_INTERVAL {
            reported = (fraction, Instant::now());
            ctx.emitter.phase(Phase::Downloading, Some(fraction.min(0.99)));
        }
    }
    file.flush().await.map_err(storage)?;
    file.sync_all().await.map_err(storage)?;
    drop(file);
    let digest: String = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();
    if written != manifest.bytes || !digest.eq_ignore_ascii_case(&manifest.sha256) {
        tracing::warn!(written, expected = manifest.bytes, "melody index download failed verification");
        return Err(MfError::network("The download was damaged. Try again."));
    }
    Ok(())
}

fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let tmp = dir.join(".manifest.json.tmp");
    let json = serde_json::to_vec_pretty(manifest)?;
    let save = || -> std::io::Result<()> {
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, dir.join(MANIFEST_FILE))
    };
    save().map_err(|_| MfError::Storage { message: "The melody index could not be saved.".into() })
}

/// Remove the installed index and its manifest.
pub fn remove(engine: &EngineCtx) -> Result<()> {
    *engine.melody.write() = None;
    let dir = melody_dir(engine);
    for name in [INDEX_FILE, MANIFEST_FILE, DOWNLOAD_FILE] {
        match std::fs::remove_file(dir.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(MfError::Storage { message: "The melody index could not be removed.".into() }),
        }
    }
    Ok(())
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
