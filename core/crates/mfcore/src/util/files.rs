//! Named byte files for engine work data: hum recordings, per-session PCM and
//! the melody index.
//!
//! Natively they are files in a directory ([`DirFiles`]). In the browser they
//! live in a SQLite database of chunked blobs (`SqliteFiles`, with the `store`
//! feature): OPFS file handles are async-only, while the engine reads and writes
//! work files synchronously, and SQLite's incremental blob I/O is synchronous on
//! the OPFS "sahpool" VFS.
//!
//! Names are relative, `/`-separated paths such as `sessions/<id>/audio.pcm`;
//! [`check_name`] rejects absolute paths, `..`, empty segments and backslashes.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{MfError, Result};

/// A store of named files. Implementations are thread-safe.
pub trait FileStore: Send + Sync {
    /// Size of `name` in bytes, `None` when it does not exist.
    fn len(&self, name: &str) -> Result<Option<u64>>;

    /// Reads up to `buf.len()` bytes at `offset`; returns how many were read
    /// (fewer at the end of the file, 0 past it).
    fn read_at(&self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Creates `name` (replacing an existing file) for writing.
    fn create(&self, name: &str) -> Result<Box<dyn FileWriter>>;

    /// Deletes `name`; false when it did not exist.
    fn remove(&self, name: &str) -> Result<bool>;

    /// Renames `from` to `to`, replacing `to`.
    fn rename(&self, from: &str, to: &str) -> Result<()>;

    /// Files whose names start with `prefix`, with their sizes, sorted by name.
    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>>;

    /// Whole contents of `name`.
    fn read(&self, name: &str) -> Result<Vec<u8>> {
        let len = self.len(name)?.ok_or_else(|| not_found(name))?;
        let mut out = vec![0u8; usize::try_from(len).map_err(|_| MfError::internal("file too large"))?];
        let mut filled = 0;
        while filled < out.len() {
            let n = self.read_at(name, filled as u64, &mut out[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        out.truncate(filled);
        Ok(out)
    }

    /// Creates or replaces `name` with `bytes`.
    fn write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let mut writer = self.create(name)?;
        writer.write_at(0, bytes)?;
        writer.finish().map(drop)
    }

    /// Deletes every file whose name starts with `prefix`; returns how many.
    fn remove_prefix(&self, prefix: &str) -> Result<u32> {
        let mut removed = 0;
        for (name, _) in self.list(prefix)? {
            if self.remove(&name)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Total size of the files whose names start with `prefix`.
    fn total_len(&self, prefix: &str) -> Result<u64> {
        Ok(self.list(prefix)?.iter().map(|(_, len)| len).sum())
    }
}

/// Writes one file. Bytes not written (gaps left by positional writes) read as
/// zeros. The file is complete after [`finish`](Self::finish); dropping the
/// writer keeps what was flushed so far.
pub trait FileWriter: Send {
    /// Writes `bytes` at `offset`.
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<()>;

    /// Appends `bytes` at the current end.
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self.len();
        self.write_at(end, bytes)
    }

    /// Current length (highest offset written).
    fn len(&self) -> u64;

    /// True when nothing was written yet.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Flushes and closes the file; returns its length.
    fn finish(self: Box<Self>) -> Result<u64>;
}

pub(crate) fn not_found(name: &str) -> MfError {
    MfError::internal(format!("work file not found: {name}"))
}

/// Rejects names that are not plain relative paths.
pub fn check_name(name: &str) -> Result<()> {
    let bad = name.is_empty()
        || name.starts_with('/')
        || name.ends_with('/')
        || name.contains('\\')
        || name.contains('\0')
        || name.split('/').any(|part| part.is_empty() || part == "." || part == "..");
    if bad { Err(MfError::internal(format!("invalid work file name: {name:?}"))) } else { Ok(()) }
}

/// Files in a directory on disk.
#[derive(Debug, Clone)]
pub struct DirFiles {
    root: PathBuf,
}

impl DirFiles {
    /// Files under `root` (created on the first write).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Path of `name` on disk.
    pub fn path(&self, name: &str) -> Result<PathBuf> {
        check_name(name)?;
        Ok(self.root.join(name))
    }

    fn io(&self, action: &str, name: &str, e: std::io::Error) -> MfError {
        MfError::internal(format!("cannot {action} {name}: {e}"))
    }
}

impl FileStore for DirFiles {
    fn len(&self, name: &str) -> Result<Option<u64>> {
        match std::fs::metadata(self.path(name)?) {
            Ok(meta) if meta.is_file() => Ok(Some(meta.len())),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(self.io("read", name, e)),
        }
    }

    fn read_at(&self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut file = File::open(self.path(name)?).map_err(|e| self.io("open", name, e))?;
        file.seek(SeekFrom::Start(offset)).map_err(|e| self.io("seek", name, e))?;
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(self.io("read", name, e)),
            }
        }
        Ok(filled)
    }

    fn create(&self, name: &str) -> Result<Box<dyn FileWriter>> {
        let path = self.path(name)?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| self.io("create the folder of", name, e))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .map_err(|e| self.io("create", name, e))?;
        Ok(Box::new(DirWriter { file, name: name.to_string(), len: 0, pos: 0 }))
    }

    fn remove(&self, name: &str) -> Result<bool> {
        match std::fs::remove_file(self.path(name)?) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(self.io("delete", name, e)),
        }
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let target = self.path(to)?;
        if let Some(dir) = target.parent() {
            std::fs::create_dir_all(dir).map_err(|e| self.io("create the folder of", to, e))?;
        }
        std::fs::rename(self.path(from)?, target).map_err(|e| self.io("rename", from, e))
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        let mut stack = vec![(self.root.clone(), String::new())];
        while let Some((dir, rel)) = stack.pop() {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(MfError::internal(format!("cannot list {}: {e}", dir.display()))),
            };
            for entry in entries.flatten() {
                let Ok(file_name) = entry.file_name().into_string() else { continue };
                let name = if rel.is_empty() { file_name } else { format!("{rel}/{file_name}") };
                let Ok(meta) = entry.metadata() else { continue };
                if meta.is_dir() {
                    // Only descend where names can still match the prefix.
                    let as_dir = format!("{name}/");
                    if as_dir.starts_with(prefix) || prefix.starts_with(&as_dir) {
                        stack.push((entry.path(), name));
                    }
                } else if meta.is_file() && name.starts_with(prefix) {
                    out.push((name, meta.len()));
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

struct DirWriter {
    file: File,
    name: String,
    len: u64,
    /// Position of the OS file cursor, to skip seeks for sequential writes.
    pos: u64,
}

impl FileWriter for DirWriter {
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        let io = |e: std::io::Error| MfError::internal(format!("cannot write {}: {e}", self.name));
        if offset != self.pos {
            self.file.seek(SeekFrom::Start(offset)).map_err(io)?;
        }
        self.file.write_all(bytes).map_err(io)?;
        self.pos = offset + bytes.len() as u64;
        self.len = self.len.max(self.pos);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn finish(self: Box<Self>) -> Result<u64> {
        let DirWriter { mut file, name, len, .. } = *self;
        file.flush().map_err(|e| MfError::internal(format!("cannot write {name}: {e}")))?;
        Ok(len)
    }
}

// Conformance checks shared by the backend tests here and the browser suite
// (`tests/wasm_store.rs`), which includes the same file.
#[cfg(test)]
include!("files_conformance.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::testutil::TempDir;

    #[test]
    fn names() {
        for good in ["a", "a/b.pcm", "sessions/1f2e/audio.pcm", "melody/index-2.mfi"] {
            check_name(good).unwrap();
        }
        for bad in ["", "/a", "a/", "a//b", "..", "a/../b", "a/./b", "a\\b"] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn directory_backend() {
        let dir = TempDir::new("files");
        let files = DirFiles::new(dir.path().join("work"));
        exercise_store(&files);
        assert_eq!(files.path("x/y").unwrap(), dir.path().join("work/x/y"));
    }
}
