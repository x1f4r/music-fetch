//! [`FileStore`] in a SQLite database (`files.sqlite3` in the browser).
//!
//! Each file is a row in `files` (name, size) plus blob rows in `chunks` of at
//! most [`CHUNK`] bytes. Writes go through incremental blob I/O, one short
//! transaction per flush (at most [`FLUSH`] bytes), so a recycled Worker never
//! waits behind a long write. Chunks that were never written are absent and
//! read as zeros; the last chunk is cut to size when the writer finishes.
//!
//! Large chunks keep random reads fast (a 384 KB read is one blob read, not a
//! join over many rows) and avoid huge `zeroblob` allocations. Deleted pages
//! are kept for reuse and released by [`SqliteFiles::reclaim`] (the database
//! uses `auto_vacuum = INCREMENTAL`; VACUUM would need the whole database in
//! memory in the browser).

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{Connection, MAIN_DB, OptionalExtension, TransactionBehavior, params};

use crate::error::{MfError, Result};
use crate::util::files::{FileStore, FileWriter, check_name, not_found};
use crate::util::now_ms;

/// Bytes per blob chunk.
pub const CHUNK: usize = 4 << 20;
/// Sequential bytes buffered by a writer before they are written.
const FLUSH: usize = 1 << 20;
/// Free space released automatically after a delete.
const AUTO_RECLAIM_BYTES: i64 = 64 << 20;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS files (
    name       TEXT PRIMARY KEY NOT NULL,
    size       INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS chunks (
    name TEXT NOT NULL,
    idx  INTEGER NOT NULL,
    data BLOB NOT NULL
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS chunks_by_name ON chunks (name, idx);
";

const DAMAGED_PREFIX: &str = "work files are damaged";

fn db(e: rusqlite::Error) -> MfError {
    use rusqlite::ErrorCode;
    match e.sqlite_error_code() {
        Some(ErrorCode::NotADatabase | ErrorCode::DatabaseCorrupt) => {
            MfError::Storage { message: format!("{DAMAGED_PREFIX}: {e}") }
        }
        _ => MfError::Storage { message: format!("work files: {e}") },
    }
}

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Named files in one SQLite database. Thread-safe.
pub struct SqliteFiles {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteFiles {
    /// Opens (creating it if needed) the database file at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        Self::init(Connection::open(path).map_err(db)?)
    }

    /// An empty database in memory.
    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory().map_err(db)?)
    }

    /// Opens `name` in the SQLite VFS `vfs` (in the browser the OPFS pool, see
    /// `store::opfs`).
    pub fn open_vfs(vfs: &str, name: &str) -> Result<Self> {
        use rusqlite::OpenFlags;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Self::init(Connection::open_with_flags_and_vfs(name, flags, vfs).map_err(db)?)
    }

    /// A database restored from [`serialize`](Self::serialize) output, in memory
    /// (the fallback where OPFS is unavailable).
    pub fn open_serialized(bytes: &[u8]) -> Result<Self> {
        let mut conn = Connection::open_in_memory().map_err(db)?;
        conn.deserialize_read_exact(MAIN_DB, bytes, bytes.len(), false).map_err(db)?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        // Page size and auto-vacuum only take effect before the first table exists.
        let fresh: bool = conn.query_row("SELECT count(*) = 0 FROM sqlite_schema", [], |row| row.get(0)).map_err(db)?;
        if fresh {
            conn.execute_batch("PRAGMA page_size = 65536; PRAGMA auto_vacuum = INCREMENTAL;").map_err(db)?;
        }
        // A rollback journal: WAL needs shared memory, which the browser VFS lacks.
        let _: String = conn.query_row("PRAGMA journal_mode = TRUNCATE", [], |row| row.get(0)).map_err(db)?;
        conn.execute_batch("PRAGMA synchronous = NORMAL; PRAGMA trusted_schema = OFF;").map_err(db)?;
        conn.execute_batch(SCHEMA).map_err(db)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// True when `e` (from an open) means the database file is damaged.
    pub fn is_damaged_error(e: &MfError) -> bool {
        matches!(e, MfError::Storage { message } if message.starts_with(DAMAGED_PREFIX))
    }

    /// The whole database as bytes (for a snapshot in IndexedDB).
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let conn = self.conn.lock();
        Ok(conn.serialize(MAIN_DB).map_err(db)?.to_vec())
    }

    /// Size of the database in bytes, including free pages.
    pub fn database_bytes(&self) -> u64 {
        let conn = self.conn.lock();
        conn.query_row("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|n| u64::try_from(n).unwrap_or(0))
        .unwrap_or(0)
    }

    /// Releases the pages of deleted files, shrinking the database.
    pub fn reclaim(&self) -> Result<()> {
        incremental_vacuum(&self.conn.lock()).map_err(db)
    }

    fn reclaim_if_large(conn: &Connection) {
        let free: rusqlite::Result<i64> = conn.query_row(
            "SELECT freelist_count * page_size FROM pragma_freelist_count(), pragma_page_size()",
            [],
            |r| r.get(0),
        );
        if free.is_ok_and(|bytes| bytes >= AUTO_RECLAIM_BYTES)
            && let Err(e) = incremental_vacuum(conn)
        {
            tracing::warn!(error = %e, "work files: could not release free pages");
        }
    }
}

/// Frees every page on the freelist. The pragma frees one page per step, so it
/// runs to completion.
fn incremental_vacuum(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA incremental_vacuum")?;
    let mut rows = stmt.query([])?;
    while rows.next()?.is_some() {}
    Ok(())
}

fn file_size(conn: &Connection, name: &str) -> rusqlite::Result<Option<u64>> {
    conn.prepare_cached("SELECT size FROM files WHERE name = ?1")?
        .query_row([name], |row| row.get::<_, i64>(0))
        .optional()
        .map(|size| size.map(|s| u64::try_from(s).unwrap_or(0)))
}

/// Rowid and stored length of chunk `idx` of `name`.
fn chunk_row(conn: &Connection, name: &str, idx: u64) -> rusqlite::Result<Option<(i64, usize)>> {
    conn.prepare_cached("SELECT rowid, length(data) FROM chunks WHERE name = ?1 AND idx = ?2")?
        .query_row(params![name, to_i64(idx)], |row| {
            Ok((row.get::<_, i64>(0)?, usize::try_from(row.get::<_, i64>(1)?).unwrap_or(0)))
        })
        .optional()
}

fn delete_file(conn: &Connection, name: &str) -> rusqlite::Result<bool> {
    conn.prepare_cached("DELETE FROM chunks WHERE name = ?1")?.execute([name])?;
    Ok(conn.prepare_cached("DELETE FROM files WHERE name = ?1")?.execute([name])? > 0)
}

impl FileStore for SqliteFiles {
    fn len(&self, name: &str) -> Result<Option<u64>> {
        check_name(name)?;
        file_size(&self.conn.lock(), name).map_err(db)
    }

    fn read_at(&self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
        check_name(name)?;
        let conn = self.conn.lock();
        let size = file_size(&conn, name).map_err(db)?.ok_or_else(|| not_found(name))?;
        if offset >= size || buf.is_empty() {
            return Ok(0);
        }
        let n = usize::try_from(size - offset).unwrap_or(usize::MAX).min(buf.len());
        let mut done = 0;
        while done < n {
            let pos = offset + done as u64;
            let idx = pos / CHUNK as u64;
            let within = (pos % CHUNK as u64) as usize;
            let take = (CHUNK - within).min(n - done);
            let out = &mut buf[done..done + take];
            match chunk_row(&conn, name, idx).map_err(db)? {
                Some((rowid, stored)) if within < stored => {
                    let have = take.min(stored - within);
                    let blob = conn.blob_open(MAIN_DB, "chunks", "data", rowid, true).map_err(db)?;
                    blob.read_at_exact(&mut out[..have], within).map_err(db)?;
                    out[have..].fill(0);
                }
                _ => out.fill(0),
            }
            done += take;
        }
        Ok(n)
    }

    fn create(&self, name: &str) -> Result<Box<dyn FileWriter>> {
        check_name(name)?;
        {
            let mut conn = self.conn.lock();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
            delete_file(&tx, name).map_err(db)?;
            tx.execute("INSERT INTO files (name, size, updated_at) VALUES (?1, 0, ?2)", params![name, now_ms()])
                .map_err(db)?;
            tx.commit().map_err(db)?;
        }
        Ok(Box::new(SqliteWriter {
            conn: Arc::clone(&self.conn),
            name: name.to_string(),
            len: 0,
            pending: Vec::new(),
            pending_at: 0,
        }))
    }

    fn remove(&self, name: &str) -> Result<bool> {
        check_name(name)?;
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
        let removed = delete_file(&tx, name).map_err(db)?;
        tx.commit().map_err(db)?;
        Self::reclaim_if_large(&conn);
        Ok(removed)
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        check_name(from)?;
        check_name(to)?;
        if from == to {
            return Ok(());
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
        if file_size(&tx, from).map_err(db)?.is_none() {
            return Err(not_found(from));
        }
        delete_file(&tx, to).map_err(db)?;
        tx.execute("UPDATE chunks SET name = ?2 WHERE name = ?1", params![from, to]).map_err(db)?;
        tx.execute("UPDATE files SET name = ?2, updated_at = ?3 WHERE name = ?1", params![from, to, now_ms()])
            .map_err(db)?;
        tx.commit().map_err(db)?;
        Self::reclaim_if_large(&conn);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached("SELECT name, size FROM files WHERE substr(name, 1, length(?1)) = ?1 ORDER BY name")
            .map_err(db)?;
        let rows = stmt
            .query_map([prefix], |row| {
                Ok((row.get::<_, String>(0)?, u64::try_from(row.get::<_, i64>(1)?).unwrap_or(0)))
            })
            .map_err(db)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(db)
    }

    fn remove_prefix(&self, prefix: &str) -> Result<u32> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
        tx.execute("DELETE FROM chunks WHERE substr(name, 1, length(?1)) = ?1", [prefix]).map_err(db)?;
        let removed = tx.execute("DELETE FROM files WHERE substr(name, 1, length(?1)) = ?1", [prefix]).map_err(db)?;
        tx.commit().map_err(db)?;
        Self::reclaim_if_large(&conn);
        Ok(u32::try_from(removed).unwrap_or(u32::MAX))
    }
}

/// Writer of one file in a [`SqliteFiles`] database.
struct SqliteWriter {
    conn: Arc<Mutex<Connection>>,
    name: String,
    len: u64,
    /// Contiguous bytes not yet written, starting at `pending_at`.
    pending: Vec<u8>,
    pending_at: u64,
}

impl SqliteWriter {
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
        let mut done = 0;
        while done < self.pending.len() {
            let pos = self.pending_at + done as u64;
            let idx = pos / CHUNK as u64;
            let within = (pos % CHUNK as u64) as usize;
            let take = (CHUNK - within).min(self.pending.len() - done);
            let rowid = match chunk_row(&tx, &self.name, idx).map_err(db)? {
                Some((rowid, stored)) if stored == CHUNK => rowid,
                Some(_) => return Err(MfError::internal("work files: chunk has the wrong size")),
                None => {
                    tx.prepare_cached("INSERT INTO chunks (name, idx, data) VALUES (?1, ?2, zeroblob(?3))")
                        .and_then(|mut st| st.execute(params![self.name, to_i64(idx), CHUNK as i64]))
                        .map_err(db)?;
                    tx.last_insert_rowid()
                }
            };
            let mut blob = tx.blob_open(MAIN_DB, "chunks", "data", rowid, false).map_err(db)?;
            blob.write_at(&self.pending[done..done + take], within).map_err(db)?;
            done += take;
        }
        tx.prepare_cached("UPDATE files SET size = max(size, ?2), updated_at = ?3 WHERE name = ?1")
            .and_then(|mut st| st.execute(params![self.name, to_i64(self.len), now_ms()]))
            .map_err(db)?;
        tx.commit().map_err(db)?;
        self.pending.clear();
        Ok(())
    }

    /// Cuts the last chunk to the file's length.
    fn trim_last_chunk(&self) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let idx = (self.len - 1) / CHUNK as u64;
        let keep = (self.len - idx * CHUNK as u64) as usize;
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db)?;
        if let Some((rowid, stored)) = chunk_row(&tx, &self.name, idx).map_err(db)?
            && stored > keep
        {
            let mut head = vec![0u8; keep];
            tx.blob_open(MAIN_DB, "chunks", "data", rowid, true)
                .and_then(|blob| blob.read_at_exact(&mut head, 0))
                .map_err(db)?;
            tx.execute("UPDATE chunks SET data = ?2 WHERE rowid = ?1", params![rowid, head]).map_err(db)?;
        }
        tx.commit().map_err(db)
    }
}

impl FileWriter for SqliteWriter {
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let end = offset + bytes.len() as u64;
        if !self.pending.is_empty() && offset != self.pending_at + self.pending.len() as u64 {
            self.flush()?;
        }
        if self.pending.is_empty() {
            self.pending_at = offset;
        }
        self.pending.extend_from_slice(bytes);
        self.len = self.len.max(end);
        if self.pending.len() >= FLUSH {
            self.flush()?;
        }
        Ok(())
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn finish(mut self: Box<Self>) -> Result<u64> {
        self.flush()?;
        self.trim_last_chunk()?;
        Ok(self.len)
    }
}

impl Drop for SqliteWriter {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            tracing::warn!(name = %self.name, error = %e, "work files: could not write the end of a file");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::testutil::TempDir;
    use crate::util::files::exercise_store;

    #[test]
    fn sqlite_backend() {
        let files = SqliteFiles::open_in_memory().unwrap();
        exercise_store(&files);
    }

    #[test]
    fn file_backend_survives_reopen_and_reclaims() {
        let dir = TempDir::new("files-sqlite");
        let path = dir.path().join("files.sqlite3");
        let files = SqliteFiles::open(&path).unwrap();
        let data = vec![7u8; 3 * CHUNK + 10];
        files.write("s/1/audio.pcm", &data).unwrap();
        let grown = files.database_bytes();
        assert!(grown >= data.len() as u64, "{grown}");
        drop(files);
        let files = SqliteFiles::open(&path).unwrap();
        assert_eq!(files.read("s/1/audio.pcm").unwrap(), data);
        let page: i64 = files.conn.lock().query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap();
        assert_eq!(page, 65536);
        // The last chunk was cut to size.
        let stored: i64 =
            files.conn.lock().query_row("SELECT sum(length(data)) FROM chunks", [], |r| r.get(0)).unwrap();
        assert_eq!(stored as usize, data.len());
        assert_eq!(files.remove_prefix("s/1/").unwrap(), 1);
        assert!(files.database_bytes() >= grown, "small deletes keep their pages for reuse");
        files.reclaim().unwrap();
        assert!(files.database_bytes() < grown / 4, "reclaim releases the pages of deleted files");
    }

    #[test]
    fn sparse_writes_and_unfinished_writers() {
        let files = SqliteFiles::open_in_memory().unwrap();
        let mut w = files.create("cap.pcm").unwrap();
        // Coverage-style writes: a run late in the file, then the start.
        w.write_at(2 * CHUNK as u64 + 5, b"late").unwrap();
        w.write_at(0, b"early").unwrap();
        assert_eq!(w.finish().unwrap(), 2 * CHUNK as u64 + 9);
        let chunks: i64 = files.conn.lock().query_row("SELECT count(*) FROM chunks", [], |r| r.get(0)).unwrap();
        assert_eq!(chunks, 2, "the untouched middle chunk is not stored");
        let mut buf = [1u8; 9];
        assert_eq!(files.read_at("cap.pcm", 2 * CHUNK as u64, &mut buf).unwrap(), 9);
        assert_eq!(&buf, b"\0\0\0\0\0late");
        let mut mid = [1u8; 4];
        assert_eq!(files.read_at("cap.pcm", CHUNK as u64 + 3, &mut mid).unwrap(), 4);
        assert_eq!(mid, [0; 4]);

        // A writer dropped without finish keeps what it wrote.
        let mut w = files.create("partial.bin").unwrap();
        w.append(b"abc").unwrap();
        drop(w);
        assert_eq!(files.read("partial.bin").unwrap(), b"abc");
    }

    #[test]
    fn damaged_databases_are_recognized() {
        let dir = TempDir::new("files-damaged");
        let path = dir.path().join("files.sqlite3");
        std::fs::write(&path, b"not a database at all ".repeat(1000)).unwrap();
        let err = SqliteFiles::open(&path).err().expect("damaged");
        assert!(SqliteFiles::is_damaged_error(&err), "{err:?}");
        assert!(!SqliteFiles::is_damaged_error(&MfError::Storage { message: "work files: busy".into() }));
    }

    #[test]
    fn snapshot_roundtrip() {
        let files = SqliteFiles::open_in_memory().unwrap();
        files.write("rec/1.wav", b"RIFF....").unwrap();
        let bytes = files.serialize().unwrap();
        let restored = SqliteFiles::open_serialized(&bytes).unwrap();
        assert_eq!(restored.read("rec/1.wav").unwrap(), b"RIFF....");
        restored.write("rec/2.wav", b"x").unwrap();
        assert_eq!(restored.list("rec/").unwrap().len(), 2);
    }
}
