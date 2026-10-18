//! 16 kHz mono PCM stored as raw little-endian i16, read by window.
//!
//! The samples live in a file on disk (the apps), in a work file of a
//! [`FileStore`] (the browser keeps them in SQLite) or in memory (short clips).
//!
//! A writer normally appends from the start. Captured media (a tab played at
//! up to 4×, with ads, stalls and seeks in between) is written by media time
//! instead ([`PcmWriter::seek_ms`]); the writer then keeps a [`Coverage`] map of
//! the ranges that hold audio, so audio that was never captured is not mistaken
//! for silence. A partial coverage map is stored next to the samples
//! (`<name>.cov`) and read back by `open`.

use std::fmt;
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{MfError, Result};
use crate::util::files::{FileStore, FileWriter};

const BYTES_PER_SAMPLE: u64 = 2;
const WRITE_BUFFER: usize = 256 * 1024;
/// Suffix of the coverage file next to a partially covered PCM file.
const COVERAGE_SUFFIX: &str = ".cov";
const COVERAGE_MAGIC: &[u8; 8] = b"MFCOV\x00\x00\x01";

fn io_error(action: &str, path: &Path, e: std::io::Error) -> MfError {
    MfError::internal(format!("{action} {}: {e}", path.display()))
}

/// Samples at the analysis rate for `ms` milliseconds.
pub fn ms_to_samples(ms: u64) -> u64 {
    ms.saturating_mul(super::SAMPLE_RATE as u64) / 1000
}

/// Milliseconds of `samples` at the analysis rate.
pub fn samples_to_ms(samples: u64) -> u64 {
    samples.saturating_mul(1000) / super::SAMPLE_RATE as u64
}

/// Sample ranges that hold audio: sorted, disjoint, half-open `[start, end)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    runs: Vec<(u64, u64)>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }

    /// The ranges, sorted and merged.
    pub fn runs(&self) -> &[(u64, u64)] {
        &self.runs
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Marks `[start, end)` as covered (touching ranges merge).
    pub fn add(&mut self, start: u64, end: u64) {
        if end <= start {
            return;
        }
        let first = self.runs.partition_point(|&(_, e)| e < start);
        let mut new = (start, end);
        let mut last = first;
        while last < self.runs.len() && self.runs[last].0 <= new.1 {
            new = (new.0.min(self.runs[last].0), new.1.max(self.runs[last].1));
            last += 1;
        }
        self.runs.splice(first..last, [new]);
    }

    /// Covered samples within `[start, end)`.
    pub fn covered(&self, start: u64, end: u64) -> u64 {
        self.runs.iter().map(|&(s, e)| e.min(end).saturating_sub(s.max(start))).sum()
    }

    /// True when all of `[start, end)` is covered.
    pub fn contains(&self, start: u64, end: u64) -> bool {
        end <= start || self.covered(start, end) == end - start
    }

    /// The uncovered parts of `[start, end)`.
    pub fn gaps(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        let mut at = start;
        for &(s, e) in &self.runs {
            if e <= at {
                continue;
            }
            if s >= end {
                break;
            }
            if s > at {
                gaps.push((at, s.min(end)));
            }
            at = at.max(e);
            if at >= end {
                break;
            }
        }
        if at < end {
            gaps.push((at, end));
        }
        gaps
    }

    /// True when the ranges are exactly `[0, len)` (or empty with `len == 0`).
    pub fn is_complete(&self, len: u64) -> bool {
        match self.runs.as_slice() {
            [] => len == 0,
            [(0, end)] => *end >= len,
            _ => false,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.runs.len() * 16);
        out.extend_from_slice(COVERAGE_MAGIC);
        for &(s, e) in &self.runs {
            out.extend_from_slice(&s.to_le_bytes());
            out.extend_from_slice(&e.to_le_bytes());
        }
        out
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let rest = bytes.strip_prefix(COVERAGE_MAGIC)?;
        let (pairs, tail) = rest.as_chunks::<16>();
        if !tail.is_empty() {
            return None;
        }
        let mut coverage = Self::new();
        for pair in pairs {
            let (s, e) = pair.split_at(8);
            coverage.add(u64::from_le_bytes(s.try_into().ok()?), u64::from_le_bytes(e.try_into().ok()?));
        }
        Some(coverage)
    }
}

/// Where the samples of a [`PcmFile`] are.
#[derive(Clone)]
enum Backing {
    Path(PathBuf),
    Store { files: Arc<dyn FileStore>, name: String },
    Memory(Arc<Vec<i16>>),
}

impl fmt::Debug for Backing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backing::Path(path) => f.debug_tuple("Path").field(path).finish(),
            Backing::Store { name, .. } => f.debug_tuple("Store").field(name).finish(),
            Backing::Memory(samples) => f.debug_tuple("Memory").field(&samples.len()).finish(),
        }
    }
}

/// Finished PCM. Cheap to clone; every read opens the file, so a `PcmFile` can
/// be shared across threads.
#[derive(Debug, Clone)]
pub struct PcmFile {
    backing: Backing,
    len_samples: u64,
    /// `None` when every sample was written.
    coverage: Option<Arc<Coverage>>,
}

fn coverage_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(COVERAGE_SUFFIX);
    PathBuf::from(name)
}

fn read_coverage(bytes: Option<Vec<u8>>, len_samples: u64) -> Option<Arc<Coverage>> {
    let coverage = Coverage::from_bytes(&bytes?)?;
    (!coverage.is_complete(len_samples)).then(|| Arc::new(coverage))
}

impl PcmFile {
    /// Open an existing PCM file. A trailing odd byte is ignored.
    pub fn open(path: &Path) -> Result<Self> {
        let meta = std::fs::metadata(path).map_err(|e| io_error("cannot open", path, e))?;
        if !meta.is_file() {
            return Err(MfError::internal(format!("not a file: {}", path.display())));
        }
        let len_samples = meta.len() / BYTES_PER_SAMPLE;
        let coverage = read_coverage(std::fs::read(coverage_path(path)).ok(), len_samples);
        Ok(Self { backing: Backing::Path(path.to_path_buf()), len_samples, coverage })
    }

    /// Open the work file `name` of `files`.
    pub fn open_in(files: Arc<dyn FileStore>, name: &str) -> Result<Self> {
        let len = files.len(name)?.ok_or_else(|| MfError::internal(format!("PCM file not found: {name}")))?;
        let len_samples = len / BYTES_PER_SAMPLE;
        let coverage = read_coverage(files.read(&format!("{name}{COVERAGE_SUFFIX}")).ok(), len_samples);
        Ok(Self { backing: Backing::Store { files, name: name.to_string() }, len_samples, coverage })
    }

    /// PCM held in memory.
    pub fn from_samples(samples: Vec<i16>) -> Self {
        let len_samples = samples.len() as u64;
        Self { backing: Backing::Memory(Arc::new(samples)), len_samples, coverage: None }
    }

    /// The file's path; for a work file its name in the store, for PCM in
    /// memory an empty path.
    pub fn path(&self) -> &Path {
        match &self.backing {
            Backing::Path(path) => path,
            Backing::Store { name, .. } => Path::new(name),
            Backing::Memory(_) => Path::new(""),
        }
    }

    /// The samples, when they are in memory.
    pub fn samples(&self) -> Option<&[i16]> {
        match &self.backing {
            Backing::Memory(samples) => Some(samples),
            _ => None,
        }
    }

    /// The samples, when they are in memory (copied only if shared).
    pub fn into_samples(self) -> Option<Vec<i16>> {
        match self.backing {
            Backing::Memory(samples) => Some(Arc::try_unwrap(samples).unwrap_or_else(|shared| (*shared).clone())),
            _ => None,
        }
    }

    pub fn len_samples(&self) -> u64 {
        self.len_samples
    }

    pub fn duration_ms(&self) -> u64 {
        self.len_samples * 1000 / super::SAMPLE_RATE as u64
    }

    /// The ranges that hold audio; `None` when all of the file does.
    pub fn coverage(&self) -> Option<&Coverage> {
        self.coverage.as_deref()
    }

    /// True when `[start_ms, start_ms + len_ms)` (clamped to the file) holds
    /// audio throughout.
    pub fn is_covered_ms(&self, start_ms: u64, len_ms: u64) -> bool {
        let start = ms_to_samples(start_ms).min(self.len_samples);
        let end = ms_to_samples(start_ms.saturating_add(len_ms)).min(self.len_samples);
        self.coverage.as_ref().is_none_or(|c| c.contains(start, end))
    }

    /// The parts of `[start_ms, start_ms + len_ms)` without audio, in ms
    /// (within the file; the part past its end counts as missing too).
    pub fn gaps_ms(&self, start_ms: u64, len_ms: u64) -> Vec<(u64, u64)> {
        let start = ms_to_samples(start_ms);
        let end = ms_to_samples(start_ms.saturating_add(len_ms));
        let mut gaps = match &self.coverage {
            Some(c) => c.gaps(start, end.min(self.len_samples)),
            None => Vec::new(),
        };
        if end > self.len_samples {
            let tail = start.max(self.len_samples);
            match gaps.last_mut() {
                Some(last) if last.1 == tail => last.1 = end,
                _ => gaps.push((tail, end)),
            }
        }
        gaps.into_iter().map(|(s, e)| (samples_to_ms(s), samples_to_ms(e))).collect()
    }

    /// Read `[start_ms, start_ms + len_ms)` clamped to the file.
    pub fn read_ms(&self, start_ms: u64, len_ms: u64) -> Result<Vec<i16>> {
        let start = ms_to_samples(start_ms);
        let count = ms_to_samples(len_ms);
        self.read_samples(start, usize::try_from(count).unwrap_or(usize::MAX))
    }

    /// Read up to `count` samples from sample `start`. Returns fewer samples when
    /// the range passes the end of the file (also if the file shrank since `open`).
    pub fn read_samples(&self, start: u64, count: usize) -> Result<Vec<i16>> {
        let mut out = Vec::new();
        self.read_samples_into(start, count, &mut out)?;
        Ok(out)
    }

    /// Like [`read_samples`](Self::read_samples), replacing the contents of `out`.
    pub fn read_samples_into(&self, start: u64, count: usize, out: &mut Vec<i16>) -> Result<()> {
        out.clear();
        if start >= self.len_samples || count == 0 {
            return Ok(());
        }
        let available = usize::try_from(self.len_samples - start).unwrap_or(usize::MAX);
        let count = count.min(available);
        let mut bytes = vec![0u8; count * BYTES_PER_SAMPLE as usize];
        let filled = match &self.backing {
            Backing::Path(path) => {
                let mut file = File::open(path).map_err(|e| io_error("cannot open", path, e))?;
                file.seek(SeekFrom::Start(start * BYTES_PER_SAMPLE)).map_err(|e| io_error("cannot seek", path, e))?;
                let mut filled = 0;
                while filled < bytes.len() {
                    match file.read(&mut bytes[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(io_error("cannot read", path, e)),
                    }
                }
                filled
            }
            Backing::Store { files, name } => {
                let mut filled = 0;
                while filled < bytes.len() {
                    let n = files.read_at(name, start * BYTES_PER_SAMPLE + filled as u64, &mut bytes[filled..])?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                filled
            }
            Backing::Memory(samples) => {
                let from = usize::try_from(start).unwrap_or(usize::MAX).min(samples.len());
                let to = from.saturating_add(count).min(samples.len());
                out.extend_from_slice(&samples[from..to]);
                return Ok(());
            }
        };
        out.extend(bytes[..filled].as_chunks::<2>().0.iter().map(|b| i16::from_le_bytes(*b)));
        Ok(())
    }
}

/// Where a [`PcmWriter`] writes.
enum Sink {
    /// `at` is the byte offset the buffered writer continues at.
    Path {
        path: PathBuf,
        out: BufWriter<File>,
        at: u64,
    },
    Store {
        files: Arc<dyn FileStore>,
        name: String,
        out: Box<dyn FileWriter>,
    },
    Memory(Vec<i16>),
}

/// Writes a new PCM file (buffered): appends by default, or at media
/// positions after [`seek_ms`](Self::seek_ms).
pub struct PcmWriter {
    sink: Sink,
    /// Next sample index written.
    pos: u64,
    /// Length in samples (highest index written + 1).
    len: u64,
    /// Kept from the first seek on.
    coverage: Option<Coverage>,
    scratch: Vec<u8>,
}

impl PcmWriter {
    fn with_sink(sink: Sink) -> Self {
        Self { sink, pos: 0, len: 0, coverage: None, scratch: Vec::new() }
    }

    /// Create (or truncate) `path`.
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).map_err(|e| io_error("cannot create", path, e))?;
        let _ = std::fs::remove_file(coverage_path(path));
        Ok(Self::with_sink(Sink::Path {
            path: path.to_path_buf(),
            out: BufWriter::with_capacity(WRITE_BUFFER, file),
            at: 0,
        }))
    }

    /// Create (or replace) the work file `name` of `files`.
    pub fn create_in(files: Arc<dyn FileStore>, name: &str) -> Result<Self> {
        let out = files.create(name)?;
        files.remove(&format!("{name}{COVERAGE_SUFFIX}"))?;
        Ok(Self::with_sink(Sink::Store { files, name: name.to_string(), out }))
    }

    /// Collect the samples in memory.
    pub fn in_memory() -> Self {
        Self::with_sink(Sink::Memory(Vec::new()))
    }

    /// Path of the file; see [`PcmFile::path`].
    pub fn path(&self) -> &Path {
        match &self.sink {
            Sink::Path { path, .. } => path,
            Sink::Store { name, .. } => Path::new(name),
            Sink::Memory(_) => Path::new(""),
        }
    }

    /// Length in samples so far (for appends: the samples pushed).
    pub fn samples_written(&self) -> u64 {
        self.len
    }

    /// Continue writing at media time `ms` (sample `ms × 16`), e.g. after a seek,
    /// an ad or a stall of the captured player. From the first call on, the
    /// writer records which ranges hold audio; positions never written read as
    /// silence but are reported as gaps.
    pub fn seek_ms(&mut self, ms: f64) {
        let sample =
            if ms.is_finite() && ms > 0.0 { (ms * super::SAMPLE_RATE as f64 / 1000.0).round() as u64 } else { 0 };
        self.seek_sample(sample);
    }

    /// Like [`seek_ms`](Self::seek_ms) by sample index.
    pub fn seek_sample(&mut self, sample: u64) {
        if self.coverage.is_none() {
            let mut coverage = Coverage::new();
            coverage.add(0, self.pos);
            self.coverage = Some(coverage);
        }
        self.pos = sample;
    }

    /// The ranges written so far; `None` while the writer only appended.
    pub fn coverage(&self) -> Option<&Coverage> {
        self.coverage.as_ref()
    }

    pub fn push(&mut self, samples: &[i16]) -> Result<()> {
        if let Sink::Memory(out) = &mut self.sink {
            write_memory(out, self.pos, samples.iter().copied());
            return self.advance(samples.len());
        }
        self.scratch.clear();
        self.scratch.reserve(samples.len() * 2);
        for s in samples {
            self.scratch.extend_from_slice(&s.to_le_bytes());
        }
        self.write_scratch(samples.len())
    }

    /// Append f32 samples in -1..=1 (clamped, rounded to i16).
    pub fn push_f32(&mut self, samples: &[f32]) -> Result<()> {
        if let Sink::Memory(out) = &mut self.sink {
            write_memory(out, self.pos, samples.iter().map(|&s| super::resample::f32_sample_to_i16(s)));
            return self.advance(samples.len());
        }
        self.scratch.clear();
        self.scratch.reserve(samples.len() * 2);
        for &s in samples {
            self.scratch.extend_from_slice(&super::resample::f32_sample_to_i16(s).to_le_bytes());
        }
        self.write_scratch(samples.len())
    }

    fn write_scratch(&mut self, samples: usize) -> Result<()> {
        let offset = self.pos * BYTES_PER_SAMPLE;
        match &mut self.sink {
            Sink::Path { path, out, at } => {
                if *at != offset {
                    out.seek(SeekFrom::Start(offset)).map_err(|e| io_error("cannot seek", path, e))?;
                }
                out.write_all(&self.scratch).map_err(|e| io_error("cannot write", path, e))?;
                *at = offset + self.scratch.len() as u64;
            }
            Sink::Store { out, .. } => out.write_at(offset, &self.scratch)?,
            Sink::Memory(_) => unreachable!("memory writes do not use the scratch buffer"),
        }
        self.advance(samples)
    }

    fn advance(&mut self, samples: usize) -> Result<()> {
        let end = self.pos + samples as u64;
        if let Some(coverage) = self.coverage.as_mut() {
            coverage.add(self.pos, end);
        }
        self.pos = end;
        self.len = self.len.max(end);
        Ok(())
    }

    /// Flush and return the finished file.
    pub fn finish(self) -> Result<PcmFile> {
        let len_samples = self.len;
        let coverage = self.coverage.filter(|c| !c.is_complete(len_samples)).map(Arc::new);
        let backing = match self.sink {
            Sink::Path { path, mut out, .. } => {
                out.flush().map_err(|e| io_error("cannot write", &path, e))?;
                if let Some(coverage) = &coverage {
                    // Sparse writes may leave the end unwritten: the file must
                    // still have its full length.
                    let file = out.into_inner().map_err(|e| io_error("cannot write", &path, e.into_error()))?;
                    file.set_len(len_samples * BYTES_PER_SAMPLE).map_err(|e| io_error("cannot write", &path, e))?;
                    std::fs::write(coverage_path(&path), coverage.to_bytes())
                        .map_err(|e| io_error("cannot write", &path, e))?;
                }
                Backing::Path(path)
            }
            Sink::Store { files, name, out } => {
                let written = out.finish()?;
                if written < len_samples * BYTES_PER_SAMPLE {
                    return Err(MfError::internal(format!("PCM file {name} is shorter than written")));
                }
                if let Some(coverage) = &coverage {
                    files.write(&format!("{name}{COVERAGE_SUFFIX}"), &coverage.to_bytes())?;
                }
                Backing::Store { files, name }
            }
            Sink::Memory(samples) => Backing::Memory(Arc::new(samples)),
        };
        Ok(PcmFile { backing, len_samples, coverage })
    }
}

/// Writes `samples` into `out` at `pos`, zero-filling a gap before it.
fn write_memory(out: &mut Vec<i16>, pos: u64, samples: impl ExactSizeIterator<Item = i16>) {
    let pos = usize::try_from(pos).unwrap_or(usize::MAX);
    let end = pos.saturating_add(samples.len());
    if out.len() < end {
        out.resize(end, 0);
    }
    for (slot, s) in out[pos..end].iter_mut().zip(samples) {
        *slot = s;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::testutil::TempDir;
    use crate::util::files::DirFiles;

    #[test]
    fn write_then_read_windows() {
        let dir = TempDir::new("pcm");
        let path = dir.path().join("a.pcm");
        let samples: Vec<i16> = (0..48_000).map(|i| (i % 3000) as i16 - 1500).collect();
        let mut w = PcmWriter::create(&path).unwrap();
        w.push(&samples[..1000]).unwrap();
        w.push(&samples[1000..]).unwrap();
        assert_eq!(w.samples_written(), 48_000);
        let pcm = w.finish().unwrap();
        assert_eq!(pcm.len_samples(), 48_000);
        assert_eq!(pcm.duration_ms(), 3000);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 96_000);
        assert!(pcm.coverage().is_none());
        assert!(!coverage_path(&path).exists());

        let reopened = PcmFile::open(&path).unwrap();
        assert_eq!(reopened.len_samples(), 48_000);
        assert_eq!(reopened.read_ms(1000, 500).unwrap(), samples[16_000..24_000]);
        // Clamped at the end.
        assert_eq!(reopened.read_ms(2900, 500).unwrap(), samples[46_400..]);
        assert!(reopened.read_ms(5000, 100).unwrap().is_empty());
        assert!(reopened.read_ms(0, 0).unwrap().is_empty());
        assert_eq!(reopened.read_samples(47_999, 10).unwrap(), vec![samples[47_999]]);
    }

    #[test]
    fn f32_samples_are_clamped() {
        let dir = TempDir::new("pcmf");
        let path = dir.path().join("b.pcm");
        let mut w = PcmWriter::create(&path).unwrap();
        w.push_f32(&[0.0, 0.5, -0.5, 1.0, -1.0, 2.0, -2.0, f32::NAN]).unwrap();
        let pcm = w.finish().unwrap();
        assert_eq!(pcm.read_samples(0, 8).unwrap(), vec![0, 16384, -16384, 32767, -32767, 32767, -32767, 0]);
    }

    #[test]
    fn short_and_odd_files() {
        let dir = TempDir::new("pcmodd");
        let path = dir.path().join("odd.pcm");
        std::fs::write(&path, [1u8, 0, 2, 0, 3]).unwrap();
        let pcm = PcmFile::open(&path).unwrap();
        assert_eq!(pcm.len_samples(), 2);
        assert_eq!(pcm.read_samples(0, 100).unwrap(), vec![1, 2]);

        // The file shrinks after open: reads return what is left.
        std::fs::write(&path, [7u8, 0]).unwrap();
        assert_eq!(pcm.read_samples(0, 2).unwrap(), vec![7]);
        assert!(pcm.read_samples(1, 1).unwrap().is_empty());

        assert!(PcmFile::open(&dir.path().join("missing.pcm")).is_err());
        assert!(PcmFile::open(dir.path()).is_err());
    }

    #[test]
    fn coverage_merges_and_reports_gaps() {
        let mut c = Coverage::new();
        c.add(10, 20);
        c.add(30, 40);
        c.add(20, 25); // touches the first run
        c.add(5, 5); // empty
        assert_eq!(c.runs(), &[(10, 25), (30, 40)]);
        assert_eq!(c.gaps(0, 50), vec![(0, 10), (25, 30), (40, 50)]);
        assert_eq!(c.gaps(12, 24), vec![]);
        assert_eq!(c.gaps(24, 31), vec![(25, 30)]);
        assert_eq!(c.covered(0, 100), 25);
        assert!(c.contains(31, 39) && !c.contains(24, 31));
        c.add(0, 100);
        assert_eq!(c.runs(), &[(0, 100)]);
        assert!(c.is_complete(100) && !c.is_complete(101));
        assert_eq!(Coverage::from_bytes(&c.to_bytes()), Some(c));
        assert_eq!(Coverage::from_bytes(b"garbage!"), None);
    }

    /// Writes like a capture: 0–1 s, then 3–4 s (a skipped ad), then 1–2 s again.
    fn captured(mut w: PcmWriter) -> PcmFile {
        let second: Vec<i16> = (0..16_000).map(|i| (i % 100) as i16 + 1).collect();
        w.push(&second).unwrap();
        w.seek_ms(3000.0);
        w.push(&second).unwrap();
        w.seek_ms(1000.0);
        w.push(&second).unwrap();
        assert_eq!(w.samples_written(), 64_000);
        assert_eq!(w.coverage().unwrap().runs(), &[(0, 32_000), (48_000, 64_000)]);
        w.finish().unwrap()
    }

    fn check_captured(pcm: &PcmFile) {
        assert_eq!(pcm.len_samples(), 64_000);
        assert_eq!(pcm.coverage().unwrap().runs(), &[(0, 32_000), (48_000, 64_000)]);
        assert!(pcm.is_covered_ms(0, 2000));
        assert!(!pcm.is_covered_ms(1500, 1000));
        assert_eq!(pcm.gaps_ms(0, 5000), vec![(2000, 3000), (4000, 5000)]);
        assert_eq!(pcm.gaps_ms(500, 1000), vec![]);
        // Never-captured audio reads as zeros.
        assert!(pcm.read_ms(2000, 1000).unwrap().iter().all(|&s| s == 0));
        assert_eq!(pcm.read_ms(3000, 10).unwrap()[..3], [1, 2, 3]);
    }

    #[test]
    fn positional_writes_keep_coverage_on_every_backing() {
        let dir = TempDir::new("pcmcov");
        let path = dir.path().join("cap.pcm");
        let pcm = captured(PcmWriter::create(&path).unwrap());
        check_captured(&pcm);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 128_000);
        check_captured(&PcmFile::open(&path).unwrap());

        let files: Arc<dyn FileStore> = Arc::new(DirFiles::new(dir.path().join("work")));
        check_captured(&captured(PcmWriter::create_in(files.clone(), "s/1/cap.pcm").unwrap()));
        check_captured(&PcmFile::open_in(files.clone(), "s/1/cap.pcm").unwrap());

        check_captured(&captured(PcmWriter::in_memory()));

        // Replacing a captured file with a complete one drops the coverage file.
        let mut w = PcmWriter::create_in(files.clone(), "s/1/cap.pcm").unwrap();
        w.push(&[5; 10]).unwrap();
        let pcm = w.finish().unwrap();
        assert!(pcm.coverage().is_none());
        assert!(PcmFile::open_in(files.clone(), "s/1/cap.pcm").unwrap().coverage().is_none());
        assert_eq!(files.len("s/1/cap.pcm.cov").unwrap(), None);
    }

    #[test]
    fn seeking_back_to_continue_sequentially_is_complete() {
        let mut w = PcmWriter::in_memory();
        w.push(&[1; 100]).unwrap();
        w.seek_sample(50);
        w.push(&[2; 100]).unwrap();
        let pcm = w.finish().unwrap();
        assert!(pcm.coverage().is_none(), "overlapping writes cover everything");
        assert_eq!(pcm.len_samples(), 150);
        assert_eq!(pcm.samples().unwrap()[48..52], [1, 1, 2, 2]);
    }

    #[test]
    fn store_and_memory_backings_read_like_files() {
        let dir = TempDir::new("pcmstore");
        let files: Arc<dyn FileStore> = Arc::new(DirFiles::new(dir.path()));
        let samples: Vec<i16> = (0..20_000).map(|i| (i * 7 % 30_000) as i16).collect();
        let mut w = PcmWriter::create_in(files.clone(), "x/a.pcm").unwrap();
        w.push(&samples).unwrap();
        let pcm = w.finish().unwrap();
        assert_eq!(pcm.path(), Path::new("x/a.pcm"));
        assert_eq!(pcm.read_samples(19_990, 50).unwrap(), samples[19_990..]);
        let memory = PcmFile::from_samples(samples.clone());
        assert_eq!(memory.read_ms(250, 125).unwrap(), samples[4000..6000]);
        assert_eq!(memory.read_samples(30_000, 5).unwrap(), Vec::<i16>::new());
        assert_eq!(memory.samples().unwrap().len(), 20_000);
        assert!(PcmFile::open_in(files, "x/missing.pcm").is_err());
    }
}
