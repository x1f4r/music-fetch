//! 16 kHz mono PCM stored as raw little-endian i16 in a file, read by window.

use std::fs::File;
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{MfError, Result};

const BYTES_PER_SAMPLE: u64 = 2;
const WRITE_BUFFER: usize = 256 * 1024;

fn io_error(action: &str, path: &Path, e: std::io::Error) -> MfError {
    MfError::internal(format!("{action} {}: {e}", path.display()))
}

/// Samples at the analysis rate for `ms` milliseconds.
pub fn ms_to_samples(ms: u64) -> u64 {
    ms.saturating_mul(super::SAMPLE_RATE as u64) / 1000
}

/// A finished PCM file. Cheap to clone; every read opens the file, so a
/// `PcmFile` can be shared across threads.
#[derive(Debug, Clone)]
pub struct PcmFile {
    path: PathBuf,
    len_samples: u64,
}

impl PcmFile {
    /// Open an existing PCM file. A trailing odd byte is ignored.
    pub fn open(path: &Path) -> Result<Self> {
        let meta = std::fs::metadata(path).map_err(|e| io_error("cannot open", path, e))?;
        if !meta.is_file() {
            return Err(MfError::internal(format!("not a file: {}", path.display())));
        }
        Ok(Self { path: path.to_path_buf(), len_samples: meta.len() / BYTES_PER_SAMPLE })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len_samples(&self) -> u64 {
        self.len_samples
    }

    pub fn duration_ms(&self) -> u64 {
        self.len_samples * 1000 / super::SAMPLE_RATE as u64
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
        let mut file = File::open(&self.path).map_err(|e| io_error("cannot open", &self.path, e))?;
        file.seek(SeekFrom::Start(start * BYTES_PER_SAMPLE)).map_err(|e| io_error("cannot seek", &self.path, e))?;

        let mut bytes = vec![0u8; count * BYTES_PER_SAMPLE as usize];
        let mut filled = 0;
        while filled < bytes.len() {
            match file.read(&mut bytes[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(io_error("cannot read", &self.path, e)),
            }
        }
        out.extend(bytes[..filled].as_chunks::<2>().0.iter().map(|b| i16::from_le_bytes(*b)));
        Ok(())
    }
}

/// Appends samples to a new PCM file (buffered).
pub struct PcmWriter {
    path: PathBuf,
    out: BufWriter<File>,
    written: u64,
    scratch: Vec<u8>,
}

impl PcmWriter {
    /// Create (or truncate) `path`.
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).map_err(|e| io_error("cannot create", path, e))?;
        Ok(Self {
            path: path.to_path_buf(),
            out: BufWriter::with_capacity(WRITE_BUFFER, file),
            written: 0,
            scratch: Vec::new(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn samples_written(&self) -> u64 {
        self.written
    }

    pub fn push(&mut self, samples: &[i16]) -> Result<()> {
        self.scratch.clear();
        self.scratch.reserve(samples.len() * 2);
        for s in samples {
            self.scratch.extend_from_slice(&s.to_le_bytes());
        }
        self.write_scratch(samples.len())
    }

    /// Append f32 samples in -1..=1 (clamped, rounded to i16).
    pub fn push_f32(&mut self, samples: &[f32]) -> Result<()> {
        self.scratch.clear();
        self.scratch.reserve(samples.len() * 2);
        for &s in samples {
            self.scratch.extend_from_slice(&super::resample::f32_sample_to_i16(s).to_le_bytes());
        }
        self.write_scratch(samples.len())
    }

    fn write_scratch(&mut self, samples: usize) -> Result<()> {
        self.out.write_all(&self.scratch).map_err(|e| io_error("cannot write", &self.path, e))?;
        self.written += samples as u64;
        Ok(())
    }

    /// Flush and return the finished file.
    pub fn finish(mut self) -> Result<PcmFile> {
        self.out.flush().map_err(|e| io_error("cannot write", &self.path, e))?;
        Ok(PcmFile { path: self.path, len_samples: self.written })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::testutil::TempDir;

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
}
