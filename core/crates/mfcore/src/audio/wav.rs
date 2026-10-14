//! Minimal WAV I/O (16-bit PCM).
//!
//! The reader is lenient in the ways real files need: unknown chunks are
//! skipped, `WAVE_FORMAT_EXTENSIBLE` with a PCM sub-format is accepted, and a
//! data chunk whose size is 0, 0xFFFFFFFF or past the end of the file (streaming
//! writers that never patched the header) runs to the end of the file.

use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{MfError, Result};

const HEADER_LEN: usize = 44;
const FORMAT_PCM: u16 = 1;
const FORMAT_EXTENSIBLE: u16 = 0xFFFE;

/// Encode 16-bit mono samples as a WAV file in memory.
pub fn encode_wav(samples: &[i16], sample_rate: u32) -> Vec<u8> {
    encode_wav_interleaved(samples, sample_rate, 1)
}

/// Encode interleaved 16-bit samples with `channels` channels.
pub fn encode_wav_interleaved(samples: &[i16], sample_rate: u32, channels: u16) -> Vec<u8> {
    let channels = channels.max(1);
    let data_len = u32::try_from(samples.len() * 2).unwrap_or(u32::MAX - HEADER_LEN as u32);
    let block_align = channels * 2;
    let mut out = Vec::with_capacity(HEADER_LEN + data_len as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36u32.saturating_add(data_len)).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&FORMAT_PCM.to_le_bytes());
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&(sample_rate.saturating_mul(block_align as u32)).to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for s in &samples[..data_len as usize / 2] {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

pub fn write_wav(path: &Path, samples: &[i16], sample_rate: u32) -> Result<()> {
    std::fs::write(path, encode_wav(samples, sample_rate))
        .map_err(|e| MfError::internal(format!("cannot write {}: {e}", path.display())))?;
    Ok(())
}

/// Format of a WAV file's audio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WavSpec {
    pub sample_rate: u32,
    pub channels: u16,
    pub bits_per_sample: u16,
    /// True for integer PCM (format 1, or extensible with the PCM sub-format).
    pub is_pcm: bool,
}

impl WavSpec {
    /// 16-bit integer PCM, the only layout [`WavReader`] decodes.
    pub fn is_pcm16(&self) -> bool {
        self.is_pcm && self.bits_per_sample == 16
    }
}

/// A whole WAV file in memory.
#[derive(Debug, Clone, PartialEq)]
pub struct WavData {
    pub spec: WavSpec,
    /// Interleaved samples.
    pub samples: Vec<i16>,
}

/// Read a whole 16-bit WAV file. Use [`WavReader`] for long files.
pub fn read_wav(path: &Path) -> Result<WavData> {
    let mut reader = WavReader::open(path)?;
    let mut samples = Vec::new();
    let mut buf = vec![0i16; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        samples.extend_from_slice(&buf[..n]);
    }
    Ok(WavData { spec: reader.spec(), samples })
}

/// Streaming reader for 16-bit PCM WAV files.
pub struct WavReader {
    path: PathBuf,
    input: BufReader<File>,
    spec: WavSpec,
    /// Data bytes left to read.
    remaining: u64,
    bytes: Vec<u8>,
    /// A byte of a sample split across reads.
    odd: Option<u8>,
}

fn invalid(path: &Path, why: &str) -> MfError {
    MfError::Decode { message: format!("{} is not a valid WAV file ({why})", file_name(path)) }
}

fn file_name(path: &Path) -> String {
    path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| path.display().to_string())
}

impl WavReader {
    /// Open `path` and parse its header. Fails for anything but 16-bit PCM.
    pub fn open(path: &Path) -> Result<Self> {
        let reader = Self::open_any(path)?;
        if !reader.spec.is_pcm16() {
            return Err(MfError::Decode {
                message: format!(
                    "{} is not 16-bit PCM ({} bits, format {})",
                    file_name(path),
                    reader.spec.bits_per_sample,
                    if reader.spec.is_pcm { "PCM" } else { "non-PCM" }
                ),
            });
        }
        Ok(reader)
    }

    /// Open `path` and parse its header without restricting the sample format.
    /// [`read`](Self::read) only works for 16-bit PCM; check [`spec`](Self::spec).
    pub fn open_any(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|e| MfError::internal(format!("cannot open {}: {e}", path.display())))?;
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(u64::MAX);
        let mut input = BufReader::with_capacity(128 * 1024, file);
        let io = |e: std::io::Error| {
            if e.kind() == ErrorKind::UnexpectedEof {
                invalid(path, "truncated header")
            } else {
                MfError::internal(format!("cannot read {}: {e}", path.display()))
            }
        };

        let mut riff = [0u8; 12];
        input.read_exact(&mut riff).map_err(io)?;
        if &riff[0..4] != b"RIFF" || &riff[8..12] != b"WAVE" {
            return Err(invalid(path, "missing RIFF/WAVE header"));
        }
        let mut pos = 12u64;
        let mut spec: Option<WavSpec> = None;
        loop {
            let mut head = [0u8; 8];
            input.read_exact(&mut head).map_err(io)?;
            pos += 8;
            let id = [head[0], head[1], head[2], head[3]];
            let size = u32::from_le_bytes([head[4], head[5], head[6], head[7]]) as u64;
            match &id {
                b"fmt " => {
                    if size < 16 {
                        return Err(invalid(path, "short fmt chunk"));
                    }
                    let mut fmt = vec![0u8; size.min(64) as usize];
                    input.read_exact(&mut fmt).map_err(io)?;
                    skip(&mut input, size - fmt.len() as u64 + (size & 1)).map_err(io)?;
                    pos += size + (size & 1);
                    let tag = u16::from_le_bytes([fmt[0], fmt[1]]);
                    let channels = u16::from_le_bytes([fmt[2], fmt[3]]);
                    let sample_rate = u32::from_le_bytes([fmt[4], fmt[5], fmt[6], fmt[7]]);
                    let bits = u16::from_le_bytes([fmt[14], fmt[15]]);
                    let is_pcm = match tag {
                        FORMAT_PCM => true,
                        // Sub-format GUID starts with the format tag.
                        FORMAT_EXTENSIBLE if fmt.len() >= 26 => u16::from_le_bytes([fmt[24], fmt[25]]) == FORMAT_PCM,
                        _ => false,
                    };
                    if channels == 0 || sample_rate == 0 {
                        return Err(invalid(path, "zero channels or sample rate"));
                    }
                    spec = Some(WavSpec { sample_rate, channels, bits_per_sample: bits, is_pcm });
                }
                b"data" => {
                    let Some(spec) = spec else { return Err(invalid(path, "data before fmt")) };
                    let rest = file_len.saturating_sub(pos);
                    let remaining = if size == 0 || size == u32::MAX as u64 || size > rest { rest } else { size };
                    return Ok(Self { path: path.to_path_buf(), input, spec, remaining, bytes: Vec::new(), odd: None });
                }
                _ => {
                    skip(&mut input, size + (size & 1)).map_err(io)?;
                    pos += size + (size & 1);
                }
            }
        }
    }

    pub fn spec(&self) -> WavSpec {
        self.spec
    }

    /// Interleaved samples left (upper bound when the header lied about the size).
    pub fn remaining_samples(&self) -> u64 {
        self.remaining / 2
    }

    /// Read interleaved samples into `out`; returns how many were read, 0 at the end.
    pub fn read(&mut self, out: &mut [i16]) -> Result<usize> {
        if !self.spec.is_pcm16() {
            return Err(MfError::Decode { message: format!("{} is not 16-bit PCM", file_name(&self.path)) });
        }
        if out.is_empty() {
            return Ok(0);
        }
        self.bytes.clear();
        if let Some(b) = self.odd.take() {
            self.bytes.push(b);
        }
        let start = self.bytes.len();
        let to_read = (out.len() * 2 - start).min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        self.bytes.resize(start + to_read, 0);
        let mut filled = start;
        while filled < self.bytes.len() {
            match self.input.read(&mut self.bytes[filled..]) {
                Ok(0) => {
                    // The file ended early.
                    self.remaining = 0;
                    break;
                }
                Ok(n) => filled += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(MfError::internal(format!("cannot read {}: {e}", self.path.display()))),
            }
        }
        self.remaining = self.remaining.saturating_sub((filled - start) as u64);
        let whole = filled / 2 * 2;
        if whole < filled {
            self.odd = Some(self.bytes[whole]);
        }
        for (dst, b) in out.iter_mut().zip(self.bytes[..whole].as_chunks::<2>().0) {
            *dst = i16::from_le_bytes(*b);
        }
        Ok(whole / 2)
    }
}

fn skip(input: &mut BufReader<File>, bytes: u64) -> std::io::Result<()> {
    if bytes > 0 {
        input.seek(SeekFrom::Current(bytes as i64))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::testutil::TempDir;

    #[test]
    fn encode_layout() {
        let bytes = encode_wav(&[0, 1, -1, i16::MAX], 16_000);
        assert_eq!(bytes.len(), 44 + 8);
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 36 + 8);
        assert_eq!(&bytes[8..16], b"WAVEfmt ");
        assert_eq!(u16::from_le_bytes([bytes[22], bytes[23]]), 1);
        assert_eq!(u32::from_le_bytes(bytes[24..28].try_into().unwrap()), 16_000);
        assert_eq!(u32::from_le_bytes(bytes[28..32].try_into().unwrap()), 32_000);
        assert_eq!(&bytes[36..40], b"data");
        assert_eq!(u32::from_le_bytes(bytes[40..44].try_into().unwrap()), 8);
        assert_eq!(&bytes[44..], &[0, 0, 1, 0, 0xff, 0xff, 0xff, 0x7f]);
    }

    #[test]
    fn roundtrip() {
        let dir = TempDir::new("wav");
        let path = dir.path().join("a.wav");
        let samples: Vec<i16> = (0..10_000).map(|i| (i * 7 % 65_536 - 32_768) as i16).collect();
        write_wav(&path, &samples, 16_000).unwrap();
        let wav = read_wav(&path).unwrap();
        assert_eq!(wav.spec, WavSpec { sample_rate: 16_000, channels: 1, bits_per_sample: 16, is_pcm: true });
        assert_eq!(wav.samples, samples);
    }

    #[test]
    fn streaming_reads_with_odd_buffer_sizes() {
        let dir = TempDir::new("wavs");
        let path = dir.path().join("s.wav");
        let samples: Vec<i16> = (0..5_001).map(|i| i as i16).collect();
        std::fs::write(&path, encode_wav_interleaved(&samples[..5_000], 44_100, 2)).unwrap();
        let mut r = WavReader::open(&path).unwrap();
        assert_eq!(r.spec().channels, 2);
        assert_eq!(r.remaining_samples(), 5_000);
        let mut got = Vec::new();
        let mut buf = [0i16; 7];
        loop {
            let n = r.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        assert_eq!(got, samples[..5_000]);
    }

    #[test]
    fn lenient_headers() {
        let dir = TempDir::new("wavl");
        let samples: Vec<i16> = (0..100).collect();
        let base = encode_wav(&samples, 8_000);

        // Unknown chunk before data, odd-sized (padded).
        let mut with_list = base[..36].to_vec();
        with_list.extend_from_slice(b"LIST");
        with_list.extend_from_slice(&3u32.to_le_bytes());
        with_list.extend_from_slice(&[1, 2, 3, 0]);
        with_list.extend_from_slice(&base[36..]);
        let p = dir.path().join("list.wav");
        std::fs::write(&p, &with_list).unwrap();
        assert_eq!(read_wav(&p).unwrap().samples, samples);

        // Streaming writer that left the data size at 0 and at 0xFFFFFFFF.
        for size in [0u32, u32::MAX, 1_000_000] {
            let mut b = base.clone();
            b[40..44].copy_from_slice(&size.to_le_bytes());
            let p = dir.path().join(format!("size{size}.wav"));
            std::fs::write(&p, &b).unwrap();
            assert_eq!(read_wav(&p).unwrap().samples, samples, "size {size}");
        }

        // Truncated in the middle of a sample.
        let p = dir.path().join("trunc.wav");
        std::fs::write(&p, &base[..base.len() - 1]).unwrap();
        assert_eq!(read_wav(&p).unwrap().samples, samples[..99]);
    }

    #[test]
    fn extensible_pcm_is_accepted() {
        let dir = TempDir::new("wavx");
        let samples: Vec<i16> = vec![5, -5, 6, -6];
        let base = encode_wav_interleaved(&samples, 48_000, 2);
        let mut b = b"RIFF\0\0\0\0WAVEfmt ".to_vec();
        b.extend_from_slice(&40u32.to_le_bytes());
        b.extend_from_slice(&FORMAT_EXTENSIBLE.to_le_bytes());
        b.extend_from_slice(&base[22..36]);
        b.extend_from_slice(&22u16.to_le_bytes()); // cbSize
        b.extend_from_slice(&16u16.to_le_bytes()); // valid bits
        b.extend_from_slice(&3u32.to_le_bytes()); // channel mask
        b.extend_from_slice(&FORMAT_PCM.to_le_bytes());
        b.extend_from_slice(&[0, 0, 0, 0, 0x10, 0, 0x80, 0, 0, 0xaa, 0, 0x38, 0x9b, 0x71]);
        b.extend_from_slice(&base[36..]);
        let p = dir.path().join("ext.wav");
        std::fs::write(&p, &b).unwrap();
        let wav = read_wav(&p).unwrap();
        assert!(wav.spec.is_pcm16());
        assert_eq!(wav.spec.channels, 2);
        assert_eq!(wav.samples, samples);
    }

    #[test]
    fn rejects_garbage_and_other_formats() {
        let dir = TempDir::new("wavbad");
        let p = dir.path().join("bad.wav");
        std::fs::write(&p, b"not a wav file at all").unwrap();
        assert!(matches!(read_wav(&p), Err(MfError::Decode { .. })));
        std::fs::write(&p, b"RIFF").unwrap();
        assert!(matches!(read_wav(&p), Err(MfError::Decode { .. })));

        let mut float = encode_wav(&[0, 0], 16_000);
        float[20..22].copy_from_slice(&3u16.to_le_bytes());
        float[34..36].copy_from_slice(&32u16.to_le_bytes());
        std::fs::write(&p, &float).unwrap();
        assert!(matches!(WavReader::open(&p), Err(MfError::Decode { .. })));
        let any = WavReader::open_any(&p).unwrap();
        assert!(!any.spec().is_pcm16());
    }
}
