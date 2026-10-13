//! MP4 edit lists for the first sound track.
//!
//! symphonia's isomp4 reader ignores `elst`, so AAC files from ffmpeg, iTunes
//! and Apple encoders keep their encoder priming (1024–2112 samples) at the
//! start and padding at the end. This reads just enough of `moov` to find the
//! audible range so the decoder can trim it.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Largest `moov` box read into memory.
const MAX_MOOV: u64 = 64 * 1024 * 1024;
/// Top-level boxes scanned before giving up.
const MAX_TOP_LEVEL: usize = 4096;

/// The audible range of a sound track, in its media timescale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mp4Edit {
    pub timescale: u32,
    /// Leading media time to drop (encoder priming).
    pub skip: u64,
    /// Playable media duration, when the edit gives one.
    pub duration: Option<u64>,
}

impl Mp4Edit {
    /// The same edit with `delay` media units less to skip (a decoder that adds
    /// less delay than the one the edit was written for).
    pub fn with_less_skip(self, delay: u64) -> Self {
        Self { skip: self.skip.saturating_sub(delay), ..self }
    }

    /// Samples to drop at the start, at `rate`.
    pub fn skip_samples(&self, rate: u32) -> u64 {
        scale(self.skip, rate, self.timescale)
    }

    /// Playable length in samples at `rate`.
    pub fn duration_samples(&self, rate: u32) -> Option<u64> {
        self.duration.map(|d| scale(d, rate, self.timescale))
    }
}

fn scale(value: u64, to: u32, from: u32) -> u64 {
    ((value as u128 * to as u128 + from as u128 / 2) / from.max(1) as u128) as u64
}

/// The edit of the first sound track of an MP4/MOV file, if it trims anything.
pub(crate) fn sound_track_edit(path: &Path) -> Option<Mp4Edit> {
    let mut file = File::open(path).ok()?;
    let moov = read_moov(&mut file)?;
    parse_moov(&moov)
}

fn read_moov(file: &mut File) -> Option<Vec<u8>> {
    let len = file.metadata().ok()?.len();
    let mut pos = 0u64;
    for _ in 0..MAX_TOP_LEVEL {
        if pos + 8 > len {
            return None;
        }
        file.seek(SeekFrom::Start(pos)).ok()?;
        let mut head = [0u8; 16];
        file.read_exact(&mut head[..8]).ok()?;
        let mut size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as u64;
        let kind = [head[4], head[5], head[6], head[7]];
        let mut header = 8u64;
        if size == 1 {
            file.read_exact(&mut head[8..16]).ok()?;
            size = u64::from_be_bytes(head[8..16].try_into().ok()?);
            header = 16;
        } else if size == 0 {
            size = len - pos;
        }
        if size < header || pos + size > len {
            return None;
        }
        if &kind == b"moov" {
            let body = size - header;
            if body > MAX_MOOV {
                return None;
            }
            let mut buf = vec![0u8; body as usize];
            file.read_exact(&mut buf).ok()?;
            return Some(buf);
        }
        pos += size;
    }
    None
}

/// Child boxes of `data` as (type, body).
fn boxes(data: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> {
    let mut pos = 0usize;
    std::iter::from_fn(move || {
        let head = data.get(pos..pos + 8)?;
        let mut size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
        let kind = [head[4], head[5], head[6], head[7]];
        let mut header = 8;
        if size == 1 {
            size = usize::try_from(u64::from_be_bytes(data.get(pos + 8..pos + 16)?.try_into().ok()?)).ok()?;
            header = 16;
        } else if size == 0 {
            size = data.len() - pos;
        }
        if size < header || pos.checked_add(size)? > data.len() {
            return None;
        }
        let body = &data[pos + header..pos + size];
        pos += size;
        Some((kind, body))
    })
}

fn child<'a>(data: &'a [u8], kind: &[u8; 4]) -> Option<&'a [u8]> {
    boxes(data).find(|(k, _)| k == kind).map(|(_, b)| b)
}

fn be_u32(data: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(data.get(at..at + 4)?.try_into().ok()?))
}

fn be_u64(data: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(data.get(at..at + 8)?.try_into().ok()?))
}

/// Timescale from a full box laid out like `mvhd`/`mdhd` (version 0 or 1).
fn timescale(full_box: &[u8]) -> Option<u32> {
    let at = if *full_box.first()? == 1 { 20 } else { 12 };
    be_u32(full_box, at).filter(|&t| t > 0)
}

fn parse_moov(moov: &[u8]) -> Option<Mp4Edit> {
    let movie_timescale = timescale(child(moov, b"mvhd")?)?;
    let trak = boxes(moov).filter(|(k, _)| k == b"trak").map(|(_, b)| b).find(|trak| {
        child(trak, b"mdia").and_then(|m| child(m, b"hdlr")).and_then(|h| h.get(8..12)) == Some(b"soun")
    })?;
    let media_timescale = timescale(child(child(trak, b"mdia")?, b"mdhd")?)?;
    let elst = child(child(trak, b"edts")?, b"elst")?;

    let version = *elst.first()?;
    let count = be_u32(elst, 4)? as usize;
    let entry_len = if version == 1 { 20 } else { 12 };
    let mut pos = 8;
    for _ in 0..count.min(1024) {
        let (segment, media_time) = if version == 1 {
            (be_u64(elst, pos)?, be_u64(elst, pos + 8)? as i64)
        } else {
            (be_u32(elst, pos)? as u64, be_u32(elst, pos + 4)? as i32 as i64)
        };
        pos += entry_len;
        // An empty edit (-1) delays the track; it does not trim.
        if media_time < 0 {
            continue;
        }
        let duration = (segment > 0).then(|| scale(segment, media_timescale, movie_timescale));
        let edit = Mp4Edit { timescale: media_timescale, skip: media_time as u64, duration };
        return (edit.skip > 0 || edit.duration.is_some()).then_some(edit);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mp4_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = ((body.len() + 8) as u32).to_be_bytes().to_vec();
        out.extend_from_slice(kind);
        out.extend_from_slice(body);
        out
    }

    fn full_box_v0(timescale: u32) -> Vec<u8> {
        let mut body = vec![0u8; 12];
        body.extend_from_slice(&timescale.to_be_bytes());
        body.extend_from_slice(&[0u8; 8]);
        body
    }

    fn trak(handler: &[u8; 4], media_timescale: u32, elst: Option<Vec<u8>>) -> Vec<u8> {
        let mut hdlr = vec![0u8; 8];
        hdlr.extend_from_slice(handler);
        hdlr.extend_from_slice(&[0u8; 12]);
        let mdia = [mp4_box(b"mdhd", &full_box_v0(media_timescale)), mp4_box(b"hdlr", &hdlr)].concat();
        let mut body = mp4_box(b"tkhd", &[0u8; 84]);
        if let Some(elst) = elst {
            body.extend(mp4_box(b"edts", &mp4_box(b"elst", &elst)));
        }
        body.extend(mp4_box(b"mdia", &mdia));
        mp4_box(b"trak", &body)
    }

    fn elst_v0(entries: &[(u32, i32)]) -> Vec<u8> {
        let mut body = vec![0u8; 4];
        body.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (segment, media_time) in entries {
            body.extend_from_slice(&segment.to_be_bytes());
            body.extend_from_slice(&media_time.to_be_bytes());
            body.extend_from_slice(&0x0001_0000u32.to_be_bytes());
        }
        body
    }

    fn moov(traks: &[Vec<u8>]) -> Vec<u8> {
        let mut body = mp4_box(b"mvhd", &full_box_v0(1000));
        for t in traks {
            body.extend_from_slice(t);
        }
        body
    }

    #[test]
    fn finds_priming_on_the_sound_track() {
        let video = trak(b"vide", 12_800, Some(elst_v0(&[(130_520, 0)])));
        // 130.299 s movie time, 1024 samples of priming at 44.1 kHz.
        let sound = trak(b"soun", 44_100, Some(elst_v0(&[(130_299, 1024)])));
        let edit = parse_moov(&moov(&[video, sound])).unwrap();
        assert_eq!(edit, Mp4Edit { timescale: 44_100, skip: 1024, duration: Some(5_746_186) });
        assert_eq!(edit.skip_samples(16_000), 372);
        assert_eq!(edit.duration_samples(16_000), Some(2_084_784));
    }

    #[test]
    fn empty_edits_and_missing_lists() {
        // Leading empty edit, then the real one.
        let sound = trak(b"soun", 48_000, Some(elst_v0(&[(500, -1), (10_000, 2112)])));
        assert_eq!(parse_moov(&moov(&[sound])).unwrap().skip, 2112);
        let sound = trak(b"soun", 48_000, None);
        assert_eq!(parse_moov(&moov(&[sound])), None);
        assert_eq!(parse_moov(&moov(&[trak(b"vide", 1000, Some(elst_v0(&[(1, 5)])))])), None);
        // Truncated or garbage data never panics.
        let good = moov(&[trak(b"soun", 44_100, Some(elst_v0(&[(1000, 1024)])))]);
        for cut in 0..good.len() {
            let _ = parse_moov(&good[..cut]);
        }
        assert_eq!(parse_moov(&[0xff; 64]), None);
    }

    #[test]
    fn reads_moov_after_mdat() {
        let dir = crate::audio::testutil::TempDir::new("mp4");
        let path = dir.path().join("a.m4a");
        let sound = trak(b"soun", 44_100, Some(elst_v0(&[(2000, 1024)])));
        let file =
            [mp4_box(b"ftyp", b"M4A \0\0\0\0"), mp4_box(b"mdat", &[0u8; 5000]), mp4_box(b"moov", &moov(&[sound]))]
                .concat();
        std::fs::write(&path, file).unwrap();
        assert_eq!(sound_track_edit(&path).map(|e| e.skip), Some(1024));
        std::fs::write(&path, b"not an mp4 file").unwrap();
        assert_eq!(sound_track_edit(&path), None);
    }
}
