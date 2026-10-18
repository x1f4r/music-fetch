//! The melody index file: song metadata, one continuous pitch contour per song
//! and source ("track"), and an inverted multi-index over the tracks' window
//! vectors (see [`crate::melody::contour`]).
//!
//! Layout (little-endian; `.mfi`, format 2):
//!
//! ```text
//! header   256 B   magic "MFMELODY", format, counts, version string, 10 section (offset, len) pairs
//! STRS             UTF-8 string table (titles, artists, version)
//! SONG     40 B    title (off u32, len u16), artist (len u16, off u32), ISRC [12], Deezer id u64,
//!                  year u16, popularity u16 (×65535), first track u32
//! TRCK     16 B    song u32, contour byte offset u32, frames u32, base pitch u8, source u8, encoding u8, 0
//! CONT             per track, 20 fps frames: code 0 = unvoiced, else base + (code − 1) / 4 semitones;
//!                  encoding 0 = one byte per frame, 1 = runs of (code u8, length varint)
//! CODE             two codebooks (first and second half of the window vector), 256 × HALF f32 each,
//!                  then SUBS residual codebooks, 256 × SUB f32 each
//! CELL     8 B     per cell (first-half code × 256 + second-half code): postings byte offset u32, count u32
//! POST             per cell, sorted by track: track delta varint, window start / REF_HOP varint,
//!                  SUBS residual codes (u8): the window vector minus the cell's centroids, quantized
//! TITL     12 B    (hash u64, song u32) sorted by hash: title+artist keys, then title-only keys
//! KEYS     12 B    note n-gram key u32, postings byte offset u32, posting count u32 (sorted by key)
//! NPST             per key, sorted by track: track delta varint, step position varint
//! ```
//!
//! The note n-grams (see [`crate::melody::ngrams`]) are taken over each track's
//! pitch changes as [`crate::melody::steps::frame_steps`] finds them in the
//! decoded contour.
//!
//! Opening maps the file and checks the header; nothing is decoded until used.

use std::fs::File;
use std::path::Path;

use crate::error::{MfError, Result};
use crate::melody::contour::{HALF, SUB, SUBS};

pub(crate) const MAGIC: &[u8; 8] = b"MFMELODY";
/// File format this core reads and writes. Each format is published under its own
/// release tag (`melody-index-<format>`); manifests name it (`Manifest::format`).
pub const FORMAT: u32 = 2;
pub(crate) const HEADER_LEN: usize = 256;
pub(crate) const SECTIONS: usize = 10;
pub(crate) const SONG_LEN: usize = 40;
pub(crate) const TRACK_LEN: usize = 16;
pub(crate) const TITLE_LEN: usize = 12;
pub(crate) const KEY_LEN: usize = 12;
/// Centroids per half codebook.
pub const CODES: usize = 256;
/// Cells of the inverted multi-index.
pub const CELLS: usize = CODES * CODES;
pub(crate) const CELL_LEN: usize = 8;
pub(crate) const CODEBOOK_BYTES: usize = (2 * CODES * HALF + SUBS * CODES * SUB) * 4;
/// Section table starts here; each entry is (offset u64, length u64).
pub(crate) const SECTION_TABLE: usize = 48;

pub(crate) const S_STRS: usize = 0;
pub(crate) const S_SONG: usize = 1;
pub(crate) const S_TRCK: usize = 2;
pub(crate) const S_CONT: usize = 3;
pub(crate) const S_CODE: usize = 4;
pub(crate) const S_CELL: usize = 5;
pub(crate) const S_POST: usize = 6;
pub(crate) const S_TITL: usize = 7;
pub(crate) const S_KEYS: usize = 8;
pub(crate) const S_NPST: usize = 9;

/// Contour encodings.
pub(crate) const ENC_RAW: u8 = 0;
pub(crate) const ENC_RUNS: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct SongMeta {
    pub id: u32,
    pub title: String,
    pub artist: String,
    pub isrc: Option<String>,
    pub deezer_id: Option<u64>,
    pub year: Option<u16>,
    /// 0..=1, used as a prior.
    pub popularity: f32,
}

/// Where a track's melody comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackSource {
    /// A 30 s audio preview.
    Preview,
    /// The full recording.
    Audio,
    /// A MIDI transcription.
    Midi,
}

impl TrackSource {
    pub(crate) fn code(self) -> u8 {
        match self {
            TrackSource::Preview => 0,
            TrackSource::Audio => 1,
            TrackSource::Midi => 2,
        }
    }

    fn from_code(c: u8) -> Self {
        match c {
            1 => TrackSource::Audio,
            2 => TrackSource::Midi,
            _ => TrackSource::Preview,
        }
    }
}

/// A decoded track: one source's continuous melody of a song.
#[derive(Debug, Clone)]
pub struct Track {
    pub id: u32,
    pub song: u32,
    pub source: TrackSource,
    /// 20 fps contour in MIDI semitones; NaN = unvoiced.
    pub contour: Vec<f32>,
}

enum Bytes {
    #[cfg(feature = "mmap")]
    Mapped(memmap2::Mmap),
    Owned(Vec<u8>),
}

impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            #[cfg(feature = "mmap")]
            Bytes::Mapped(m) => m,
            Bytes::Owned(v) => v,
        }
    }
}

/// An open melody index. All reads are bounds-checked; a damaged file yields
/// missing songs or tracks, never a panic.
pub struct MelodyIndex {
    bytes: Bytes,
    sections: [(usize, usize); SECTIONS],
    song_count: u32,
    track_count: u32,
    frame_count: u32,
    window_count: u32,
    key_count: u32,
    step_count: u32,
    version: String,
    codebooks: Box<[[[f32; HALF]; CODES]; 2]>,
    residuals: Box<[[[f32; SUB]; CODES]; SUBS]>,
}

/// The format number in an index file's header, when it is a melody index at all.
pub fn file_format(path: &Path) -> Option<u32> {
    use std::io::Read;
    let mut head = [0u8; 12];
    File::open(path).ok()?.read_exact(&mut head).ok()?;
    (&head[..8] == MAGIC).then(|| u32::from_le_bytes([head[8], head[9], head[10], head[11]]))
}

impl MelodyIndex {
    /// Map an index file and check its header.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|_| MfError::IndexUnavailable)?;
        // Safety: the index is written once to a temp file and renamed into place;
        // nothing modifies it while mapped.
        #[cfg(feature = "mmap")]
        let bytes = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(map) => Bytes::Mapped(map),
            Err(_) => Bytes::Owned(std::fs::read(path).map_err(|_| MfError::IndexUnavailable)?),
        };
        #[cfg(not(feature = "mmap"))]
        let bytes = {
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut &file, &mut bytes).map_err(|_| MfError::IndexUnavailable)?;
            Bytes::Owned(bytes)
        };
        Self::parse(bytes)
    }

    /// Open an index held in memory.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::parse(Bytes::Owned(bytes))
    }

    fn parse(bytes: Bytes) -> Result<Self> {
        let bad = || MfError::Internal { message: "the melody index is damaged".into() };
        if bytes.len() < 16 || &bytes[..8] != MAGIC {
            return Err(bad());
        }
        let format = le_u32(&bytes, 8).ok_or_else(bad)?;
        if format != FORMAT {
            let message = if format > FORMAT {
                "the melody index needs a newer app version"
            } else {
                "the melody index is from an older app version"
            };
            return Err(MfError::Internal { message: message.into() });
        }
        if bytes.len() < HEADER_LEN {
            return Err(bad());
        }
        let header_len = le_u32(&bytes, 12).ok_or_else(bad)? as usize;
        let song_count = le_u32(&bytes, 16).ok_or_else(bad)?;
        let track_count = le_u32(&bytes, 20).ok_or_else(bad)?;
        let frame_count = le_u32(&bytes, 24).ok_or_else(bad)?;
        let window_count = le_u32(&bytes, 28).ok_or_else(bad)?;
        let version_off = le_u32(&bytes, 32).ok_or_else(bad)? as usize;
        let version_len = le_u32(&bytes, 36).ok_or_else(bad)? as usize;
        let key_count = le_u32(&bytes, 40).ok_or_else(bad)?;
        let step_count = le_u32(&bytes, 44).ok_or_else(bad)?;
        let mut sections = [(0usize, 0usize); SECTIONS];
        for (i, s) in sections.iter_mut().enumerate() {
            let off = le_u64(&bytes, SECTION_TABLE + 16 * i).ok_or_else(bad)?;
            let len = le_u64(&bytes, SECTION_TABLE + 8 + 16 * i).ok_or_else(bad)?;
            let end = off.checked_add(len).ok_or_else(bad)?;
            if (off as usize) < header_len.min(HEADER_LEN) || end > bytes.len() as u64 {
                return Err(bad());
            }
            *s = (off as usize, len as usize);
        }
        let expect = [
            (S_SONG, song_count as usize * SONG_LEN),
            (S_TRCK, track_count as usize * TRACK_LEN),
            (S_CODE, CODEBOOK_BYTES),
            (S_CELL, CELLS * CELL_LEN),
            (S_TITL, 2 * song_count as usize * TITLE_LEN),
            (S_KEYS, key_count as usize * KEY_LEN),
        ];
        if expect.iter().any(|&(s, len)| sections[s].1 != len) {
            return Err(bad());
        }
        let strs = &bytes[sections[S_STRS].0..sections[S_STRS].0 + sections[S_STRS].1];
        let version = strs
            .get(version_off..version_off.saturating_add(version_len))
            .and_then(|b| std::str::from_utf8(b).ok())
            .ok_or_else(bad)?
            .to_string();
        let code = &bytes[sections[S_CODE].0..sections[S_CODE].0 + CODEBOOK_BYTES];
        let mut codebooks = Box::new([[[0f32; HALF]; CODES]; 2]);
        let mut residuals = Box::new([[[0f32; SUB]; CODES]; SUBS]);
        let values = codebooks.iter_mut().flatten().flatten().chain(residuals.iter_mut().flatten().flatten());
        for (i, x) in values.enumerate() {
            *x = f32::from_le_bytes(code[4 * i..4 * i + 4].try_into().map_err(|_| bad())?);
            if !x.is_finite() {
                return Err(bad());
            }
        }
        Ok(Self {
            bytes,
            sections,
            song_count,
            track_count,
            frame_count,
            window_count,
            key_count,
            step_count,
            version,
            codebooks,
            residuals,
        })
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn song_count(&self) -> u32 {
        self.song_count
    }

    pub fn track_count(&self) -> u32 {
        self.track_count
    }

    /// Contour frames over all tracks.
    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }

    /// Indexed windows (postings).
    pub fn window_count(&self) -> u32 {
        self.window_count
    }

    /// Distinct note n-gram keys.
    pub fn key_count(&self) -> u32 {
        self.key_count
    }

    /// Pitch changes over all tracks (n-gram positions).
    pub fn step_count(&self) -> u32 {
        self.step_count
    }

    /// Postings of a note n-gram key: (track, step position), and the count.
    pub(crate) fn note_postings(&self, key: u32) -> Option<(NotePostings<'_>, u32)> {
        let keys = self.section(S_KEYS);
        let (mut lo, mut hi) = (0usize, self.key_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let k = le_u32(keys, mid * KEY_LEN)?;
            match k.cmp(&key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let off = le_u32(keys, mid * KEY_LEN + 4)? as usize;
                    let count = le_u32(keys, mid * KEY_LEN + 8)?;
                    let blob = self.section(S_NPST);
                    return Some((NotePostings { blob, pos: off, left: count, track: 0 }, count));
                }
            }
        }
        None
    }

    /// File size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn section(&self, s: usize) -> &[u8] {
        let (off, len) = self.sections[s];
        &self.bytes[off..off + len]
    }

    fn string(&self, off: u32, len: usize) -> Option<String> {
        let strs = self.section(S_STRS);
        let b = strs.get(off as usize..(off as usize).checked_add(len)?)?;
        std::str::from_utf8(b).ok().map(str::to_string)
    }

    fn song_record(&self, id: u32) -> Option<&[u8]> {
        if id >= self.song_count {
            return None;
        }
        let at = id as usize * SONG_LEN;
        self.section(S_SONG).get(at..at + SONG_LEN)
    }

    pub fn song(&self, id: u32) -> Option<SongMeta> {
        let r = self.song_record(id)?;
        let title = self.string(le_u32(r, 0)?, le_u16(r, 4)? as usize)?;
        let artist = self.string(le_u32(r, 8)?, le_u16(r, 6)? as usize)?;
        let isrc = (r[12] != 0).then(|| String::from_utf8_lossy(&r[12..24]).trim_end_matches('\0').to_string());
        let deezer_id = le_u64(r, 24).filter(|&d| d != 0);
        let year = le_u16(r, 32).filter(|&y| y != 0);
        let popularity = le_u16(r, 34)? as f32 / 65535.0;
        Some(SongMeta { id, title, artist, isrc, deezer_id, year, popularity })
    }

    /// Song popularity (0..=1) without decoding strings.
    pub fn popularity(&self, id: u32) -> f32 {
        self.song_record(id).and_then(|r| le_u16(r, 34)).map(|p| p as f32 / 65535.0).unwrap_or(0.0)
    }

    /// Track ids of a song.
    pub fn song_tracks(&self, id: u32) -> std::ops::Range<u32> {
        let first = |s: u32| self.song_record(s).and_then(|r| le_u32(r, 36));
        let Some(a) = first(id) else { return 0..0 };
        let b = if id + 1 < self.song_count { first(id + 1).unwrap_or(a) } else { self.track_count };
        a.min(self.track_count)..b.clamp(a, self.track_count)
    }

    fn track_record(&self, id: u32) -> Option<&[u8]> {
        if id >= self.track_count {
            return None;
        }
        let at = id as usize * TRACK_LEN;
        self.section(S_TRCK).get(at..at + TRACK_LEN)
    }

    /// Song of a track.
    pub fn track_song(&self, track: u32) -> Option<u32> {
        le_u32(self.track_record(track)?, 0)
    }

    /// Decode a track's contour.
    pub fn track(&self, id: u32) -> Option<Track> {
        let r = self.track_record(id)?;
        let song = le_u32(r, 0)?;
        let off = le_u32(r, 4)? as usize;
        let frames = le_u32(r, 8)? as usize;
        let base = r[12] as f32;
        let source = TrackSource::from_code(r[13]);
        let blob = self.section(S_CONT);
        if frames > blob.len().saturating_mul(1 << 16) {
            return None;
        }
        let pitch = |code: u8| if code == 0 { f32::NAN } else { base + (code - 1) as f32 / 4.0 };
        let mut contour = Vec::with_capacity(frames);
        match r[14] {
            ENC_RAW => contour.extend(blob.get(off..off.checked_add(frames)?)?.iter().map(|&c| pitch(c))),
            ENC_RUNS => {
                let mut pos = off;
                while contour.len() < frames {
                    let code = *blob.get(pos)?;
                    pos += 1;
                    let run = read_varint(blob, &mut pos)? as usize;
                    if run == 0 || contour.len() + run > frames {
                        return None;
                    }
                    contour.extend(std::iter::repeat_n(pitch(code), run));
                }
            }
            _ => return None,
        }
        Some(Track { id, song, source, contour })
    }

    /// The half-vector codebooks.
    pub(crate) fn codebooks(&self) -> &[[[f32; HALF]; CODES]; 2] {
        &self.codebooks
    }

    /// The residual sub-codebooks.
    pub(crate) fn residuals(&self) -> &[[[f32; SUB]; CODES]; SUBS] {
        &self.residuals
    }

    /// Number of postings in a cell.
    pub fn cell_len(&self, cell: usize) -> u32 {
        let t = self.section(S_CELL);
        le_u32(t, cell * CELL_LEN + 4).unwrap_or(0)
    }

    /// Postings of a cell: (track, window start frame, residual codes).
    pub(crate) fn cell(&self, cell: usize) -> Postings<'_> {
        let t = self.section(S_CELL);
        let off = le_u32(t, cell * CELL_LEN).unwrap_or(0) as usize;
        let count = le_u32(t, cell * CELL_LEN + 4).unwrap_or(0);
        Postings { blob: self.section(S_POST), pos: off, left: count, track: 0 }
    }

    /// A song whose normalized title and artist match; falls back to a title match
    /// whose artist overlaps. The most popular wins among equals.
    pub fn find_song(&self, title: &str, artist: &str) -> Option<SongMeta> {
        let t = normalize_title(title);
        let a = normalize_artist(artist);
        if t.is_empty() {
            return None;
        }
        let table = self.section(S_TITL);
        let n = self.song_count as usize;
        let full = self.lookup(&table[..n * TITLE_LEN], fnv1a(&format!("{t}\u{1f}{a}")));
        let mut best: Option<SongMeta> = None;
        let consider = |s: SongMeta, best: &mut Option<SongMeta>| {
            if best.as_ref().is_none_or(|b| s.popularity > b.popularity) {
                *best = Some(s);
            }
        };
        for id in full {
            if let Some(s) = self.song(id)
                && normalize_title(&s.title) == t
                && normalize_artist(&s.artist) == a
            {
                consider(s, &mut best);
            }
        }
        if best.is_some() {
            return best;
        }
        for id in self.lookup(&table[n * TITLE_LEN..], fnv1a(&t)) {
            if let Some(s) = self.song(id)
                && normalize_title(&s.title) == t
            {
                let sa = normalize_artist(&s.artist);
                if a.is_empty() || sa.contains(&a) || a.contains(&sa) || shares_word(&sa, &a) {
                    consider(s, &mut best);
                }
            }
        }
        best
    }

    /// Song ids stored under `hash` in a sorted (hash, song) table.
    fn lookup(&self, table: &[u8], hash: u64) -> Vec<u32> {
        let n = table.len() / TITLE_LEN;
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match le_u64(table, mid * TITLE_LEN) {
                Some(h) if h < hash => lo = mid + 1,
                Some(_) => hi = mid,
                None => return Vec::new(),
            }
        }
        let mut out = Vec::new();
        let mut i = lo;
        while i < n && le_u64(table, i * TITLE_LEN) == Some(hash) {
            if let Some(id) = le_u32(table, i * TITLE_LEN + 8) {
                out.push(id);
            }
            i += 1;
        }
        out
    }
}

/// Iterator over a cell's postings: (track, window start frame, residual codes).
pub(crate) struct Postings<'a> {
    blob: &'a [u8],
    pos: usize,
    left: u32,
    track: u32,
}

impl Iterator for Postings<'_> {
    type Item = (u32, u32, [u8; SUBS]);

    fn next(&mut self) -> Option<(u32, u32, [u8; SUBS])> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let delta = read_varint(self.blob, &mut self.pos)?;
        let hop = read_varint(self.blob, &mut self.pos)?;
        let codes: [u8; SUBS] = self.blob.get(self.pos..self.pos + SUBS)?.try_into().ok()?;
        self.pos += SUBS;
        self.track = self.track.checked_add(delta)?;
        Some((self.track, hop.saturating_mul(crate::melody::contour::REF_HOP as u32), codes))
    }
}

/// Iterator over a note n-gram key's postings: (track, step position).
pub(crate) struct NotePostings<'a> {
    blob: &'a [u8],
    pos: usize,
    left: u32,
    track: u32,
}

impl Iterator for NotePostings<'_> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<(u32, u32)> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let delta = read_varint(self.blob, &mut self.pos)?;
        let at = read_varint(self.blob, &mut self.pos)?;
        self.track = self.track.checked_add(delta)?;
        Some((self.track, at))
    }
}

/// Lowercase, accents folded, bracketed parts and "feat." credits removed,
/// punctuation collapsed to single spaces.
pub fn normalize_title(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth = 0i32;
    let lower = s.to_lowercase();
    let cut = [" feat. ", " feat ", " ft. ", " ft ", " featuring ", " - "]
        .iter()
        .filter_map(|p| lower.find(p))
        .min()
        .unwrap_or(lower.len());
    for c in lower[..cut].chars() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = (depth - 1).max(0),
            _ if depth > 0 => {}
            _ => push_folded(&mut out, c),
        }
    }
    collapse_spaces(&out)
}

/// Like [`normalize_title`] for the first credited artist only.
pub fn normalize_artist(s: &str) -> String {
    let lower = s.to_lowercase();
    let cut = [",", " & ", " x ", " and ", " feat", " ft.", " with ", ";", "/"]
        .iter()
        .filter_map(|p| lower.find(p))
        .min()
        .unwrap_or(lower.len());
    let mut out = String::with_capacity(cut);
    let first = lower[..cut].trim_start_matches("the ");
    for c in first.chars() {
        push_folded(&mut out, c);
    }
    collapse_spaces(&out)
}

fn push_folded(out: &mut String, c: char) {
    let folded = match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' => 'a',
        'ç' | 'ć' | 'č' => 'c',
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ę' => 'e',
        'ì' | 'í' | 'î' | 'ï' | 'ī' => 'i',
        'ñ' | 'ń' => 'n',
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' => 'o',
        'ù' | 'ú' | 'û' | 'ü' | 'ū' => 'u',
        'ý' | 'ÿ' => 'y',
        'ś' | 'š' => 's',
        'ź' | 'ż' | 'ž' => 'z',
        'ł' => 'l',
        'ß' => {
            out.push_str("ss");
            return;
        }
        '&' => {
            out.push_str(" and ");
            return;
        }
        '\'' | '’' | '`' => return,
        c if c.is_alphanumeric() => c,
        _ => ' ',
    };
    out.push(folded);
}

fn collapse_spaces(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn shares_word(a: &str, b: &str) -> bool {
    a.split(' ').filter(|w| w.len() > 2).any(|w| b.split(' ').any(|v| v == w))
}

/// FNV-1a, 64-bit.
pub(crate) fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3))
}

pub(crate) fn read_varint(b: &[u8], pos: &mut usize) -> Option<u32> {
    let mut v = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= ((byte & 0x7f) as u32).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

pub(crate) fn write_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(crate) fn le_u16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

pub(crate) fn le_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

pub(crate) fn le_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip() {
        let mut b = Vec::new();
        for v in [0, 1, 127, 128, 300, 70_000, u32::MAX] {
            write_varint(&mut b, v);
        }
        let mut pos = 0;
        for v in [0, 1, 127, 128, 300, 70_000, u32::MAX] {
            assert_eq!(read_varint(&b, &mut pos), Some(v));
        }
        assert_eq!(read_varint(&[0x80, 0x80], &mut 0), None);
    }

    #[test]
    fn normalization() {
        assert_eq!(normalize_title("Hello (Remastered 2011) - Live"), "hello");
        assert_eq!(normalize_title("Café del Mar feat. Someone"), "cafe del mar");
        assert_eq!(normalize_title("月亮代表我的心"), "月亮代表我的心");
        assert_eq!(normalize_artist("The Beatles"), "beatles");
        assert_eq!(normalize_artist("Calvin Harris & Dua Lipa"), "calvin harris");
        assert_eq!(normalize_artist("Beyoncé, JAY-Z"), "beyonce");
    }

    #[test]
    fn damaged_and_old_files_are_rejected() {
        assert!(MelodyIndex::from_bytes(Vec::new()).is_err());
        assert!(MelodyIndex::from_bytes(vec![0; 400]).is_err());
        let mut b = vec![0u8; HEADER_LEN];
        b[..8].copy_from_slice(MAGIC);
        b[8..12].copy_from_slice(&FORMAT.to_le_bytes());
        b[SECTION_TABLE..SECTION_TABLE + 8].copy_from_slice(&1_000_000u64.to_le_bytes());
        assert!(MelodyIndex::from_bytes(b.clone()).is_err());
        b[8..12].copy_from_slice(&1u32.to_le_bytes());
        let old = MelodyIndex::from_bytes(b).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(old.contains("older"), "{old}");
    }
}
