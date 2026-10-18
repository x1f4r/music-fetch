//! Builds a melody index file from the offline builder's JSONL output.
//!
//! See `docs/CORE_API.md` ("Melody builder input") for the input and
//! [`crate::melody::index`] for the file layout. Each song gets one track per
//! source: the source's frame contour when the line has one, else its phrases'
//! notes placed at their `start_ms` and rendered to a contour. Phrases of one
//! source that overlap in time (e.g. two MIDI parts) become separate tracks.
//! Invalid lines, notes and frames are skipped and counted; songs without a
//! usable track are left out.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::Deserialize;

use crate::error::{MfError, Result};
use crate::melody::contour::{self, DIMS, Filled, HALF, REF_HOP, REF_MIN_VOICED, SUB, SUBS, WINDOW};
use crate::melody::index::{
    self, CELL_LEN, CELLS, CODES, ENC_RAW, ENC_RUNS, FORMAT, HEADER_LEN, MAGIC, S_CELL, S_CODE, S_CONT, S_KEYS, S_NPST,
    S_POST, S_SONG, S_STRS, S_TITL, S_TRCK, SECTION_TABLE, SECTIONS, SONG_LEN, TITLE_LEN, TRACK_LEN, TrackSource,
};
use crate::melody::notes::{CONTOUR_MS, Note};
use crate::melody::{ngrams, steps};

/// Rests shorter than this hold the previous note when notes are rendered (ms).
const NOTE_HOLD_MS: u32 = 300;
/// Unvoiced gaps up to this many frames hold the previous pitch (matches the query's 200 ms).
const GAP_HOLD_FRAMES: usize = 4;
/// Tracks need this many voiced frames (2 s).
const MIN_VOICED_FRAMES: usize = 40;
/// Longest track (15 min).
const MAX_TRACK_FRAMES: usize = 18_000;
/// Parallel lines of one source kept as separate tracks.
const MAX_LANES: usize = 3;
/// Windows sampled for training the codebooks.
const TRAIN_WINDOWS: usize = 400_000;
const KMEANS_ITERS: usize = 15;

#[derive(Debug, Clone, Default)]
pub struct BuildStats {
    pub songs: u32,
    pub tracks: u32,
    pub frames: u64,
    pub windows: u64,
    /// Note n-gram positions and distinct keys.
    pub steps: u64,
    pub keys: u32,
    pub bytes: u64,
    /// Input lines that were not a usable song.
    pub skipped: u32,
}

/// One input line.
#[derive(Debug, Clone, Deserialize)]
pub struct SongInput {
    pub title: String,
    #[serde(default)]
    pub artist: String,
    #[serde(default)]
    pub isrc: Option<String>,
    #[serde(default)]
    pub deezer_id: Option<u64>,
    #[serde(default)]
    pub year: Option<u16>,
    #[serde(default)]
    pub popularity: Option<f32>,
    #[serde(default)]
    pub phrases: Vec<PhraseInput>,
    /// Frame contours; a source with a contour ignores its phrases.
    #[serde(default)]
    pub contours: Vec<ContourInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PhraseInput {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub start_ms: u32,
    /// `[pitch, onset_ms, dur_ms]`.
    pub notes: Vec<[f64; 3]>,
}

/// A source's melody as frames: `pitch[i]` (fractional MIDI, null = unvoiced) at
/// `i / fps` seconds.
#[derive(Debug, Clone, Deserialize)]
pub struct ContourInput {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default = "default_fps")]
    pub fps: f32,
    pub pitch: Vec<Option<f32>>,
}

fn default_fps() -> f32 {
    contour::FPS as f32
}

/// Read `input_jsonl`, write the index to `output` (atomically) and report counts.
pub fn build_index(input_jsonl: &Path, output: &Path, version: &str) -> Result<BuildStats> {
    let file = std::fs::File::open(input_jsonl)
        .map_err(|e| MfError::invalid(format!("cannot read the builder input: {e}")))?;
    let mut songs = Vec::new();
    let mut skipped = 0u32;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<SongInput>(&line) {
            Ok(s) => songs.push(s),
            Err(_) => skipped += 1,
        }
    }
    let (bytes, mut stats) = build_bytes(&songs, version)?;
    stats.skipped += skipped;
    let tmp = output.with_extension("mfi.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, output)?;
    Ok(stats)
}

/// A track before encoding.
struct TrackData {
    source: TrackSource,
    base: u8,
    codes: Vec<u8>,
}

impl TrackData {
    fn decoded(&self) -> Vec<f32> {
        self.codes.iter().map(|&c| if c == 0 { f32::NAN } else { self.base as f32 + (c - 1) as f32 / 4.0 }).collect()
    }
}

/// Build an index in memory.
pub fn build_bytes(songs: &[SongInput], version: &str) -> Result<(Vec<u8>, BuildStats)> {
    let mut stats = BuildStats::default();
    let mut kept: Vec<(&SongInput, Vec<TrackData>)> = Vec::with_capacity(songs.len());
    for s in songs {
        let tracks = song_tracks(s);
        if s.title.trim().is_empty() || tracks.is_empty() {
            stats.skipped += 1;
            continue;
        }
        kept.push((s, tracks));
    }
    if kept.len() > u32::MAX as usize / 2 {
        return Err(MfError::invalid("too many songs for one index"));
    }

    // Window vectors of every track, from the contour exactly as it will be decoded.
    let mut windows: Vec<(u32, u32, contour::Vector)> = Vec::new();
    let mut ngram_postings: Vec<(u32, u32, u32)> = Vec::new();
    let mut track_id = 0u32;
    for (_, tracks) in &kept {
        for t in tracks {
            let decoded = t.decoded();
            let track_steps = steps::frame_steps(&decoded);
            stats.steps += track_steps.len() as u64;
            for (pos, key) in ngrams::ngram_keys(&track_steps) {
                ngram_postings.push((key, track_id, pos));
            }
            if let Some(filled) = Filled::new(&decoded) {
                let mut start = 0;
                while start + WINDOW <= filled.len() {
                    if let Some(v) = filled.window(start as f32, WINDOW as f32, REF_MIN_VOICED)
                        && contour::rms(&v) >= contour::MIN_RMS
                    {
                        windows.push((track_id, (start / REF_HOP) as u32, v));
                    }
                    start += REF_HOP;
                }
            }
            track_id += 1;
        }
    }
    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).min(16);
    let codebooks = train_codebooks(&windows, threads);
    let cells = assign_cells(&windows, &codebooks, threads);
    let residuals = train_residuals(&windows, &cells, &codebooks, threads);
    let codes = encode_residuals(&windows, &cells, &codebooks, &residuals, threads);
    let mut postings: Vec<(u32, u32, u32, [u8; SUBS])> = windows
        .iter()
        .zip(&cells)
        .zip(&codes)
        .map(|((&(track, hop, _), &cell), &code)| (cell, track, hop, code))
        .collect();
    drop(windows);
    postings.sort_unstable();
    let mut cell_tab = vec![0u8; CELLS * CELL_LEN];
    let mut post = Vec::new();
    let mut i = 0;
    while i < postings.len() {
        let cell = postings[i].0 as usize;
        let off = post.len() as u32;
        let mut prev = 0u32;
        let mut count = 0u32;
        while i < postings.len() && postings[i].0 as usize == cell {
            let (_, track, hop, code) = postings[i];
            index::write_varint(&mut post, track - prev);
            index::write_varint(&mut post, hop);
            post.extend_from_slice(&code);
            prev = track;
            count += 1;
            i += 1;
        }
        cell_tab[cell * CELL_LEN..cell * CELL_LEN + 4].copy_from_slice(&off.to_le_bytes());
        cell_tab[cell * CELL_LEN + 4..cell * CELL_LEN + 8].copy_from_slice(&count.to_le_bytes());
    }
    if post.len() > u32::MAX as usize {
        return Err(MfError::invalid("too many windows for one index"));
    }
    stats.windows = postings.len() as u64;
    drop(postings);

    let mut strs = Vec::new();
    let mut add_str = |s: &str, cap: usize| -> (u32, u16) {
        let mut end = s.len().min(cap);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        let off = strs.len() as u32;
        strs.extend_from_slice(&s.as_bytes()[..end]);
        (off, end as u16)
    };
    let (version_off, version_len) = add_str(version, 255);
    let mut song_recs = Vec::with_capacity(kept.len() * SONG_LEN);
    let mut track_recs = Vec::new();
    let mut cont = Vec::new();
    let mut titles = Vec::with_capacity(kept.len());
    let mut track_id = 0u32;
    for (song_id, (s, tracks)) in kept.iter().enumerate() {
        let song_id = song_id as u32;
        let (title_off, title_len) = add_str(s.title.trim(), 512);
        let (artist_off, artist_len) = add_str(s.artist.trim(), 512);
        let mut rec = [0u8; SONG_LEN];
        rec[0..4].copy_from_slice(&title_off.to_le_bytes());
        rec[4..6].copy_from_slice(&title_len.to_le_bytes());
        rec[6..8].copy_from_slice(&artist_len.to_le_bytes());
        rec[8..12].copy_from_slice(&artist_off.to_le_bytes());
        if let Some(isrc) =
            s.isrc.as_deref().map(str::trim).filter(|i| i.len() == 12 && i.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            rec[12..24].copy_from_slice(&isrc.to_ascii_uppercase().as_bytes()[..12]);
        }
        rec[24..32].copy_from_slice(&s.deezer_id.unwrap_or(0).to_le_bytes());
        rec[32..34].copy_from_slice(&s.year.unwrap_or(0).to_le_bytes());
        let pop = s.popularity.filter(|p| p.is_finite()).unwrap_or(0.5).clamp(0.0, 1.0);
        rec[34..36].copy_from_slice(&((pop * 65535.0).round() as u16).to_le_bytes());
        rec[36..40].copy_from_slice(&track_id.to_le_bytes());
        song_recs.extend_from_slice(&rec);
        let t = index::normalize_title(&s.title);
        let a = index::normalize_artist(&s.artist);
        titles.push((index::fnv1a(&format!("{t}\u{1f}{a}")), index::fnv1a(&t), song_id));

        for t in tracks {
            let off = u32::try_from(cont.len()).map_err(|_| MfError::invalid("too many frames for one index"))?;
            let runs = encode_runs(&t.codes);
            let encoding = if runs.len() < t.codes.len() {
                cont.extend_from_slice(&runs);
                ENC_RUNS
            } else {
                cont.extend_from_slice(&t.codes);
                ENC_RAW
            };
            let mut rec = [0u8; TRACK_LEN];
            rec[0..4].copy_from_slice(&song_id.to_le_bytes());
            rec[4..8].copy_from_slice(&off.to_le_bytes());
            rec[8..12].copy_from_slice(&(t.codes.len() as u32).to_le_bytes());
            rec[12] = t.base;
            rec[13] = t.source.code();
            rec[14] = encoding;
            track_recs.extend_from_slice(&rec);
            stats.frames += t.codes.len() as u64;
            track_id += 1;
        }
    }

    let mut title_tab = Vec::with_capacity(2 * titles.len() * TITLE_LEN);
    for which in 0..2 {
        let mut entries: Vec<(u64, u32)> =
            titles.iter().map(|&(full, title, id)| (if which == 0 { full } else { title }, id)).collect();
        entries.sort_unstable();
        for (h, id) in entries {
            title_tab.extend_from_slice(&h.to_le_bytes());
            title_tab.extend_from_slice(&id.to_le_bytes());
        }
    }
    let mut code_tab = Vec::with_capacity(index::CODEBOOK_BYTES);
    for x in codebooks.iter().flatten().flatten().chain(residuals.iter().flatten().flatten()) {
        code_tab.extend_from_slice(&x.to_le_bytes());
    }

    ngram_postings.sort_unstable();
    let mut keys = Vec::new();
    let mut npst = Vec::new();
    let mut i = 0;
    while i < ngram_postings.len() {
        let key = ngram_postings[i].0;
        let off = npst.len() as u32;
        let mut prev = 0u32;
        let mut count = 0u32;
        while i < ngram_postings.len() && ngram_postings[i].0 == key {
            let (_, track, pos) = ngram_postings[i];
            index::write_varint(&mut npst, track - prev);
            index::write_varint(&mut npst, pos);
            prev = track;
            count += 1;
            i += 1;
        }
        keys.extend_from_slice(&key.to_le_bytes());
        keys.extend_from_slice(&off.to_le_bytes());
        keys.extend_from_slice(&count.to_le_bytes());
    }
    if npst.len() > u32::MAX as usize {
        return Err(MfError::invalid("too many note n-grams for one index"));
    }
    drop(ngram_postings);
    let key_count = (keys.len() / index::KEY_LEN) as u32;
    let step_count = u32::try_from(stats.steps).map_err(|_| MfError::invalid("too many notes for one index"))?;

    let song_count = kept.len() as u32;
    let frame_count = u32::try_from(stats.frames).map_err(|_| MfError::invalid("too many frames for one index"))?;
    let window_count = u32::try_from(stats.windows).map_err(|_| MfError::invalid("too many windows for one index"))?;
    let mut sections: [&[u8]; SECTIONS] = [&[]; SECTIONS];
    sections[S_STRS] = &strs;
    sections[S_SONG] = &song_recs;
    sections[S_TRCK] = &track_recs;
    sections[S_CONT] = &cont;
    sections[S_CODE] = &code_tab;
    sections[S_CELL] = &cell_tab;
    sections[S_POST] = &post;
    sections[S_TITL] = &title_tab;
    sections[S_KEYS] = &keys;
    sections[S_NPST] = &npst;

    let mut out = vec![0u8; HEADER_LEN];
    out[..8].copy_from_slice(MAGIC);
    out[8..12].copy_from_slice(&FORMAT.to_le_bytes());
    out[12..16].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    out[16..20].copy_from_slice(&song_count.to_le_bytes());
    out[20..24].copy_from_slice(&track_id.to_le_bytes());
    out[24..28].copy_from_slice(&frame_count.to_le_bytes());
    out[28..32].copy_from_slice(&window_count.to_le_bytes());
    out[32..36].copy_from_slice(&version_off.to_le_bytes());
    out[36..40].copy_from_slice(&(version_len as u32).to_le_bytes());
    out[40..44].copy_from_slice(&key_count.to_le_bytes());
    out[44..48].copy_from_slice(&step_count.to_le_bytes());
    for (i, data) in sections.iter().enumerate() {
        while !out.len().is_multiple_of(8) {
            out.push(0);
        }
        let off = out.len() as u64;
        let at = SECTION_TABLE + 16 * i;
        out[at..at + 8].copy_from_slice(&off.to_le_bytes());
        out[at + 8..at + 16].copy_from_slice(&(data.len() as u64).to_le_bytes());
        out.extend_from_slice(data);
    }

    stats.songs = song_count;
    stats.tracks = track_id;
    stats.keys = key_count;
    stats.bytes = out.len() as u64;
    Ok((out, stats))
}

fn source_of(s: Option<&str>) -> TrackSource {
    match s {
        Some("audio") => TrackSource::Audio,
        Some("midi") => TrackSource::Midi,
        _ => TrackSource::Preview,
    }
}

/// The song's tracks: per source its contour, else its phrases' notes.
fn song_tracks(s: &SongInput) -> Vec<TrackData> {
    let mut out = Vec::new();
    for source in [TrackSource::Preview, TrackSource::Audio, TrackSource::Midi] {
        let contours: Vec<&ContourInput> =
            s.contours.iter().filter(|c| source_of(c.source.as_deref()) == source).collect();
        if !contours.is_empty() {
            for c in contours {
                out.extend(encode_track(source, resample(c)));
            }
            continue;
        }
        let phrases: Vec<&PhraseInput> =
            s.phrases.iter().filter(|p| source_of(p.source.as_deref()) == source).collect();
        for lane in lanes(&phrases) {
            out.extend(encode_track(source, contour::render_notes(&lane, NOTE_HOLD_MS)));
        }
    }
    out
}

/// A contour input at 20 fps (nearest frame when another rate is given).
fn resample(c: &ContourInput) -> Vec<f32> {
    let value = |v: Option<f32>| v.filter(|p| p.is_finite() && (1.0..140.0).contains(p)).unwrap_or(f32::NAN);
    if !(c.fps.is_finite() && c.fps > 0.0) {
        return Vec::new();
    }
    if (c.fps - contour::FPS as f32).abs() < 1e-3 {
        return c.pitch.iter().map(|&v| value(v)).collect();
    }
    let frames = ((c.pitch.len() as f32 / c.fps * contour::FPS as f32) as usize).min(MAX_TRACK_FRAMES * 2);
    (0..frames)
        .map(|i| {
            let j = ((i as f32 + 0.5) * c.fps / contour::FPS as f32) as usize;
            c.pitch.get(j).copied().map(value).unwrap_or(f32::NAN)
        })
        .collect()
}

/// Notes of a source's phrases at their absolute times, split into lanes so that
/// phrases overlapping in time (parallel parts) end up in different tracks.
fn lanes(phrases: &[&PhraseInput]) -> Vec<Vec<Note>> {
    let mut items: Vec<Vec<Note>> = phrases.iter().filter_map(|p| clean_phrase(p)).collect();
    items.sort_by_key(|n| n[0].onset_ms);
    let mut lanes: Vec<Vec<Note>> = Vec::new();
    for notes in items {
        let start = notes[0].onset_ms;
        let free = lanes.iter().position(|l| l.last().is_none_or(|n| n.onset_ms + n.dur_ms <= start + CONTOUR_MS));
        match free {
            Some(i) => {
                let lane = &mut lanes[i];
                if let Some(last) = lane.last_mut() {
                    last.dur_ms = last.dur_ms.min(start.saturating_sub(last.onset_ms).max(1));
                }
                lane.extend(notes);
            }
            None if lanes.len() < MAX_LANES => lanes.push(notes),
            None => {}
        }
    }
    lanes
}

/// Valid, sorted, monophonic notes of a phrase at absolute times; None if fewer than 2 remain.
fn clean_phrase(p: &PhraseInput) -> Option<Vec<Note>> {
    let mut notes: Vec<Note> = p
        .notes
        .iter()
        .filter(|[pitch, onset, dur]| {
            pitch.is_finite()
                && (1.0..140.0).contains(pitch)
                && onset.is_finite()
                && *onset >= 0.0
                && dur.is_finite()
                && *dur > 0.0
        })
        .map(|&[pitch, onset, dur]| Note {
            pitch: pitch as f32,
            onset_ms: p.start_ms.saturating_add(onset.min(u32::MAX as f64 / 4.0) as u32),
            dur_ms: dur.min(600_000.0) as u32,
        })
        .collect();
    notes.sort_by_key(|n| n.onset_ms);
    for i in 1..notes.len() {
        let next = notes[i].onset_ms;
        let prev = &mut notes[i - 1];
        if prev.onset_ms + prev.dur_ms > next {
            prev.dur_ms = next - prev.onset_ms;
        }
    }
    notes.retain(|n| n.dur_ms > 0);
    (notes.len() >= 2).then_some(notes)
}

/// Trim, hold short gaps, quantize to quarter tones above the base pitch.
fn encode_track(source: TrackSource, mut frames: Vec<f32>) -> Option<TrackData> {
    contour::hold_short_gaps(&mut frames, GAP_HOLD_FRAMES + 1);
    let first = frames.iter().position(|v| v.is_finite())?;
    let last = frames.iter().rposition(|v| v.is_finite())?;
    let frames = &frames[first..=last.min(first + MAX_TRACK_FRAMES - 1)];
    if frames.iter().filter(|v| v.is_finite()).count() < MIN_VOICED_FRAMES {
        return None;
    }
    let lo = frames.iter().copied().filter(|v| v.is_finite()).fold(f32::INFINITY, f32::min);
    let base = lo.floor().clamp(0.0, 200.0);
    let codes = frames
        .iter()
        .map(|&v| if v.is_finite() { (((v - base) * 4.0).round() + 1.0).clamp(1.0, 255.0) as u8 } else { 0 })
        .collect();
    Some(TrackData { source, base: base as u8, codes })
}

/// (code, run length varint) pairs.
fn encode_runs(codes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < codes.len() {
        let mut j = i + 1;
        while j < codes.len() && codes[j] == codes[i] {
            j += 1;
        }
        out.push(codes[i]);
        index::write_varint(&mut out, (j - i) as u32);
        i = j;
    }
    out
}

/// k-means codebooks for both halves, trained on a deterministic sample.
fn train_codebooks(windows: &[(u32, u32, contour::Vector)], threads: usize) -> [[[f32; HALF]; CODES]; 2] {
    let step = (windows.len() / TRAIN_WINDOWS).max(1);
    let sample: Vec<&contour::Vector> = windows.iter().step_by(step).map(|w| &w.2).collect();
    let mut books = [[[0f32; HALF]; CODES]; 2];
    for (h, book) in books.iter_mut().enumerate() {
        let points: Vec<[f32; HALF]> = sample
            .iter()
            .map(|v| {
                let mut p = [0f32; HALF];
                p.copy_from_slice(&v[h * HALF..(h + 1) * HALF]);
                p
            })
            .collect();
        let centroids = contour::kmeans(&points, CODES, KMEANS_ITERS, threads);
        for (i, slot) in book.iter_mut().enumerate() {
            // Too few windows to fill the codebook: unused codes sit far away.
            *slot = centroids.get(i).copied().unwrap_or([1.0e6; HALF]);
        }
    }
    books
}

/// Cell of every window: nearest first-half code × 256 + nearest second-half code.
fn assign_cells(
    windows: &[(u32, u32, contour::Vector)],
    books: &[[[f32; HALF]; CODES]; 2],
    threads: usize,
) -> Vec<u32> {
    if windows.is_empty() {
        return Vec::new();
    }
    let chunk = windows.len().div_ceil(threads.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = windows
            .chunks(chunk)
            .map(|part| {
                s.spawn(move || {
                    part.iter()
                        .map(|(_, _, v)| {
                            let (a, b) = contour::halves(v);
                            let ca = contour::nearest_centroid(&a, &books[0]).0;
                            let cb = contour::nearest_centroid(&b, &books[1]).0;
                            (ca * CODES + cb) as u32
                        })
                        .collect::<Vec<u32>>()
                })
            })
            .collect();
        handles.into_iter().flat_map(|h| h.join().unwrap_or_default()).collect()
    })
}

/// A window's residual from its cell's centroids.
fn residual(v: &contour::Vector, cell: u32, books: &[[[f32; HALF]; CODES]; 2]) -> contour::Vector {
    let (a, b) = (cell as usize / CODES, cell as usize % CODES);
    let mut r = *v;
    for (k, x) in r.iter_mut().enumerate() {
        *x -= if k < HALF { books[0][a][k] } else { books[1][b][k - HALF] };
    }
    r
}

/// k-means sub-codebooks for the residuals, trained on a deterministic sample.
fn train_residuals(
    windows: &[(u32, u32, contour::Vector)],
    cells: &[u32],
    books: &[[[f32; HALF]; CODES]; 2],
    threads: usize,
) -> [[[f32; SUB]; CODES]; SUBS] {
    let step = (windows.len() / TRAIN_WINDOWS).max(1);
    let sample: Vec<contour::Vector> =
        windows.iter().zip(cells).step_by(step).map(|(w, &c)| residual(&w.2, c, books)).collect();
    let mut out = [[[0f32; SUB]; CODES]; SUBS];
    for (s, book) in out.iter_mut().enumerate() {
        let points: Vec<[f32; SUB]> = sample
            .iter()
            .map(|r| {
                let mut p = [0f32; SUB];
                p.copy_from_slice(&r[s * SUB..(s + 1) * SUB]);
                p
            })
            .collect();
        let centroids = contour::kmeans(&points, CODES, KMEANS_ITERS, threads);
        for (i, slot) in book.iter_mut().enumerate() {
            *slot = centroids.get(i).copied().unwrap_or([1.0e6; SUB]);
        }
    }
    out
}

/// Residual codes of every window.
fn encode_residuals(
    windows: &[(u32, u32, contour::Vector)],
    cells: &[u32],
    books: &[[[f32; HALF]; CODES]; 2],
    residuals: &[[[f32; SUB]; CODES]; SUBS],
    threads: usize,
) -> Vec<[u8; SUBS]> {
    if windows.is_empty() {
        return Vec::new();
    }
    let chunk = windows.len().div_ceil(threads.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = windows
            .chunks(chunk)
            .zip(cells.chunks(chunk))
            .map(|(part, part_cells)| {
                s.spawn(move || {
                    part.iter()
                        .zip(part_cells)
                        .map(|((_, _, v), &cell)| {
                            let r = residual(v, cell, books);
                            let mut code = [0u8; SUBS];
                            for (k, c) in code.iter_mut().enumerate() {
                                let mut p = [0f32; SUB];
                                p.copy_from_slice(&r[k * SUB..(k + 1) * SUB]);
                                *c = contour::nearest_centroid(&p, &residuals[k]).0 as u8;
                            }
                            code
                        })
                        .collect::<Vec<[u8; SUBS]>>()
                })
            })
            .collect();
        handles.into_iter().flat_map(|h| h.join().unwrap_or_default()).collect()
    })
}

const _: () = assert!(DIMS == 2 * HALF && DIMS == SUBS * SUB && HALF.is_multiple_of(SUB));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::melody::index::MelodyIndex;

    fn song(title: &str, artist: &str, pitches: &[f64]) -> SongInput {
        SongInput {
            title: title.into(),
            artist: artist.into(),
            isrc: Some("USUM71703861".into()),
            deezer_id: Some(42),
            year: Some(2017),
            popularity: Some(0.83),
            phrases: vec![PhraseInput {
                source: Some("preview".into()),
                start_ms: 48_000,
                notes: pitches.iter().enumerate().map(|(i, &p)| [p, i as f64 * 250.0, 240.0]).collect(),
            }],
            contours: Vec::new(),
        }
    }

    fn melody(n: usize, seed: u64) -> Vec<f64> {
        let mut x = seed;
        (0..n)
            .map(|_| {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                60.0 + ((x >> 33) % 12) as f64
            })
            .collect()
    }

    #[test]
    fn round_trip() {
        let mut songs = vec![
            song("Song One (Remastered)", "Artist A & B", &melody(40, 1)),
            song("Other", "Someone", &melody(30, 2)),
            SongInput { title: " ".into(), ..song("x", "y", &[60.0, 61.0]) },
        ];
        songs[1].contours = vec![ContourInput {
            source: Some("audio".into()),
            fps: 20.0,
            pitch: (0..200).map(|i| if i % 50 < 45 { Some(62.0 + (i / 20) as f32 * 0.5) } else { None }).collect(),
        }];
        let (bytes, stats) = build_bytes(&songs, "2026.10.1").expect("build");
        assert_eq!((stats.songs, stats.tracks, stats.skipped), (2, 3, 1));
        assert!(stats.windows > 20);
        let idx = MelodyIndex::from_bytes(bytes).expect("open");
        assert_eq!(idx.version(), "2026.10.1");
        assert_eq!(idx.song_count(), 2);
        assert_eq!(idx.track_count(), 3);
        let s = idx.song(1).expect("song");
        assert_eq!(s.title, "Other");
        assert_eq!(s.isrc.as_deref(), Some("USUM71703861"));
        assert_eq!((s.deezer_id, s.year), (Some(42), Some(2017)));
        assert!((s.popularity - 0.83).abs() < 1e-4);
        assert_eq!(idx.song_tracks(0), 0..1);
        assert_eq!(idx.song_tracks(1), 1..3);
        let t = idx.track(0).expect("track");
        assert_eq!(t.source, TrackSource::Preview);
        // 40 notes of 240 ms every 250 ms: 10 s at 20 fps, rests held.
        assert_eq!(t.contour.len(), 200);
        assert!(t.contour.iter().all(|v| v.is_finite()));
        let a = idx.track(2).expect("audio track");
        assert_eq!(a.source, TrackSource::Audio);
        assert!((a.contour[0] - 62.0).abs() < 1e-6 && (a.contour[25] - 62.5).abs() < 1e-6);
        assert!(a.contour[46].is_nan(), "{:?}", &a.contour[40..50]);
        assert_eq!(idx.find_song("song one", "Artist A").map(|s| s.id), Some(0));
        assert_eq!(idx.find_song("SONG ONE - 2011 Remaster", "artist a feat. c").map(|s| s.id), Some(0));
        assert!(idx.find_song("Song One", "Nobody").is_none());
        // Every window of track 0 is posted in the cell its vector falls in.
        let filled = Filled::new(&t.contour).expect("filled");
        let v = filled.window(20.0, WINDOW as f32, REF_MIN_VOICED).expect("window");
        let (h1, h2) = contour::halves(&v);
        let books = idx.codebooks();
        let cell = contour::nearest_centroid(&h1, &books[0]).0 * CODES + contour::nearest_centroid(&h2, &books[1]).0;
        assert!(idx.cell(cell).any(|(track, start, _)| track == 0 && start == 20));
        assert_eq!(idx.cell(cell).count() as u32, idx.cell_len(cell));
    }

    #[test]
    fn overlapping_phrases_become_lanes_and_hostile_notes_are_cleaned() {
        let mut s = song("t", "a", &[]);
        s.phrases = vec![
            PhraseInput {
                source: Some("midi".into()),
                start_ms: 0,
                notes: vec![[60.0, 0.0, 3000.0], [62.0, 3000.0, 3000.0]],
            },
            PhraseInput {
                source: Some("midi".into()),
                start_ms: 1000,
                notes: vec![[48.0, 0.0, 3000.0], [50.0, 3000.0, 3000.0]],
            },
            PhraseInput {
                source: Some("preview".into()),
                start_ms: 0,
                notes: vec![
                    [f64::NAN, 0.0, 10.0],
                    [60.0, 500.0, 900.0],
                    [62.0, 100.0, 1e12],
                    [64.0, -5.0, 10.0],
                    [65.0, 700.0, 3000.0],
                ],
            },
        ];
        let (bytes, stats) = build_bytes(&[s], "v").expect("build");
        assert_eq!(stats.tracks, 3);
        let idx = MelodyIndex::from_bytes(bytes).expect("open");
        let sources: Vec<TrackSource> = (0..3).filter_map(|t| idx.track(t)).map(|t| t.source).collect();
        assert_eq!(sources, vec![TrackSource::Preview, TrackSource::Midi, TrackSource::Midi]);
        let p = idx.track(0).expect("preview");
        // 62 from 100 ms (cut at 500), 60 from 500 (cut at 700), 65 from 700 for 3 s.
        assert_eq!(p.contour.len(), 72);
        assert_eq!((p.contour[0], p.contour[8], p.contour[12]), (62.0, 60.0, 65.0));
        assert_eq!(idx.track(2).expect("lane").contour[0], 48.0);
    }

    #[test]
    fn empty_input_builds_an_empty_index() {
        let (bytes, stats) = build_bytes(&[], "v").expect("build");
        assert_eq!((stats.songs, stats.tracks, stats.windows), (0, 0, 0));
        let idx = MelodyIndex::from_bytes(bytes).expect("open");
        assert_eq!(idx.song_count(), 0);
        assert!(idx.track(0).is_none());
        assert_eq!(idx.cell(0).count(), 0);
    }
}
