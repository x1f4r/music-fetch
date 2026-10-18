//! Learned melody encoder: a recall stage next to the matcher's own retrieval.
//!
//! A small network (tools/melody-encoder) maps 8 s of the 20 fps query contour
//! to a unit vector. Every reference window of the most popular songs is
//! embedded offline and stored, 4-bit product-quantized, in a companion file
//! (`melody-encoder-<tier>.mfe`) that belongs to exactly one melody index: it
//! names the index by a digest of its track table and is refused for any other.
//! At query time the query's windows are embedded with tract, every coded
//! window is scored by a fast scan ([`scan`]), and the songs whose windows come
//! closest become [`EncoderHit`]s, which the matcher can re-rank as anchored
//! placements ([`crate::melody::matcher::Hint`]).
//!
//! The file is optional: without it, or when it does not match the index, the
//! matcher runs as before. Format 1 (little-endian):
//!
//! ```text
//! header  192 B  magic "MFENCODE", format u32 = 1, header length u32,
//!                index track digest u64 (FNV-1a 64 over the index's TRCK section),
//!                index track count u32, window count u32,
//!                dim u16, sub-quantizers u16, bits u8 = 4, 0, window frames u16,
//!                reference hop u16, query hop u16, min voiced ‰ u16, max query windows u8, 0,
//!                8 × (offset u64, length u64) sections, 16 B reserved
//! MODL           the encoder as ONNX: input "features" (batch, 3, window), output (batch, dim)
//! PQCB           sub-quantizers × 16 × (dim / sub-quantizers) f32 centroids
//! TWIN           (tracks + 1) × u32: first window of each track (windows ordered by track, start)
//! WSTA           windows × u16: window start in reference hops
//! CODE           ⌈windows / 32⌉ blocks of sub-quantizers × 16 B (see [`scan`])
//! META           UTF-8 version string
//! ```
//!
//! See docs/PIPELINES.md ("Melody encoder") for how files are built and published.

pub mod build;
pub mod scan;

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tract_onnx::pb;
use tract_onnx::prelude::*;
use tract_onnx::tract_core::internal::ensure;

use crate::error::{MfError, Result};
use crate::melody::index::{MelodyIndex, le_u16, le_u32, le_u64};
use crate::melody::matcher::{Hint, HintOptions};
use crate::melody::notes::MelodyQuery;
use crate::util::rt::Instant;

use scan::{BLOCK, KSUB, Kernel};

pub(crate) const MAGIC: &[u8; 8] = b"MFENCODE";
/// Companion file format this core reads and writes.
pub const FORMAT: u32 = 1;
pub(crate) const HEADER_LEN: usize = 192;
pub(crate) const SECTIONS: usize = 8;
pub(crate) const SECTION_TABLE: usize = 48;
pub(crate) const S_MODL: usize = 0;
pub(crate) const S_PQCB: usize = 1;
pub(crate) const S_TWIN: usize = 2;
pub(crate) const S_WSTA: usize = 3;
pub(crate) const S_CODE: usize = 4;
pub(crate) const S_META: usize = 5;
/// Feature channels per frame (relative pitch, voicing, pitch change).
pub const CHANNELS: usize = 3;
/// Batch sizes the encoder is planned for; larger inputs are split, smaller padded.
const BATCHES: [usize; 4] = [1, 2, 4, 8];

/// How much encoder work one search may do.
#[derive(Debug, Clone)]
pub struct EncoderBudget {
    /// Query windows embedded (evenly spaced over the query when there are more).
    pub max_query_windows: usize,
    /// Nearest coded windows kept per query window.
    pub window_neighbours: usize,
    /// Songs returned.
    pub songs: usize,
    /// Song score: the sum over query windows of each one's best similarity
    /// (default), or the single best.
    pub pooling: Pooling,
    /// How hits become matcher placements.
    pub hints: HintOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pooling {
    Sum,
    Max,
}

impl EncoderBudget {
    /// Phones (the Android tier).
    pub fn android() -> Self {
        Self {
            max_query_windows: 6,
            window_neighbours: 1000,
            songs: 200,
            pooling: Pooling::Sum,
            hints: HintOptions { offsets: vec![(1.0, -20.0), (1.0, 0.0), (1.0, 20.0)], votes: false, reserve: 0 },
        }
    }

    /// Macs (the large tier).
    pub fn mac() -> Self {
        Self {
            max_query_windows: 8,
            window_neighbours: 1000,
            songs: 500,
            pooling: Pooling::Sum,
            hints: HintOptions {
                offsets: vec![(0.8, 0.0), (1.0, -20.0), (1.0, 0.0), (1.0, 20.0), (1.25, 0.0)],
                votes: false,
                reserve: 0,
            },
        }
    }

    /// Low-power mode on any device.
    pub fn low_power() -> Self {
        Self {
            max_query_windows: 4,
            window_neighbours: 500,
            songs: 100,
            pooling: Pooling::Sum,
            hints: HintOptions { offsets: vec![(1.0, 0.0)], votes: false, reserve: 0 },
        }
    }
}

/// A song the encoder ranks highly, with where its best window met the query.
#[derive(Debug, Clone, PartialEq)]
pub struct EncoderHit {
    pub song: u32,
    /// Track of the best window pair.
    pub track: u32,
    /// Start of the best reference window (frames).
    pub ref_start: u32,
    /// Start of the query window it met (frames of the query contour).
    pub query_start: u32,
    /// Melody frames in that query window (shorter than a window for short queries).
    pub span: u32,
    /// Pooled song score (see [`Pooling`]).
    pub score: f32,
    /// Cosine similarity of the best window pair.
    pub best: f32,
}

impl EncoderHit {
    /// The hit as a matcher hint: the centres of the two windows meet.
    pub fn hint(&self) -> Hint {
        let half = self.span as f32 / 2.0;
        Hint { track: self.track, ref_frame: self.ref_start as f32 + half, query_frame: self.query_start as f32 + half }
    }
}

/// Where the time of one search went.
#[derive(Debug, Clone, Copy, Default)]
pub struct EncoderTiming {
    pub windows: usize,
    pub encode_ms: f64,
    pub scan_ms: f64,
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

/// File parameters from the header.
#[derive(Debug, Clone, Copy)]
pub struct Params {
    pub tracks: u32,
    pub windows: u32,
    pub dim: usize,
    /// Sub-quantizers.
    pub m: usize,
    /// Model input frames.
    pub window: usize,
    pub ref_hop: usize,
    pub query_hop: usize,
    /// Least voiced share of a window, per mille.
    pub min_voiced: u32,
    pub max_query_windows: usize,
}

/// An open encoder file.
pub struct EncoderIndex {
    bytes: Bytes,
    sections: [(usize, usize); SECTIONS],
    params: Params,
    /// m × 16 × (dim / m).
    centroids: Vec<f32>,
    version: String,
    kernel: Kernel,
    model: OnceLock<Option<Model>>,
}

impl std::fmt::Debug for EncoderIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncoderIndex").field("params", &self.params).field("version", &self.version).finish()
    }
}

impl EncoderIndex {
    /// Map an encoder file and check it against `index`.
    pub fn open(path: &Path, index: &MelodyIndex) -> Result<Self> {
        let file = File::open(path).map_err(|_| MfError::internal("the melody encoder file is missing"))?;
        // Safety: like the index, the file is written once and renamed into place.
        #[cfg(feature = "mmap")]
        let bytes = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(map) => Bytes::Mapped(map),
            Err(_) => Bytes::Owned(
                std::fs::read(path).map_err(|_| MfError::internal("the melody encoder file is unreadable"))?,
            ),
        };
        #[cfg(not(feature = "mmap"))]
        let bytes = {
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut &file, &mut bytes)
                .map_err(|_| MfError::internal("the melody encoder file is unreadable"))?;
            Bytes::Owned(bytes)
        };
        Self::parse(bytes, index)
    }

    /// Open an encoder file held in memory.
    pub fn from_bytes(bytes: Vec<u8>, index: &MelodyIndex) -> Result<Self> {
        Self::parse(Bytes::Owned(bytes), index)
    }

    /// [`EncoderIndex::open`] for optional use: any problem is logged and the
    /// matcher runs without the encoder.
    pub fn open_optional(path: &Path, index: &MelodyIndex) -> Option<Self> {
        if !path.exists() {
            return None;
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Self::open(path, index))) {
            Ok(Ok(e)) => Some(e),
            Ok(Err(e)) => {
                tracing::warn!("melody encoder not used: {e}");
                None
            }
            Err(_) => {
                tracing::error!("opening the melody encoder panicked");
                None
            }
        }
    }

    fn parse(bytes: Bytes, index: &MelodyIndex) -> Result<Self> {
        let bad = || MfError::internal("the melody encoder file is damaged");
        if bytes.len() < 16 || &bytes[..8] != MAGIC {
            return Err(bad());
        }
        let format = le_u32(&bytes, 8).ok_or_else(bad)?;
        if format != FORMAT {
            return Err(MfError::internal(if format > FORMAT {
                "the melody encoder file needs a newer app version"
            } else {
                "the melody encoder file is from an older app version"
            }));
        }
        let header_len = le_u32(&bytes, 12).ok_or_else(bad)? as usize;
        if bytes.len() < HEADER_LEN || header_len < HEADER_LEN || header_len > bytes.len() {
            return Err(bad());
        }
        let digest = le_u64(&bytes, 16).ok_or_else(bad)?;
        let tracks = le_u32(&bytes, 24).ok_or_else(bad)?;
        if digest != index.tracks_digest() || tracks != index.track_count() {
            return Err(MfError::internal("the melody encoder file belongs to another melody index"));
        }
        let windows = le_u32(&bytes, 28).ok_or_else(bad)?;
        let dim = le_u16(&bytes, 32).ok_or_else(bad)? as usize;
        let m = le_u16(&bytes, 34).ok_or_else(bad)? as usize;
        let bits = bytes[36];
        let window = le_u16(&bytes, 38).ok_or_else(bad)? as usize;
        let ref_hop = le_u16(&bytes, 40).ok_or_else(bad)? as usize;
        let query_hop = le_u16(&bytes, 42).ok_or_else(bad)? as usize;
        let min_voiced = le_u16(&bytes, 44).ok_or_else(bad)? as u32;
        let max_query_windows = bytes[46] as usize;
        if bits != 4
            || dim == 0
            || m == 0
            || m > 256
            || !dim.is_multiple_of(m)
            || window == 0
            || ref_hop == 0
            || query_hop == 0
            || max_query_windows == 0
            || min_voiced > 1000
        {
            return Err(bad());
        }
        let mut sections = [(0usize, 0usize); SECTIONS];
        for (i, s) in sections.iter_mut().enumerate() {
            let off = le_u64(&bytes, SECTION_TABLE + 16 * i).ok_or_else(bad)?;
            let len = le_u64(&bytes, SECTION_TABLE + 8 + 16 * i).ok_or_else(bad)?;
            let end = off.checked_add(len).ok_or_else(bad)?;
            if len > 0 && ((off as usize) < header_len || end > bytes.len() as u64) {
                return Err(bad());
            }
            *s = (off as usize, len as usize);
        }
        let dsub = dim / m;
        let blocks = (windows as usize).div_ceil(BLOCK);
        let expect = [
            (S_PQCB, m * KSUB * dsub * 4),
            (S_TWIN, (tracks as usize + 1) * 4),
            (S_WSTA, windows as usize * 2),
            (S_CODE, blocks * m * KSUB),
        ];
        if sections[S_MODL].1 == 0 || expect.iter().any(|&(s, len)| sections[s].1 != len) {
            return Err(bad());
        }
        let section = |s: usize| &bytes[sections[s].0..sections[s].0 + sections[s].1];
        // Track ranges must be ordered and cover every window once, so window → track
        // lookups by bisection are sound.
        let twin = section(S_TWIN);
        let mut prev = 0u32;
        for t in 0..=tracks as usize {
            let w = le_u32(twin, 4 * t).ok_or_else(bad)?;
            if w < prev || w > windows || (t == 0 && w != 0) {
                return Err(bad());
            }
            prev = w;
        }
        if prev != windows {
            return Err(bad());
        }
        let centroids: Vec<f32> = section(S_PQCB).as_chunks::<4>().0.iter().map(|b| f32::from_le_bytes(*b)).collect();
        if centroids.iter().any(|x| !x.is_finite()) {
            return Err(bad());
        }
        let version = std::str::from_utf8(section(S_META)).map_err(|_| bad())?.to_string();
        let params = Params { tracks, windows, dim, m, window, ref_hop, query_hop, min_voiced, max_query_windows };
        Ok(Self { bytes, sections, params, centroids, version, kernel: Kernel::detect(), model: OnceLock::new() })
    }

    pub fn params(&self) -> &Params {
        &self.params
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn size_bytes(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// Bytes of the embedded model.
    pub fn model_bytes(&self) -> usize {
        self.sections[S_MODL].1
    }

    fn section(&self, s: usize) -> &[u8] {
        let (off, len) = self.sections[s];
        &self.bytes[off..off + len]
    }

    /// Use a specific fast-scan kernel (for tests and benchmarks).
    pub fn set_kernel(&mut self, kernel: Kernel) {
        self.kernel = kernel;
    }

    /// Coded windows of a track.
    pub fn track_windows(&self, track: u32) -> std::ops::Range<u32> {
        let twin = self.section(S_TWIN);
        let at = |t: u32| le_u32(twin, 4 * t as usize).unwrap_or(0);
        if track >= self.params.tracks {
            return 0..0;
        }
        at(track)..at(track + 1)
    }

    /// Track and start frame of a coded window.
    fn window_place(&self, w: u32) -> (u32, u32) {
        let twin = self.section(S_TWIN);
        let at = |t: usize| le_u32(twin, 4 * t).unwrap_or(u32::MAX);
        // Last track whose first window is ≤ w (empty tracks share their start).
        let (mut lo, mut hi) = (0usize, self.params.tracks as usize);
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            if at(mid) <= w { lo = mid } else { hi = mid - 1 }
        }
        let start = le_u16(self.section(S_WSTA), 2 * w as usize).unwrap_or(0) as u32;
        (lo as u32, start * self.params.ref_hop as u32)
    }

    fn model(&self) -> Result<&Model> {
        self.model
            .get_or_init(|| match Model::load(self.section(S_MODL), self.params.window, self.params.dim) {
                Ok(m) => Some(m),
                Err(e) => {
                    tracing::error!("melody encoder model: {e:#}");
                    None
                }
            })
            .as_ref()
            .ok_or_else(|| MfError::internal("the melody encoder model could not be loaded"))
    }

    /// Unit embeddings of featurized windows (`n × 3 × window` → `n × dim`).
    pub fn embed(&self, features: &[f32]) -> Result<Vec<f32>> {
        self.model()?.embed(features)
    }

    /// The songs whose coded windows come closest to the query's.
    pub fn search(&self, index: &MelodyIndex, query: &MelodyQuery, budget: &EncoderBudget) -> Result<Vec<EncoderHit>> {
        self.search_timed(index, query, budget).map(|(hits, _)| hits)
    }

    /// [`EncoderIndex::search`] with timings.
    pub fn search_timed(
        &self,
        index: &MelodyIndex,
        query: &MelodyQuery,
        budget: &EncoderBudget,
    ) -> Result<(Vec<EncoderHit>, EncoderTiming)> {
        let p = self.params;
        let started = Instant::now();
        let cap = budget.max_query_windows.min(p.max_query_windows).max(1);
        let windows = query_windows(&query.contour, p.window, p.query_hop, p.min_voiced, cap);
        let mut timing = EncoderTiming { windows: windows.len(), ..Default::default() };
        if windows.is_empty() || p.windows == 0 {
            return Ok((Vec::new(), timing));
        }
        let mut features = vec![0f32; windows.len() * CHANNELS * p.window];
        for (w, out) in windows.iter().zip(features.chunks_exact_mut(CHANNELS * p.window)) {
            featurize(&w.frames, out);
        }
        let emb = self.embed(&features)?;
        timing.encode_ms = started.elapsed().as_secs_f64() * 1e3;
        let started = Instant::now();
        let mut songs: HashMap<u32, SongAcc> = HashMap::new();
        let mut per_window: HashMap<u32, (f32, u32)> = HashMap::new();
        for (w, z) in windows.iter().zip(emb.chunks_exact(p.dim)) {
            per_window.clear();
            for (sim, win) in self.nearest(z, budget.window_neighbours) {
                let (track, _) = self.window_place(win);
                let Some(song) = index.track_song(track) else { continue };
                let e = per_window.entry(song).or_insert((f32::NEG_INFINITY, win));
                if sim > e.0 {
                    *e = (sim, win);
                }
            }
            for (&song, &(sim, win)) in &per_window {
                let acc =
                    songs.entry(song).or_insert(SongAcc { sum: 0.0, best: f32::NEG_INFINITY, win: 0, qw: 0, qspan: 0 });
                acc.sum += sim.max(0.0);
                if sim > acc.best {
                    acc.best = sim;
                    acc.win = win;
                    acc.qw = w.start as u32;
                    acc.qspan = w.span as u32;
                }
            }
        }
        let mut hits: Vec<EncoderHit> = songs
            .into_iter()
            .map(|(song, a)| {
                let (track, ref_start) = self.window_place(a.win);
                let score = match budget.pooling {
                    Pooling::Sum => a.sum,
                    Pooling::Max => a.best,
                };
                EncoderHit { song, track, ref_start, query_start: a.qw, span: a.qspan, score, best: a.best }
            })
            .collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(b.best.total_cmp(&a.best)).then(a.song.cmp(&b.song)));
        hits.truncate(budget.songs);
        timing.scan_ms = started.elapsed().as_secs_f64() * 1e3;
        Ok((hits, timing))
    }

    /// The `k` coded windows nearest to unit vector `z`: (similarity, window), best first.
    /// Candidates come from the 4-bit fast scan; their similarity is then
    /// recomputed from the float tables.
    pub fn nearest(&self, z: &[f32], k: usize) -> Vec<(f32, u32)> {
        let p = self.params;
        let (m, dsub) = (p.m, p.dim / p.m);
        let mut sim = vec![0f32; m * KSUB];
        for s in 0..m {
            let q = &z[s * dsub..(s + 1) * dsub];
            for c in 0..KSUB {
                let cent = &self.centroids[(s * KSUB + c) * dsub..(s * KSUB + c + 1) * dsub];
                sim[s * KSUB + c] = q.iter().zip(cent).map(|(a, b)| a * b).sum();
            }
        }
        let mut lut = vec![0u8; m * KSUB];
        scan::quantize_tables(&sim, &mut lut);
        let codes = self.section(S_CODE);
        let block_bytes = m * KSUB;
        let total = p.windows as usize;
        let k = k.max(1);
        // Max-heap on the quantized distance: the k smallest survive.
        let mut heap: std::collections::BinaryHeap<(u16, u32)> = std::collections::BinaryHeap::with_capacity(k + 1);
        let mut threshold = u16::MAX;
        let mut acc = [0u16; BLOCK];
        for (b, block) in codes.chunks_exact(block_bytes).enumerate() {
            scan::scan_block(self.kernel, block, &lut, &mut acc);
            let base = b * BLOCK;
            let n = BLOCK.min(total - base);
            for (j, &d) in acc[..n].iter().enumerate() {
                if d < threshold || heap.len() < k {
                    heap.push((d, (base + j) as u32));
                    if heap.len() > k {
                        heap.pop();
                    }
                    if heap.len() == k {
                        threshold = heap.peek().map(|e| e.0).unwrap_or(u16::MAX);
                    }
                }
            }
        }
        let mut out: Vec<(f32, u32)> = heap
            .into_iter()
            .map(|(_, w)| {
                let block = &codes[(w as usize / BLOCK) * block_bytes..][..block_bytes];
                let j = w as usize % BLOCK;
                let s: f32 = (0..m).map(|s| sim[s * KSUB + scan::code_at(block, j, s) as usize]).sum();
                (s, w)
            })
            .collect();
        out.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        out
    }
}

struct SongAcc {
    sum: f32,
    best: f32,
    win: u32,
    qw: u32,
    qspan: u32,
}

/// One query window: frames of the contour from `start`, `span` of them melody.
#[derive(Debug, Clone)]
pub struct QueryWindow {
    pub start: usize,
    pub span: usize,
    /// `window` frames, NaN = unvoiced (padding included).
    pub frames: Vec<f32>,
}

/// Cut a query contour into model windows (mirrors `melenc.evaluate.query_windows`):
/// trimmed to the voiced span, windows every `hop` frames with the last one
/// aligned to the end, kept when at least `min_voiced` (per mille) of their
/// frames are voiced. A query shorter than a window is one window padded with
/// rests. More than `cap` windows are thinned evenly.
pub fn query_windows(contour: &[f32], window: usize, hop: usize, min_voiced: u32, cap: usize) -> Vec<QueryWindow> {
    let Some(first) = contour.iter().position(|x| x.is_finite()) else { return Vec::new() };
    let last = contour.iter().rposition(|x| x.is_finite()).unwrap_or(first);
    let c = &contour[first..=last];
    let mut out = Vec::new();
    if c.len() <= window {
        let mut frames = vec![f32::NAN; window];
        frames[..c.len()].copy_from_slice(c);
        out.push(QueryWindow { start: first, span: c.len(), frames });
    } else {
        let end = c.len() - window;
        let mut starts: Vec<usize> = (0..=end).step_by(hop.max(1)).collect();
        if starts.last() != Some(&end) {
            starts.push(end);
        }
        for s in starts {
            let w = &c[s..s + window];
            if voiced_enough(w, window, min_voiced) {
                out.push(QueryWindow { start: first + s, span: window, frames: w.to_vec() });
            }
        }
    }
    if out.len() > cap {
        let n = out.len();
        let keep: Vec<usize> = if cap == 1 {
            vec![n / 2]
        } else {
            (0..cap).map(|i| ((i * (n - 1)) as f64 / (cap - 1) as f64).round() as usize).collect()
        };
        out = keep.into_iter().map(|i| out[i].clone()).collect();
    }
    out
}

/// Reference windows of a track (mirrors `melenc.evaluate.build_index`): every
/// `hop` frames from 0 while fewer than 40 frames remain, padded with rests past
/// the end, kept when at least `min_voiced` (per mille) of their frames are
/// voiced. Returns the start frames.
pub fn reference_windows(contour: &[f32], window: usize, hop: usize, min_voiced: u32) -> Vec<usize> {
    let limit = contour.len().saturating_sub(40).max(1);
    (0..limit)
        .step_by(hop.max(1))
        .filter(|&s| voiced_enough(&contour[s.min(contour.len())..(s + window).min(contour.len())], window, min_voiced))
        .collect()
}

/// At least `per_mille` of `window` frames are voiced among `frames` (the rest are rests).
fn voiced_enough(frames: &[f32], window: usize, per_mille: u32) -> bool {
    frames.iter().filter(|x| x.is_finite()).count() * 1000 >= per_mille as usize * window
}

/// `window` frames of `contour` from `start`, rests past the end.
pub fn reference_window(contour: &[f32], start: usize, window: usize) -> Vec<f32> {
    let mut out = vec![f32::NAN; window];
    if start < contour.len() {
        let n = (contour.len() - start).min(window);
        out[..n].copy_from_slice(&contour[start..start + n]);
    }
    out
}

/// Model features of one window (the spec is `melenc.latency.featurize_np`):
/// channel 0 the pitch relative to the window's voiced median (the lower middle
/// value), rests holding the previous pitch and leading rests taking the median,
/// ÷ 6 and clipped to ±2; channel 1 the voicing flag; channel 2 the change of the
/// held pitch, ÷ 3 and clipped to ±2. `out` is `3 × frames.len()`, channel-major.
pub fn featurize(frames: &[f32], out: &mut [f32]) {
    let n = frames.len();
    debug_assert_eq!(out.len(), CHANNELS * n);
    let mut voiced: Vec<f32> = frames.iter().copied().filter(|x| x.is_finite()).collect();
    let med = if voiced.is_empty() {
        0.0
    } else {
        voiced.sort_by(f32::total_cmp);
        voiced[(voiced.len() - 1) / 2]
    };
    let (rel, rest) = out.split_at_mut(n);
    let (flag, delta) = rest.split_at_mut(n);
    let mut held: Option<f32> = None;
    let mut prev = 0f32;
    for i in 0..n {
        let x = frames[i];
        if x.is_finite() {
            held = Some(x);
        }
        let filled = held.unwrap_or(med);
        rel[i] = ((filled - med) / 6.0).clamp(-2.0, 2.0);
        flag[i] = if x.is_finite() { 1.0 } else { 0.0 };
        delta[i] = if i == 0 { 0.0 } else { ((filled - prev) / 3.0).clamp(-2.0, 2.0) };
        prev = filled;
    }
}

/// The encoder network with plans per batch size.
struct Model {
    proto: pb::ModelProto,
    window: usize,
    dim: usize,
    plans: Mutex<HashMap<usize, Arc<TypedSimplePlan>>>,
}

impl Model {
    fn load(onnx: &[u8], window: usize, dim: usize) -> TractResult<Self> {
        let proto = tract_onnx::onnx().proto_model_for_read(&mut &onnx[..])?;
        let model = Self { proto, window, dim, plans: Mutex::new(HashMap::new()) };
        // Fail now, not on the first hum, when the graph is unusable.
        model.plan(1)?;
        Ok(model)
    }

    fn plan(&self, batch: usize) -> TractResult<Arc<TypedSimplePlan>> {
        if let Some(p) = self.plans.lock().get(&batch) {
            return Ok(p.clone());
        }
        let mut model = tract_onnx::onnx().model_for_proto_model(&self.proto)?;
        model.set_input_fact(0, f32::fact([batch, CHANNELS, self.window]).into())?;
        let plan = model.into_optimized()?.into_runnable()?;
        self.plans.lock().insert(batch, plan.clone());
        Ok(plan)
    }

    /// Output size of the network.
    fn probe_dim(&self) -> TractResult<usize> {
        let input = Tensor::from_shape(&[1, CHANNELS, self.window], &vec![0f32; CHANNELS * self.window])?;
        let result = self.plan(1)?.run(tvec!(input.into()))?;
        Ok(result[0].len())
    }

    /// Embeddings in runs of up to 8 windows (the plans the app keeps).
    fn embed(&self, features: &[f32]) -> Result<Vec<f32>> {
        self.embed_batched(features, BATCHES[BATCHES.len() - 1])
    }

    /// Embeddings in runs of `largest` windows; a shorter last run is padded to
    /// the next planned batch size (or to `largest`).
    fn embed_batched(&self, features: &[f32], largest: usize) -> Result<Vec<f32>> {
        self.embed_inner(features, largest.max(1)).map_err(|e| {
            tracing::error!("melody encoder run: {e:#}");
            MfError::internal("the melody encoder failed")
        })
    }

    fn embed_inner(&self, features: &[f32], largest: usize) -> TractResult<Vec<f32>> {
        let per = CHANNELS * self.window;
        let n = features.len() / per;
        let mut out = Vec::with_capacity(n * self.dim);
        for chunk in features.chunks(per * largest) {
            let rows = chunk.len() / per;
            let batch = BATCHES.iter().copied().find(|&b| b >= rows && b <= largest).unwrap_or(largest);
            let mut x = vec![0f32; batch * per];
            x[..chunk.len()].copy_from_slice(chunk);
            let input = Tensor::from_shape(&[batch, CHANNELS, self.window], &x)?;
            let result = self.plan(batch)?.run(tvec!(input.into()))?;
            let y = result[0].to_plain_array_view::<f32>()?;
            ensure!(y.len() == batch * self.dim, "unexpected encoder output");
            out.extend(y.iter().take(rows * self.dim).copied());
        }
        ensure!(out.iter().all(|x| x.is_finite()), "non-finite embedding");
        Ok(out)
    }
}

#[cfg(test)]
mod tests;
