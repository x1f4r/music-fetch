//! Writing encoder files (`mf index encoder build`): embed every reference window
//! of the chosen songs with the same featurizer and tract plan the app uses, train
//! a 4-bit product quantizer on a sample and write the coded windows.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{MfError, Result};
use crate::melody::contour::kmeans;
use crate::melody::index::MelodyIndex;
use crate::util::rt::Instant;

use super::scan::{self, BLOCK, KSUB};
use super::{CHANNELS, FORMAT, HEADER_LEN, MAGIC, Model, SECTION_TABLE, SECTIONS, featurize};
use super::{S_CODE, S_META, S_MODL, S_PQCB, S_TWIN, S_WSTA, reference_window, reference_windows};

/// What goes into an encoder file.
#[derive(Debug, Clone)]
pub struct BuildOptions {
    /// Only the most popular songs get windows (all when None).
    pub top_songs: Option<usize>,
    /// Model input frames.
    pub window: usize,
    /// Reference window hop (frames).
    pub hop: usize,
    /// Query window hop the file asks for (frames).
    pub query_hop: usize,
    /// Least voiced share of a window, per mille.
    pub min_voiced: u32,
    /// Sub-quantizers (bytes per window × 2).
    pub sub_quantizers: usize,
    /// Most query windows a search embeds.
    pub max_query_windows: usize,
    /// Windows sampled to train the quantizer.
    pub train_windows: usize,
    pub kmeans_iters: usize,
    pub threads: usize,
    /// Stored in the file (META).
    pub version: String,
    /// Keep the window embeddings here and reuse them when index, model and window
    /// options match (another quantizer for the same windows costs seconds).
    pub embeddings_cache: Option<std::path::PathBuf>,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            top_songs: Some(30_000),
            window: 160,
            hop: 40,
            query_hop: 40,
            min_voiced: 300,
            sub_quantizers: 32,
            max_query_windows: 8,
            train_windows: 300_000,
            kmeans_iters: 20,
            threads: 4,
            version: String::new(),
            embeddings_cache: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BuildStats {
    pub songs: usize,
    pub tracks: usize,
    pub windows: usize,
    pub dim: usize,
    pub bytes: usize,
    pub model_bytes: usize,
    /// Mean dot product of an embedding (unit length) with its quantized reconstruction.
    pub reconstruction: f32,
    pub embed_s: f64,
    pub quantize_s: f64,
}

/// Build an encoder file for `index` and write it to `out` (atomically).
pub fn build_file(
    index: &MelodyIndex,
    onnx: &[u8],
    options: &BuildOptions,
    out: &Path,
    progress: &(dyn Fn(&str) + Sync),
) -> Result<BuildStats> {
    let (bytes, stats) = build(index, onnx, options, progress)?;
    let tmp = out.with_extension("mfe.tmp");
    let io = |e: std::io::Error| MfError::Storage { message: format!("writing {}: {e}", out.display()) };
    std::fs::write(&tmp, &bytes).map_err(io)?;
    std::fs::rename(&tmp, out).map_err(io)?;
    Ok(stats)
}

/// Build an encoder file for `index` in memory.
pub fn build(
    index: &MelodyIndex,
    onnx: &[u8],
    o: &BuildOptions,
    progress: &(dyn Fn(&str) + Sync),
) -> Result<(Vec<u8>, BuildStats)> {
    let fail = |what: &str| MfError::internal(format!("encoder build: {what}"));
    if o.window == 0 || o.hop == 0 || o.query_hop == 0 || o.max_query_windows == 0 || o.min_voiced > 1000 {
        return Err(fail("bad options"));
    }
    let mut model = Model::load(onnx, o.window, 0).map_err(|e| fail(&format!("model: {e:#}")))?;
    model.dim = model.probe_dim().map_err(|e| fail(&format!("model: {e:#}")))?;
    let dim = model.dim;
    let m = o.sub_quantizers;
    if m == 0 || m > 256 || !dim.is_multiple_of(m) || !matches!(dim / m, 1 | 2 | 4 | 8) {
        return Err(fail(&format!("{dim} dimensions do not split into {m} sub-quantizers of 1, 2, 4 or 8")));
    }
    let dsub = dim / m;

    // Songs and their windows.
    let songs = index.song_count() as usize;
    let mut order: Vec<u32> = (0..songs as u32).collect();
    order.sort_by(|a, b| index.popularity(*b).total_cmp(&index.popularity(*a)).then(a.cmp(b)));
    let mut chosen = vec![false; songs];
    for &s in order.iter().take(o.top_songs.unwrap_or(songs)) {
        chosen[s as usize] = true;
    }
    let tracks = index.track_count() as usize;
    let starts: Vec<Vec<u16>> = (0..tracks as u32)
        .map(|t| {
            let wanted = index.track_song(t).is_some_and(|s| chosen.get(s as usize).copied().unwrap_or(false));
            let Some(track) = wanted.then(|| index.track(t)).flatten() else { return Vec::new() };
            reference_windows(&track.contour, o.window, o.hop, o.min_voiced)
                .into_iter()
                .filter_map(|s| u16::try_from(s / o.hop).ok())
                .collect()
        })
        .collect();
    let mut first = Vec::with_capacity(tracks + 1);
    let mut total = 0usize;
    for s in &starts {
        first.push(total as u32);
        total += s.len();
    }
    first.push(total as u32);
    if total > u32::MAX as usize {
        return Err(fail("too many windows"));
    }
    progress(&format!(
        "{} songs, {} tracks with windows, {total} windows, {dim} dimensions",
        chosen.iter().filter(|c| **c).count(),
        starts.iter().filter(|s| !s.is_empty()).count()
    ));

    // Embeddings, in window order: runs of whole tracks of about 256 windows, each
    // embedded in batches of 8 (the plans the app uses) into its own output slice.
    let started = Instant::now();
    let key = cache_key(index, onnx, o, total, dim);
    let cached = o.embeddings_cache.as_deref().and_then(|p| read_cache(p, &key, total * dim));
    if cached.is_some() {
        progress("embeddings from the cache");
    }
    let fresh = cached.is_none();
    let mut emb = cached.unwrap_or_else(|| vec![0f32; total * dim]);
    let mut work: Vec<(std::ops::Range<usize>, &mut [f32])> = Vec::new();
    {
        let mut rest: &mut [f32] = &mut emb;
        let mut t0 = 0;
        while fresh && t0 < tracks {
            let mut t1 = t0;
            while t1 < tracks && (first[t1] - first[t0]) < 256 {
                t1 += 1;
            }
            let n = (first[t1] - first[t0]) as usize * dim;
            let (head, tail) = std::mem::take(&mut rest).split_at_mut(n);
            work.push((t0..t1, head));
            rest = tail;
            t0 = t1;
        }
    }
    let queue = std::sync::Mutex::new(work);
    let done = AtomicUsize::new(0);
    let step = (total / 20).max(1);
    let threads = o.threads.clamp(1, 64);
    let results: Vec<Result<()>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (model, starts, queue, done) = (&model, &starts, &queue, &done);
                scope.spawn(move || -> Result<()> {
                    let per = CHANNELS * o.window;
                    // Not `while let`: the queue's lock must be released before the work.
                    #[allow(clippy::while_let_loop)]
                    loop {
                        let Some((range, out)) = queue.lock().ok().and_then(|mut q| q.pop()) else { break };
                        let n: usize = range.clone().map(|t| starts[t].len()).sum();
                        let mut features = vec![0f32; n * per];
                        let mut f = features.chunks_exact_mut(per);
                        for t in range {
                            if starts[t].is_empty() {
                                continue;
                            }
                            let contour = index.track(t as u32).map(|tr| tr.contour).unwrap_or_default();
                            for &start in &starts[t] {
                                if let Some(f) = f.next() {
                                    featurize(&reference_window(&contour, start as usize * o.hop, o.window), f);
                                }
                            }
                        }
                        let z = model.embed(&features)?;
                        out.copy_from_slice(&z);
                        let before = done.fetch_add(n, Ordering::Relaxed);
                        if (before + n) / step > before / step {
                            progress(&format!("embedded {} of {total} windows", before + n));
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(fail("embedding thread panicked")))).collect()
    });
    for r in results {
        r?;
    }
    if fresh && let Some(path) = &o.embeddings_cache {
        write_cache(path, &key, &emb).map_err(|e| fail(&format!("embedding cache: {e}")))?;
    }
    let embed_s = started.elapsed().as_secs_f64();

    // Product quantizer: 16 centroids per sub-space, trained on an even sample.
    let started = Instant::now();
    let sample: Vec<usize> = if total <= o.train_windows {
        (0..total).collect()
    } else {
        (0..o.train_windows).map(|i| i * total / o.train_windows).collect()
    };
    let mut centroids = vec![0f32; m * KSUB * dsub];
    for s in 0..m {
        let points: Vec<&[f32]> = sample.iter().map(|&w| &emb[w * dim + s * dsub..w * dim + (s + 1) * dsub]).collect();
        let c = match dsub {
            1 => train::<1>(&points, o.kmeans_iters, threads),
            2 => train::<2>(&points, o.kmeans_iters, threads),
            4 => train::<4>(&points, o.kmeans_iters, threads),
            _ => train::<8>(&points, o.kmeans_iters, threads),
        };
        centroids[s * KSUB * dsub..(s + 1) * KSUB * dsub].copy_from_slice(&c);
    }
    // Codes, packed in blocks of 32 windows.
    let blocks = total.div_ceil(BLOCK);
    let block_bytes = m * KSUB;
    let mut code = vec![0u8; blocks * block_bytes];
    let mut cosine_sum = 0f64;
    let chunk_blocks = blocks.div_ceil(threads).max(1);
    let sums: Vec<f64> = std::thread::scope(|scope| {
        let handles: Vec<_> = code
            .chunks_mut(chunk_blocks * block_bytes)
            .enumerate()
            .map(|(c, part)| {
                let (emb, centroids) = (&emb, &centroids);
                scope.spawn(move || {
                    let mut cos = 0f64;
                    for (bi, block) in part.chunks_exact_mut(block_bytes).enumerate() {
                        let b = c * chunk_blocks + bi;
                        let codes: Vec<Vec<u8>> = (b * BLOCK..((b + 1) * BLOCK).min(total))
                            .map(|w| {
                                let z = &emb[w * dim..(w + 1) * dim];
                                (0..m)
                                    .map(|s| {
                                        let q = &z[s * dsub..(s + 1) * dsub];
                                        let (best, dot) =
                                            nearest(q, &centroids[s * KSUB * dsub..(s + 1) * KSUB * dsub]);
                                        cos += dot as f64;
                                        best as u8
                                    })
                                    .collect()
                            })
                            .collect();
                        scan::pack_block(&codes, m, block);
                    }
                    cos
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap_or(0.0)).collect()
    });
    cosine_sum += sums.iter().sum::<f64>();
    let quantize_s = started.elapsed().as_secs_f64();

    // The file.
    let mut twin = Vec::with_capacity(first.len() * 4);
    for w in &first {
        twin.extend_from_slice(&w.to_le_bytes());
    }
    let mut wsta = Vec::with_capacity(total * 2);
    for s in starts.iter().flatten() {
        wsta.extend_from_slice(&s.to_le_bytes());
    }
    let pqcb: Vec<u8> = centroids.iter().flat_map(|x| x.to_le_bytes()).collect();
    let mut sections: Vec<(usize, &[u8])> = vec![
        (S_MODL, onnx),
        (S_PQCB, &pqcb),
        (S_TWIN, &twin),
        (S_WSTA, &wsta),
        (S_CODE, &code),
        (S_META, o.version.as_bytes()),
    ];
    sections.sort_by_key(|s| s.0);
    let mut bytes = vec![0u8; HEADER_LEN];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..12].copy_from_slice(&FORMAT.to_le_bytes());
    bytes[12..16].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&index.tracks_digest().to_le_bytes());
    bytes[24..28].copy_from_slice(&(tracks as u32).to_le_bytes());
    bytes[28..32].copy_from_slice(&(total as u32).to_le_bytes());
    bytes[32..34].copy_from_slice(&(dim as u16).to_le_bytes());
    bytes[34..36].copy_from_slice(&(m as u16).to_le_bytes());
    bytes[36] = 4;
    bytes[38..40].copy_from_slice(&(o.window as u16).to_le_bytes());
    bytes[40..42].copy_from_slice(&(o.hop as u16).to_le_bytes());
    bytes[42..44].copy_from_slice(&(o.query_hop as u16).to_le_bytes());
    bytes[44..46].copy_from_slice(&(o.min_voiced as u16).to_le_bytes());
    bytes[46] = o.max_query_windows.min(255) as u8;
    for (s, data) in sections {
        // 64-byte aligned sections.
        bytes.resize(bytes.len().div_ceil(64) * 64, 0);
        let at = SECTION_TABLE + 16 * s;
        debug_assert!(s < SECTIONS);
        let offset = bytes.len() as u64;
        bytes[at..at + 8].copy_from_slice(&offset.to_le_bytes());
        bytes[at + 8..at + 16].copy_from_slice(&(data.len() as u64).to_le_bytes());
        bytes.extend_from_slice(data);
    }
    let stats = BuildStats {
        songs: chosen.iter().filter(|c| **c).count(),
        tracks: starts.iter().filter(|s| !s.is_empty()).count(),
        windows: total,
        dim,
        bytes: bytes.len(),
        model_bytes: onnx.len(),
        reconstruction: if total > 0 { (cosine_sum / total as f64) as f32 } else { 0.0 },
        embed_s,
        quantize_s,
    };
    Ok((bytes, stats))
}

/// What cached embeddings depend on.
fn cache_key(index: &MelodyIndex, onnx: &[u8], o: &BuildOptions, windows: usize, dim: usize) -> Vec<u8> {
    let mut key = b"MFENCEMB".to_vec();
    for v in [
        index.tracks_digest(),
        crate::melody::index::fnv1a_bytes(onnx),
        windows as u64,
        dim as u64,
        o.top_songs.map_or(0, |t| t as u64 + 1),
        o.window as u64,
        o.hop as u64,
        o.min_voiced as u64,
    ] {
        key.extend_from_slice(&v.to_le_bytes());
    }
    key
}

fn read_cache(path: &Path, key: &[u8], len: usize) -> Option<Vec<f32>> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.len() != key.len() + 4 * len || &bytes[..key.len()] != key {
        return None;
    }
    Some(bytes[key.len()..].as_chunks::<4>().0.iter().map(|b| f32::from_le_bytes(*b)).collect())
}

fn write_cache(path: &Path, key: &[u8], emb: &[f32]) -> std::io::Result<()> {
    let mut bytes = Vec::with_capacity(key.len() + 4 * emb.len());
    bytes.extend_from_slice(key);
    for x in emb {
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)
}

/// k-means with 16 centroids over `N`-dimensional sub-vectors, flattened.
fn train<const N: usize>(points: &[&[f32]], iters: usize, threads: usize) -> Vec<f32> {
    let pts: Vec<[f32; N]> = points.iter().map(|p| std::array::from_fn(|i| p[i])).collect();
    let mut c = kmeans(&pts, KSUB, iters, threads);
    while c.len() < KSUB {
        c.push(c.last().copied().unwrap_or([0.0; N]));
    }
    c.into_iter().flatten().collect()
}

/// Nearest of 16 flattened centroids: (index, dot product of `q` and the centroid).
fn nearest(q: &[f32], centroids: &[f32]) -> (usize, f32) {
    let n = q.len();
    let mut best = (0usize, f32::INFINITY, 0f32);
    for (i, c) in centroids.chunks_exact(n).enumerate() {
        let d: f32 = q.iter().zip(c).map(|(a, b)| (a - b) * (a - b)).sum();
        if d < best.1 {
            best = (i, d, q.iter().zip(c).map(|(a, b)| a * b).sum());
        }
    }
    (best.0, best.2)
}
