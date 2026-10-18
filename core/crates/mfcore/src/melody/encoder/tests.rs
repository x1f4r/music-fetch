use super::build::{BuildOptions, build};
use super::scan::{self, BLOCK, KSUB, Kernel};
use super::*;
use crate::melody::builder::{PhraseInput, SongInput, build_bytes};
use crate::melody::matcher::{self, MatchOptions};

static TINY: &[u8] = include_bytes!("fixtures/tiny.onnx");
static WINDOWS: &[u8] = include_bytes!("fixtures/windows.bin");
static FEATURES: &[u8] = include_bytes!("fixtures/features.bin");
static EMBEDDINGS: &[u8] = include_bytes!("fixtures/embeddings.bin");
static QUERY: &[u8] = include_bytes!("fixtures/query.bin");
static QUERY_WINDOWS: &[u8] = include_bytes!("fixtures/query-windows.bin");
static META: &str = include_str!("fixtures/fixtures.json");

fn f32s(b: &[u8]) -> Vec<f32> {
    b.as_chunks::<4>().0.iter().map(|c| f32::from_le_bytes(*c)).collect()
}

fn same_bits(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()))
}

/// Deterministic pseudo-random numbers.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn unit(&mut self) -> f32 {
        (self.next() >> 7) as f32 / (1u64 << 24) as f32
    }
}

#[test]
fn featurizer_matches_the_python_spec_bit_for_bit() {
    let windows = f32s(WINDOWS);
    let want = f32s(FEATURES);
    let w = 160;
    let mut got = vec![0f32; want.len()];
    for (x, out) in windows.chunks_exact(w).zip(got.chunks_exact_mut(CHANNELS * w)) {
        featurize(x, out);
    }
    for (i, (a, b)) in got.chunks_exact(CHANNELS * w).zip(want.chunks_exact(CHANNELS * w)).enumerate() {
        assert!(same_bits(a, b), "window {i} differs");
    }
}

#[test]
fn windows_match_the_python_spec() {
    let meta: serde_json::Value = serde_json::from_str(META).expect("meta");
    let query = f32s(QUERY);
    let want = f32s(QUERY_WINDOWS);
    let got = query_windows(&query, 160, 40, 300, usize::MAX);
    assert_eq!(got.len(), meta["query_windows"].as_u64().unwrap_or(0) as usize);
    for (g, w) in got.iter().zip(want.as_chunks::<160>().0) {
        assert!(same_bits(&g.frames, w), "query window at {} differs", g.start);
        assert_eq!(g.span, 160);
    }
    let starts: Vec<usize> =
        meta["ref_starts"].as_array().expect("starts").iter().filter_map(|v| v.as_u64()).map(|v| v as usize).collect();
    assert_eq!(reference_windows(&query, 160, 40, 300), starts);
    // Thinning keeps the first and the last window.
    let thin = query_windows(&query, 160, 40, 300, 4);
    assert_eq!(thin.len(), 4);
    assert_eq!(thin[0].start, got[0].start);
    assert_eq!(thin[3].start, got[got.len() - 1].start);
    // Short queries are one padded window.
    let short = query_windows(&[f32::NAN, 60.0, 61.0, f32::NAN, 62.0, f32::NAN], 160, 40, 300, 8);
    assert_eq!(short.len(), 1);
    assert_eq!((short[0].start, short[0].span), (1, 4));
    assert!(short[0].frames[4..].iter().all(|x| x.is_nan()));
    assert!(query_windows(&[f32::NAN; 30], 160, 40, 300, 8).is_empty());
}

#[test]
fn tract_matches_onnxruntime() {
    let features = f32s(FEATURES);
    let want = f32s(EMBEDDINGS);
    let model = Model::load(TINY, 160, 16).expect("model");
    assert_eq!(model.probe_dim().expect("dim"), 16);
    let per = CHANNELS * 160;
    for (rows, largest) in [(1, 8), (3, 8), (8, 8), (16, 8), (16, 64), (5, 2)] {
        let got = model.embed_batched(&features[..rows * per], largest).expect("embed");
        let diff = got.iter().zip(&want).map(|(a, b)| (a - b).abs()).fold(0f32, f32::max);
        assert_eq!(got.len(), rows * 16);
        assert!(diff <= 1e-5, "{rows} rows in runs of {largest}: max |Δ| {diff}");
    }
}

#[test]
fn fast_scan_kernels_agree_with_the_scalar_one() {
    let mut rng = Rng(7);
    let detected = Kernel::detect();
    for m in [1usize, 2, 16, 32, 64, 256] {
        let codes: Vec<u8> = (0..m * KSUB).map(|_| rng.next() as u8).collect();
        let mut lut: Vec<u8> = (0..m * KSUB).map(|_| rng.next() as u8).collect();
        let (mut a, mut b) = ([0u16; BLOCK], [0u16; BLOCK]);
        scan::scan_block_scalar(&codes, &lut, &mut a);
        scan::scan_block(detected, &codes, &lut, &mut b);
        assert_eq!(a, b, "m = {m}, {detected:?}");
        // The extreme: every table entry 255.
        lut.fill(255);
        scan::scan_block_scalar(&codes, &lut, &mut a);
        scan::scan_block(detected, &codes, &lut, &mut b);
        assert_eq!(a, b);
        assert!(a.iter().all(|&x| x as usize == 255 * m));
    }
    #[cfg(target_arch = "aarch64")]
    assert_eq!(detected, Kernel::Neon);
    #[cfg(target_arch = "x86_64")]
    assert_eq!(detected, Kernel::Ssse3);
}

#[test]
fn packing_round_trips_and_scans_like_adc() {
    let mut rng = Rng(11);
    let m = 8;
    let codes: Vec<Vec<u8>> = (0..27).map(|_| (0..m).map(|_| (rng.next() % 16) as u8).collect()).collect();
    let mut block = vec![0u8; m * KSUB];
    scan::pack_block(&codes, m, &mut block);
    for (w, c) in codes.iter().enumerate() {
        for (s, &code) in c.iter().enumerate() {
            assert_eq!(scan::code_at(&block, w, s), code);
        }
    }
    let sim: Vec<f32> = (0..m * KSUB).map(|_| rng.unit() * 2.0 - 1.0).collect();
    let mut lut = vec![0u8; m * KSUB];
    let (scale, offset) = scan::quantize_tables(&sim, &mut lut);
    let mut acc = [0u16; BLOCK];
    scan::scan_block(Kernel::detect(), &block, &lut, &mut acc);
    for (w, c) in codes.iter().enumerate() {
        let exact: f32 = c.iter().enumerate().map(|(s, &code)| sim[s * KSUB + code as usize]).sum();
        let approx = offset - acc[w] as f32 / scale;
        assert!((exact - approx).abs() <= 0.5 * m as f32 / scale + 1e-4, "{exact} vs {approx}");
    }
}

/// A random stepwise melody of `n` notes: [pitch, onset, duration].
fn melody(rng: &mut Rng, n: usize) -> Vec<[f64; 3]> {
    let mut pitch = 60.0f64 + (rng.next() % 12) as f64;
    let mut t = 0.0f64;
    (0..n)
        .map(|_| {
            let step = [-4.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 5.0, -5.0, 7.0][(rng.next() % 10) as usize];
            pitch = (pitch + step).clamp(50.0, 80.0);
            let dur = [250.0, 250.0, 500.0, 375.0, 750.0][(rng.next() % 5) as usize];
            let note = [pitch, t, dur * 0.9];
            t += dur;
            note
        })
        .collect()
}

fn library(seed: u64, songs: usize) -> MelodyIndex {
    let mut rng = Rng(seed);
    let songs: Vec<SongInput> = (0..songs)
        .map(|i| SongInput {
            title: if i == 37 { "Target".into() } else { format!("Song {i}") },
            artist: "Test".into(),
            isrc: None,
            deezer_id: None,
            year: None,
            popularity: Some(0.1 + 0.8 * rng.unit()),
            phrases: vec![PhraseInput { source: Some("preview".into()), start_ms: 0, notes: melody(&mut rng, 70) }],
            contours: Vec::new(),
        })
        .collect();
    let (bytes, _) = build_bytes(&songs, "test").expect("index");
    MelodyIndex::from_bytes(bytes).expect("open")
}

fn options() -> BuildOptions {
    BuildOptions {
        top_songs: None,
        sub_quantizers: 16,
        threads: 2,
        version: "test 1".into(),
        ..BuildOptions::default()
    }
}

#[test]
fn files_round_trip_and_belong_to_one_index() {
    let index = library(1, 60);
    let (bytes, stats) = build(&index, TINY, &options(), &|_| {}).expect("build");
    assert_eq!(stats.dim, 16);
    assert!(stats.windows > 100, "{stats:?}");
    assert!(stats.reconstruction > 0.9, "{stats:?}");
    let enc = EncoderIndex::from_bytes(bytes.clone(), &index).expect("open");
    assert_eq!(enc.params().windows as usize, stats.windows);
    assert_eq!((enc.params().dim, enc.params().m), (16, 16));
    assert_eq!(enc.version(), "test 1");
    let covered: u32 = (0..index.track_count()).map(|t| enc.track_windows(t).len() as u32).sum();
    assert_eq!(covered, enc.params().windows);
    // Every window maps back to its track and start.
    for t in 0..index.track_count() {
        for w in enc.track_windows(t) {
            assert_eq!(enc.window_place(w).0, t);
        }
    }

    // Another index (same songs in a different build) refuses it.
    let other = library(2, 60);
    let err = EncoderIndex::from_bytes(bytes.clone(), &other).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(err.contains("another melody index"), "{err}");

    // Damage of any kind is refused, never a panic.
    for cut in [0, 15, HEADER_LEN - 1, HEADER_LEN + 10, bytes.len() - 1] {
        assert!(EncoderIndex::from_bytes(bytes[..cut].to_vec(), &index).is_err(), "cut at {cut}");
    }
    let mut newer = bytes.clone();
    newer[8] = 2;
    let err = EncoderIndex::from_bytes(newer, &index).err().map(|e| e.to_string()).unwrap_or_default();
    assert!(err.contains("newer"), "{err}");
    let mut twisted = bytes.clone();
    let twin = le_u64(&bytes, SECTION_TABLE + 16 * S_TWIN).unwrap_or(0) as usize;
    twisted[twin + 4..twin + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(EncoderIndex::from_bytes(twisted, &index).is_err());
    let mut rng = Rng(3);
    for _ in 0..200 {
        let mut b = bytes.clone();
        let at = (rng.next() as usize) % HEADER_LEN.min(b.len());
        b[at] ^= 1 << (rng.next() % 8);
        let _ = EncoderIndex::from_bytes(b, &index);
    }

    // Only the most popular songs get windows.
    let top = BuildOptions { top_songs: Some(10), ..options() };
    let (bytes, stats) = build(&index, TINY, &top, &|_| {}).expect("build");
    assert_eq!(stats.songs, 10);
    let enc = EncoderIndex::from_bytes(bytes, &index).expect("open");
    let mut order: Vec<u32> = (0..index.song_count()).collect();
    order.sort_by(|a, b| index.popularity(*b).total_cmp(&index.popularity(*a)));
    for (rank, &song) in order.iter().enumerate() {
        let windows: usize = index.song_tracks(song).map(|t| enc.track_windows(t).len()).sum();
        assert_eq!(windows > 0, rank < 10, "song {song} at popularity rank {rank}");
    }
}

#[test]
fn nearest_windows_are_exact_on_their_codes() {
    let index = library(4, 40);
    let (bytes, _) = build(&index, TINY, &options(), &|_| {}).expect("build");
    let enc = EncoderIndex::from_bytes(bytes, &index).expect("open");
    let mut rng = Rng(5);
    let mut z: Vec<f32> = (0..16).map(|_| rng.unit() - 0.5).collect();
    let norm = z.iter().map(|x| x * x).sum::<f32>().sqrt();
    z.iter_mut().for_each(|x| *x /= norm);
    let all = enc.params().windows as usize;
    let full = enc.nearest(&z, all);
    assert_eq!(full.len(), all);
    assert!(full.windows(2).all(|p| p[0].0 >= p[1].0));
    // The fast scan's top 20 hold most of the exact top 10.
    let top: std::collections::HashSet<u32> = enc.nearest(&z, 20).into_iter().map(|x| x.1).collect();
    let hits = full.iter().take(10).filter(|x| top.contains(&x.1)).count();
    assert!(hits >= 8, "{hits} of the exact top 10");
}

#[test]
fn an_encoder_hit_reaches_the_rerank() {
    let index = library(6, 80);
    let (bytes, _) = build(&index, TINY, &options(), &|_| {}).expect("build");
    let enc = EncoderIndex::from_bytes(bytes, &index).expect("open");
    let target = index.find_song("Target", "Test").expect("target");
    let track = index.song_tracks(target.id).start;
    let contour = index.track(track).expect("track").contour;
    // An 8 s excerpt that starts and ends voiced on a window boundary, a fifth up.
    let start = (0..contour.len().saturating_sub(160))
        .step_by(40)
        .find(|&s| contour[s].is_finite() && contour[s + 159].is_finite())
        .expect("excerpt");
    let excerpt: Vec<f32> = contour[start..start + 160].iter().map(|x| x + 7.0).collect();
    let voiced = excerpt.iter().filter(|x| x.is_finite()).count() as u32;
    let query = MelodyQuery { contour: excerpt, notes: Vec::new(), voiced_ms: voiced * 50 };
    let budget = EncoderBudget::android();
    let hits = enc.search(&index, &query, &budget).expect("search");
    let hit = hits.iter().position(|h| h.song == target.id);
    assert!(hit.is_some_and(|r| r < 5), "target at {hit:?} of {}", hits.len());
    let h = &hits[hit.unwrap_or(0)];
    assert_eq!((h.track, h.ref_start, h.query_start, h.span), (track, start as u32, 0, 160));

    // With retrieval switched off only the hints can find the song.
    let blind = MatchOptions { probe: 0, shortlist: 0, ..MatchOptions::default() };
    assert!(matcher::search_with(&index, &query, 5, &blind).is_empty());
    let hints: Vec<_> = hits.iter().map(EncoderHit::hint).collect();
    let found = matcher::search_with_hints(&index, &query, 5, &blind, &hints, &budget.hints);
    assert_eq!(found.first().map(|m| m.song.id), Some(target.id), "{found:?}");
    assert!((found[0].key_shift - 7.0).abs() < 0.5);
}

#[test]
fn cached_embeddings_give_the_same_file() {
    let index = library(8, 30);
    let path = std::env::temp_dir().join(format!("mfenc-cache-{}.bin", crate::util::new_id()));
    let o = BuildOptions { embeddings_cache: Some(path.clone()), ..options() };
    let said = std::sync::Mutex::new(Vec::<String>::new());
    let log = |m: &str| said.lock().unwrap().push(m.to_string());
    let (first, _) = build(&index, TINY, &o, &log).expect("build");
    let (again, _) = build(&index, TINY, &o, &log).expect("build");
    assert_eq!(first, again);
    assert!(said.lock().unwrap().iter().any(|m| m.contains("from the cache")));
    // Other windows (a different song cut) do not reuse it.
    said.lock().unwrap().clear();
    let other = BuildOptions { top_songs: Some(5), ..o.clone() };
    build(&index, TINY, &other, &log).expect("build");
    assert!(!said.lock().unwrap().iter().any(|m| m.contains("from the cache")));
    let _ = std::fs::remove_file(path);
}

#[test]
fn hinted_tracks_get_votes_in_one_pass_and_the_encoder_evidence_scores() {
    let index = library(6, 80);
    let (bytes, _) = build(&index, TINY, &options(), &|_| {}).expect("build");
    let enc = EncoderIndex::from_bytes(bytes, &index).expect("open");
    let target = index.find_song("Target", "Test").expect("target");
    let track = index.song_tracks(target.id).start;
    let contour = index.track(track).expect("track").contour;
    let start = (0..contour.len().saturating_sub(160))
        .step_by(40)
        .find(|&s| contour[s].is_finite() && contour[s + 159].is_finite())
        .expect("excerpt");
    let excerpt: Vec<f32> = contour[start..start + 160].iter().map(|x| x + 3.0).collect();
    let voiced = excerpt.iter().filter(|x| x.is_finite()).count() as u32;
    let query = MelodyQuery { contour: excerpt, notes: Vec::new(), voiced_ms: voiced * 50 };
    let budget = EncoderBudget::android();
    let hits = enc.search(&index, &query, &budget).expect("search");
    assert!(hits.iter().enumerate().all(|(i, h)| h.rank == i as u32 + 1));
    let hints: Vec<_> = hits.iter().map(EncoderHit::hint).collect();
    // No track passes the shortlists by itself: only forced tracks keep their votes.
    let narrow = MatchOptions { shortlist: 0, ..MatchOptions::default() };
    let votes_of = |how: &HintOptions| {
        matcher::candidates_with_hints(&index, &query, &narrow, &hints, how)
            .into_iter()
            .filter(|c| c.song == target.id)
            .map(|c| (c.features.votes, c.features.enc_best, c.features.enc_hit))
            .fold((0.0f32, 0.0f32, 0.0f32), |a, b| (a.0.max(b.0), a.1.max(b.1), a.2.max(b.2)))
    };
    let without = votes_of(&HintOptions { votes: false, ..budget.hints.clone() });
    let with = votes_of(&HintOptions { votes: true, ..budget.hints.clone() });
    assert_eq!(without.0, 0.0, "hint placements alone carry no votes");
    assert!(with.0 > 0.0, "forced retrieval gives the hinted track its votes: {with:?}");
    let best = hits.iter().find(|h| h.song == target.id).expect("hit").best;
    assert!((with.1 - best).abs() < 1e-6 && with.2 == 1.0, "encoder evidence: {with:?}");
    // Without hints nothing is shortlisted, and no encoder evidence appears.
    assert!(
        matcher::candidates(&index, &query, &narrow)
            .iter()
            .all(|c| c.features.votes == 0.0 && c.features.enc_hit == 0.0)
    );
}
