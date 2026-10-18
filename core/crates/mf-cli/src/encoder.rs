//! Learned-encoder companion files: `mf index encoder build|info` and the
//! encoder side of `mf eval qbsh --encoder`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, bail};
use clap::Subcommand;
use mfcore::melody::encoder::build::{BuildOptions, build_file};
use mfcore::melody::encoder::{EncoderBudget, EncoderHit, EncoderIndex, EncoderTiming, Pooling};
use mfcore::melody::matcher::{self, Candidate, MatchOptions};
use mfcore::melody::{MelodyIndex, MelodyQuery};

#[derive(Subcommand)]
pub enum EncoderCommand {
    /// Embed and code the reference windows of an index with an encoder model
    /// (ONNX from tools/melody-encoder), writing its companion `.mfe` file.
    Build {
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        model: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// Only the N most popular songs get windows (0 = all).
        #[arg(long, default_value_t = 30_000)]
        top: usize,
        /// Sub-quantizers: half the bytes per window (32 = 16 B, 64 = 32 B for 64-d).
        #[arg(long, default_value_t = 32)]
        sub_quantizers: usize,
        /// Reference window hop in 20 fps frames.
        #[arg(long, default_value_t = 40)]
        hop: usize,
        /// Most query windows a search embeds (budgets may lower it).
        #[arg(long, default_value_t = 8)]
        max_query_windows: usize,
        #[arg(long, default_value_t = 8)]
        threads: usize,
        /// Version string stored in the file.
        #[arg(long, default_value = "")]
        version: String,
        /// Reuse (or keep) the window embeddings in this file.
        #[arg(long)]
        embeddings_cache: Option<PathBuf>,
    },
    /// Print an encoder file's header after checking it against its index.
    Info {
        mfe: PathBuf,
        #[arg(long)]
        index: PathBuf,
    },
    /// Time encoder searches on one thread with excerpts of the index's own
    /// references as queries (transposed, 10 s), and count how often the source song
    /// comes first.
    Bench {
        mfe: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 100)]
        queries: usize,
        /// android, mac or low-power.
        #[arg(long, default_value = "android")]
        budget: String,
        /// Use the portable fast-scan kernel instead of the SIMD one.
        #[arg(long)]
        scalar: bool,
    },
}

pub fn run(cmd: EncoderCommand) -> anyhow::Result<()> {
    match cmd {
        EncoderCommand::Build {
            index,
            model,
            out,
            top,
            sub_quantizers,
            hop,
            max_query_windows,
            threads,
            version,
            embeddings_cache,
        } => {
            let started = Instant::now();
            let index = MelodyIndex::open(&index)?;
            let onnx = std::fs::read(&model).with_context(|| format!("reading {}", model.display()))?;
            let options = BuildOptions {
                top_songs: (top > 0).then_some(top),
                hop,
                sub_quantizers,
                max_query_windows,
                threads,
                version,
                embeddings_cache,
                ..BuildOptions::default()
            };
            let stats = build_file(&index, &onnx, &options, &out, &|m| eprintln!("{m}"))?;
            println!(
                "{} songs, {} tracks, {} windows, {}-d, {} B/window, {} bytes ({} model), ⟨z, ẑ⟩ {:.4}, \
                 embedded in {:.0} s, quantized in {:.0} s, {:.0} s in all",
                stats.songs,
                stats.tracks,
                stats.windows,
                stats.dim,
                sub_quantizers / 2,
                stats.bytes,
                stats.model_bytes,
                stats.reconstruction,
                stats.embed_s,
                stats.quantize_s,
                started.elapsed().as_secs_f64()
            );
            if let Some(kb) = peak_rss_kb() {
                println!("peak RSS {:.0} MB", kb as f64 / 1024.0);
            }
            Ok(())
        }
        EncoderCommand::Info { mfe, index } => {
            let index = MelodyIndex::open(&index)?;
            let started = Instant::now();
            let enc = EncoderIndex::open(&mfe, &index)?;
            let opened = started.elapsed().as_secs_f64() * 1e3;
            let p = enc.params();
            let songs = (0..index.song_count())
                .filter(|&s| index.song_tracks(s).any(|t| !enc.track_windows(t).is_empty()))
                .count();
            println!("format    {}", mfcore::melody::encoder::FORMAT);
            println!("version   {}", enc.version());
            println!("songs     {songs} of {}", index.song_count());
            println!("windows   {} ({}-frame, every {} frames)", p.windows, p.window, p.ref_hop);
            println!("codes     {}-d, {} × 4-bit ({} B/window)", p.dim, p.m, p.m / 2);
            println!(
                "queries   windows every {} frames, ≥ {} ‰ voiced, at most {}",
                p.query_hop, p.min_voiced, p.max_query_windows
            );
            println!("bytes     {} (model {})", enc.size_bytes(), enc.model_bytes());
            println!("opened in {opened:.2} ms");
            Ok(())
        }
        EncoderCommand::Bench { mfe, index, queries, budget, scalar } => bench(&mfe, &index, queries, &budget, scalar),
    }
}

fn bench(mfe: &Path, index: &Path, queries: usize, budget: &str, scalar: bool) -> anyhow::Result<()> {
    let index = MelodyIndex::open(index)?;
    let args = EncoderArgs {
        path: mfe.to_path_buf(),
        budget: budget.into(),
        songs: None,
        offsets: None,
        pooling: None,
        votes: false,
        reserve: None,
        recall_only: true,
    };
    let mut eval = EncoderEval::open(&args, &index)?;
    if scalar {
        eval.encoder.set_kernel(mfcore::melody::encoder::scan::Kernel::Scalar);
    }
    let tracks: Vec<u32> = (0..index.track_count()).filter(|&t| eval.encoder.track_windows(t).len() >= 4).collect();
    if tracks.is_empty() {
        bail!("no track has enough windows");
    }
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let (mut encode, mut scan, mut first) = (Vec::new(), Vec::new(), 0usize);
    let started = Instant::now();
    // The first search loads the model and builds its plans.
    let mut load_ms = None;
    for _ in 0..queries {
        let t = tracks[(next() % tracks.len() as u64) as usize];
        let Some(track) = index.track(t) else { continue };
        let len = track.contour.len();
        let start = (next() % len.saturating_sub(200).max(1) as u64) as usize;
        let contour: Vec<f32> = track.contour[start..(start + 200).min(len)].iter().map(|x| x + 3.0).collect();
        let voiced = contour.iter().filter(|x| x.is_finite()).count() as u32;
        if voiced < 60 {
            continue;
        }
        let query = MelodyQuery { contour, notes: Vec::new(), voiced_ms: voiced * 50 };
        let t0 = Instant::now();
        let (hits, timing) = eval.encoder.search_timed(&index, &query, &eval.budget)?;
        if load_ms.is_none() {
            load_ms = Some(t0.elapsed().as_secs_f64() * 1e3);
            continue;
        }
        encode.push(timing.encode_ms / timing.windows.max(1) as f64);
        scan.push(timing.scan_ms / timing.windows.max(1) as f64);
        first += usize::from(hits.first().map(|h| h.song) == Some(track.song));
    }
    let n = encode.len();
    for v in [&mut encode, &mut scan] {
        v.sort_by(f64::total_cmp);
    }
    let med = |v: &[f64]| v.get(v.len() / 2).copied().unwrap_or(0.0);
    println!(
        "{n} queries: per query window encode {:.2} ms, scan {:.2} ms (medians, one thread); first search {:.0} ms; \
         source song first {:.1} %; {:.1} s",
        med(&encode),
        med(&scan),
        load_ms.unwrap_or(0.0),
        100.0 * first as f64 / n.max(1) as f64,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The encoder as one evaluation run uses it.
pub struct EncoderEval {
    pub encoder: EncoderIndex,
    pub budget: EncoderBudget,
    /// Measure the encoder's recall only; the rerank gets no hints.
    pub recall_only: bool,
}

/// What the encoder did for one query.
#[derive(Debug, Clone, Default)]
pub struct EncoderRow {
    /// Rank of the expected song among the encoder's hits.
    pub rank: Option<usize>,
    pub timing: EncoderTiming,
    pub hits: Vec<EncoderHit>,
}

/// `mf eval qbsh --encoder …` options.
pub struct EncoderArgs {
    pub path: PathBuf,
    /// android, mac or low-power.
    pub budget: String,
    pub songs: Option<usize>,
    /// "tempo:shift,…"
    pub offsets: Option<String>,
    /// sum or max.
    pub pooling: Option<String>,
    pub votes: bool,
    /// Hits that always reach the full rerank.
    pub reserve: Option<usize>,
    pub recall_only: bool,
}

impl EncoderEval {
    /// Open the encoder file and apply the CLI's budget overrides.
    pub fn open(args: &EncoderArgs, index: &MelodyIndex) -> anyhow::Result<Self> {
        let encoder = EncoderIndex::open(&args.path, index)?;
        let mut budget = match args.budget.as_str() {
            "android" => EncoderBudget::android(),
            "mac" => EncoderBudget::mac(),
            "low-power" => EncoderBudget::low_power(),
            other => bail!("unknown encoder budget {other} (android, mac or low-power)"),
        };
        if let Some(n) = args.songs {
            budget.songs = n;
        }
        if let Some(list) = &args.offsets {
            budget.hints.offsets = list
                .split(',')
                .map(|p| {
                    let (t, s) = p.split_once(':').context("offsets are tempo:shift")?;
                    Ok((t.trim().parse()?, s.trim().parse()?))
                })
                .collect::<anyhow::Result<_>>()?;
        }
        budget.hints.votes |= args.votes;
        if let Some(n) = args.reserve {
            budget.hints.reserve = n;
        }
        match args.pooling.as_deref() {
            None => {}
            Some("sum") => budget.pooling = Pooling::Sum,
            Some("max") => budget.pooling = Pooling::Max,
            Some(other) => bail!("unknown pooling {other} (sum or max)"),
        }
        Ok(Self { encoder, budget, recall_only: args.recall_only })
    }

    /// Candidates for one query with the encoder's hits as hints.
    pub fn candidates(
        &self,
        index: &MelodyIndex,
        query: &MelodyQuery,
        options: &MatchOptions,
        expected: Option<u32>,
    ) -> (Vec<Candidate>, EncoderRow) {
        let (hits, timing) = self.encoder.search_timed(index, query, &self.budget).unwrap_or_default();
        let rank = expected.and_then(|e| hits.iter().position(|h| h.song == e).map(|p| p + 1));
        let candidates = if self.recall_only {
            matcher::candidates(index, query, options)
        } else {
            let hints: Vec<_> = hits.iter().map(EncoderHit::hint).collect();
            matcher::candidates_with_hints(index, query, options, &hints, &self.budget.hints)
        };
        (candidates, EncoderRow { rank, timing, hits })
    }
}

impl EncoderRow {
    /// Encoder features of a candidate song for the fusion fit: pooled score, best
    /// window cosine, rank among the hits (0 = not a hit) and a hit flag.
    pub fn features(&self, song: u32) -> serde_json::Value {
        match self.hits.iter().position(|h| h.song == song) {
            Some(i) => serde_json::json!({
                "enc_score": self.hits[i].score, "enc_best": self.hits[i].best, "enc_rank": i + 1, "enc_hit": 1.0,
            }),
            None => serde_json::json!({"enc_score": 0.0, "enc_best": 0.0, "enc_rank": 0, "enc_hit": 0.0}),
        }
    }

    /// The row's encoder summary for the JSONL output.
    pub fn json(&self) -> serde_json::Value {
        let top: Vec<serde_json::Value> =
            self.hits.iter().take(20).map(|h| serde_json::json!([h.song, h.score, h.best])).collect();
        serde_json::json!({
            "rank": self.rank, "windows": self.timing.windows, "encode_ms": self.timing.encode_ms,
            "scan_ms": self.timing.scan_ms, "hits": self.hits.len(), "top": top,
        })
    }
}

/// Encoder recall, union with the matcher's own retrieval, and cost.
/// `rows`: (encoder row, the expected song's rank in the matcher's retrieval).
pub fn print_summary(rows: &[(&EncoderRow, Option<usize>)], songs: usize) {
    let n = rows.len().max(1) as f64;
    let at = |k: usize| rows.iter().filter(|r| r.0.rank.is_some_and(|x| x <= k)).count() as f64 / n;
    let retrieved = rows.iter().filter(|r| r.1.is_some()).count() as f64 / n;
    let union = rows.iter().filter(|r| r.1.is_some() || r.0.rank.is_some()).count() as f64 / n;
    println!(
        "encoder @1 {:.4}  @10 {:.4}  @100 {:.4}  @{songs} {:.4}; retrieved {retrieved:.4}, with the encoder's songs {union:.4} \
         (+{:.4})",
        at(1),
        at(10),
        at(100),
        at(songs),
        union - retrieved
    );
    let mut encode: Vec<f64> = rows.iter().filter(|r| r.0.timing.windows > 0).map(|r| r.0.timing.encode_ms).collect();
    let mut scan: Vec<f64> = rows.iter().filter(|r| r.0.timing.windows > 0).map(|r| r.0.timing.scan_ms).collect();
    let mut windows: Vec<f64> = rows.iter().map(|r| r.0.timing.windows as f64).collect();
    for v in [&mut encode, &mut scan, &mut windows] {
        v.sort_by(f64::total_cmp);
    }
    let pct = |v: &[f64], p: f64| v.get(((v.len() as f64 - 1.0).max(0.0) * p).round() as usize).copied().unwrap_or(0.0);
    println!(
        "encoder windows median {:.0}; encode median {:.1} ms p95 {:.1} ms; scan median {:.1} ms p95 {:.1} ms",
        pct(&windows, 0.5),
        pct(&encode, 0.5),
        pct(&encode, 0.95),
        pct(&scan, 0.5),
        pct(&scan, 0.95)
    );
    if let Some(kb) = peak_rss_kb() {
        println!("peak RSS {:.0} MB", kb as f64 / 1024.0);
    }
}

/// Peak resident memory of this process (Linux).
pub fn peak_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find(|l| l.starts_with("VmHWM:"))?.split_whitespace().nth(1)?.parse().ok()
}
