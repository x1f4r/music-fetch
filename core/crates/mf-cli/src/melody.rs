//! Melody commands: pitch tracking, humming search, melody-index building and
//! QBSH evaluation.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, bail};
use clap::Subcommand;
use mfcore::melody::matcher::{self, MatchOptions};
use mfcore::melody::notes::query_from_details;
use mfcore::melody::pitch::{FrameDetail, PitchTracker, StreamingPitch};
use mfcore::melody::{MelodyIndex, builder};
use std::sync::Mutex;

#[derive(Subcommand)]
pub enum MelodyCommand {
    /// Track pitch in a WAV file and print frames as CSV.
    Pitch {
        wav: PathBuf,
        /// Print the reference SwiftF0 frames (no level normalization, gate or whistle path).
        #[arg(long)]
        raw: bool,
        /// Run the streaming tracker in 20 ms pushes instead of the batch tracker.
        #[arg(long)]
        stream: bool,
        /// Write the CSV here instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Time this many batch analyses of the first 10 s and report the median.
        #[arg(long)]
        bench: Option<usize>,
        /// Worker threads for batch analysis (default: up to 4).
        #[arg(long)]
        threads: Option<usize>,
    },
    /// Find the song someone hums, whistles or sings in a WAV file.
    Hum {
        wav: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 5)]
        k: usize,
        /// Print the query notes and how this song (title) scores.
        #[arg(long)]
        expect: Option<String>,
    },
    /// Export the matcher's view of query recordings (TSV: wav path first) as JSONL:
    /// pitch frames, the 20 fps contour and the notes.
    Queries {
        #[arg(long)]
        queries: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 8)]
        threads: usize,
        /// Resolve relative wav paths against this directory.
        #[arg(long)]
        base: Option<PathBuf>,
    },
    /// Build or inspect a melody index.
    /// Dispatched through the shared `mf index` command.
    #[command(skip)]
    Index(IndexCommand),
    /// Evaluate humming search on a query corpus.
    /// Dispatched through the shared `mf eval` command.
    #[command(skip)]
    Eval(EvalCommand),
}

#[derive(Subcommand)]
pub enum IndexCommand {
    /// Build an index from melody-builder JSONL.
    Build {
        jsonl: PathBuf,
        out: PathBuf,
        #[arg(long)]
        version: String,
    },
    /// Print an index's header and size.
    Info { mfi: PathBuf },
    /// Build or inspect an index's learned-encoder companion file.
    #[command(subcommand)]
    Encoder(crate::encoder::EncoderCommand),
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum EvalCommand {
    /// Run QBSH queries (TSV: wav path, expected title, expected artist) against an index.
    Qbsh {
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        queries: PathBuf,
        /// Per-query results with candidate features, as JSONL.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Worker threads (queries run in parallel, one thread each).
        #[arg(long, default_value_t = 8)]
        threads: usize,
        /// Only the first N queries.
        #[arg(long)]
        limit: Option<usize>,
        /// Index cells probed per query window.
        #[arg(long)]
        probe: Option<usize>,
        /// Tracks whose alignment votes are pooled after the coarse vote.
        #[arg(long)]
        shortlist: Option<usize>,
        /// Placements given the quick linear-scaling check.
        #[arg(long)]
        pool: Option<usize>,
        /// Candidates re-ranked with full linear scaling and DTW.
        #[arg(long)]
        rerank: Option<usize>,
        /// Logit weight of the popularity prior (0 = off, the app uses 0.3).
        #[arg(long, default_value_t = 0.0)]
        popularity: f32,
        /// Skip following the expected song through the stages (halves the run time).
        #[arg(long)]
        no_trace: bool,
        /// The index's learned-encoder file: its hits join the rerank as anchored placements.
        #[arg(long)]
        encoder: Option<PathBuf>,
        /// Encoder budget: android, mac or low-power.
        #[arg(long, default_value = "mac")]
        encoder_budget: String,
        /// Songs the encoder returns (overrides the budget).
        #[arg(long)]
        encoder_songs: Option<usize>,
        /// Placements per encoder hit as tempo:shift frames, comma-separated (overrides the budget).
        #[arg(long)]
        encoder_offsets: Option<String>,
        /// Encoder song score: sum or max (overrides the budget).
        #[arg(long)]
        encoder_pooling: Option<String>,
        /// Also pool the index's votes for the encoder's tracks (a second, restricted retrieval).
        #[arg(long)]
        encoder_votes: bool,
        /// The encoder's first N hits always reach the full rerank (overrides the budget).
        #[arg(long)]
        encoder_reserve: Option<usize>,
        /// Measure the encoder's recall without giving its hits to the rerank.
        #[arg(long)]
        encoder_recall_only: bool,
    },
    /// Run the impression detector over labelled clips (TSV: wav, impression|music,
    /// start s, duration s) as a live session would, and report its verdicts.
    Detect {
        #[arg(long)]
        list: PathBuf,
        /// Per-clip features and verdicts, as JSONL.
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, default_value_t = 8)]
        threads: usize,
    },
}

pub fn run(cmd: MelodyCommand) -> anyhow::Result<()> {
    match cmd {
        MelodyCommand::Pitch { wav, bench: Some(n), threads, .. } => bench_pitch(&wav, n, threads),
        MelodyCommand::Pitch { wav, raw, stream, out, bench: None, .. } => pitch(&wav, raw, stream, out.as_deref()),
        MelodyCommand::Hum { wav, index, k, expect } => hum(&wav, &index, k, expect.as_deref()),
        MelodyCommand::Queries { queries, out, threads, base } => {
            export_queries(&queries, &out, threads, base.as_deref())
        }
        MelodyCommand::Index(IndexCommand::Build { jsonl, out, version }) => {
            let started = Instant::now();
            let stats = builder::build_index(&jsonl, &out, &version)?;
            println!(
                "{} songs, {} tracks, {} frames, {} windows, {} bytes ({:.0} B/song), {} skipped, {:.1} s",
                stats.songs,
                stats.tracks,
                stats.frames,
                stats.windows,
                stats.bytes,
                stats.bytes as f64 / stats.songs.max(1) as f64,
                stats.skipped,
                started.elapsed().as_secs_f64()
            );
            Ok(())
        }
        MelodyCommand::Index(IndexCommand::Info { mfi }) => index_info(&mfi),
        MelodyCommand::Index(IndexCommand::Encoder(command)) => crate::encoder::run(command),
        MelodyCommand::Eval(EvalCommand::Qbsh {
            index,
            queries,
            out,
            threads,
            limit,
            probe,
            shortlist,
            pool,
            rerank,
            popularity,
            no_trace,
            encoder,
            encoder_budget,
            encoder_songs,
            encoder_offsets,
            encoder_pooling,
            encoder_votes,
            encoder_reserve,
            encoder_recall_only,
        }) => {
            let mut options = MatchOptions { popularity_weight: popularity, ..MatchOptions::default() };
            if let Some(p) = probe {
                options.probe = p;
            }
            if let Some(s) = shortlist {
                options.shortlist = s;
            }
            if let Some(p) = pool {
                options.pool = p;
            }
            if let Some(r) = rerank {
                options.rerank = r;
            }
            let encoder = encoder.map(|path| crate::encoder::EncoderArgs {
                path,
                budget: encoder_budget,
                songs: encoder_songs,
                offsets: encoder_offsets,
                pooling: encoder_pooling,
                votes: encoder_votes,
                reserve: encoder_reserve,
                recall_only: encoder_recall_only,
            });
            eval_qbsh(&index, &queries, out.as_deref(), threads, limit, &options, !no_trace, encoder)
        }
        MelodyCommand::Eval(EvalCommand::Detect { list, out, threads }) => eval_detect(&list, out.as_deref(), threads),
    }
}

struct DetectRow {
    impression: bool,
    verdict: Option<bool>,
    decided_ms: Option<u64>,
    json: String,
}

fn eval_detect(list: &Path, out: Option<&Path>, threads: usize) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(list)?;
    let items: Vec<(String, bool, f64, f64)> = text
        .lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            Some((f.first()?.to_string(), *f.get(1)? == "impression", f.get(2)?.parse().ok()?, f.get(3)?.parse().ok()?))
        })
        .collect();
    let tracker = Arc::new(PitchTracker::with_threads(1)?);
    let next = AtomicUsize::new(0);
    let rows: Vec<Mutex<Option<DetectRow>>> = items.iter().map(|_| Mutex::new(None)).collect();
    std::thread::scope(|s| {
        for _ in 0..threads.max(1) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(i) else { break };
                    let row = detect_one(&tracker, item);
                    if let Ok(mut slot) = rows[i].lock() {
                        *slot = Some(row);
                    }
                }
            });
        }
    });
    let rows: Vec<DetectRow> = rows.into_iter().filter_map(|r| r.into_inner().ok().flatten()).collect();
    if let Some(out) = out {
        let mut w = std::io::BufWriter::new(std::fs::File::create(out)?);
        for r in &rows {
            writeln!(w, "{}", r.json)?;
        }
    }
    let count = |imp: bool, v: Option<bool>| rows.iter().filter(|r| r.impression == imp && r.verdict == v).count();
    println!("                     verdict impression   verdict music   undecided");
    for (name, imp) in [("impression clips", true), ("music clips     ", false)] {
        println!(
            "{name}     {:>10}   {:>13}   {:>9}",
            count(imp, Some(true)),
            count(imp, Some(false)),
            count(imp, None)
        );
    }
    let mut times: Vec<u64> = rows.iter().filter_map(|r| r.decided_ms).collect();
    times.sort_unstable();
    if !times.is_empty() {
        println!(
            "decided after median {} ms, p95 {} ms of audio",
            times[times.len() / 2],
            times[times.len() * 95 / 100]
        );
    }
    Ok(())
}

fn detect_one(tracker: &Arc<PitchTracker>, (wav, impression, start, dur): &(String, bool, f64, f64)) -> DetectRow {
    let mut row = DetectRow { impression: *impression, verdict: None, decided_ms: None, json: String::new() };
    let samples = match read_wav_16k(Path::new(wav)) {
        Ok(s) => s,
        Err(e) => {
            row.json = serde_json::json!({"wav": wav, "error": e.to_string()}).to_string();
            return row;
        }
    };
    let a = ((start * 16_000.0) as usize).min(samples.len());
    let b = (a + (dur * 16_000.0) as usize).min(samples.len());
    let clip = &samples[a..b];
    let mut stream = StreamingPitch::new(tracker.clone());
    let mut detector = mfcore::melody::ImpressionDetector::new();
    let checkpoints = [2000u64, 2500, 3000, 3500, 4000];
    let mut snaps: Vec<serde_json::Value> = Vec::new();
    for (k, chunk) in clip.chunks(320).enumerate() {
        let frames = stream.push(chunk);
        detector.push(&frames, chunk);
        while snaps.len() < checkpoints.len() && detector.sounding_ms() >= checkpoints[snaps.len()] {
            snaps.push(serde_json::json!({"at": checkpoints[snaps.len()], "features": detector.features(), "p": detector.probability()}));
        }
        if row.verdict.is_none()
            && let Some(v) = detector.verdict()
        {
            row.verdict = Some(v);
            row.decided_ms = Some(((k + 1) * 320 / 16) as u64);
        }
    }
    let frames = stream.finish();
    detector.push(&frames, &[]);
    if row.verdict.is_none() {
        row.verdict = detector.verdict();
    }
    row.json = serde_json::json!({
        "wav": wav, "start": start, "impression": impression, "verdict": row.verdict, "decided_ms": row.decided_ms,
        "sounding_ms": detector.sounding_ms(), "final": {"features": detector.features(), "p": detector.probability()},
        "checkpoints": snaps,
    })
    .to_string();
    row
}

fn pitch(wav: &Path, raw: bool, stream: bool, out: Option<&Path>) -> anyhow::Result<()> {
    let samples = read_wav_16k(wav)?;
    let tracker = Arc::new(PitchTracker::new()?);
    let mut w: Box<dyn Write> = match out {
        Some(p) => Box::new(std::io::BufWriter::new(std::fs::File::create(p)?)),
        None => Box::new(std::io::BufWriter::new(std::io::stdout().lock())),
    };
    let started = Instant::now();
    if raw {
        let frames = tracker.reference_frames(&samples)?;
        eprintln!("{} frames in {:.1} ms", frames.len(), ms(started));
        writeln!(w, "t_ms,hz,confidence,loudness_db")?;
        for (f, r) in frames.iter().enumerate() {
            writeln!(w, "{},{:.4},{:.6},{:.3}", f * 16, r.hz, r.confidence, r.loudness_db)?;
        }
        return Ok(());
    }
    let (frames, whistle): (Vec<FrameDetail>, bool) = if stream {
        let mut s = StreamingPitch::new(tracker.clone());
        let mut frames = Vec::new();
        for chunk in samples.chunks(320) {
            frames.extend(s.push_detailed(chunk));
        }
        frames.extend(s.finish_detailed());
        (frames, s.is_whistle())
    } else {
        let a = tracker.analyze_detailed(&samples)?;
        (a.frames, a.whistle)
    };
    eprintln!("{} frames in {:.1} ms{}", frames.len(), ms(started), if whistle { " (whistle path)" } else { "" });
    writeln!(w, "t_ms,hz,midi,confidence,loudness_db,voiced")?;
    for f in &frames {
        writeln!(
            w,
            "{},{:.4},{:.4},{:.6},{:.3},{}",
            f.t_ms,
            f.hz,
            f.midi(),
            f.confidence,
            f.loudness_db,
            f.voiced as u8
        )?;
    }
    Ok(())
}

fn bench_pitch(wav: &Path, n: usize, threads: Option<usize>) -> anyhow::Result<()> {
    let samples = read_wav_16k(wav)?;
    let clip = &samples[..samples.len().min(160_000)];
    let started = Instant::now();
    let tracker = match threads {
        Some(t) => PitchTracker::with_threads(t)?,
        None => PitchTracker::new()?,
    };
    eprintln!("model loaded in {:.1} ms", ms(started));
    let _ = tracker.analyze_detailed(clip)?;
    let mut times = Vec::with_capacity(n);
    for _ in 0..n.max(1) {
        let t = Instant::now();
        let a = tracker.analyze_detailed(clip)?;
        times.push(ms(t));
        std::hint::black_box(a);
    }
    times.sort_by(f64::total_cmp);
    println!(
        "{:.2} s of audio: median {:.1} ms, min {:.1} ms over {} runs",
        clip.len() as f64 / 16_000.0,
        times[times.len() / 2],
        times[0],
        times.len()
    );
    Ok(())
}

fn index_info(path: &Path) -> anyhow::Result<()> {
    let started = Instant::now();
    let index = MelodyIndex::open(path)?;
    let opened = ms(started);
    println!("format    {}", mfcore::melody::index::FORMAT);
    println!("version   {}", index.version());
    println!("songs     {}", index.song_count());
    println!("tracks    {}", index.track_count());
    println!("frames    {} ({:.1} h)", index.frame_count(), index.frame_count() as f64 / 20.0 / 3600.0);
    println!("windows   {}", index.window_count());
    let mut cells: Vec<u32> = (0..mfcore::melody::index::CELLS).map(|c| index.cell_len(c)).filter(|&n| n > 0).collect();
    cells.sort_unstable();
    let pct = |p: f64| cells.get(((cells.len() as f64 - 1.0) * p) as usize).copied().unwrap_or(0);
    let total: u64 = cells.iter().map(|&n| n as u64).sum();
    let top1: u64 = cells.iter().rev().take(cells.len() / 100).map(|&n| n as u64).sum();
    println!(
        "cells     {} used; postings p50 {} p90 {} p99 {} max {}; top 1 % of cells hold {:.0} %",
        cells.len(),
        pct(0.5),
        pct(0.9),
        pct(0.99),
        pct(1.0),
        100.0 * top1 as f64 / total.max(1) as f64
    );
    println!(
        "bytes     {} ({:.0} B/song)",
        index.size_bytes(),
        index.size_bytes() as f64 / index.song_count().max(1) as f64
    );
    println!("opened in {opened:.2} ms");
    Ok(())
}

fn hum(wav: &Path, index: &Path, k: usize, expect: Option<&str>) -> anyhow::Result<()> {
    let samples = read_wav_16k(wav)?;
    let started = Instant::now();
    let index = MelodyIndex::open(index)?;
    let opened = ms(started);
    let tracker = PitchTracker::new()?;
    let t = Instant::now();
    let analysis = tracker.analyze_detailed(&samples)?;
    let pitch_ms = ms(t);
    let Some(query) = query_from_details(&analysis.frames) else {
        bail!("not enough humming, whistling or singing to search with");
    };
    let t = Instant::now();
    let matches = matcher::search(&index, &query, k);
    eprintln!(
        "index opened in {opened:.1} ms; pitch {pitch_ms:.1} ms{}; {} notes, {:.1} s voiced; search {:.1} ms",
        if analysis.whistle { " (whistle)" } else { "" },
        query.notes.len(),
        query.voiced_ms as f64 / 1000.0,
        ms(t)
    );
    for (i, m) in matches.iter().enumerate() {
        println!(
            "{:>2}. {:.3}  {} - {}  (key {:+.1}, tempo x{:.2})",
            i + 1,
            m.score,
            m.song.artist,
            m.song.title,
            m.key_shift,
            m.tempo
        );
    }
    if let Some(title) = expect {
        let notes: Vec<String> =
            query.notes.iter().map(|n| format!("{:.1}@{}+{}", n.pitch, n.onset_ms, n.dur_ms)).collect();
        println!("query notes: {}", notes.join(" "));
        let Some(song) = index
            .find_song(title, "")
            .or_else(|| (0..index.song_count()).filter_map(|i| index.song(i)).find(|s| s.title == title))
        else {
            bail!("{title} is not in the index");
        };
        for t in index.song_tracks(song.id) {
            if let Some(track) = index.track(t) {
                let voiced = track.contour.iter().filter(|v| v.is_finite()).count();
                println!(
                    "reference track {t} ({:?}): {:.1} s, {:.0} % voiced",
                    track.source,
                    track.contour.len() as f64 / 20.0,
                    100.0 * voiced as f64 / track.contour.len().max(1) as f64
                );
            }
        }
        let options = MatchOptions { popularity_weight: 0.0, ..MatchOptions::default() };
        let trace = matcher::trace(&index, &query, &options, song.id);
        println!("stages: retrieval {:?}, linear {:?}, reranked {:?}", trace.retrieval, trace.linear, trace.reranked);
        let all = matcher::candidates(&index, &query, &options);
        match all.iter().position(|c| c.song == song.id) {
            Some(i) => println!("expected song: candidate #{} {:?}", i + 1, all[i]),
            None => println!("expected song not among {} candidates", all.len()),
        }
        if let Some(top) = all.first() {
            println!("top candidate: {top:?}");
        }
        match matcher::score_song(&index, &query, song.id) {
            Some(m) => println!("score_song: {:.3} key {:+.1} tempo x{:.2}", m.score, m.key_shift, m.tempo),
            None => println!("score_song: none"),
        }
    }
    Ok(())
}

fn export_queries(list: &Path, out: &Path, threads: usize, base: Option<&Path>) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(list)?;
    let wavs: Vec<String> =
        text.lines().filter_map(|l| l.split('\t').next()).filter(|w| !w.trim().is_empty()).map(String::from).collect();
    let tracker = PitchTracker::with_threads(1)?;
    let next = AtomicUsize::new(0);
    let rows: Vec<Mutex<String>> = wavs.iter().map(|_| Mutex::new(String::new())).collect();
    std::thread::scope(|s| {
        for _ in 0..threads.max(1) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(wav) = wavs.get(i) else { break };
                    let path = base.map(|b| b.join(wav)).unwrap_or_else(|| PathBuf::from(wav));
                    let json = match read_wav_16k(&path).and_then(|s| Ok(tracker.analyze_detailed(&s)?)) {
                        Ok(a) => {
                            let q = query_from_details(&a.frames);
                            let frames: Vec<[f32; 3]> =
                                a.frames.iter().map(|f| [f.midi(), f.confidence, f.loudness_db]).collect();
                            let nan = |v: f32| if v.is_finite() { Some(v) } else { None };
                            serde_json::json!({
                                "wav": wav, "whistle": a.whistle,
                                "frames": frames.iter().map(|f| [nan(f[0]), nan(f[1]), nan(f[2])]).collect::<Vec<_>>(),
                                "contour": q.as_ref().map(|q| q.contour.iter().map(|&m| nan(m)).collect::<Vec<_>>()),
                                "notes": q.as_ref().map(|q| q.notes.iter().map(|n| (n.pitch, n.onset_ms, n.dur_ms)).collect::<Vec<_>>()),
                                "voiced_ms": q.as_ref().map(|q| q.voiced_ms),
                            })
                        }
                        Err(e) => serde_json::json!({"wav": wav, "error": e.to_string()}),
                    };
                    if let Ok(mut slot) = rows[i].lock() {
                        *slot = json.to_string();
                    }
                }
            });
        }
    });
    let mut w = std::io::BufWriter::new(std::fs::File::create(out)?);
    for r in rows {
        writeln!(w, "{}", r.into_inner().unwrap_or_default())?;
    }
    Ok(())
}

struct EvalRow {
    wav: String,
    rank: Option<usize>,
    pitch_ms: f64,
    search_ms: f64,
    voiced_ms: u32,
    top: Option<(String, f32)>,
    trace: matcher::Trace,
    encoder: Option<crate::encoder::EncoderRow>,
    json: String,
}

#[allow(clippy::too_many_arguments)]
fn eval_qbsh(
    index: &Path,
    queries: &Path,
    out: Option<&Path>,
    threads: usize,
    limit: Option<usize>,
    options: &MatchOptions,
    trace: bool,
    encoder: Option<crate::encoder::EncoderArgs>,
) -> anyhow::Result<()> {
    let index = MelodyIndex::open(index)?;
    let encoder = match encoder {
        Some(args) => Some(crate::encoder::EncoderEval::open(&args, &index)?),
        None => None,
    };
    let list = std::fs::read_to_string(queries)?;
    let mut items: Vec<(String, String, String)> = list
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| {
            let mut f = l.split('\t');
            Some((f.next()?.to_string(), f.next()?.to_string(), f.next().unwrap_or("").to_string()))
        })
        .collect();
    if let Some(n) = limit {
        items.truncate(n);
    }
    let tracker = PitchTracker::with_threads(1)?;
    let next = AtomicUsize::new(0);
    let rows: Vec<Mutex<Option<EvalRow>>> = items.iter().map(|_| Mutex::new(None)).collect();
    let started = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads.max(1) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some((wav, title, artist)) = items.get(i) else {
                        break;
                    };
                    let row = eval_one(&index, &tracker, wav, title, artist, options, trace, encoder.as_ref());
                    if let Ok(mut slot) = rows[i].lock() {
                        *slot = Some(row);
                    }
                }
            });
        }
    });
    let wall = started.elapsed().as_secs_f64();
    let rows: Vec<EvalRow> = rows.into_iter().filter_map(|r| r.into_inner().ok().flatten()).collect();
    if let Some(out) = out {
        let mut w = std::io::BufWriter::new(std::fs::File::create(out)?);
        for r in &rows {
            writeln!(w, "{}", r.json)?;
        }
    }
    let n = rows.len().max(1) as f64;
    let top = |k: usize| rows.iter().filter(|r| r.rank.is_some_and(|x| x <= k)).count() as f64 / n;
    let mrr = rows.iter().map(|r| r.rank.map(|x| 1.0 / x as f64).unwrap_or(0.0)).sum::<f64>() / n;
    let no_query = rows.iter().filter(|r| r.voiced_ms == 0).count();
    let mut search: Vec<f64> = rows.iter().filter(|r| r.voiced_ms > 0).map(|r| r.search_ms).collect();
    let mut pitch: Vec<f64> = rows.iter().map(|r| r.pitch_ms).collect();
    search.sort_by(f64::total_cmp);
    pitch.sort_by(f64::total_cmp);
    let pct = |v: &[f64], p: f64| v.get(((v.len() as f64 - 1.0).max(0.0) * p).round() as usize).copied().unwrap_or(0.0);
    println!("queries {}  (too little voiced audio: {no_query})", rows.len());
    println!("top-1   {:.4}", top(1));
    println!("top-3   {:.4}", top(3));
    println!("top-10  {:.4}", top(10));
    println!("MRR     {mrr:.4}");
    let recall = |f: &dyn Fn(&matcher::Trace) -> Option<usize>, k: usize| {
        rows.iter().filter(|r| f(&r.trace).is_some_and(|x| x <= k)).count() as f64 / n
    };
    println!(
        "stages  retrieved {:.3} (top-50 {:.3}), linear top-10 {:.3} / top-{} {:.3}, reranked top-10 {:.3}",
        recall(&|t| t.retrieval, usize::MAX),
        recall(&|t| t.retrieval, 50),
        recall(&|t| t.linear, 10),
        options.rerank,
        recall(&|t| t.linear, options.rerank),
        recall(&|t| t.reranked, 10),
    );
    println!("pitch   median {:.1} ms, p95 {:.1} ms (1 thread per query)", pct(&pitch, 0.5), pct(&pitch, 0.95));
    println!(
        "search  median {:.1} ms, p95 {:.1} ms, max {:.1} ms",
        pct(&search, 0.5),
        pct(&search, 0.95),
        pct(&search, 1.0)
    );
    println!("wall    {wall:.1} s with {threads} threads");
    if let Some(e) = &encoder {
        let enc: Vec<_> = rows.iter().filter_map(|r| Some((r.encoder.as_ref()?, r.trace.retrieval))).collect();
        crate::encoder::print_summary(&enc, e.budget.songs);
    }
    println!("top-1 score   queries   correct");
    for b in 0..10 {
        let (lo, hi) = (b as f32 / 10.0, (b + 1) as f32 / 10.0);
        let in_bin: Vec<&EvalRow> =
            rows.iter().filter(|r| r.top.as_ref().is_some_and(|t| t.1 >= lo && (t.1 < hi || b == 9))).collect();
        if !in_bin.is_empty() {
            let correct = in_bin.iter().filter(|r| r.rank == Some(1)).count();
            println!("  {lo:.1}-{hi:.1}   {:>7}   {:.3}", in_bin.len(), correct as f64 / in_bin.len() as f64);
        }
    }
    for r in rows.iter().filter(|r| r.rank != Some(1)).take(5) {
        eprintln!("miss {} rank {:?} top {:?}", r.wav, r.rank, r.top);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn eval_one(
    index: &MelodyIndex,
    tracker: &PitchTracker,
    wav: &str,
    title: &str,
    artist: &str,
    options: &MatchOptions,
    trace: bool,
    encoder: Option<&crate::encoder::EncoderEval>,
) -> EvalRow {
    let mut row = EvalRow {
        wav: wav.to_string(),
        rank: None,
        pitch_ms: 0.0,
        search_ms: 0.0,
        voiced_ms: 0,
        top: None,
        trace: Default::default(),
        encoder: None,
        json: String::new(),
    };
    let expected = index.find_song(title, artist).map(|s| s.id);
    let samples = match read_wav_16k(Path::new(wav)) {
        Ok(s) => s,
        Err(e) => {
            row.json = serde_json::json!({"wav": wav, "error": e.to_string()}).to_string();
            return row;
        }
    };
    let t = Instant::now();
    let frames = tracker.analyze_detailed(&samples).map(|a| a.frames).unwrap_or_default();
    row.pitch_ms = ms(t);
    let Some(query) = query_from_details(&frames) else {
        row.json = serde_json::json!({"wav": wav, "expected": expected, "rank": null, "voiced_ms": 0}).to_string();
        return row;
    };
    row.voiced_ms = query.voiced_ms;
    let t = Instant::now();
    let candidates = match encoder {
        Some(e) => {
            let (candidates, enc) = e.candidates(index, &query, options, expected);
            row.encoder = Some(enc);
            candidates
        }
        None => matcher::candidates(index, &query, options),
    };
    row.search_ms = ms(t);
    let mut seen = HashSet::new();
    let mut songs: Vec<_> = candidates.iter().filter(|c| seen.insert(c.song)).collect();
    // The popularity prior, as the app applies it.
    let prior = |c: &matcher::Candidate| {
        let p = c.score.clamp(1e-6, 1.0 - 1e-6);
        let z = (p / (1.0 - p)).ln() + options.popularity_weight * ((index.popularity(c.song) + 0.05) / 0.55).ln();
        1.0 / (1.0 + (-z).exp())
    };
    songs.sort_by(|a, b| prior(b).total_cmp(&prior(a)));
    row.rank = expected.and_then(|e| songs.iter().position(|c| c.song == e).map(|p| p + 1));
    row.top = songs.first().and_then(|c| index.song(c.song).map(|s| (s.title, prior(c))));
    if trace {
        row.trace = expected.map(|e| matcher::trace(index, &query, options, e)).unwrap_or_default();
    }
    let cands: Vec<serde_json::Value> = songs
        .iter()
        .map(|c| {
            let mut v = serde_json::json!({
                "song": c.song, "track": c.track, "label": Some(c.song) == expected, "score": c.score,
                "votes": c.features.votes, "linear": c.features.linear, "dtw": c.features.dtw,
                "coverage": c.features.coverage, "tempo_dev": c.features.tempo, "align": c.features.align,
                "edit": c.features.edit, "note_votes": c.features.note_votes, "key": c.key_shift, "tempo": c.tempo,
                "popularity": index.popularity(c.song),
            });
            if let (Some(e), Some(obj)) = (&row.encoder, v.as_object_mut())
                && let serde_json::Value::Object(f) = e.features(c.song)
            {
                obj.extend(f);
            }
            v
        })
        .collect();
    // How the expected song scores when asked about directly (an upper bound for retrieval).
    let oracle =
        if trace { expected.and_then(|e| matcher::score_song(index, &query, e)).map(|m| m.score) } else { None };
    row.json = serde_json::json!({
        "wav": wav, "expected": expected, "rank": row.rank, "voiced_ms": query.voiced_ms,
        "notes": query.notes.len(), "pitch_ms": row.pitch_ms, "search_ms": row.search_ms, "candidates": cands,
        "trace": [row.trace.retrieval, row.trace.linear, row.trace.reranked], "oracle_score": oracle,
        "encoder": row.encoder.as_ref().map(|e| e.json()),
    })
    .to_string();
    row
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1000.0
}

/// Read a PCM or float WAV, mix to mono and resample to 16 kHz.
pub fn read_wav_16k(path: &Path) -> anyhow::Result<Vec<f32>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let (samples, rate) = parse_wav(&bytes).with_context(|| format!("parsing {}", path.display()))?;
    Ok(resample(&samples, rate, 16_000))
}

fn parse_wav(b: &[u8]) -> anyhow::Result<(Vec<f32>, u32)> {
    if b.len() < 12 || &b[0..4] != b"RIFF" || &b[8..12] != b"WAVE" {
        bail!("not a WAV file");
    }
    let u16_at = |i: usize| u16::from_le_bytes([b[i], b[i + 1]]);
    let u32_at = |i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
    let mut pos = 12;
    let mut fmt: Option<(u16, u16, u32, u16)> = None;
    while pos + 8 <= b.len() {
        let id = &b[pos..pos + 4];
        let size = u32_at(pos + 4) as usize;
        let body = pos + 8;
        let end = body.saturating_add(size).min(b.len());
        if id == b"fmt " && size >= 16 && body + 16 <= b.len() {
            let mut tag = u16_at(body);
            if tag == 0xFFFE && size >= 26 && body + 26 <= b.len() {
                tag = u16_at(body + 24);
            }
            fmt = Some((tag, u16_at(body + 2), u32_at(body + 4), u16_at(body + 14)));
        } else if id == b"data" {
            let Some((tag, channels, rate, bits)) = fmt else { bail!("data before fmt chunk") };
            let channels = channels.max(1) as usize;
            let width = (bits as usize).div_ceil(8);
            if width == 0 || !matches!((tag, bits), (1, 8) | (1, 16) | (1, 24) | (1, 32) | (3, 32)) {
                bail!("unsupported WAV format {tag}/{bits}-bit");
            }
            let decode = |s: &[u8]| -> f32 {
                match (tag, bits) {
                    (1, 8) => (s[0] as f32 - 128.0) / 128.0,
                    (1, 16) => i16::from_le_bytes([s[0], s[1]]) as f32 / 32768.0,
                    (1, 24) => (i32::from_le_bytes([0, s[0], s[1], s[2]]) >> 8) as f32 / 8_388_608.0,
                    (1, 32) => i32::from_le_bytes([s[0], s[1], s[2], s[3]]) as f32 / 2_147_483_648.0,
                    _ => f32::from_le_bytes([s[0], s[1], s[2], s[3]]),
                }
            };
            let frame = width * channels;
            let out = b[body..end]
                .chunks_exact(frame)
                .map(|f| (0..channels).map(|c| decode(&f[c * width..(c + 1) * width])).sum::<f32>() / channels as f32)
                .collect();
            return Ok((out, rate));
        }
        pos = body.saturating_add(size).saturating_add(size & 1);
    }
    bail!("no audio data")
}

/// Windowed-sinc resampling (Kaiser, 16 zero crossings).
pub fn resample(x: &[f32], from: u32, to: u32) -> Vec<f32> {
    if from == to || x.is_empty() || from == 0 {
        return x.to_vec();
    }
    let ratio = to as f64 / from as f64;
    let cutoff = ratio.min(1.0) * 0.97;
    let half = (16.0 / cutoff).ceil() as isize;
    let n_out = (x.len() as f64 * ratio).floor() as usize;
    let beta = 8.0;
    let i0b = bessel_i0(beta);
    let mut out = Vec::with_capacity(n_out);
    for n in 0..n_out {
        let t = n as f64 / ratio;
        let center = t.floor() as isize;
        let mut acc = 0.0f64;
        for k in center - half + 1..=center + half {
            let d = t - k as f64;
            let r = d / half as f64;
            if r.abs() >= 1.0 || k < 0 || k as usize >= x.len() {
                continue;
            }
            let arg = std::f64::consts::PI * d * cutoff;
            let sinc = if arg.abs() < 1e-9 { 1.0 } else { arg.sin() / arg };
            acc += cutoff * sinc * bessel_i0(beta * (1.0 - r * r).sqrt()) / i0b * x[k as usize] as f64;
        }
        out.push(acc as f32);
    }
    out
}

fn bessel_i0(x: f64) -> f64 {
    let mut sum = 1.0;
    let mut term = 1.0;
    for k in 1..60 {
        term *= (x / (2.0 * k as f64)).powi(2);
        sum += term;
        if term < 1e-14 * sum {
            break;
        }
    }
    sum
}
