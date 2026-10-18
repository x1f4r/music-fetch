//! `mf eval qbsh --cloud acrcloud`: the app's cloud humming path next to the
//! local matcher, on labelled takes.
//!
//! Every take is tracked and searched locally as the app does (index plus, with
//! `--encoder`, the learned encoder), its newest 12 s go to ACRCloud's humming
//! engine as the app sends them (8 kHz WAV), and both answers are fused as an
//! impression session fuses them. The report compares local, cloud and merged
//! top-1 / top-10 and the merged tiers.
//!
//! Requests are sent one at a time, at least `--cloud-pace-ms` apart (1 s to a
//! real host), at most `--cloud-max-requests` per run, and stop at the first
//! quota, rate-limit or credential error. Every answer is kept in `--cloud-cache`
//! under a digest of the take's file, so nothing is ever sent twice; with
//! `--cloud-offline` only the cache is read. Credentials come from a JSON file
//! written by tools/eval/store-acrcloud-credentials.sh and are never printed.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use mfcore::identity::{normalize_artists, normalize_title};
use mfcore::lyrics::{Backend, LyricMatch, SearchOptions};
use mfcore::melody::notes::query_from_details;
use mfcore::melody::{MelodyIndex, PitchTracker, matcher};
use mfcore::model::{Candidate, CandidateSourceKind, CandidateTier, SpeechTranscript};
use mfcore::pipeline::hum::{self, MelodySearcher};
use mfcore::providers::ProviderMatch;
use mfcore::providers::acrcloud::{self, AcrCloud};
use mfcore::util::cancel::CancelToken;
use mfcore::{AcrCredentials, MfError};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::encoder::{EncoderArgs, EncoderEval};
use crate::melody::read_wav_16k;

/// Least spacing of requests to a real ACRCloud host.
const MIN_PACE: Duration = Duration::from_millis(1000);
/// Network failures in a row that end the cloud part of a run.
const MAX_FAILURES: usize = 3;
/// Bump when the clip sent for a take changes, so old answers are not reused.
const CLIP_VERSION: &str = "acrcloud-humming-clip-1";

/// `--cloud …` options.
pub struct CloudArgs {
    pub provider: String,
    /// JSON with host, access_key, access_secret.
    pub credentials: PathBuf,
    pub cache: PathBuf,
    /// Another origin instead of `https://{host}` (a local stub).
    pub base_url: Option<String>,
    pub pace_ms: u64,
    pub max_requests: usize,
    /// Read answers from the cache only.
    pub offline: bool,
    /// A fixed, spread sample of this many takes (by file name and song).
    pub sample: Option<usize>,
    /// Price of 1,000 requests, for the cost line.
    pub price_per_1000: Option<f64>,
    /// Transcripts of the takes (JSONL: wav file name, text, alternatives,
    /// confidence): sung words are searched as lyrics, as a session does.
    pub transcripts: Option<PathBuf>,
    /// Keep lyric-search responses here.
    pub lyrics_cache: Option<PathBuf>,
}

#[derive(Deserialize)]
struct TranscriptLine {
    wav: String,
    text: String,
    #[serde(default)]
    alternatives: Vec<String>,
    #[serde(default)]
    confidence: Option<f32>,
}

#[derive(Deserialize)]
struct CredentialFile {
    host: String,
    access_key: String,
    access_secret: String,
}

/// One take's outcome.
struct Row {
    name: String,
    in_index: bool,
    local: Option<usize>,
    lyrics: Option<usize>,
    cloud: Option<usize>,
    merged: Option<usize>,
    /// Merged top answer: score, tier, right?
    top: Option<(f32, CandidateTier, bool)>,
    /// Cloud top answer: score, right?
    cloud_top: Option<(f32, bool)>,
    /// Where the cloud answer came from: "cache", "sent", "skipped" or "error".
    origin: &'static str,
}

/// Run the cloud evaluation for the queries in `items` (wav, title, artist).
pub fn eval(
    index_path: &Path,
    items: Vec<(String, String, String)>,
    out: Option<&Path>,
    encoder: Option<&EncoderArgs>,
    args: &CloudArgs,
) -> anyhow::Result<()> {
    if args.provider != "acrcloud" && args.provider != "none" {
        bail!("unknown cloud provider {} (acrcloud or none)", args.provider);
    }
    let transcripts = match &args.transcripts {
        Some(path) => read_transcripts(path)?,
        None => Default::default(),
    };
    let items = match args.sample {
        Some(n) => sample(items, n),
        None => items,
    };
    let index = Arc::new(MelodyIndex::open(index_path)?);
    let encoder = match encoder {
        Some(a) => {
            let e = EncoderEval::open(a, &index)?;
            Some((Arc::new(e.encoder), e.budget))
        }
        None => None,
    };
    let searcher = MelodySearcher { index: index.clone(), encoder };
    std::fs::create_dir_all(&args.cache).with_context(|| format!("creating {}", args.cache.display()))?;
    let local_only = args.offline || args.provider == "none";
    let pace = match &args.base_url {
        Some(url) if is_local(url) => Duration::from_millis(args.pace_ms),
        _ => Duration::from_millis(args.pace_ms).max(MIN_PACE),
    };
    let client = if local_only { None } else { Some(client(args)?) };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let tracker = PitchTracker::new()?;
    let cancel = CancelToken::new();

    let mut sending = !local_only;
    let http = mfcore::lyrics::eval::http_client()?;
    // Never YouTube Music from evaluation machines.
    let lyric_options = SearchOptions {
        backends: vec![Backend::ITunes, Backend::Genius, Backend::Deezer],
        cache_dir: args.lyrics_cache.clone(),
        ..SearchOptions::default()
    };
    let mut sent = 0usize;
    let mut failures = 0usize;
    let mut last: Option<Instant> = None;
    let mut rows = Vec::new();
    let mut lines = Vec::new();
    for (wav, title, artist) in &items {
        let name = Path::new(wav).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let bytes = std::fs::read(wav).with_context(|| format!("reading {wav}"))?;
        let samples = read_wav_16k(Path::new(wav))?;
        let expected = index.find_song(title, artist).map(|s| s.id);

        // Local, as the app searches.
        let frames = tracker.analyze_detailed(&samples).map(|a| a.frames).unwrap_or_default();
        let query = query_from_details(&frames);
        let melody = query.as_ref().map(|q| searcher.search(q)).unwrap_or_default();
        let expected_work = expected.map(|e| matcher::work_key(&index, e));

        // Cloud, from the cache or one paced request.
        let key = cache_key(&bytes);
        let path = args.cache.join(format!("{key}.json"));
        let mut origin = if args.provider == "none" { "off" } else { "skipped" };
        let mut response: Option<Value> = cached(&path);
        if response.is_some() {
            origin = "cache";
        } else if sending && sent >= args.max_requests {
            sending = false;
            eprintln!("cloud: request limit of {} reached; the rest stay local", args.max_requests);
        }
        if response.is_none()
            && sending
            && let Some(acr) = &client
        {
            if let Some(at) = last {
                let wait = pace.saturating_sub(at.elapsed());
                std::thread::sleep(wait);
            }
            last = Some(Instant::now());
            sent += 1;
            let clip = hum::acr_humming_clip(&samples);
            match runtime.block_on(acr.humming_response(clip, &cancel)) {
                Ok(body) => {
                    failures = 0;
                    store(&path, &body)?;
                    response = Some(body);
                    origin = "sent";
                }
                Err(e) => {
                    origin = "error";
                    let stop = matches!(e, MfError::RateLimited { .. } | MfError::Platform { .. });
                    failures += 1;
                    eprintln!("cloud: {name}: {e}");
                    if stop || failures >= MAX_FAILURES {
                        sending = false;
                        eprintln!("cloud: stopped sending; the answers so far are cached");
                    }
                }
            }
        }
        let acr: Vec<ProviderMatch> = response.as_ref().map(acrcloud::parse_humming).unwrap_or_default();
        let is_expected = |t: &str, a: &str| same_song(t, a, title, artist);
        let cloud = acr.iter().position(|m| is_expected(&m.track.title, &m.track.artist)).map(|p| p + 1);
        let cloud_top = acr.first().map(|m| (m.score, is_expected(&m.track.title, &m.track.artist)));

        // Sung words, as the session searches them.
        let sung: Vec<LyricMatch> = match transcripts.get(&name) {
            Some(t) => {
                let hypotheses = hum::sung_hypotheses(t);
                if hypotheses.is_empty() {
                    Vec::new()
                } else {
                    let search = mfcore::lyrics::search_with(&http, &hypotheses, "US", &cancel, &lyric_options);
                    let weight = hum::transcript_weight(t.confidence);
                    match runtime.block_on(search) {
                        Ok(report) => report
                            .matches
                            .into_iter()
                            .map(|mut m| {
                                m.score *= weight;
                                m
                            })
                            .collect(),
                        Err(e) => {
                            eprintln!("lyrics: {name}: {e}");
                            Vec::new()
                        }
                    }
                }
            }
            None => Vec::new(),
        };
        let lyrics = sung.iter().position(|m| is_expected(&m.track.title, &m.track.artist)).map(|p| p + 1);

        // Merged, as the session fuses them.
        let merged_list = hum::fuse_answers(Some(&index), query.clone(), melody.clone(), sung.clone(), acr.clone());
        // Right: any recording of the expected work, as the matcher's answers count.
        let right = |c: &Candidate| {
            let song: Option<u32> = c.track.ids.get("melody").and_then(|m| m.parse().ok());
            song.zip(expected_work).is_some_and(|(s, w)| matcher::work_key(&index, s) == w)
                || is_expected(&c.track.title, &c.track.artist)
        };
        let merged = merged_list.iter().position(&right).map(|p| p + 1);
        // Local: what the app shows from the melody search alone.
        let local_list = hum::fuse_answers(Some(&index), query.clone(), melody.clone(), Vec::new(), Vec::new());
        let local = local_list.iter().position(&right).map(|p| p + 1);
        let top = merged_list.first().map(|c| (c.score, c.tier, right(c)));
        let row = Row {
            name: name.clone(),
            in_index: expected.is_some(),
            local,
            lyrics,
            cloud,
            merged,
            top,
            cloud_top,
            origin,
        };
        lines.push(json!({
            "wav": wav, "expected": [title, artist], "in_index": row.in_index, "local_rank": local,
            "lyrics_rank": lyrics, "cloud_rank": cloud, "merged_rank": merged, "cloud": origin,
            "transcript": transcripts.get(&name).map(|t| t.text.clone()),
            "lyrics_answers": sung.iter().map(|m| json!([m.track.title, m.track.artist, m.score])).collect::<Vec<_>>(),
            "local_top": melody.first().map(|m| json!([m.song.title, m.song.artist, m.score])),
            "cloud_answers": acr.iter().map(|m| json!([m.track.title, m.track.artist, m.score])).collect::<Vec<_>>(),
            "merged": merged_list.iter().map(|c| json!({
                "title": c.track.title, "artist": c.track.artist, "score": c.score, "tier": format!("{:?}", c.tier),
                "sources": c.sources.iter().map(|s| kind(s.kind)).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        }));
        rows.push(row);
    }
    if let Some(out) = out {
        let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(out, text)?;
    }
    report(&rows, sent, args.price_per_1000);
    Ok(())
}

/// Transcripts by wav file name.
fn read_transcripts(path: &Path) -> anyhow::Result<std::collections::HashMap<String, SpeechTranscript>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = std::collections::HashMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let t: TranscriptLine = serde_json::from_str(line).context("a transcript line")?;
        let name = Path::new(&t.wav).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or(t.wav);
        let transcript = SpeechTranscript {
            text: t.text,
            alternatives: t.alternatives,
            confidence: t.confidence,
            is_final: true,
            ..SpeechTranscript::default()
        };
        out.insert(name, transcript);
    }
    Ok(out)
}

fn client(args: &CloudArgs) -> anyhow::Result<AcrCloud> {
    let http = mfcore::lyrics::eval::http_client()?;
    let creds = match std::fs::read_to_string(&args.credentials) {
        Ok(text) => {
            let f: CredentialFile = serde_json::from_str(&text)
                .with_context(|| format!("{} is not a credentials file", args.credentials.display()))?;
            AcrCredentials { host: f.host, access_key: f.access_key, access_secret: f.access_secret }
        }
        // A local stub does not check them.
        Err(_) if args.base_url.as_deref().is_some_and(is_local) => {
            AcrCredentials { host: "localhost".into(), access_key: "stub".into(), access_secret: "stub".into() }
        }
        Err(_) => bail!(
            "no ACRCloud credentials at {}: run tools/eval/store-acrcloud-credentials.sh first (or use --cloud-offline)",
            args.credentials.display()
        ),
    };
    let acr = AcrCloud::new(http, creds);
    Ok(match &args.base_url {
        Some(url) => acr.with_base_url(url),
        None => acr,
    })
}

fn is_local(url: &str) -> bool {
    let rest = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or(url);
    rest.starts_with("127.0.0.1") || rest.starts_with("localhost") || rest.starts_with("[::1]")
}

/// Digest of the take's file (stable across machines, unlike the decoded clip).
fn cache_key(file: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(CLIP_VERSION.as_bytes());
    h.update(file);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn cached(path: &Path) -> Option<Value> {
    let v: Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    v.get("response").cloned()
}

fn store(path: &Path, body: &Value) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&json!({"provider": "acrcloud", "response": body}))?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// The same song: equal normalized titles and a shared artist ("-" or empty
/// expected artist: any).
pub fn same_song(title: &str, artist: &str, want_title: &str, want_artist: &str) -> bool {
    let t = normalize_title(title);
    if t.is_empty() || t != normalize_title(want_title) {
        return false;
    }
    let want = normalize_artists(want_artist);
    if want.is_empty() || want_artist.trim() == "-" {
        return true;
    }
    let have: HashSet<String> = normalize_artists(artist).split(';').map(str::to_string).collect();
    want.split(';').any(|a| have.contains(a))
}

/// A query: wav path, expected title, expected artist.
type Query = (String, String, String);

/// A fixed sample of `n` takes spread over songs: songs in digest order, each
/// song's takes in digest order of their file names, one take per song per round.
pub fn sample(items: Vec<(String, String, String)>, n: usize) -> Vec<(String, String, String)> {
    let digest = |s: &str| -> String { Sha256::digest(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect() };
    let mut songs: BTreeMap<String, Vec<(String, Query)>> = BTreeMap::new();
    for item in items {
        let song = digest(&format!("{}\u{0}{}", normalize_title(&item.1), normalize_artists(&item.2)));
        let file = Path::new(&item.0).file_name().map(|f| f.to_string_lossy().into_owned()).unwrap_or_default();
        songs.entry(song).or_default().push((digest(&file), item));
    }
    let mut queues: Vec<Vec<(String, String, String)>> = songs
        .into_values()
        .map(|mut takes| {
            takes.sort_by(|a, b| a.0.cmp(&b.0));
            takes.into_iter().rev().map(|(_, item)| item).collect()
        })
        .collect();
    let mut out = Vec::new();
    while out.len() < n && queues.iter().any(|q| !q.is_empty()) {
        for q in &mut queues {
            if out.len() >= n {
                break;
            }
            if let Some(item) = q.pop() {
                out.push(item);
            }
        }
    }
    out
}

fn kind(k: CandidateSourceKind) -> &'static str {
    match k {
        CandidateSourceKind::Melody => "melody",
        CandidateSourceKind::Lyrics => "lyrics",
        CandidateSourceKind::AcrHumming => "acrcloud",
        CandidateSourceKind::Fingerprint => "fingerprint",
    }
}

fn report(rows: &[Row], sent: usize, price: Option<f64>) {
    let n = rows.len().max(1) as f64;
    let at = |f: &dyn Fn(&Row) -> Option<usize>, k: usize| rows.iter().filter(|r| f(r).is_some_and(|x| x <= k)).count();
    let count = |o: &str| rows.iter().filter(|r| r.origin == o).count();
    println!(
        "takes {} ({} in the index); cloud answers: {} sent, {} from the cache, {} skipped, {} failed",
        rows.len(),
        rows.iter().filter(|r| r.in_index).count(),
        count("sent"),
        count("cache"),
        count("skipped"),
        count("error")
    );
    println!("            top-1          top-10");
    for (label, f) in [
        ("local", &(|r: &Row| r.local) as &dyn Fn(&Row) -> Option<usize>),
        ("lyrics", &|r: &Row| r.lyrics),
        ("cloud", &|r: &Row| r.cloud),
        ("merged", &|r: &Row| r.merged),
    ] {
        let (a, b) = (at(f, 1), at(f, 10));
        println!("  {label:8}  {a:4} {:6.1} %  {b:4} {:6.1} %", 100.0 * a as f64 / n, 100.0 * b as f64 / n);
    }
    for tier in [CandidateTier::Strong, CandidateTier::Possible, CandidateTier::LongShot] {
        let t: Vec<&Row> = rows.iter().filter(|r| r.top.is_some_and(|x| x.1 == tier)).collect();
        if !t.is_empty() {
            let right = t.iter().filter(|r| r.top.is_some_and(|x| x.2)).count();
            println!(
                "  merged {tier:?}: {} takes ({:.1} %), right {:.1} %",
                t.len(),
                100.0 * t.len() as f64 / n,
                100.0 * right as f64 / t.len() as f64
            );
        }
    }
    println!("  cloud top score   takes   right");
    for b in 0..5 {
        let (lo, hi) = (b as f32 / 5.0, (b + 1) as f32 / 5.0);
        let bin: Vec<&Row> =
            rows.iter().filter(|r| r.cloud_top.is_some_and(|x| x.0 >= lo && (x.0 < hi || b == 4))).collect();
        if !bin.is_empty() {
            let right = bin.iter().filter(|r| r.cloud_top.is_some_and(|x| x.1)).count();
            println!("    {lo:.1}-{hi:.1}        {:5}   {:.3}", bin.len(), right as f64 / bin.len() as f64);
        }
    }
    match price {
        Some(p) => println!(
            "cost: {sent} requests now = {:.2}; one request per search, so 1,000 searches = {p:.2} at {p:.2} per 1,000",
            sent as f64 * p / 1000.0
        ),
        None => {
            println!("cost: {sent} requests now; one request per search (give --cloud-price-per-1000 for the cost)")
        }
    }
    if rows.len() <= 60 {
        println!("\n  take                                                    local lyrics cloud merged  merged top");
        let rank = |r: Option<usize>| r.map(|x| x.to_string()).unwrap_or_else(|| "-".into());
        for r in rows {
            let top = r
                .top
                .map(|(s, t, ok)| format!("{s:.2} {t:?}{}", if ok { " (right)" } else { "" }))
                .unwrap_or_else(|| "no answer".into());
            println!(
                "  {:55} {:>5} {:>6} {:>5} {:>6}  {top}{}",
                r.name,
                rank(r.local),
                rank(r.lyrics),
                rank(r.cloud),
                rank(r.merged),
                if r.in_index { "" } else { " [not in the index]" }
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn songs_compare_by_normalized_title_and_a_shared_artist() {
        assert!(same_song(
            "Never Gonna Give You Up (Remastered)",
            "Rick Astley",
            "Never Gonna Give You Up",
            "Rick Astley"
        ));
        assert!(same_song("Africa", "TOTO", "Africa", "Toto"));
        assert!(same_song("Twinkle Twinkle Little Star", "Super Simple Songs", "Twinkle Twinkle Little Star", "-"));
        assert!(!same_song("Africa", "Weezer & Toto", "Africa", "Rick Astley"));
        assert!(!same_song("Hello", "Adele", "Africa", "Toto"));
    }

    #[test]
    fn the_sample_is_fixed_and_spread_over_songs() {
        let items: Vec<(String, String, String)> =
            (0..40).map(|i| (format!("/x/{i}.wav"), format!("Song {}", i % 10), "A".to_string())).collect();
        let a = sample(items.clone(), 15);
        let mut shuffled = items.clone();
        shuffled.reverse();
        assert_eq!(a, sample(shuffled, 15), "independent of the list order");
        let songs: HashSet<&String> = a[..10].iter().map(|x| &x.1).collect();
        assert_eq!(songs.len(), 10, "every song once before any twice");
        assert_eq!(sample(items, 100).len(), 40);
    }

    /// A stand-in for ACRCloud: answers every identify request with `body` and
    /// counts them.
    fn stub(body: &'static str) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut payload = vec![0u8; length];
                let _ = reader.read_exact(&mut payload);
                counter.fetch_add(1, Ordering::SeqCst);
                let mut s = stream;
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        (url, hits)
    }

    #[test]
    fn cloud_answers_are_paced_cached_and_merged() {
        let dir = std::env::temp_dir().join(format!("mf-cloud-{}", mfcore::util::new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        // An index with the hummed tune and a few others.
        let notes = [57.0, 59.0, 60.0, 62.0, 64.0, 62.0, 60.0, 59.0];
        let song = |title: &str, shift: f32| -> String {
            let n: Vec<String> = (0..64)
                .map(|i| format!("[{}, {}, 400]", notes[(i * 3 + (shift as usize)) % 8] + shift, i * 400))
                .collect();
            format!(
                r#"{{"title": "{title}", "artist": "Band", "isrc": null, "deezer_id": null, "year": 2001, "popularity": 0.5, "phrases": [{{"source": "midi", "start_ms": 0, "notes": [{}]}}]}}"#,
                n.join(", ")
            )
        };
        let mut lines = vec![song("Hummed Tune", 0.0)];
        lines.push(song("Cloud Only", 3.0));
        let jsonl = dir.join("songs.jsonl");
        std::fs::write(&jsonl, lines.join("\n")).unwrap();
        let mfi = dir.join("index.mfi");
        mfcore::melody::builder::build_index(&jsonl, &mfi, "test").unwrap();
        // Three hummed takes of "Hummed Tune".
        let rate = 16_000.0;
        let freqs = [220.0, 246.9, 261.6, 293.7, 329.6, 293.7, 261.6, 246.9];
        let mut items = Vec::new();
        for k in 0..3 {
            let audio: Vec<i16> = (0..(10.0 * rate) as usize)
                .map(|i| {
                    let t = i as f32 / rate;
                    let f = freqs[(t * 2.5) as usize % 8] * (1.0 + 0.01 * k as f32);
                    (8000.0 * (2.0 * std::f32::consts::PI * f * t).sin()) as i16
                })
                .collect();
            let wav = dir.join(format!("take{k}.wav"));
            mfcore::audio::wav::write_wav(&wav, &audio, 16_000).unwrap();
            items.push((wav.to_string_lossy().into_owned(), "Hummed Tune".to_string(), "Band".to_string()));
        }
        const BODY: &str = r#"{"status": {"code": 0, "msg": "Success"}, "metadata": {"humming": [
            {"title": "Hummed Tune", "artists": [{"name": "Band"}], "acrid": "a1", "score": "0.81"},
            {"title": "Elsewhere", "artists": [{"name": "Other"}], "acrid": "a2", "score": "0.40"}]}}"#;
        let (url, hits) = stub(BODY);
        let args = CloudArgs {
            provider: "acrcloud".into(),
            credentials: dir.join("missing.json"),
            cache: dir.join("cache"),
            base_url: Some(url),
            pace_ms: 150,
            max_requests: 2,
            offline: false,
            sample: None,
            price_per_1000: Some(5.0),
            transcripts: None,
            lyrics_cache: None,
        };
        let out = dir.join("out.jsonl");
        let started = Instant::now();
        eval(&mfi, items.clone(), Some(&out), None, &args).unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 2, "the request limit holds");
        assert!(started.elapsed() >= Duration::from_millis(150), "paced");
        let rows: Vec<Value> =
            std::fs::read_to_string(&out).unwrap().lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(rows.iter().filter(|r| r["cloud"] == "sent").count(), 2);
        assert_eq!(rows[2]["cloud"], "skipped");
        assert_eq!(rows[0]["cloud_rank"], 1);
        assert_eq!(rows[0]["merged_rank"], 1);
        let sources = rows[0]["merged"][0]["sources"].as_array().unwrap();
        assert!(sources.iter().any(|s| s == "acrcloud"), "{rows:?}");
        // Run again: the cached answers are reused, only the third take is sent.
        eval(&mfi, items.clone(), Some(&out), None, &args).unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        // Offline: nothing is sent at all.
        let offline = CloudArgs { offline: true, ..args };
        eval(&mfi, items, Some(&out), None, &offline).unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        let rows = std::fs::read_to_string(&out).unwrap();
        assert_eq!(rows.matches("\"cloud\":\"cache\"").count(), 3, "{rows}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
