//! `mf dev stub-recognizer` and `mf dev header-rules` (`mf dev youtube-probe`
//! lives in youtube_record.rs).
//!
//! The stub recognizer is a local stand-in for every service the engine needs
//! to identify the synthetic fixtures (`tools/fixtures/synth_songs.py`): Shazam
//! (it decodes the posted signature and matches its peaks against the fixture
//! songs with the engine's own landmark matcher, speed and pitch changes
//! included), Deezer, iTunes and their previews, LRCLIB and the melody index
//! (manifest plus an index built from the fixtures' exact melodies). It listens
//! on 127.0.0.1 only and prints one JSON line when ready:
//!
//! ```json
//! {"url": "http://127.0.0.1:47700", "endpoints": {"routes": {…}, "isolate": "…"}, "manifest": "…", "songs": 12}
//! ```
//!
//! `endpoints` is the map to install (`--stub <url>` on the engine commands, the
//! extension's test builds): requests to the public services go to the stub,
//! anything else to its `/blocked` sink, so nothing leaves the machine.
//! `GET /__requests` lists what the stub received (method, path, the headers
//! the header policy cares about), `POST /__reset` clears that list.

use std::collections::{BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use clap::{Args, Subcommand};
use mfcore::audio::decode::decode_file;
use mfcore::fingerprint::{self, Index, Match, Peak};
use mfcore::melody::builder::{SongInput, build_bytes};
use mfcore::shazam::signature;
use mfcore::util::cancel::CancelToken;
use mfcore::util::endpoints::TestEndpoints;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Subcommand)]
pub enum Command {
    /// Serve the synthetic fixtures as Shazam, Deezer, iTunes, LRCLIB and the melody index (127.0.0.1 only).
    StubRecognizer(StubArgs),
    /// Print the header policy table (what the browser extension installs as rules) as JSON.
    HeaderRules(HeaderRulesArgs),
    /// Read YouTube links as the browser engine does (no yt-dlp); optionally record the answers.
    YoutubeProbe(crate::youtube_record::ProbeArgs),
}

#[derive(Args)]
pub struct StubArgs {
    /// Fixture folder written by tools/fixtures/synth_songs.py.
    #[arg(long, value_name = "DIR")]
    refs: PathBuf,
    /// Port on 127.0.0.1 (0 picks a free one).
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Also write the ready line (JSON) to this file.
    #[arg(long, value_name = "FILE")]
    ready_file: Option<PathBuf>,
    /// Answer the first N recognizer requests with HTTP 429.
    #[arg(long, default_value_t = 0, value_name = "N")]
    throttle_first: u32,
    /// Retry-After of those answers, in seconds.
    #[arg(long, default_value_t = 30, value_name = "S")]
    retry_after: u32,
    /// Print every request to stderr.
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Args)]
pub struct HeaderRulesArgs {
    /// App version for the neutral agent.
    #[arg(long, default_value_t = mfcore::core_version())]
    app_version: String,
    /// Which of the eight Shazam agents the shazam rule carries.
    #[arg(long, default_value_t = 0)]
    agent: usize,
}

pub fn run(command: Command) -> Result<()> {
    match command {
        Command::StubRecognizer(args) => serve(args),
        Command::YoutubeProbe(args) => crate::youtube_record::run(args),
        Command::HeaderRules(args) => {
            let rules = mfcore::util::http::header_rules(&args.app_version, args.agent);
            println!("{}", serde_json::to_string_pretty(&rules)?);
            Ok(())
        }
    }
}

// ------------------------------------------------------------------ fixtures

#[derive(Debug, Clone, Deserialize)]
struct CatalogSong {
    id: String,
    title: String,
    artist: String,
    album: String,
    year: u16,
    genre: String,
    duration_ms: u64,
    file: String,
    isrc: String,
    deezer_id: u64,
    apple_id: u64,
    shazam_key: String,
    preview_start_ms: u64,
    preview_ms: u64,
    #[serde(default)]
    lyrics: Vec<(u64, String)>,
}

#[derive(Deserialize)]
struct Catalog {
    songs: Vec<CatalogSong>,
}

/// Albums get ids from their titles so every song of one album shares one.
fn album_id(title: &str) -> u64 {
    let digest = Sha256::digest(title.as_bytes());
    800_000_000 + u64::from(u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]]) % 1_000_000)
}

const PLAYLIST_ID: u64 = 700_000_001;
const MAX_BODY: usize = 4 * 1024 * 1024;

/// What the recognizer accepts as a match; below it the stub answers "no match",
/// as Shazam would. A true match aligns several hashes per query peak (150–2500
/// for a 3–12 s query); songs with similar drums and tempos align a few dozen
/// by chance, which the per-peak rate and the lead keep out.
fn confident(m: &Match, query_peaks: usize) -> bool {
    m.score >= 40
        && m.score as f32 >= 2.0 * m.other as f32
        && m.score as f32 >= 8.0 * m.noise.max(1.0)
        && m.score as f32 >= 0.6 * query_peaks as f32
}

struct Logged {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

struct Stub {
    root: PathBuf,
    songs: Vec<CatalogSong>,
    /// 16 kHz mono audio of each song (previews are cut from it).
    audio: Vec<Vec<i16>>,
    index: Index,
    endpoints: TestEndpoints,
    melody: Vec<u8>,
    manifest: Value,
    log: Mutex<Vec<Logged>>,
    throttle_left: Mutex<u32>,
    retry_after: u32,
    verbose: bool,
}

fn decode(path: &Path) -> Result<Vec<i16>> {
    let temp = std::env::temp_dir().join(format!("mf-stub-{}.pcm", mfcore::util::new_id()));
    let pcm = decode_file(path, &temp, None, &CancelToken::new(), &|_| {})
        .with_context(|| format!("cannot decode {}", path.display()))?;
    let samples = pcm.read_samples(0, pcm.len_samples() as usize)?;
    let _ = std::fs::remove_file(&temp);
    Ok(samples)
}

fn load(args: &StubArgs, base: &str) -> Result<Stub> {
    let root = args.refs.canonicalize().with_context(|| format!("no fixtures at {}", args.refs.display()))?;
    let catalog: Catalog = serde_json::from_str(
        &std::fs::read_to_string(root.join("catalog.json"))
            .context("catalog.json is missing: run tools/fixtures/synth_songs.py first")?,
    )
    .context("catalog.json is not a fixture catalog")?;
    if catalog.songs.is_empty() {
        bail!("the catalog has no songs");
    }
    let mut index = Index::new();
    let mut audio = Vec::new();
    for song in &catalog.songs {
        let samples = decode(&root.join(&song.file))?;
        index.add(&fingerprint::peaks(&samples));
        audio.push(samples);
    }
    let melody_input = std::fs::read_to_string(root.join("melody.jsonl")).context("melody.jsonl is missing")?;
    let inputs: Vec<SongInput> = melody_input
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .context("melody.jsonl is not builder input")?;
    let digest = Sha256::digest(melody_input.as_bytes());
    let version = format!("synth-{}", hex(&digest[..4]));
    let (melody, stats) = build_bytes(&inputs, &version)?;
    let manifest = json!({
        "version": version,
        "url": format!("{base}/melody/index.mfi"),
        "sha256": hex(&Sha256::digest(&melody)),
        "bytes": melody.len(),
        "songs": stats.songs,
        "format": mfcore::melody::index::FORMAT,
    });
    Ok(Stub {
        root,
        songs: catalog.songs,
        audio,
        index,
        endpoints: TestEndpoints::stub(base)?,
        melody,
        manifest,
        log: Mutex::new(Vec::new()),
        throttle_left: Mutex::new(args.throttle_first),
        retry_after: args.retry_after,
        verbose: args.verbose,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ------------------------------------------------------------------ server

fn serve(args: StubArgs) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    runtime.block_on(async move {
        let listener = TcpListener::bind(("127.0.0.1", args.port)).await.context("cannot listen on 127.0.0.1")?;
        let base = format!("http://{}", listener.local_addr()?);
        let stub = Arc::new(tokio::task::block_in_place(|| load(&args, &base))?);
        let ready = json!({
            "url": base,
            "endpoints": stub.endpoints.to_json(),
            "manifest": format!("{base}/melody/manifest.json"),
            "songs": stub.songs.len(),
        });
        if let Some(path) = &args.ready_file {
            std::fs::write(path, format!("{ready}\n")).with_context(|| format!("cannot write {}", path.display()))?;
        }
        println!("{ready}");
        loop {
            let (stream, _) = listener.accept().await?;
            let stub = stub.clone();
            tokio::spawn(async move {
                if let Err(e) = connection(stream, &stub).await
                    && stub.verbose
                {
                    eprintln!("stub: connection failed: {e}");
                }
            });
        }
    })
}

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Request {
    fn param(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

struct Response {
    status: u16,
    content_type: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn json(status: u16, value: &Value) -> Self {
        Self { status, content_type: "application/json", headers: Vec::new(), body: value.to_string().into_bytes() }
    }

    fn bytes(content_type: &'static str, body: Vec<u8>) -> Self {
        Self { status: 200, content_type, headers: Vec::new(), body }
    }

    fn empty(status: u16) -> Self {
        Self { status, content_type: "text/plain", headers: Vec::new(), body: Vec::new() }
    }
}

async fn connection(stream: TcpStream, stub: &Stub) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let mut headers = HashMap::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let length = headers.get("content-length").and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
    if length > MAX_BODY {
        return respond(&mut write, &method, Response::empty(413)).await;
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    let url =
        url::Url::parse(&format!("http://stub{target}")).unwrap_or_else(|_| url::Url::parse("http://stub/").unwrap());
    let request = Request {
        method: method.clone(),
        path: url.path().to_string(),
        query: url.query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect(),
        headers,
        body,
    };
    let response = stub.route(&request);
    if stub.verbose {
        eprintln!("stub: {} {} -> {}", request.method, target, response.status);
    }
    respond(&mut write, &method, response).await
}

async fn respond(write: &mut tokio::net::tcp::OwnedWriteHalf, method: &str, response: Response) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} Stub\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    for (k, v) in &response.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    write.write_all(head.as_bytes()).await?;
    if method != "HEAD" {
        write.write_all(&response.body).await?;
    }
    write.flush().await
}

/// Serve `bytes` honouring a single `Range: bytes=a-b` (players seek in the media).
fn ranged(request: &Request, content_type: &'static str, bytes: &[u8]) -> Response {
    let total = bytes.len();
    let range = request.headers.get("range").and_then(|r| r.strip_prefix("bytes=")).and_then(|r| {
        let (a, b) = r.split_once('-')?;
        let start = a.trim().parse::<usize>().ok()?;
        let end = if b.trim().is_empty() { total.checked_sub(1)? } else { b.trim().parse::<usize>().ok()? };
        (start <= end && start < total).then_some((start, end.min(total - 1)))
    });
    let mut response = match range {
        Some((start, end)) => {
            let mut r = Response::bytes(content_type, bytes[start..=end].to_vec());
            r.status = 206;
            r.headers.push(("Content-Range".into(), format!("bytes {start}-{end}/{total}")));
            r
        }
        None => Response::bytes(content_type, bytes.to_vec()),
    };
    response.headers.push(("Accept-Ranges".into(), "bytes".into()));
    response
}

// ------------------------------------------------------------------ routes

/// Lower-case alphanumeric words.
fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric()).filter(|w| !w.is_empty()).map(str::to_lowercase).collect()
}

/// `artist:"…" track:"…"` (Deezer's advanced search) or plain words.
fn fields(query: &str) -> (Option<String>, Option<String>) {
    let quoted = |key: &str| {
        let start = query.find(&format!("{key}:\""))? + key.len() + 2;
        let end = query[start..].find('"')? + start;
        Some(query[start..end].to_string())
    };
    (quoted("artist"), quoted("track"))
}

impl Stub {
    fn route(&self, r: &Request) -> Response {
        let watched = ["user-agent", "origin", "referer", "cookie", "accept-language", "content-language"];
        if !r.path.starts_with("/__") {
            self.log.lock().push(Logged {
                method: r.method.clone(),
                path: r.path.clone(),
                headers: watched.iter().filter_map(|h| r.headers.get(*h).map(|v| (h.to_string(), v.clone()))).collect(),
            });
        }
        let path = r.path.as_str();
        match path {
            "/__endpoints" => return Response::json(200, &self.endpoints.to_json()),
            "/__requests" => {
                let log: Vec<Value> = self
                    .log
                    .lock()
                    .iter()
                    .map(|l| json!({"method": l.method, "path": l.path, "headers": l.headers.iter().cloned().collect::<HashMap<_, _>>()}))
                    .collect();
                return Response::json(200, &Value::Array(log));
            }
            "/__reset" => {
                self.log.lock().clear();
                return Response::empty(204);
            }
            "/blocked" => {
                eprintln!("stub: blocked a request to {}", r.param("url").unwrap_or("?"));
                return Response::json(403, &json!({"blocked": r.param("url")}));
            }
            _ => {}
        }
        let (service, rest) = match path[1..].split_once('/') {
            Some((s, rest)) => (s, format!("/{rest}")),
            None => (&path[1..], "/".to_string()),
        };
        match service {
            "shazam" if r.method == "POST" && rest.starts_with("/discovery/v5/") => self.shazam(r),
            "shazam" => Response::empty(404),
            "deezer" => self.deezer(&rest, r),
            "dzcdn" => self.preview(&rest, |s| s.deezer_id),
            "itunes" => self.itunes(&rest, r),
            "itunes-audio" => self.preview(&rest, |s| s.apple_id),
            "lrclib" => self.lrclib(&rest, r),
            "melody" | "github" if rest.ends_with("/manifest.json") || rest == "/manifest.json" => {
                Response::json(200, &self.manifest)
            }
            "melody" if rest == "/index.mfi" => ranged(r, "application/octet-stream", &self.melody),
            "media" => self.media(&rest, r),
            _ => Response::json(404, &json!({"error": "not a stub route"})),
        }
    }

    // ---------------------------------------------------------------- Shazam

    fn shazam(&self, r: &Request) -> Response {
        {
            let mut left = self.throttle_left.lock();
            if *left > 0 {
                *left -= 1;
                let mut resp = Response::empty(429);
                resp.headers.push(("Retry-After".into(), self.retry_after.to_string()));
                return resp;
            }
        }
        let query = match signature_peaks(&r.body) {
            Ok(peaks) => peaks,
            Err(e) => return Response::json(400, &json!({"error": e.to_string()})),
        };
        let now = mfcore::util::now_ms();
        let tag = mfcore::util::new_id().to_ascii_uppercase();
        let best = self.index.search(&query, &fingerprint::warps(0.08, 0.005)).into_iter().next();
        let Some(m) = best.clone().filter(|m| confident(m, query.len())) else {
            if self.verbose {
                let near =
                    best.map(|m| format!(", best {} with {} vs {}", self.songs[m.reference].id, m.score, m.other));
                eprintln!("stub: recognizer: no match ({} peaks{})", query.len(), near.unwrap_or_default());
            }
            return Response::json(
                200,
                &json!({"matches": [], "timestamp": now, "timezone": "Europe/Paris", "tagid": tag}),
            );
        };
        let song = &self.songs[m.reference];
        let offset_s = -m.offset_frames * m.warp.time * fingerprint::FRAME_MS as f32 / 1000.0;
        if self.verbose {
            eprintln!(
                "stub: recognizer: {} at {offset_s:.2} s (score {}, other {}, speed {:+.1} %, pitch {:+.1} %)",
                song.id,
                m.score,
                m.other,
                (m.warp.time - 1.0) * 100.0,
                (m.warp.freq - 1.0) * 100.0
            );
        }
        Response::json(200, &shazam_answer(song, offset_s, &m, now, &tag))
    }

    // ---------------------------------------------------------------- Deezer

    fn deezer_track(&self, s: &CatalogSong) -> Value {
        json!({
            "id": s.deezer_id,
            "readable": true,
            "title": s.title,
            "title_short": s.title,
            "isrc": s.isrc,
            "link": format!("https://www.deezer.com/track/{}", s.deezer_id),
            "duration": s.duration_ms / 1000,
            "rank": 500_000,
            "release_date": format!("{}-01-01", s.year),
            "preview": format!("https://cdnt-preview.dzcdn.net/api/1/1/stub/{}.mp3", s.deezer_id),
            "artist": {"id": 600_000_000 + s.deezer_id % 1000, "name": s.artist, "type": "artist"},
            "contributors": [{"id": 600_000_000 + s.deezer_id % 1000, "name": s.artist, "role": "Main"}],
            "album": {"id": album_id(&s.album), "title": s.album, "release_date": format!("{}-01-01", s.year), "type": "album"},
            "type": "track",
        })
    }

    fn deezer(&self, rest: &str, r: &Request) -> Response {
        let missing =
            || Response::json(200, &json!({"error": {"type": "DataException", "message": "no data", "code": 800}}));
        let parts: Vec<&str> = rest.trim_matches('/').split('/').collect();
        match parts.as_slice() {
            ["track", id] => {
                let found = match id.strip_prefix("isrc:") {
                    Some(isrc) => self.songs.iter().find(|s| s.isrc.eq_ignore_ascii_case(isrc)),
                    None => self.songs.iter().find(|s| id.parse() == Ok(s.deezer_id)),
                };
                found.map_or_else(missing, |s| Response::json(200, &self.deezer_track(s)))
            }
            ["search"] | ["search", "track"] => {
                let q = r.param("q").unwrap_or_default();
                let data: Vec<Value> = self.search(q).into_iter().map(|s| self.deezer_track(s)).collect();
                Response::json(200, &json!({"data": data, "total": data.len()}))
            }
            ["album", id] => {
                let songs: Vec<&CatalogSong> =
                    self.songs.iter().filter(|s| id.parse() == Ok(album_id(&s.album))).collect();
                let Some(first) = songs.first() else { return missing() };
                let tracks: Vec<Value> = songs.iter().map(|s| self.deezer_track(s)).collect();
                Response::json(
                    200,
                    &json!({
                        "id": album_id(&first.album), "title": first.album, "release_date": format!("{}-01-01", first.year),
                        "artist": {"name": "Various Artists"}, "nb_tracks": tracks.len(), "tracks": {"data": tracks}, "type": "album",
                    }),
                )
            }
            ["playlist", id] if id.parse() == Ok(PLAYLIST_ID) => {
                let tracks: Vec<Value> = self.songs.iter().map(|s| self.deezer_track(s)).collect();
                Response::json(
                    200,
                    &json!({
                        "id": PLAYLIST_ID, "title": "Synthetic Fixtures", "nb_tracks": tracks.len(),
                        "creator": {"name": "Music Fetch"}, "tracks": {"data": tracks}, "type": "playlist",
                    }),
                )
            }
            _ => missing(),
        }
    }

    /// Catalog songs a search query names: Deezer's `artist:"…" track:"…"` form,
    /// or every title word among the query words (and the artist's, when given).
    fn search(&self, query: &str) -> Vec<&CatalogSong> {
        let (artist, track) = fields(query);
        let query_words: BTreeSet<String> = words(query).into_iter().collect();
        self.songs
            .iter()
            .filter(|s| match &track {
                Some(t) => words(t) == words(&s.title) && artist.as_ref().is_none_or(|a| words(a) == words(&s.artist)),
                None => {
                    let title = words(&s.title);
                    !title.is_empty() && title.iter().all(|w| query_words.contains(w))
                }
            })
            .collect()
    }

    fn preview(&self, rest: &str, id: impl Fn(&CatalogSong) -> u64) -> Response {
        let name = rest.rsplit('/').next().unwrap_or_default();
        let digits: String = name.chars().take_while(char::is_ascii_digit).collect();
        let Some(i) = self.songs.iter().position(|s| digits.parse() == Ok(id(s))) else {
            return Response::empty(404);
        };
        let song = &self.songs[i];
        let audio = &self.audio[i];
        let start = (song.preview_start_ms * 16) as usize;
        let end = ((song.preview_start_ms + song.preview_ms) * 16) as usize;
        let window = &audio[start.min(audio.len())..end.min(audio.len())];
        // WAV in place of MP3/AAC: the engine's decoder reads it by content.
        Response::bytes("audio/wav", mfcore::audio::wav::encode_wav(window, 16_000))
    }

    // ---------------------------------------------------------------- iTunes

    fn itunes_song(&self, s: &CatalogSong) -> Value {
        let slug = words(&s.title).join("-");
        json!({
            "wrapperType": "track",
            "kind": "song",
            "trackId": s.apple_id,
            "collectionId": album_id(&s.album),
            "trackName": s.title,
            "artistName": s.artist,
            "collectionName": s.album,
            "trackTimeMillis": s.duration_ms,
            "releaseDate": format!("{}-01-01T12:00:00Z", s.year),
            "primaryGenreName": s.genre,
            "previewUrl": format!("https://audio-ssl.itunes.apple.com/itunes-assets/AudioPreview/stub/{}.m4a", s.apple_id),
            "trackViewUrl": format!("https://music.apple.com/us/album/{slug}/{}?i={}", album_id(&s.album), s.apple_id),
            "country": "USA",
        })
    }

    fn itunes(&self, rest: &str, r: &Request) -> Response {
        let results: Vec<Value> = match rest {
            "/lookup" => {
                let ids: Vec<&str> = r.param("id").unwrap_or_default().split(',').collect();
                self.songs
                    .iter()
                    .filter(|s| ids.iter().any(|id| id.parse() == Ok(s.apple_id)))
                    .map(|s| self.itunes_song(s))
                    .collect()
            }
            "/search" => {
                self.search(r.param("term").unwrap_or_default()).into_iter().map(|s| self.itunes_song(s)).collect()
            }
            _ => return Response::empty(404),
        };
        Response::json(200, &json!({"resultCount": results.len(), "results": results}))
    }

    // ---------------------------------------------------------------- LRCLIB

    fn lrclib_record(&self, s: &CatalogSong) -> Value {
        let synced: Vec<String> = s
            .lyrics
            .iter()
            .map(|(ms, text)| format!("[{:02}:{:02}.{:02}] {text}", ms / 60_000, ms / 1000 % 60, ms % 1000 / 10))
            .collect();
        let plain: Vec<&str> = s.lyrics.iter().map(|(_, t)| t.as_str()).collect();
        json!({
            "id": s.deezer_id % 1_000_000,
            "trackName": s.title,
            "artistName": s.artist,
            "albumName": s.album,
            "duration": s.duration_ms as f64 / 1000.0,
            "instrumental": false,
            "plainLyrics": plain.join("\n"),
            "syncedLyrics": synced.join("\n"),
        })
    }

    fn lrclib(&self, rest: &str, r: &Request) -> Response {
        let title = r.param("track_name").or_else(|| r.param("q")).unwrap_or_default();
        let artist = r.param("artist_name");
        let matches = |s: &&CatalogSong| {
            words(title) == words(&s.title) && artist.is_none_or(|a| words(&s.artist).starts_with(&words(a)))
        };
        match rest {
            "/api/get" => match self.songs.iter().find(matches) {
                Some(s) => Response::json(200, &self.lrclib_record(s)),
                None => Response::json(
                    404,
                    &json!({"code": 404, "name": "TrackNotFound", "message": "Failed to find specified track"}),
                ),
            },
            "/api/search" => {
                let found: Vec<Value> = self.songs.iter().filter(matches).map(|s| self.lrclib_record(s)).collect();
                Response::json(200, &Value::Array(found))
            }
            _ => Response::empty(404),
        }
    }

    // ---------------------------------------------------------------- media

    fn media(&self, rest: &str, r: &Request) -> Response {
        let relative = Path::new(rest.trim_start_matches('/'));
        if relative.components().any(|c| !matches!(c, Component::Normal(_))) {
            return Response::empty(404);
        }
        let path = self.root.join(relative);
        let Ok(bytes) = std::fs::read(&path) else { return Response::empty(404) };
        let content_type = match path.extension().and_then(|e| e.to_str()) {
            Some("wav") => "audio/wav",
            Some("json") => "application/json",
            Some("txt") => "text/plain; charset=utf-8",
            _ => "application/octet-stream",
        };
        ranged(r, content_type, &bytes)
    }
}

/// Peaks of the signature in a `discovery/v5` request body.
fn signature_peaks(body: &[u8]) -> Result<Vec<Peak>> {
    let value: Value = serde_json::from_slice(body).context("the body is not JSON")?;
    let uri = value.pointer("/signature/uri").and_then(Value::as_str).context("no signature.uri")?;
    let data = uri.split_once("base64,").map(|(_, d)| d).context("the signature is not a base64 data URI")?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(data.trim()).context("bad base64")?;
    let decoded = signature::decode(&bytes)?;
    let mut peaks: Vec<Peak> = decoded
        .bands
        .iter()
        .flatten()
        .filter(|p| p.fbin > 0)
        .map(|p| Peak { frame: p.frame, log_hz: p.hz().ln(), magnitude: p.magnitude })
        .collect();
    peaks.sort_by(|a, b| a.frame.cmp(&b.frame).then(a.log_hz.total_cmp(&b.log_hz)));
    Ok(peaks)
}

/// A `discovery/v5` answer shaped like Shazam's (what `shazam::parse_match` reads).
fn shazam_answer(song: &CatalogSong, offset_s: f32, m: &Match, now: i64, tag: &str) -> Value {
    json!({
        "matches": [{
            "id": song.shazam_key,
            "offset": (offset_s as f64 * 1000.0).round() / 1000.0,
            "timeskew": m.warp.time - 1.0,
            "frequencyskew": m.warp.freq - 1.0,
        }],
        "location": {"latitude": 45.0, "longitude": 2.0, "altitude": 300.0, "accuracy": 0.01},
        "timestamp": now,
        "timezone": "Europe/Paris",
        "tagid": tag,
        "track": {
            "layout": "5",
            "type": "MUSIC",
            "key": song.shazam_key,
            "title": song.title,
            "subtitle": song.artist,
            "isrc": song.isrc,
            "genres": {"primary": song.genre},
            "hub": {
                "type": "APPLEMUSIC",
                "actions": [
                    {"name": "apple", "type": "applemusicplay", "id": song.apple_id.to_string()},
                    {"name": "apple", "type": "uri",
                     "uri": format!("https://audio-ssl.itunes.apple.com/itunes-assets/AudioPreview/stub/{}.m4a", song.apple_id)},
                ],
            },
            "sections": [{"type": "SONG", "metadata": [
                {"title": "Album", "text": song.album},
                {"title": "Released", "text": song.year.to_string()},
            ]}],
            "url": format!("https://www.shazam.com/track/{}", song.shazam_key),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deezer_queries() {
        assert_eq!(
            fields(r#"artist:"Sine Parade" track:"Glass Lanterns""#),
            (Some("Sine Parade".into()), Some("Glass Lanterns".into()))
        );
        assert_eq!(fields("Sine Parade Glass Lanterns"), (None, None));
        assert_eq!(words("Glass Lanterns (Radio Edit)"), ["glass", "lanterns", "radio", "edit"]);
    }

    #[test]
    fn answers_parse_like_shazams() {
        let song = CatalogSong {
            id: "song-01".into(),
            title: "Copper Orchard".into(),
            artist: "Northbound Echo".into(),
            album: "Test Pressing".into(),
            year: 2019,
            genre: "Electronic".into(),
            duration_ms: 180_000,
            file: "songs/song-01.wav".into(),
            isrc: "ZZMF01900001".into(),
            deezer_id: 900_000_001,
            apple_id: 1_900_000_001,
            shazam_key: "990000001".into(),
            preview_start_ms: 30_000,
            preview_ms: 30_000,
            lyrics: Vec::new(),
        };
        let m = Match {
            reference: 0,
            score: 100,
            other: 3,
            noise: 1.0,
            warp: fingerprint::Warp { time: 1.03, freq: 1.03 },
            offset_frames: -100.0,
            span: (0, 1000),
            coverage: 0.1,
        };
        let answer = shazam_answer(&song, 12.5, &m, 1_791_924_035_549, "X");
        let parsed = mfcore::shazam::parse_match(&answer, "US").expect("parses");
        assert_eq!(parsed.track.title, "Copper Orchard");
        assert_eq!(parsed.track.artist, "Northbound Echo");
        assert_eq!(parsed.track.isrc.as_deref(), Some("ZZMF01900001"));
        assert_eq!(parsed.track.ids.get("apple").map(String::as_str), Some("1900000001"));
        assert_eq!(parsed.offset_ms, 12_500);
        assert!((parsed.timeskew - 0.03).abs() < 1e-4);
        assert!(parsed.track.preview_url.as_deref().is_some_and(|u| u.ends_with("/1900000001.m4a")));
    }
}
