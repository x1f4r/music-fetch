//! YouTube Music search through the InnerTube web API (keyless, unofficial),
//! restricted to songs. Tolerant of altered lyrics; play counts give a
//! popularity hint.

use serde_json::{Value, json};

use super::{Hit, ParseError, non_empty};
use crate::util::now_ms;

const URL: &str = "https://music.youtube.com/youtubei/v1/search?alt=json";
const ORIGIN: &str = "https://music.youtube.com";
/// `params` of the "Songs" search filter.
const SONGS_FILTER: &str = "EgWKAQIIAWoMEA4QChADEAQQCRAF";
/// Renderers collected from one response, at most.
const MAX_ITEMS: usize = 30;

pub(crate) fn request(http: &reqwest::Client, query: &str, country: &str) -> reqwest::RequestBuilder {
    let body = json!({
        "context": {"client": {"clientName": "WEB_REMIX", "clientVersion": client_version(now_ms()), "hl": "en", "gl": country}},
        "query": query,
        "params": SONGS_FILTER,
    });
    http.post(URL)
        .header(reqwest::header::ORIGIN, ORIGIN)
        .header(reqwest::header::REFERER, format!("{ORIGIN}/"))
        .json(&body)
}

/// `1.YYYYMMDD.01.00` for the UTC date of `unix_ms`, the version scheme of the web client.
pub(crate) fn client_version(unix_ms: i64) -> String {
    let (y, m, d) = civil_date(unix_ms.div_euclid(86_400_000));
    format!("1.{y:04}{m:02}{d:02}.01.00")
}

/// Gregorian date of a day count since 1970-01-01 (Howard Hinnant's algorithm).
fn civil_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

pub(crate) fn parse(body: &str) -> Result<Vec<Hit>, ParseError> {
    let root: Value = serde_json::from_str(body)?;
    if root.get("contents").is_none() {
        return Err(ParseError::Malformed("missing contents".into()));
    }
    let mut renderers = Vec::new();
    collect_renderers(&root, &mut renderers);
    Ok(renderers.into_iter().filter_map(parse_item).collect())
}

fn collect_renderers<'a>(value: &'a Value, out: &mut Vec<&'a Value>) {
    if out.len() >= MAX_ITEMS {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "musicResponsiveListItemRenderer" {
                    if out.len() < MAX_ITEMS {
                        out.push(child);
                    }
                } else {
                    collect_renderers(child, out);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|item| collect_renderers(item, out)),
        _ => {}
    }
}

fn parse_item(renderer: &Value) -> Option<Hit> {
    let columns = renderer.get("flexColumns")?.as_array()?;
    let runs = |i: usize| -> Vec<&Value> {
        columns
            .get(i)
            .and_then(|c| c.pointer("/musicResponsiveListItemFlexColumnRenderer/text/runs"))
            .and_then(Value::as_array)
            .map(|r| r.iter().collect())
            .unwrap_or_default()
    };
    let title: String = runs(0).iter().filter_map(|r| run_text(r)).collect();
    let video_id = renderer
        .pointer("/playlistItemData/videoId")
        .or_else(|| runs(0).first().and_then(|r| r.pointer("/navigationEndpoint/watchEndpoint/videoId")))
        .or_else(|| {
            renderer.pointer(
                "/overlay/musicItemThumbnailOverlayRenderer/content/musicPlayButtonRenderer/playNavigationEndpoint/watchEndpoint/videoId",
            )
        })
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut hit = Hit { title: title.trim().to_string(), id: video_id.clone(), ..Hit::default() };
    hit.url = video_id.map(|id| format!("{ORIGIN}/watch?v={id}"));

    // Second column: "Artist & Artist • Album • 3:23", groups separated by " • ".
    let mut groups: Vec<Vec<&Value>> = vec![Vec::new()];
    for run in runs(1) {
        if run_text(run).is_some_and(|t| t.trim() == "•") {
            groups.push(Vec::new());
        } else if let Some(group) = groups.last_mut() {
            group.push(run);
        }
    }
    let mut artists_group: Option<usize> = None;
    for (index, group) in groups.iter().enumerate() {
        let text: String = group.iter().filter_map(|r| run_text(r)).collect();
        let text = text.trim();
        let page = group.iter().find_map(|r| page_type(r));
        match page {
            Some("MUSIC_PAGE_TYPE_ARTIST" | "MUSIC_PAGE_TYPE_USER_CHANNEL") if artists_group.is_none() => {
                artists_group = Some(index);
            }
            Some("MUSIC_PAGE_TYPE_ALBUM") => hit.album = non_empty(Some(text.to_string())),
            _ => {
                if let Some(ms) = parse_duration(text) {
                    hit.duration_ms = Some(ms);
                } else if artists_group.is_none()
                    && (index == 1 || (index == 0 && !matches!(text, "Song" | "Video" | "Episode")))
                {
                    // Artists without a channel page have no link; a leading type label is skipped.
                    artists_group = Some(index);
                }
            }
        }
    }
    if let Some(group) = artists_group.and_then(|i| groups.get(i)) {
        hit.artist = group.iter().filter_map(|r| run_text(r)).collect::<String>().trim().to_string();
        hit.artists = group
            .iter()
            .filter_map(|r| run_text(r))
            .map(str::trim)
            .filter(|t| !t.is_empty() && !matches!(*t, "&" | "," | "and" | "x"))
            .map(str::to_string)
            .collect();
    }
    hit.plays = runs(2).iter().filter_map(|r| run_text(r)).find_map(parse_plays);
    hit.artwork_url = renderer
        .pointer("/thumbnail/musicThumbnailRenderer/thumbnail/thumbnails")
        .and_then(Value::as_array)
        .and_then(|t| t.last())
        .and_then(|t| t.get("url"))
        .and_then(Value::as_str)
        .map(large_thumbnail);
    Some(hit)
}

fn run_text(run: &Value) -> Option<&str> {
    run.get("text").and_then(Value::as_str)
}

fn page_type(run: &Value) -> Option<&str> {
    run.pointer("/navigationEndpoint/browseEndpoint/browseEndpointContextSupportedConfigs/browseEndpointContextMusicConfig/pageType")
        .and_then(Value::as_str)
}

/// "3:23" or "1:02:03" in milliseconds.
fn parse_duration(text: &str) -> Option<u64> {
    let parts: Vec<&str> = text.split(':').collect();
    if !(2..=3).contains(&parts.len()) || parts.iter().any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit())) {
        return None;
    }
    let seconds = parts.iter().try_fold(0u64, |acc, p| p.parse::<u64>().ok().map(|v| acc * 60 + v))?;
    Some(seconds * 1000)
}

/// "120M plays", "1.2K plays", "79 plays".
fn parse_plays(text: &str) -> Option<u64> {
    let number = text.split_whitespace().next()?;
    let (digits, factor) = match number.chars().last()? {
        'K' | 'k' => (&number[..number.len() - 1], 1e3),
        'M' | 'm' => (&number[..number.len() - 1], 1e6),
        'B' | 'b' => (&number[..number.len() - 1], 1e9),
        _ => (number, 1.0),
    };
    let value: f64 = digits.replace(',', "").parse().ok()?;
    (value.is_finite() && value >= 0.0).then_some((value * factor) as u64)
}

/// Google image URLs encode the size ("=w120-h120-l90-rj"); ask for 544 px.
fn large_thumbnail(url: &str) -> String {
    match url.rfind("=w") {
        Some(pos) if url[pos..].contains("-h") => {
            let rest = &url[pos + 2..];
            let options = rest.split_once('-').map(|(_, o)| o).unwrap_or_default();
            let options = options.split_once('-').map(|(_, o)| o).unwrap_or_default();
            if options.is_empty() {
                format!("{}=w544-h544", &url[..pos])
            } else {
                format!("{}=w544-h544-{options}", &url[..pos])
            }
        }
        _ => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(title: &str, video: &str, second: Value, plays: &str) -> Value {
        json!({"musicResponsiveListItemRenderer": {
            "thumbnail": {"musicThumbnailRenderer": {"thumbnail": {"thumbnails": [
                {"url": "https://yt3.googleusercontent.com/abc=w60-h60-l90-rj", "width": 60},
                {"url": "https://yt3.googleusercontent.com/abc=w120-h120-l90-rj", "width": 120}]}}},
            "flexColumns": [
                {"musicResponsiveListItemFlexColumnRenderer": {"text": {"runs": [
                    {"text": title, "navigationEndpoint": {"watchEndpoint": {"videoId": video}}}]}}},
                {"musicResponsiveListItemFlexColumnRenderer": {"text": {"runs": second}}},
                {"musicResponsiveListItemFlexColumnRenderer": {"text": {"runs": [{"text": plays}]}}}
            ],
            "playlistItemData": {"videoId": video}
        }})
    }

    fn artist(name: &str) -> Value {
        json!({"text": name, "navigationEndpoint": {"browseEndpoint": {"browseId": "UC1",
            "browseEndpointContextSupportedConfigs": {"browseEndpointContextMusicConfig": {"pageType": "MUSIC_PAGE_TYPE_ARTIST"}}}}})
    }

    fn album(name: &str) -> Value {
        json!({"text": name, "navigationEndpoint": {"browseEndpoint": {"browseId": "MPRE",
            "browseEndpointContextSupportedConfigs": {"browseEndpointContextMusicConfig": {"pageType": "MUSIC_PAGE_TYPE_ALBUM"}}}}})
    }

    fn body(items: Vec<Value>) -> String {
        json!({"contents": {"tabbedSearchResultsRenderer": {"tabs": [{"tabRenderer": {"content": {"sectionListRenderer": {
            "contents": [{"itemSectionRenderer": {"contents": []}}, {"musicShelfRenderer": {"contents": items}}]}}}}]}}})
        .to_string()
    }

    #[test]
    fn parses_song_rows() {
        let dot = json!({"text": " • "});
        let rows = vec![
            item(
                "Hold Me Closer",
                "-A1tuJSRBJQ",
                json!([artist("Elton John"), {"text": " & "}, artist("Britney Spears"), dot, album("The Lockdown Sessions"), dot, {"text": "3:23"}]),
                "120M plays",
            ),
            item("Tiny Dancer", "aqlGlaNlcWE", json!([{"text": "Elton John"}, dot, {"text": "6:18"}]), "355M plays"),
        ];
        let hits = parse(&body(rows)).unwrap_or_default();
        assert_eq!(hits.len(), 2);
        let h = &hits[0];
        assert_eq!(h.title, "Hold Me Closer");
        assert_eq!(h.artist, "Elton John & Britney Spears");
        assert_eq!(h.artists, ["Elton John", "Britney Spears"]);
        assert_eq!(h.album.as_deref(), Some("The Lockdown Sessions"));
        assert_eq!(h.duration_ms, Some(203_000));
        assert_eq!(h.id.as_deref(), Some("-A1tuJSRBJQ"));
        assert_eq!(h.url.as_deref(), Some("https://music.youtube.com/watch?v=-A1tuJSRBJQ"));
        assert_eq!(h.plays, Some(120_000_000));
        assert_eq!(h.artwork_url.as_deref(), Some("https://yt3.googleusercontent.com/abc=w544-h544-l90-rj"));
        assert_eq!(hits[1].artist, "Elton John");
        assert_eq!(hits[1].duration_ms, Some(378_000));
    }

    #[test]
    fn rejects_unexpected_bodies() {
        assert!(parse("{}").is_err());
        assert!(parse("[]").is_err());
        assert_eq!(parse(&body(vec![])).map(|h| h.len()), Ok(0));
    }

    #[test]
    fn client_version_uses_utc_date() {
        assert_eq!(client_version(0), "1.19700101.01.00");
        // 2026-10-13T20:00:00Z
        assert_eq!(client_version(1_791_921_600_000), "1.20261013.01.00");
        // 2024-02-29T12:00:00Z (leap day)
        assert_eq!(client_version(1_709_208_000_000), "1.20240229.01.00");
    }

    #[test]
    fn numbers() {
        assert_eq!(parse_duration("3:23"), Some(203_000));
        assert_eq!(parse_duration("1:02:03"), Some(3_723_000));
        assert_eq!(parse_duration("Elton John"), None);
        assert_eq!(parse_duration("3:"), None);
        assert_eq!(parse_plays("1.2K plays"), Some(1200));
        assert_eq!(parse_plays("1.4B plays"), Some(1_400_000_000));
        assert_eq!(parse_plays("79 plays"), Some(79));
        assert_eq!(parse_plays("plays"), None);
    }
}
