//! HTML helpers: meta tags, JSON-LD, inline JSON, media elements and iframes.
//!
//! Parsing is synchronous and returns owned data (`scraper::Html` is not `Send`,
//! so it never lives across an await).

use std::collections::HashMap;

use scraper::{Html, Selector};
use serde_json::Value;

/// What a page says about itself.
#[derive(Debug, Default, Clone)]
pub(crate) struct Document {
    /// `<meta property|name=… content=…>` pairs in document order, keys lowercased.
    pub meta: Vec<(String, String)>,
    /// JSON-LD nodes, flattened (`@graph` and arrays expanded).
    pub json_ld: Vec<Value>,
    /// Text of `<script id=…>` elements by id.
    pub scripts: HashMap<String, String>,
    /// `src` of `<audio>`, `<video>` and `<source>` elements.
    pub media_sources: Vec<String>,
    /// `src` (or lazy `data-src`) of iframes.
    pub iframes: Vec<String>,
    /// `<link rel="alternate" type="application/json+oembed">` target.
    pub oembed: Option<String>,
    /// `<title>` text.
    pub title: Option<String>,
}

fn selector(css: &str) -> Selector {
    // Selectors are compile-time constants covered by the tests below.
    Selector::parse(css).expect("valid selector")
}

impl Document {
    pub(crate) fn parse(html: &str) -> Self {
        let doc = Html::parse_document(html);
        let mut out = Document::default();
        for el in doc.select(&selector("meta[content]")) {
            let attrs = el.value();
            let Some(key) = attrs.attr("property").or_else(|| attrs.attr("name")).or_else(|| attrs.attr("itemprop"))
            else {
                continue;
            };
            let content = attrs.attr("content").unwrap_or_default().trim();
            if !content.is_empty() {
                out.meta.push((key.trim().to_ascii_lowercase(), content.to_string()));
            }
        }
        for el in doc.select(&selector("script")) {
            let attrs = el.value();
            let text: String = el.text().collect();
            if attrs.attr("type").is_some_and(|t| t.eq_ignore_ascii_case("application/ld+json"))
                && let Ok(value) = serde_json::from_str::<Value>(text.trim())
            {
                flatten_json_ld(value, &mut out.json_ld);
            }
            if let Some(id) = attrs.attr("id") {
                out.scripts.insert(id.to_string(), text);
            }
        }
        for el in doc.select(&selector("audio[src], video[src], source[src]")) {
            if let Some(src) = el.value().attr("src").map(str::trim).filter(|s| !s.is_empty()) {
                out.media_sources.push(src.to_string());
            }
        }
        for el in doc.select(&selector("iframe")) {
            let attrs = el.value();
            if let Some(src) =
                attrs.attr("src").or_else(|| attrs.attr("data-src")).map(str::trim).filter(|s| !s.is_empty())
            {
                out.iframes.push(src.to_string());
            }
        }
        out.oembed = doc
            .select(&selector("link[rel=alternate][href]"))
            .find(|el| el.value().attr("type").is_some_and(|t| t.eq_ignore_ascii_case("application/json+oembed")))
            .and_then(|el| el.value().attr("href"))
            .map(str::to_string);
        out.title = doc
            .select(&selector("title"))
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string())
            .filter(|t| !t.is_empty());
        out
    }

    /// First meta value for `key` (e.g. "og:title").
    pub(crate) fn meta(&self, key: &str) -> Option<&str> {
        self.meta.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    /// First meta value among `keys`, in order of preference.
    pub(crate) fn meta_any(&self, keys: &[&str]) -> Option<&str> {
        keys.iter().find_map(|k| self.meta(k))
    }

    /// All meta values for `key`.
    pub(crate) fn meta_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.meta.iter().filter(move |(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    /// JSON-LD nodes whose `@type` includes one of `types`.
    pub(crate) fn json_ld_of<'a>(&'a self, types: &'a [&'a str]) -> impl Iterator<Item = &'a Value> + 'a {
        self.json_ld.iter().filter(move |node| ld_types(node).iter().any(|t| types.contains(&t.as_str())))
    }

    /// Parsed JSON of `<script id=…>`.
    pub(crate) fn script_json(&self, id: &str) -> Option<Value> {
        self.scripts.get(id).and_then(|text| serde_json::from_str(text.trim()).ok())
    }
}

fn flatten_json_ld(value: Value, out: &mut Vec<Value>) {
    match value {
        Value::Array(items) => items.into_iter().for_each(|v| flatten_json_ld(v, out)),
        Value::Object(mut map) => {
            if let Some(graph) = map.remove("@graph") {
                flatten_json_ld(graph, out);
            }
            out.push(Value::Object(map));
        }
        _ => {}
    }
}

/// `@type` of a JSON-LD node (string or array), without a `schema:` prefix.
pub(crate) fn ld_types(node: &Value) -> Vec<String> {
    let strip = |s: &str| s.rsplit([':', '/']).next().unwrap_or(s).to_string();
    match node.get("@type") {
        Some(Value::String(s)) => vec![strip(s)],
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).map(strip).collect(),
        _ => Vec::new(),
    }
}

/// The JSON object that follows `marker` in raw HTML, e.g. `var ytInitialData = {…};`.
pub(crate) fn json_after(html: &str, marker: &str) -> Option<Value> {
    let start = html.find(marker)? + marker.len();
    let brace = start + html[start..].find('{')?;
    // Only whitespace may sit between the marker and the object.
    if !html[start..brace].trim().is_empty() {
        return None;
    }
    serde_json::Deserializer::from_str(&html[brace..]).into_iter::<Value>().next()?.ok()
}

/// Text of a JSON value that is either a string or a `{content|simpleText|text}` object,
/// or `{runs: [{text}]}`.
pub(crate) fn text_of(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(s) => s.clone(),
        Value::Object(map) => {
            if let Some(s) = ["content", "simpleText", "text"].iter().find_map(|k| map.get(*k).and_then(Value::as_str))
            {
                s.to_string()
            } else {
                let runs = map.get("runs").and_then(Value::as_array)?;
                runs.iter().filter_map(|r| r.get("text").and_then(Value::as_str)).collect()
            }
        }
        _ => return None,
    };
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

/// Non-empty trimmed string at a JSON pointer.
pub(crate) fn str_at(value: &Value, pointer: &str) -> Option<String> {
    value.pointer(pointer).and_then(Value::as_str).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string)
}

/// Number (or numeric string) at a JSON pointer.
pub(crate) fn u64_at(value: &Value, pointer: &str) -> Option<u64> {
    match value.pointer(pointer)? {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().filter(|f| *f >= 0.0).map(|f| f as u64)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// ISO 8601 duration (`PT3M33S`, `PT1H2M3.5S`, `P1DT1S`) in milliseconds. The
/// common malformed form without `T` (`P00H03M05S`) reads as a time.
pub(crate) fn parse_iso_duration(text: &str) -> Option<u64> {
    let rest = text.trim().strip_prefix('P')?;
    let (date, time) = match rest.split_once('T') {
        Some(parts) => parts,
        None if rest.contains(['H', 'S']) => ("", rest),
        None => (rest, ""),
    };
    let mut total = 0.0f64;
    let mut take = |part: &str, units: &[(char, f64)]| -> Option<()> {
        let mut number = String::new();
        for c in part.chars() {
            if c.is_ascii_digit() || c == '.' {
                number.push(c);
            } else {
                let factor = units.iter().find(|(u, _)| *u == c)?.1;
                total += number.parse::<f64>().ok()? * factor;
                number.clear();
            }
        }
        number.is_empty().then_some(())
    };
    take(date, &[('D', 86_400.0), ('W', 604_800.0)])?;
    take(time, &[('H', 3600.0), ('M', 60.0), ('S', 1.0)])?;
    (total > 0.0).then(|| (total * 1000.0).round() as u64)
}

/// Four-digit year at the start of a date string ("1987-11-12", "1987").
pub(crate) fn year_of(date: &str) -> Option<u16> {
    let digits: String = date.trim().chars().take(4).collect();
    digits.parse().ok().filter(|y| (1000..=2999).contains(y))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_meta_json_ld_and_elements() {
        let html = r#"<html><head><title> Page </title>
            <meta property="og:title" content="Song &amp; Co">
            <meta name="twitter:player:stream" content="https://cdn.example/a.mp4">
            <meta property="music:song" content="https://x/1"><meta property="music:song" content="https://x/2">
            <link rel="alternate" type="application/json+oembed" href="https://example.com/oembed?url=x">
            <script type="application/ld+json">{"@context":"https://schema.org","@graph":[{"@type":"MusicRecording","name":"A"},{"@type":["CreativeWork","schema:AudioObject"],"contentUrl":"https://cdn.example/a.mp3"}]}</script>
            <script id="data" type="application/json">{"a": {"b": 3}}</script>
            </head><body><audio src="/media/a.mp3"></audio><video><source src="b.webm"></video>
            <iframe data-src="https://www.youtube.com/embed/dQw4w9WgXcQ"></iframe></body></html>"#;
        let doc = Document::parse(html);
        assert_eq!(doc.title.as_deref(), Some("Page"));
        assert_eq!(doc.meta("og:title"), Some("Song & Co"));
        assert_eq!(doc.meta_all("music:song").count(), 2);
        assert_eq!(doc.meta_any(&["og:audio", "twitter:player:stream"]), Some("https://cdn.example/a.mp4"));
        assert_eq!(doc.json_ld_of(&["MusicRecording"]).count(), 1);
        assert_eq!(
            doc.json_ld_of(&["AudioObject"]).next().and_then(|n| n.get("contentUrl")).and_then(Value::as_str),
            Some("https://cdn.example/a.mp3")
        );
        assert_eq!(doc.script_json("data").and_then(|v| u64_at(&v, "/a/b")), Some(3));
        assert_eq!(doc.media_sources, vec!["/media/a.mp3", "b.webm"]);
        assert_eq!(doc.iframes, vec!["https://www.youtube.com/embed/dQw4w9WgXcQ"]);
        assert_eq!(doc.oembed.as_deref(), Some("https://example.com/oembed?url=x"));
    }

    #[test]
    fn tolerates_broken_html() {
        let doc = Document::parse(
            "<meta property=og:title content='x'><script type=application/ld+json>{not json</script><<<",
        );
        assert_eq!(doc.meta("og:title"), Some("x"));
        assert!(doc.json_ld.is_empty());
        assert!(Document::parse("").meta.is_empty());
    }

    #[test]
    fn extracts_inline_json() {
        let html = r#"<script>var ytInitialData = {"a":[1,{"b":"};"}]};var other = 1;</script>"#;
        assert_eq!(json_after(html, "var ytInitialData =").and_then(|v| str_at(&v, "/a/1/b")).as_deref(), Some("};"));
        assert!(json_after(html, "var missing =").is_none());
        assert!(json_after("var x = 1; {\"a\":1}", "var x =").is_none());
        assert!(json_after("var x = {broken", "var x =").is_none());
    }

    #[test]
    fn text_shapes() {
        assert_eq!(text_of(&serde_json::json!("  a ")).as_deref(), Some("a"));
        assert_eq!(text_of(&serde_json::json!({"content": "b"})).as_deref(), Some("b"));
        assert_eq!(text_of(&serde_json::json!({"simpleText": "c"})).as_deref(), Some("c"));
        assert_eq!(text_of(&serde_json::json!({"runs": [{"text": "d"}, {"text": "e"}]})).as_deref(), Some("de"));
        assert_eq!(text_of(&serde_json::json!({"x": 1})), None);
        assert_eq!(text_of(&serde_json::json!("  ")), None);
    }

    #[test]
    fn iso_durations() {
        assert_eq!(parse_iso_duration("PT3M33S"), Some(213_000));
        assert_eq!(parse_iso_duration("PT4M54S"), Some(294_000));
        assert_eq!(parse_iso_duration("PT1H2M3.5S"), Some(3_723_500));
        assert_eq!(parse_iso_duration("P1DT1S"), Some(86_401_000));
        assert_eq!(parse_iso_duration("P00H03M05S"), Some(185_000));
        assert_eq!(parse_iso_duration("P0D"), None);
        assert_eq!(parse_iso_duration("3:33"), None);
        assert_eq!(parse_iso_duration("PT3X"), None);
        assert_eq!(parse_iso_duration("PT3"), None);
    }

    #[test]
    fn years() {
        assert_eq!(year_of("1987-11-12T00:00:00Z"), Some(1987));
        assert_eq!(year_of("July 27, 1987"), None);
        assert_eq!(year_of(""), None);
    }
}
