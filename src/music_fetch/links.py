from __future__ import annotations

from urllib.parse import quote, quote_plus, unquote


def build_search_links(title: str, artist: str | None = None) -> dict[str, str]:
    query = " ".join(part for part in [artist, title] if part).strip()
    if not query:
        return {}
    quoted = quote(query)
    plus_quoted = quote_plus(query)
    return {
        "spotify": f"https://open.spotify.com/search/{quoted}",
        "youtube_music": f"https://music.youtube.com/search?q={plus_quoted}",
        "deezer": f"https://www.deezer.com/search/{quoted}",
        "apple_music": f"https://music.apple.com/us/search?term={plus_quoted}",
    }


def provider_search_links_from_shazam(track: dict) -> dict[str, str]:
    links: dict[str, str] = {}
    title = track.get("title") or ""
    artist = track.get("subtitle")
    links.update(build_search_links(title, artist))

    hub = track.get("hub") or {}
    for provider in hub.get("providers") or []:
        provider_type = (provider.get("type") or "").upper()
        for action in provider.get("actions") or []:
            uri = action.get("uri") or ""
            if not uri:
                continue
            if provider_type == "YOUTUBEMUSIC":
                links["youtube_music"] = normalize_uri(uri, links.get("youtube_music", ""))
            elif provider_type == "SPOTIFY":
                links["spotify"] = normalize_uri(uri, links.get("spotify", ""))
            elif provider_type == "DEEZER":
                links["deezer"] = normalize_uri(uri, links.get("deezer", ""))

    share = track.get("share") or {}
    if share.get("href"):
        links["shazam"] = share["href"]
    return {key: value for key, value in links.items() if value}


def normalize_uri(uri: str, fallback: str) -> str:
    if uri.startswith("https://"):
        return uri
    if uri.startswith("spotify:search:"):
        query = uri.removeprefix("spotify:search:")
        return f"https://open.spotify.com/search/{quote(unquote(query))}"
    if uri.startswith("deezer-query://"):
        query = extract_between(uri, "track%3A%27", "%27")
        artist = extract_between(uri, "artist%3A%27", "%27")
        return build_search_links(unquote(query or ""), unquote(artist or "")).get("deezer", fallback)
    return fallback


def extract_between(value: str, start: str, end: str) -> str | None:
    if start not in value:
        return None
    tail = value.split(start, 1)[1]
    if end not in tail:
        return tail
    return tail.split(end, 1)[0]
