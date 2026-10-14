"""Stage 1: the song universe.

Sources: Billboard Hot 100 history, kworb Spotify totals (global, all-time and
per country), Apple Music most-played RSS per storefront, Deezer charts and the
Deezer "Top <country>" playlists. Entries are merged by normalized artist/title
and ranked by a monotone combination of chart signals.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from . import db as dbm
from .config import Paths
from .net import DeezerClient, DeezerError, PoliteFetcher
from .normalize import match_keys, song_key

log = logging.getLogger("mfidx.universe")

KWORB_COUNTRIES = [
    "us",
    "gb",
    "de",
    "fr",
    "es",
    "it",
    "br",
    "mx",
    "jp",
    "kr",
    "in",
    "nl",
    "se",
    "au",
    "ca",
    "ph",
    "id",
    "ar",
    "pl",
    "tr",
    "co",
]
APPLE_STOREFRONTS = [
    "us",
    "gb",
    "de",
    "fr",
    "es",
    "it",
    "br",
    "mx",
    "jp",
    "kr",
    "in",
    "ca",
    "au",
    "nl",
    "se",
    "no",
    "dk",
    "fi",
    "pl",
    "tr",
    "ar",
    "co",
    "cl",
    "ph",
    "id",
    "th",
    "za",
    "ng",
    "at",
    "ch",
    "be",
    "ie",
    "pt",
    "nz",
    "sa",
    "ae",
    "tw",
    "hk",
    "vn",
]
DEEZER_CHARTS_USER = 637006841
DEEZER_SKIP_GENRES = {457}  # audiobooks

# Weights of the noisy-or popularity combination (each component is 0..1).
W_BILLBOARD = 0.95
W_SPOTIFY_GLOBAL = 1.0
W_SPOTIFY_COUNTRY = 0.7
W_APPLE = 0.5
W_DEEZER = 0.4
W_BREADTH = 0.3


@dataclass
class ChartEntry:
    source: str
    chart: str
    artist: str
    title: str
    rank: int | None = None
    peak: int | None = None
    weeks: int | None = None
    streams: int | None = None
    points: float | None = None
    first_date: str | None = None
    last_date: str | None = None
    year: int | None = None
    deezer_id: int | None = None


@dataclass
class Song:
    key: str
    title: str
    artist: str
    year: int | None
    score: float
    signals: dict[str, Any] = field(default_factory=dict)
    aliases: list[list[str]] = field(default_factory=list)
    deezer_hint: int | None = None
    popularity: float = 0.0


# --------------------------------------------------------------------------- sources


def load_billboard(path: Path) -> list[ChartEntry]:
    charts = json.loads(path.read_text())
    agg: dict[tuple[str, str], ChartEntry] = {}
    for chart in charts:
        date = chart.get("date")
        for row in chart.get("data") or []:
            artist, title, pos = row.get("artist"), row.get("song"), row.get("this_week")
            if not artist or not title or not isinstance(pos, int):
                continue
            e = agg.get((artist, title))
            if e is None:
                e = ChartEntry(
                    "billboard",
                    "hot100",
                    artist,
                    title,
                    peak=pos,
                    weeks=0,
                    points=0.0,
                    first_date=date,
                    last_date=date,
                )
                agg[(artist, title)] = e
            e.weeks = (e.weeks or 0) + 1
            e.points = (e.points or 0.0) + (101 - pos)
            e.peak = min(e.peak or pos, pos)
            if date and (e.first_date is None or date < e.first_date):
                e.first_date = date
            if date and (e.last_date is None or date > e.last_date):
                e.last_date = date
    for e in agg.values():
        e.year = int(e.first_date[:4]) if e.first_date else None
    log.info("billboard: %d charts, %d distinct entries", len(charts), len(agg))
    return list(agg.values())


def _int(text: str) -> int | None:
    t = text.replace(",", "").strip()
    return int(t) if t.isdigit() else None


def parse_kworb(html: str, chart: str) -> list[ChartEntry]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        return []
    heads = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    out: list[ChartEntry] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        div = tds[0].find("div") or tds[0]
        full = div.get_text("", strip=False).strip()
        track_a = next((a for a in div.find_all("a") if "/track/" in (a.get("href") or "")), None)
        if track_a is not None:
            title = track_a.get_text().strip()
            cut = full.rfind(" - " + title)
            artist = full[:cut].strip() if cut > 0 else full.split(" - ", 1)[0].strip()
        else:
            if " - " not in full:
                continue
            artist, title = (s.strip() for s in full.split(" - ", 1))
        if not artist or not title:
            continue
        e = ChartEntry("kworb", chart, artist, title)
        cells = [td.get_text(strip=True) for td in tds]
        if "wks" in heads:
            e.weeks = _int(cells[1])
            e.peak = _int(cells[3]) if len(cells) > 3 else None
            e.streams = _int(cells[-1])
        else:
            e.streams = _int(cells[1])
        if e.streams:
            out.append(e)
    return out


def _cached(cache: Path, name: str, max_age_days: float, refresh: bool) -> Path | None:
    p = cache / name
    if not refresh and p.exists() and time.time() - p.stat().st_mtime < max_age_days * 86400:
        return p
    return None


def fetch_kworb(paths: Paths, refresh: bool) -> list[ChartEntry]:
    cache = paths.cache / "kworb"
    cache.mkdir(parents=True, exist_ok=True)
    fetcher = PoliteFetcher(min_interval=2.0)
    pages = [
        ("alltime", "https://kworb.net/spotify/songs.html"),
        ("global", "https://kworb.net/spotify/country/global_weekly_totals.html"),
    ]
    pages += [
        (cc, f"https://kworb.net/spotify/country/{cc}_weekly_totals.html") for cc in KWORB_COUNTRIES
    ]
    out: list[ChartEntry] = []
    for chart, url in pages:
        name = f"{chart}.html"
        p = _cached(cache, name, 7, refresh)
        if p is None:
            try:
                r = fetcher.get(url)
            except Exception as e:  # noqa: BLE001 - one page failing must not stop the stage
                log.warning("kworb %s: %s", chart, e)
                continue
            if r.status_code != 200:
                log.warning("kworb %s: HTTP %d", chart, r.status_code)
                continue
            p = cache / name
            p.write_bytes(r.content)
        rows = parse_kworb(p.read_text(encoding="utf-8", errors="replace"), chart)
        log.info("kworb %s: %d rows", chart, len(rows))
        out.extend(rows)
    return out


def fetch_apple(paths: Paths, refresh: bool) -> list[ChartEntry]:
    cache = paths.cache / "apple"
    cache.mkdir(parents=True, exist_ok=True)
    fetcher = PoliteFetcher(min_interval=1.0)
    out: list[ChartEntry] = []
    for cc in APPLE_STOREFRONTS:
        name = f"{cc}.json"
        p = _cached(cache, name, 3, refresh)
        if p is None:
            url = (
                f"https://rss.marketingtools.apple.com/api/v2/{cc}/music/most-played/100/songs.json"
            )
            try:
                r = fetcher.get(url)
            except Exception as e:  # noqa: BLE001
                log.warning("apple %s: %s", cc, e)
                continue
            if r.status_code != 200:
                log.warning("apple %s: HTTP %d", cc, r.status_code)
                continue
            p = cache / name
            p.write_bytes(r.content)
        try:
            results = json.loads(p.read_text())["feed"]["results"]
        except (ValueError, KeyError, TypeError):
            log.warning("apple %s: unexpected payload", cc)
            continue
        for i, item in enumerate(results, 1):
            artist, title = item.get("artistName"), item.get("name")
            if not artist or not title:
                continue
            rd = item.get("releaseDate") or ""
            year = int(rd[:4]) if rd[:4].isdigit() else None
            out.append(ChartEntry("apple", cc, artist, title, rank=i, year=year))
    log.info("apple: %d rows from %d storefronts", len(out), len(APPLE_STOREFRONTS))
    return out


def _deezer_rows(items: Iterable[dict[str, Any]], chart: str) -> list[ChartEntry]:
    out = []
    for i, t in enumerate(items, 1):
        if not isinstance(t, dict):
            continue
        artist = (t.get("artist") or {}).get("name")
        title = t.get("title")
        if artist and title:
            out.append(ChartEntry("deezer", chart, artist, title, rank=i, deezer_id=t.get("id")))
    return out


def fetch_deezer(paths: Paths, refresh: bool, client: DeezerClient) -> list[ChartEntry]:
    cache = paths.cache / "deezer"
    cache.mkdir(parents=True, exist_ok=True)

    def cached_json(
        name: str, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        p = _cached(cache, name, 3, refresh)
        if p is not None:
            return json.loads(p.read_text())
        try:
            data = client.get(path, params)
        except DeezerError as e:
            log.warning("deezer %s: %s", path, e)
            return None
        (cache / name).write_text(json.dumps(data))
        return data

    out: list[ChartEntry] = []
    data = cached_json("chart0.json", "/chart/0/tracks", {"limit": 300})
    if data:
        out += _deezer_rows(data.get("data") or [], "chart:0")
    editorial = cached_json("editorial.json", "/editorial") or {}
    for ed in editorial.get("data") or []:
        gid = ed.get("id")
        if not isinstance(gid, int) or gid == 0 or gid in DEEZER_SKIP_GENRES:
            continue
        data = cached_json(f"chart{gid}.json", f"/chart/{gid}/tracks", {"limit": 100})
        if data:
            out += _deezer_rows(data.get("data") or [], f"chart:{gid}")
    pls = (
        cached_json("charts-user.json", f"/user/{DEEZER_CHARTS_USER}/playlists", {"limit": 300})
        or {}
    )
    for pl in pls.get("data") or []:
        title, pid = pl.get("title") or "", pl.get("id")
        if not title.startswith("Top ") or not isinstance(pid, int):
            continue
        items: list[dict[str, Any]] = []
        index = 0
        while True:
            page = cached_json(
                f"pl{pid}-{index}.json", f"/playlist/{pid}/tracks", {"limit": 100, "index": index}
            )
            if not page:
                break
            batch = page.get("data") or []
            items += batch
            index += len(batch)
            if not batch or index >= int(page.get("total") or 0):
                break
        out += _deezer_rows(items, f"pl:{title}")
    log.info("deezer: %d rows", len(out))
    return out


# ----------------------------------------------------------------------- popularity


def _clip01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def recency(year: int | None) -> float:
    """0.6 for 1958, rising linearly to 1.0 for the current year."""
    if year is None:
        return 0.8
    return 0.6 + 0.4 * _clip01((year - 1958) / (time.gmtime().tm_year - 1958))


def components(entries: list[ChartEntry], country_max: dict[str, int]) -> list[tuple[float, float]]:
    """(weight, strength) pairs; each strength is monotone in its chart signal.

    Per-market charts contribute their best position plus a breadth term (how many
    markets), so a song charting in 20 countries does not saturate the score.
    """
    comps: list[tuple[float, float]] = []
    bb = [e for e in entries if e.source == "billboard"]
    if bb:
        points = sum(e.points or 0 for e in bb)
        last = max(int(e.last_date[:4]) for e in bb if e.last_date)
        s = _clip01(math.log1p(points) / math.log1p(6000)) * recency(last)
        comps.append((W_BILLBOARD, s))
    sp_global = max(
        (
            e.streams or 0
            for e in entries
            if e.source == "kworb" and e.chart in ("global", "alltime")
        ),
        default=0,
    )
    if sp_global > 0:
        comps.append(
            (W_SPOTIFY_GLOBAL, _clip01((math.log10(sp_global) - 7.0) / (math.log10(4e9) - 7.0)))
        )
    best_country: dict[str, int] = {}
    for e in entries:
        if e.source == "kworb" and e.chart not in ("global", "alltime") and e.streams:
            best_country[e.chart] = max(best_country.get(e.chart, 0), e.streams)
    if best_country:
        strengths = []
        for cc, streams in best_country.items():
            top = max(country_max.get(cc, streams), 10**6 + 1)
            strengths.append(_clip01((math.log10(streams) - 6.0) / (math.log10(top) - 6.0)))
        comps.append((W_SPOTIFY_COUNTRY, max(strengths)))
        comps.append((W_BREADTH, min(1.0, len(best_country) / 12.0)))
    for source, weight, markets in (("apple", W_APPLE, 20.0), ("deezer", W_DEEZER, 30.0)):
        best: dict[str, int] = {}
        for e in entries:
            if e.source == source and e.rank:
                best[e.chart] = min(best.get(e.chart, 999), e.rank)
        if best:
            comps.append((weight, max(_clip01(1.0 - (r - 1) / 150.0) for r in best.values())))
            comps.append((W_BREADTH, min(1.0, len(best) / markets)))
    return comps


def combine(comps: list[tuple[float, float]]) -> float:
    """Noisy-or: 1 - prod(1 - w*s). Monotone increasing in every component."""
    p = 1.0
    for w, s in comps:
        p *= 1.0 - w * s
    return 1.0 - p


def percentiles(scores: list[float]) -> list[float]:
    """Rank-based popularity in (0, 1]; ties share the mean rank."""
    n = len(scores)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: scores[i])
    out = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and scores[order[j + 1]] == scores[order[i]]:
            j += 1
        mean_rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            out[order[k]] = mean_rank / n
        i = j + 1
    return out


# -------------------------------------------------------------------------- merging


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


_SOURCE_PRIORITY = {"billboard": 0, "kworb": 1, "apple": 2, "deezer": 3}


def _canonical(entries: list[ChartEntry]) -> ChartEntry:
    def rank(e: ChartEntry) -> tuple:
        return (
            _SOURCE_PRIORITY[e.source],
            e.chart not in ("hot100", "global", "alltime"),
            -(e.points or 0),
            -(e.streams or 0),
            e.rank or 999,
        )

    return min(entries, key=rank)


def merge(entries: list[ChartEntry]) -> list[Song]:
    """Group entries that are the same song and score each group."""
    uf = _UnionFind(len(entries))
    buckets: dict[tuple[str, str, str], int] = {}
    for i, e in enumerate(entries):
        for k in match_keys(e.artist, e.title):
            j = buckets.setdefault(k, i)
            if j != i:
                uf.union(i, j)
    groups: dict[int, list[ChartEntry]] = defaultdict(list)
    for i, e in enumerate(entries):
        groups[uf.find(i)].append(e)

    by_key: dict[str, list[ChartEntry]] = defaultdict(list)
    for members in groups.values():
        c = _canonical(members)
        key = song_key(c.artist, c.title)
        if key.startswith("::") or key.endswith("::"):
            continue
        by_key[key].extend(members)

    country_max: dict[str, int] = defaultdict(int)
    for e in entries:
        if e.source == "kworb" and e.streams:
            country_max[e.chart] = max(country_max[e.chart], e.streams)

    songs: list[Song] = []
    for key, members in by_key.items():
        c = _canonical(members)
        years = [e.year for e in members if e.source == "billboard" and e.year]
        year = min(years) if years else min((e.year for e in members if e.year), default=None)
        sig: dict[str, Any] = {}
        bb = [e for e in members if e.source == "billboard"]
        if bb:
            sig["bb"] = {
                "points": round(sum(e.points or 0 for e in bb), 1),
                "weeks": sum(e.weeks or 0 for e in bb),
                "peak": min(e.peak or 100 for e in bb),
                "first": min(e.first_date or "9999" for e in bb),
                "last": max(e.last_date or "" for e in bb),
            }
        sp = {e.chart: e.streams for e in members if e.source == "kworb"}
        if sp:
            sig["sp"] = sp
        am = {e.chart: e.rank for e in members if e.source == "apple"}
        if am:
            sig["am"] = am
        dz = {e.chart: e.rank for e in members if e.source == "deezer"}
        if dz:
            sig["dz"] = dz
        aliases = sorted({(e.artist, e.title) for e in members})
        hint = next(
            (e.deezer_id for e in sorted(members, key=lambda e: e.rank or 999) if e.deezer_id), None
        )
        songs.append(
            Song(
                key=key,
                title=c.title,
                artist=c.artist,
                year=year,
                score=combine(components(members, country_max)),
                signals=sig,
                aliases=[list(a) for a in aliases[:12]],
                deezer_hint=hint,
            )
        )
    return songs


# ---------------------------------------------------------------------------- stage


def run(paths: Paths, refresh: bool = False, max_songs: int = 60000) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    dbm.stage_started(conn, "universe")
    client = DeezerClient(paths.state)
    entries = load_billboard(paths.billboard)
    entries += fetch_kworb(paths, refresh)
    entries += fetch_apple(paths, refresh)
    entries += fetch_deezer(paths, refresh, client)

    with dbm.Tx(conn):
        conn.execute("DELETE FROM chart_entries")
        conn.executemany(
            "INSERT OR REPLACE INTO chart_entries(source, chart, artist, title, rank, peak, weeks, streams,"
            " points, first_date, last_date, year, deezer_id) VALUES (:source, :chart, :artist, :title, :rank,"
            " :peak, :weeks, :streams, :points, :first_date, :last_date, :year, :deezer_id)",
            [asdict(e) for e in entries],
        )

    songs = merge(entries)
    songs.sort(key=lambda s: s.score, reverse=True)
    kept = songs[:max_songs]
    for s, p in zip(kept, percentiles([s.score for s in kept]), strict=True):
        s.popularity = round(p, 4)

    with dbm.Tx(conn):
        conn.execute("UPDATE songs SET in_universe=0 WHERE origin='chart'")
        conn.executemany(
            "INSERT INTO songs(key, title, artist, year, score, popularity, signals, aliases, deezer_hint,"
            " origin, in_universe) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'chart', 1)"
            " ON CONFLICT(key) DO UPDATE SET title=excluded.title, artist=excluded.artist, year=excluded.year,"
            " score=excluded.score, popularity=excluded.popularity, signals=excluded.signals,"
            " aliases=excluded.aliases, deezer_hint=excluded.deezer_hint, origin='chart', in_universe=1",
            [
                (
                    s.key,
                    s.title,
                    s.artist,
                    s.year,
                    s.score,
                    s.popularity,
                    json.dumps(s.signals, ensure_ascii=False),
                    json.dumps(s.aliases, ensure_ascii=False),
                    s.deezer_hint,
                )
                for s in kept
            ],
        )
    by_source: dict[str, int] = defaultdict(int)
    for e in entries:
        by_source[e.source] += 1
    info = {
        "entries": len(entries),
        "by_source": dict(by_source),
        "merged": len(songs),
        "kept": len(kept),
        "min_score_kept": kept[-1].score if kept else None,
    }
    dbm.stage_finished(conn, "universe", info)
    log.info("universe: %s", info)
    return info
