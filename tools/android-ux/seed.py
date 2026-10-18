#!/usr/bin/env python3
"""Build a seed library for UI checks on an emulator, without any network or recognizer.

    python3 -I tools/android-ux/seed.py <out-dir>

Writes <out-dir>/musicfetch.sqlite3 (the engine's schema, read from
core/crates/mfcore/src/store/schema.rs), artwork PNGs and a quiet 30 s preview WAV. Artwork
and previews are referenced as files in the app's external files directory, where
push-seed.sh puts them. Lyrics are cached as made-up text or "none", so opening a result
never fetches lyrics. All songs are well-known public releases; the ISRCs are fake.
"""
import json
import math
import os
import random
import re
import sqlite3
import struct
import sys
import time
import uuid
import wave
import zlib

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_RS = os.path.join(ROOT, "core", "crates", "mfcore", "src", "store", "schema.rs")
DEVICE_DIR = "/sdcard/Android/data/app.musicfetch/files/ux"
DAY = 86_400_000

random.seed(7)


def schema_v1():
    text = open(SCHEMA_RS, encoding="utf-8").read()
    match = re.search(r'const V1: &str = "(.*?)";', text, re.S)
    if not match:
        sys.exit("V1 schema not found in schema.rs")
    return match.group(1)


def fold(text):
    out = re.sub(r"[’'ʼ]", "", (text or "").lower())
    return " ".join(re.sub(r"[^\w]+", " ", out).split())


def png(path, top, bottom, accent):
    """A 600 × 600 vertical gradient with a soft disc, as a PNG (no dependencies)."""
    w = h = 600
    rows = []
    cx, cy, r = w * 0.62, h * 0.4, w * 0.28
    for y in range(h):
        t = y / (h - 1)
        base = [int(top[i] + (bottom[i] - top[i]) * t) for i in range(3)]
        row = bytearray([0])
        for x in range(w):
            d = math.hypot(x - cx, y - cy)
            a = max(0.0, min(1.0, (r - d) / 40.0)) * 0.55
            px = [int(base[i] * (1 - a) + accent[i] * a) for i in range(3)]
            row += bytes(px)
        rows.append(bytes(row))
    raw = b"".join(rows)

    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    data = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
    data += chunk(b"IDAT", zlib.compress(raw, 6)) + chunk(b"IEND", b"")
    with open(path, "wb") as f:
        f.write(data)


def preview_wav(path, seconds=30, rate=22050):
    with wave.open(path, "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(rate)
        frames = bytearray()
        for n in range(seconds * rate):
            v = 0.05 * math.sin(2 * math.pi * 330 * n / rate) * (0.6 + 0.4 * math.sin(2 * math.pi * n / rate))
            frames += struct.pack("<h", int(v * 32767))
        f.writeframes(bytes(frames))


PALETTE = [
    ((236, 72, 88), (60, 16, 60), (255, 190, 90)),
    ((40, 120, 220), (10, 30, 70), (120, 220, 255)),
    ((250, 200, 60), (200, 70, 30), (255, 250, 200)),
    ((30, 160, 120), (10, 50, 50), (160, 255, 200)),
    ((120, 80, 220), (30, 20, 80), (250, 150, 255)),
    ((240, 120, 160), (90, 30, 90), (255, 220, 230)),
    ((70, 70, 80), (15, 15, 20), (230, 60, 60)),
    ((255, 140, 40), (120, 30, 10), (255, 230, 120)),
    ((90, 200, 230), (20, 60, 120), (255, 255, 255)),
    ((200, 40, 40), (40, 10, 10), (250, 200, 80)),
    ((150, 200, 60), (30, 70, 20), (240, 255, 160)),
    ((60, 60, 160), (10, 10, 40), (120, 255, 220)),
]

SONGS = [
    # title, artist, album, year
    ("Blinding Lights", "The Weeknd", "After Hours", 2020),
    ("Shape of You", "Ed Sheeran", "÷", 2017),
    ("Take On Me", "a-ha", "Hunting High and Low", 1985),
    ("Billie Jean", "Michael Jackson", "Thriller", 1982),
    ("Never Gonna Give You Up", "Rick Astley", "Whenever You Need Somebody", 1987),
    ("Shake It Off", "Taylor Swift", "1989", 2014),
    ("Viva La Vida", "Coldplay", "Viva la Vida or Death and All His Friends", 2008),
    ("99 Luftballons", "Nena", "Nena", 1983),
    ("Wake Me Up", "Avicii", "True", 2013),
    ("Levitating", "Dua Lipa", "Future Nostalgia", 2020),
    ("As It Was", "Harry Styles", "Harry's House", 2022),
    ("Midnight City", "M83", "Hurry Up, We're Dreaming", 2011),
]


def track(i, art=True, preview=False, title=None):
    t, artist, album, year = SONGS[i]
    t = title or t
    isrc = "ZZUX26%06d" % (i + 1)
    links = [
        {"service": "Spotify", "url": "https://open.spotify.com/search/" + t.replace(" ", "%20"), "is_search": True},
        {"service": "YouTubeMusic", "url": "https://music.youtube.com/search?q=" + t.replace(" ", "+"), "is_search": True},
    ]
    return {
        "title": t,
        "artist": artist,
        "artists": [artist],
        "album": album,
        "isrc": isrc,
        "duration_ms": 200_000 + i * 7_000,
        "release_year": year,
        "genre": None,
        "artwork_url": f"file://{DEVICE_DIR}/art{i:02d}.png" if art else None,
        "preview_url": f"file://{DEVICE_DIR}/preview.wav" if preview else None,
        "links": links,
        "ids": {},
        "identity_key": "isrc::" + isrc,
    }


def contour(seconds=11, frame_ms=50, seed=1):
    rnd = random.Random(seed)
    notes = [0, 0, 2, 4, 4, 2, 0, -1, -3, -3, -1, 0, 2, 0]
    frames = int(seconds * 1000 / frame_ms)
    per = frames // len(notes)
    ref, query = [], []
    for n in notes:
        for k in range(per):
            ref.append(float(n))
            gap = k > per - 3
            query.append(None if gap else round(n + rnd.uniform(-0.45, 0.45), 2))
    return ref, query


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    for i, (top, bottom, accent) in enumerate(PALETTE):
        png(os.path.join(out, f"art{i:02d}.png"), top, bottom, accent)
    preview_wav(os.path.join(out, "preview.wav"))
    db_path = os.path.join(out, "musicfetch.sqlite3")
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(db_path + suffix):
            os.remove(db_path + suffix)
    db = sqlite3.connect(db_path)
    db.executescript(schema_v1())
    db.execute("PRAGMA user_version = 1")
    now = int(time.time() * 1000)
    ids = {}

    def session(key, kind, status, at, title="", subtitle=None, art=None, trk=None, count=0, duration=None,
                url=None, source=None, error=None, pinned=False):
        sid = str(uuid.UUID(int=random.getrandbits(128)))
        ids[key] = sid
        search = fold(" ".join(x for x in [title, subtitle or "", (trk or {}).get("title", ""), (trk or {}).get("artist", "")] if x))
        db.execute(
            "INSERT INTO sessions (id, kind, status, created_at, updated_at, title, subtitle, artwork_url, track_json,"
            " track_count, duration_ms, source_url, source_json, error, pinned, search_text) VALUES"
            " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, kind, status, at, at, title, subtitle, art, json.dumps(trk) if trk else None, count, duration, url,
             json.dumps(source) if source else None, error, 1 if pinned else 0, search),
        )
        return sid

    def matched(key, i, kind, at, pinned=False, preview=False, title=None, art=True):
        t = track(i, art=art, preview=preview, title=title)
        sid = session(key, kind, "matched", at, t["title"], t["artist"], t["artwork_url"], t, 1, pinned=pinned)
        seg = {"id": "match", "item_index": 0, "start_ms": 0, "end_ms": 9000, "kind": "Track", "track": t, "confidence": 0.97,
               "tier": "Confirmed", "evidence": [{"source": "Shazam", "at_ms": 0, "score": 0.97, "detail": "offset 1:14, skew 0.1%"}],
               "alternates": [], "source_url": None}
        db.execute("INSERT INTO segments VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (sid, "match", 0, 0, 9000, "track", "confirmed", 0.97, t["identity_key"], fold(t["title"] + " " + t["artist"]), json.dumps(seg)))
        return sid, t

    lyrics = {"plain": "\n".join(
        ["City lights are calling out my name", "Running through the night again", "Every signal turning green",
         "Brighter than I've ever seen", "", "Hold on, hold on", "We're almost there", "Hold on, hold on",
         "Static in the air", "", "Streets are humming like a song", "Feels like I've been here all along",
         "Every window, every door", "Takes me back to you once more"] * 2), "synced": []}

    # Today
    sid, t = matched("blinding", 0, "system_audio", now - 12 * 60_000, pinned=True, preview=True)
    db.execute("INSERT INTO cache VALUES (?,?,?,?)", ("lyrics", t["identity_key"], json.dumps(lyrics), now + 365 * DAY))
    for key, i, kind, ago in [("shape", 1, "listen", 40), ("billie", 3, "listen", 95), ("levitating", 9, "system_audio", 150)]:
        _, t = matched(key, i, kind, now - ago * 60_000)
        db.execute("INSERT INTO cache VALUES (?,?,?,?)", ("lyrics", t["identity_key"], "null", now + 365 * DAY))
    # A hum with candidates.
    ref, query = contour()
    cands = []
    for rank, (i, score, tier) in enumerate([(2, 0.82, "Strong"), (11, 0.46, "Possible"), (8, 0.21, "LongShot")]):
        t = track(i)
        cands.append((t, score, tier))
    hum = session("hum", "hum", "candidates", now - 25 * 60_000, cands[0][0]["title"], cands[0][0]["artist"], cands[0][0]["artwork_url"])
    for rank, (t, score, tier) in enumerate(cands):
        cand = {"track": t, "score": score, "tier": tier,
                "sources": [{"kind": "Melody", "score": score, "detail": "transposed −2, tempo ×0.9" if rank == 0 else None}],
                "alignment": {"frame_ms": 50, "query": query, "reference": [r + (0 if rank == 0 else rank) for r in ref]}}
        db.execute("INSERT INTO candidates VALUES (?,?,?,?,?,?)",
                   (hum, rank, score, {"Strong": "strong", "Possible": "possible", "LongShot": "long_shot"}[tier],
                    fold(t["title"] + " " + t["artist"]), json.dumps(cand)))
    # Nothing found, and one saved offline.
    session("nomatch", "listen", "no_match", now - 55 * 60_000)
    session("waiting", "listen", "waiting", now - 70 * 60_000)

    # Yesterday
    matched("takeonme", 2, "hum", now - DAY - 2 * 3600_000)
    matched("nggyu", 4, "listen", now - DAY - 3 * 3600_000, pinned=True)
    matched("long", 6, "listen", now - DAY - 5 * 3600_000, art=False,
            title="Viva La Vida (Live from Paris, with the Orchestra and the Stadium Choir) – Remastered")
    # A link with a position inside long media.
    t = track(10)
    link = session("link", "link", "completed", now - DAY - 6 * 3600_000, "Summer Sessions · Live Set", "Concert Hall",
                   t["artwork_url"], None, 1, 5_400_000, "https://www.example.org/watch/summer-sessions",
                   {"url": "https://www.example.org/watch/summer-sessions", "title": "Summer Sessions · Live Set",
                    "uploader": "Concert Hall", "thumbnail_url": t["artwork_url"], "duration_ms": 1_000_000, "service": None,
                    "site": "example.org", "item_count": 1})
    seg = {"id": "s1", "item_index": 0, "start_ms": 754_000, "end_ms": 950_000, "kind": "Track", "track": t, "confidence": 0.93,
           "tier": "Confirmed", "evidence": [{"source": "Shazam", "at_ms": 760_000, "score": 0.93, "detail": None}], "alternates": [], "source_url": None}
    db.execute("INSERT INTO segments VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               (link, "s1", 0, 754_000, 950_000, "track", "confirmed", 0.93, t["identity_key"], fold(t["title"]), json.dumps(seg)))

    # Earlier: a mix with a timeline, a playlist and a failure.
    mix = session("mix", "link", "completed", now - 3 * DAY, "Late Night Mix · Vol. 3", "Night Shift Radio", f"file://{DEVICE_DIR}/art11.png",
                  None, 7, 3_600_000, "https://www.example.org/mix/late-night-3",
                  {"url": "https://www.example.org/mix/late-night-3", "title": "Late Night Mix · Vol. 3", "uploader": "Night Shift Radio",
                   "thumbnail_url": f"file://{DEVICE_DIR}/art11.png", "duration_ms": 3_600_000, "service": None, "site": "example.org",
                   "item_count": 1}, pinned=True)
    start = 0
    for n, (i, tier) in enumerate([(11, "Confirmed"), (0, "Confirmed"), (None, "Unknown"), (9, "Likely"), (5, "Possible"),
                                   (1, "Confirmed"), (None, "Unknown"), (7, "Confirmed"), (3, "Likely")]):
        length = 380_000 + (n % 3) * 60_000
        t = track(i) if i is not None else None
        kind = "Track" if t else "Unidentified"
        conf = {"Confirmed": 0.96, "Likely": 0.8, "Possible": 0.55, "Unknown": 0.0}[tier]
        seg = {"id": f"m{n}", "item_index": 0, "start_ms": start, "end_ms": start + length, "kind": kind, "track": t, "confidence": conf,
               "tier": tier, "evidence": [{"source": "Shazam", "at_ms": start + 30_000, "score": conf, "detail": None}] if t else [],
               "alternates": [], "source_url": None}
        db.execute("INSERT INTO segments VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (mix, f"m{n}", 0, start, start + length, kind.lower(), tier.lower(), conf, t["identity_key"] if t else None,
                    fold(t["title"] + " " + t["artist"]) if t else "", json.dumps(seg)))
        start += length
    playlist = session("playlist", "link", "completed", now - 4 * DAY, "Road Trip Classics", "Spotify", f"file://{DEVICE_DIR}/art07.png",
                       None, 5, None, "https://open.spotify.com/playlist/0000000000000000000000",
                       {"url": "https://open.spotify.com/playlist/0000000000000000000000", "title": "Road Trip Classics", "uploader": "Spotify",
                        "thumbnail_url": f"file://{DEVICE_DIR}/art07.png", "duration_ms": None, "service": "Spotify", "site": "Spotify",
                        "item_count": 5})
    for n, i in enumerate([4, 3, 7, 5, 2]):
        t = track(i)
        seg = {"id": f"p{n}", "item_index": n, "start_ms": 0, "end_ms": 0, "kind": "Track", "track": t, "confidence": 1.0,
               "tier": "Confirmed", "evidence": [{"source": "Metadata", "at_ms": 0, "score": 1.0, "detail": None}], "alternates": [], "source_url": None}
        db.execute("INSERT INTO segments VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (playlist, f"p{n}", n, 0, 0, "track", "confirmed", 1.0, t["identity_key"], fold(t["title"]), json.dumps(seg)))
    session("failed", "link", "failed", now - 5 * DAY, "Concert clip", None, None, None, 0, None, "https://www.example.org/clip/42",
            error="This video isn't available.")
    matched("shakeitoff", 5, "listen", now - 6 * DAY)
    matched("luftballons", 7, "hum", now - 6 * DAY - 3600_000)
    matched("wakemeup", 8, "system_audio", now - 9 * DAY)
    db.execute("UPDATE sessions SET search_tracks = '' ")
    db.commit()
    db.close()
    with open(os.path.join(out, "ids.json"), "w") as f:
        json.dump(ids, f, indent=1)
    print(db_path)


if __name__ == "__main__":
    main()
