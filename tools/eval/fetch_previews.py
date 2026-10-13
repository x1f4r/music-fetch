"""Fetch a small, varied set of Deezer 30 s previews as 16 kHz mono WAVs.

Usage: python fetch_previews.py <out dir> <app version>

Used to calibrate the impression detector against recorded music. Searches the
public Deezer API (no account) for a fixed list of styles, takes the first track
with a preview, and decodes it with ffmpeg. Requests are spaced out and carry
only the neutral MusicFetch user agent. Keep the output out of the repository.
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

QUERIES = [
    "Pentatonix", "Home Free a cappella", "Bach cello suite", "Chopin nocturne", "Ludovico Einaudi", "Hans Zimmer Time",
    "Vivaldi four seasons", "Beethoven symphony 5", "Daft Punk", "Avicii Levels", "Metallica", "AC/DC",
    "Miles Davis So What", "Norah Jones", "Bob Dylan", "Simon Garfunkel", "Adele Hello", "Billie Eilish",
    "Kendrick Lamar", "Eminem", "Bad Bunny", "BTS", "Ed Sheeran", "Taylor Swift", "The Weeknd", "Queen Bohemian",
    "Michael Jackson Billie Jean", "Shakira", "Coldplay", "Sia Chandelier", "Imagine Dragons", "Enya",
]


def get(url, ua):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read()


def main(out, version):
    os.makedirs(out, exist_ok=True)
    ua = f"MusicFetch/{version} (+https://github.com/x1f4r/music-fetch)"
    manifest = []
    for q in QUERIES:
        time.sleep(0.4)
        data = json.loads(get("https://api.deezer.com/search?" + urllib.parse.urlencode({"q": q, "limit": 5}), ua))
        track = next((t for t in data.get("data", []) if t.get("preview")), None)
        if not track:
            print("no preview for", q, file=sys.stderr)
            continue
        name = f"dz{track['id']}"
        mp3 = os.path.join(out, name + ".mp3")
        wav = os.path.join(out, name + ".wav")
        time.sleep(0.4)
        with open(mp3, "wb") as f:
            f.write(get(track["preview"], ua))
        subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", mp3, "-ac", "1", "-ar", "16000", "-sample_fmt", "s16", wav], check=True)
        os.remove(mp3)
        manifest.append({"query": q, "id": track["id"], "artist": track["artist"]["name"], "title": track["title"], "wav": wav})
        print(q, "->", track["artist"]["name"], "-", track["title"])
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
