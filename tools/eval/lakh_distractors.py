"""Distractor melodies from the Lakh MIDI Dataset's clean_midi subset.

Usage: python lakh_distractors.py <clean_midi dir> <out.jsonl> [max songs] [workers]

clean_midi is laid out as <artist>/<title>[.N].mid. Each file's most melody-like
track (see midi_melody.py) becomes one "midi" phrase; files without a plausible
melody track are skipped, and only the first file per normalized artist/title is
kept. Titles keep the dataset's artist and title so they never collide with the
QBSH ground truth ("qbsh-…", "ioacas-…", "thinkit-…").
"""

import json
import os
import re
import sys
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from midi_melody import melody_notes  # noqa: E402


def work(path):
    try:
        notes = melody_notes(path, pick_track=True)
    except Exception:
        return None
    if len(notes) < 40:
        return None
    artist = os.path.basename(os.path.dirname(path))
    title = re.sub(r"\.\d+$", "", os.path.splitext(os.path.basename(path))[0])
    return {
        "title": title,
        "artist": artist,
        "isrc": None,
        "deezer_id": None,
        "year": None,
        "popularity": 0.5,
        "phrases": [{"source": "midi", "start_ms": 0, "notes": notes}],
    }


def main(root, out, limit, workers):
    files = []
    for dirpath, _, names in os.walk(root):
        for n in sorted(names):
            if n.lower().endswith(".mid"):
                files.append(os.path.join(dirpath, n))
    files.sort()
    seen = set()
    kept = 0
    with Pool(workers) as pool, open(out, "w") as w:
        for song in pool.imap(work, files, chunksize=16):
            if song is None:
                continue
            key = (song["artist"].lower().strip(), song["title"].lower().strip())
            if key in seen:
                continue
            seen.add(key)
            w.write(json.dumps(song, ensure_ascii=False) + "\n")
            kept += 1
            if kept >= limit:
                pool.terminate()
                break
    print(f"{kept} distractor songs from {len(files)} files")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 1_000_000, int(sys.argv[4]) if len(sys.argv) > 4 else 8)
