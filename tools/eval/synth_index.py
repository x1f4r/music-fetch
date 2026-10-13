"""Synthetic preview-only melody index input, for size and latency tests at scale.

Usage: python synth_index.py <lakh.jsonl> <truth.jsonl> <out.jsonl> [songs]

Fills the index up to `songs` (default 50,000) with 30 s windows of the Lakh
distractor melodies, each randomly transposed (±5 semitones) and time-scaled
(0.85–1.15×) so repeats of a source song are distinct, as single "preview"
phrases like the app's preview-only references. The QBSH ground truth is
appended unchanged so accuracy can be checked at the same scale.
"""

import json
import random
import sys


def main(lakh, truth, out, total):
    rng = random.Random(5)
    sources = [json.loads(l) for l in open(lakh)]
    truth_songs = [json.loads(l) for l in open(truth)]
    with open(out, "w") as w:
        made = 0
        while made < total - len(truth_songs):
            src = rng.choice(sources)
            notes = src["phrases"][0]["notes"]
            if len(notes) < 30:
                continue
            end = notes[-1][1]
            start = rng.uniform(0, max(0.0, end - 30_000))
            window = [n for n in notes if start <= n[1] < start + 30_000]
            if len(window) < 15:
                continue
            shift = rng.randint(-5, 5)
            scale = rng.uniform(0.85, 1.15)
            t0 = window[0][1]
            phrase = [[round(p + shift, 2), round((o - t0) * scale, 1), round(d * scale, 1)] for p, o, d in window]
            song = {
                "title": f"Synthetic {made:05d}",
                "artist": src["artist"],
                "isrc": None,
                "deezer_id": None,
                "year": None,
                "popularity": round(rng.random(), 3),
                "phrases": [{"source": "preview", "start_ms": int(start), "notes": phrase}],
            }
            w.write(json.dumps(song, ensure_ascii=False) + "\n")
            made += 1
        for s in truth_songs:
            w.write(json.dumps(s, ensure_ascii=False) + "\n")
    print(f"{made} synthetic + {len(truth_songs)} ground-truth songs")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else 50_000)
