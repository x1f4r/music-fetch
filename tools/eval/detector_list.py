"""Labelled clip list for `mf eval detect` (TSV: wav, impression|music, start s, duration s).

Usage: python detector_list.py <sung dir> <mirqbsh.tsv> <ioacas.tsv> <previews dir> <out.tsv> [extra impression wavs...]

Impressions: the a cappella and hummed recordings cut into 6 s clips, a random
sample of MIR-QBSH and IOACAS queries, and any extra files (e.g. whistles).
Music: every preview cut into five 6 s clips, plus the vocal-with-guitar
recording of Amazing Grace (a recorded performance, not an impression).
"""

import json
import os
import random
import sys
import wave


def duration(path):
    with wave.open(path) as w:
        return w.getnframes() / w.getframerate()


def main(sung, mir, ioa, previews, out, extra):
    rng = random.Random(11)
    rows = []
    for name, label in [("amazing.wav", "music"), ("twinkle.wav", "impression"), ("hum.wav", "impression")]:
        path = os.path.join(sung, name)
        d = duration(path)
        start = 0.0
        while start + 4.5 <= d:
            rows.append((path, label, start, 6.0))
            start += 8.0
    for tsv, n in [(mir, 250), (ioa, 150)]:
        wavs = [l.split("\t")[0] for l in open(tsv) if l.strip()]
        for w in rng.sample(wavs, min(n, len(wavs))):
            rows.append((w, "impression", 0.0, 8.0))
    for path in extra:
        rows.append((path, "impression", 0.0, 6.0))
    for item in json.load(open(os.path.join(previews, "manifest.json"))):
        for start in (0.0, 6.0, 12.0, 18.0, 24.0):
            rows.append((item["wav"], "music", start, 6.0))
    with open(out, "w") as f:
        for r in rows:
            f.write(f"{r[0]}\t{r[1]}\t{r[2]:.1f}\t{r[3]:.1f}\n")
    print(len(rows), "clips:", sum(r[1] == "impression" for r in rows), "impression,", sum(r[1] == "music" for r in rows), "music")


if __name__ == "__main__":
    main(*sys.argv[1:6], sys.argv[6:])
