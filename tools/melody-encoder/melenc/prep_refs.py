"""Decode a format-2 index into a compact reference bundle (.npz + songs.json).

Usage: python -m melenc.prep_refs <index.mfi> <out-prefix>
"""

import json
import sys

import numpy as np

from melenc.mfi import Index


def main():
    path, out = sys.argv[1], sys.argv[2]
    idx = Index(path)
    parts = []
    offs = [0]
    for t in range(len(idx.tracks)):
        c = idx.contour(t)
        parts.append(c.astype(np.float16))
        offs.append(offs[-1] + len(c))
    contours = np.concatenate(parts)
    np.savez(
        out + ".npz",
        contours=contours,
        track_off=np.asarray(offs, dtype=np.int64),
        track_song=idx.tracks["song"].astype(np.int32),
        track_source=idx.tracks["source"].astype(np.uint8),
    )
    with open(out + "-songs.json", "w") as f:
        json.dump(idx.songs, f)
    src = idx.tracks["source"]
    frames = np.diff(np.asarray(offs))
    for s in (0, 1, 2):
        m = src == s
        print(f"source {s}: {m.sum()} tracks, {frames[m].sum() / 20 / 3600:.1f} h, median {np.median(frames[m]) / 20 if m.any() else 0:.1f} s")
    print(f"{len(idx.songs)} songs, {len(idx.tracks)} tracks, {len(contours)} frames")


if __name__ == "__main__":
    main()
