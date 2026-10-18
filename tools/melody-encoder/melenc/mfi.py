"""Read a format-2 `.mfi` melody index (see core/crates/mfcore/src/melody/index.rs).

Only what the encoder needs: song metadata and every track's decoded 20 fps contour
(MIDI semitones, NaN = unvoiced), exactly as the matcher sees it.
"""

import struct

import numpy as np

SOURCES = {0: "preview", 1: "audio", 2: "midi"}


def _varints(blob: bytes, pos: int, count_frames: int):
    """Decode (code, run) pairs until `count_frames` frames are produced."""
    codes = []
    runs = []
    n = 0
    while n < count_frames:
        code = blob[pos]
        pos += 1
        v = 0
        shift = 0
        while True:
            b = blob[pos]
            pos += 1
            v |= (b & 0x7F) << shift
            if b & 0x80 == 0:
                break
            shift += 7
        codes.append(code)
        runs.append(v)
        n += v
    return codes, runs


class Index:
    def __init__(self, path: str):
        b = open(path, "rb").read()
        if b[:8] != b"MFMELODY":
            raise ValueError("not a melody index")
        fmt, _hlen, n_songs = struct.unpack_from("<III", b, 8)
        if fmt != 2:
            raise ValueError(f"format {fmt}, expected 2")
        secs = [struct.unpack_from("<QQ", b, 48 + 16 * i) for i in range(10)]
        self._b = b
        self._secs = secs
        so = secs[0][0]
        s1 = secs[1][0]
        songs = []
        for i in range(n_songs):
            r = b[s1 + 40 * i : s1 + 40 * (i + 1)]
            toff, tlen, alen, aoff = struct.unpack_from("<IHHI", r, 0)
            dz, year, pop, first = struct.unpack_from("<QHHI", r, 24)
            songs.append(
                dict(
                    id=i,
                    title=b[so + toff : so + toff + tlen].decode("utf-8", "replace"),
                    artist=b[so + aoff : so + aoff + alen].decode("utf-8", "replace"),
                    deezer_id=dz or None,
                    year=year or None,
                    popularity=pop / 65535.0,
                    first_track=first,
                )
            )
        self.songs = songs
        off, ln = secs[2]
        self.tracks = np.frombuffer(
            b,
            dtype=np.dtype(
                [("song", "<u4"), ("coff", "<u4"), ("frames", "<u4"), ("base", "u1"), ("source", "u1"), ("enc", "u1"), ("pad", "u1")]
            ),
            count=ln // 16,
            offset=off,
        )

    def contour(self, t: int) -> np.ndarray:
        r = self.tracks[t]
        blob = self._b
        start = self._secs[3][0] + int(r["coff"])
        frames = int(r["frames"])
        base = float(r["base"])
        if r["enc"] == 0:
            codes = np.frombuffer(blob, dtype=np.uint8, count=frames, offset=start).astype(np.int32)
        else:
            c, n = _varints(blob, start, frames)
            codes = np.repeat(np.asarray(c, dtype=np.int32), np.asarray(n, dtype=np.int64))[:frames]
        out = np.where(codes == 0, np.nan, base + (codes - 1) / 4.0).astype(np.float32)
        return out
