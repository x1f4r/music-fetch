"""Place each song's preview melody inside its MIDI melody (clean <-> extracted pairs).

For every song with both a preview track and MIDI tracks, search a global tempo (MIDI frames per
preview frame), a lag and a key by FFT cross-correlation of pitch-class activations: soft classes
for the preview (its notes are often off-integer), one-hot for the MIDI. The score is the share of
voiced preview frames that meet the MIDI's pitch class.

Usage: python -m melenc.align_midi <refs-prefix> <out.npz> [procs]
"""

import sys
from multiprocessing import Pool

import numpy as np

from melenc.refs import MIDI, PREVIEW, Refs

SCALES = np.geomspace(0.85, 1.18, 23)
SIGMA = 0.3

_refs = None


def _init(prefix):
    global _refs
    _refs = Refs(prefix)


def soft_pc(p):
    v = np.isfinite(p)
    c = np.arange(12)[:, None]
    d = (np.where(v, p, 0)[None, :] - c) % 12
    d = np.minimum(d, 12 - d)
    w = np.exp(-(d**2) / (2 * SIGMA**2)) * v[None, :]
    return w.astype(np.float32)


def hard_pc(p):
    v = np.isfinite(p)
    out = np.zeros((12, len(p)), np.float32)
    idx = np.round(np.where(v, p, 0)).astype(int) % 12
    out[idx[v], np.flatnonzero(v)] = 1
    return out


def align(job):
    song, pt, mts = job
    P = _refs.track(pt)
    nv = np.isfinite(P).sum()
    if nv < 100:
        return None
    Pw = soft_pc(P)
    best = (-1.0, None)
    for mt in mts:
        M = _refs.track(mt)
        for s in SCALES:
            n = int(len(M) / s)
            if n < 100:
                continue
            Ms = M[np.minimum((np.arange(n) * s).astype(int), len(M) - 1)]
            Mh = hard_pc(Ms)
            nfft = 1 << int(np.ceil(np.log2(len(P) + n)))
            Pf = np.fft.rfft(Pw, nfft, axis=1)
            Mf = np.fft.rfft(Mh, nfft, axis=1)
            # corr_k[lag] = sum_c sum_t P[c, t] * M[(c + k) % 12, t + lag]
            rolled = np.stack([np.roll(Mf, -k, axis=0) for k in range(12)])
            spec = np.einsum("cf,kcf->kf", np.conj(Pf), rolled)
            corr = np.fft.irfft(spec, nfft, axis=1)[:, : n]  # lags 0..n-1 (preview starts inside MIDI)
            k, lag = np.unravel_index(np.argmax(corr), corr.shape)
            score = corr[k, lag] / nv
            if score > best[0]:
                best = (float(score), (int(mt), float(s), int(lag), int(k)))
    if best[1] is None:
        return None
    mt, s, lag, k = best[1]
    return (song, pt, mt, s, lag, k, best[0])


def main():
    prefix, out = sys.argv[1], sys.argv[2]
    procs = int(sys.argv[3]) if len(sys.argv) > 3 else 12
    refs = Refs(prefix)
    by_song = {}
    for t in range(refs.n_tracks):
        by_song.setdefault(int(refs.song[t]), []).append(t)
    jobs = []
    for song, ts in by_song.items():
        pv = [t for t in ts if refs.source[t] == PREVIEW]
        md = [t for t in ts if refs.source[t] == MIDI]
        if pv and md:
            jobs.append((song, pv[0], md))
    print(len(jobs), "songs with preview and MIDI", flush=True)
    with Pool(procs, initializer=_init, initargs=(prefix,)) as pool:
        res = [r for r in pool.imap_unordered(align, jobs, chunksize=8) if r is not None]
    a = np.array(res, dtype=np.float64)
    np.savez(out, song=a[:, 0].astype(np.int32), preview=a[:, 1].astype(np.int32), midi=a[:, 2].astype(np.int32),
             scale=a[:, 3], lag=a[:, 4].astype(np.int32), key=a[:, 5].astype(np.int32), score=a[:, 6])
    sc = a[:, 6]
    print("score p10/25/50/75/90", np.round(np.percentile(sc, [10, 25, 50, 75, 90]), 3))
    for th in (0.4, 0.45, 0.5, 0.6):
        print(f">= {th}: {(sc >= th).sum()}")


if __name__ == "__main__":
    main()
