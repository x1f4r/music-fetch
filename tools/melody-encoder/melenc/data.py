"""Training pairs: (query window, positive reference window, song id).

Pair kinds (weights in `KINDS`):
  pself   hum of a humanized preview melody  <-> the preview track
  mself   hum of a MIDI melody               <-> the MIDI track
  aself   hum of a humanized full-track melody <-> the full track
  m2p     hum of the MIDI melody placed inside the preview (align_midi) <-> the preview track
  tracked hums rendered as audio and pitch-tracked by the app's front end (synth_audio)
  qbsh    real MIR-QBSH hums <-> their ground-truth MIDI (qbsh_prep)
"""

import pickle

import numpy as np
import torch

from melenc.refs import AUDIO, MIDI, PREVIEW, Refs
from melenc.sim import W, humanize, ref_window, simulate

HOP_JITTER = 20
MIN_VOICED = 0.3


class Pool:
    def __init__(self, prefix, train_songs: set[int], m2p_path=None, m2p_min=0.40, tracked=None, qbsh=None):
        self.refs = Refs(prefix)
        r = self.refs
        self.contours = r.contours.astype(np.float16)
        r.contours = None
        ok = np.array([int(s) in train_songs for s in r.song])
        frames = np.diff(r.off)
        self.pv = np.flatnonzero(ok & (r.source == PREVIEW) & (frames >= 80))
        self.md = np.flatnonzero(ok & (r.source == MIDI) & (frames >= 200))
        self.au = np.flatnonzero(ok & (r.source == AUDIO) & (frames >= 200))
        self.all = np.flatnonzero(ok & (frames >= 80))
        self.m2p = None
        if m2p_path:
            z = np.load(m2p_path)
            keep = (z["score"] >= m2p_min) & np.array([int(s) in train_songs for s in z["song"]])
            self.m2p = {k: z[k][keep] for k in ("song", "preview", "midi", "scale", "lag")}
        self.tracked = pickle.load(open(tracked, "rb")) if tracked else None
        self.qbsh = pickle.load(open(qbsh, "rb")) if qbsh else None

    def track(self, t):
        return self.contours[self.refs.off[t] : self.refs.off[t + 1]].astype(np.float32)


KINDS = {"pself": 0.40, "mself": 0.15, "aself": 0.05, "m2p": 0.25, "tracked": 0.10, "qbsh": 0.05}


def _voiced(x):
    return np.isfinite(x).mean()


def _short(q, rng, p=0.15):
    if rng.random() < p:
        L = int(rng.integers(60, W))
        q = q.copy()
        q[L:] = np.nan
    return q


def sample_pair(pool: Pool, kind: str, rng):
    for _ in range(20):
        if kind in ("pself", "mself", "aself"):
            ids = {"pself": pool.pv, "mself": pool.md, "aself": pool.au}[kind]
            if len(ids) == 0:
                return None
            t = int(ids[rng.integers(len(ids))])
            tr = pool.track(t)
            N = len(tr)
            start = float(rng.uniform(0, max(1, N - 60)))
            a0 = max(0, int(start) - 10)
            seg = tr[a0 : int(start) + 300]
            if kind != "mself" and rng.random() < 0.8:
                seg = humanize(seg, rng)
            q, pos = simulate(seg, -a0, 1.0, start, rng)
            ref_track, song = tr, int(pool.refs.song[t])
        elif kind == "m2p":
            m = pool.m2p
            if m is None or len(m["song"]) == 0:
                return None
            i = int(rng.integers(len(m["song"])))
            P = pool.track(int(m["preview"][i]))
            M = pool.track(int(m["midi"][i]))
            s, lag = float(m["scale"][i]), int(m["lag"][i])
            start = float(rng.uniform(0, max(1, len(P) - 60)))
            q, pos = simulate(M, lag * s, s, start, rng)
            ref_track, song = P, int(m["song"][i])
        elif kind == "tracked":
            if not pool.tracked:
                return None
            it = pool.tracked[int(rng.integers(len(pool.tracked)))]
            qc, rp = it["contour"], it["ref_pos"]
            L = len(qc)
            c = int(rng.integers(0, max(1, L - W + 1)))
            q = np.full(W, np.nan, np.float32)
            n = min(W, L - c)
            q[:n] = qc[c : c + n]
            start = float(rp[min(c, len(rp) - 1)])
            ref_track, song = pool.track(it["track"]), it["song"]
        elif kind == "qbsh":
            if not pool.qbsh:
                return None
            it = pool.qbsh["queries"][int(rng.integers(len(pool.qbsh["queries"])))]
            qc = it["contour"]
            L = len(qc)
            c = int(rng.integers(0, max(1, L - W + 1))) if rng.random() < 0.5 else 0
            q = np.full(W, np.nan, np.float32)
            n = min(W, L - c)
            q[:n] = qc[c : c + n]
            start = it["offset"] + c / it["tempo"]
            ref_track = pool.qbsh["refs"][it["ref"]]
            song = 10_000_000 + it["ref"]
        else:
            raise ValueError(kind)
        if kind not in ("tracked", "qbsh"):
            q = _short(q, rng)
        r0 = int(round(start + rng.uniform(-HOP_JITTER, HOP_JITTER)))
        r0 = int(np.clip(r0, 0, max(0, len(ref_track) - 40)))
        r = ref_window(ref_track, r0)
        if _voiced(q) >= MIN_VOICED * (0.5 if kind in ("tracked", "qbsh") else 1) and _voiced(r) >= MIN_VOICED:
            return q, r, song
    return None


def sample_negative(pool: Pool, rng):
    for _ in range(20):
        t = int(pool.all[rng.integers(len(pool.all))])
        tr = pool.track(t)
        r0 = int(rng.integers(0, max(1, len(tr) - 40)))
        r = ref_window(tr, r0)
        if _voiced(r) >= MIN_VOICED:
            return r, int(pool.refs.song[t])
    return r, int(pool.refs.song[t])


class PairStream(torch.utils.data.IterableDataset):
    def __init__(self, pool_args, batch, negs, kinds=None, seed=0):
        self.pool_args = pool_args
        self.batch = batch
        self.negs = negs
        self.kinds = kinds or KINDS
        self.seed = seed

    def __iter__(self):
        info = torch.utils.data.get_worker_info()
        wid = info.id if info else 0
        rng = np.random.default_rng(self.seed * 1000 + wid)
        pool = Pool(**self.pool_args)
        names = [k for k, w in self.kinds.items() if w > 0 and (k != "tracked" or pool.tracked) and (k != "qbsh" or pool.qbsh)]
        w = np.array([self.kinds[k] for k in names])
        w = w / w.sum()
        while True:
            Q = np.empty((self.batch, W), np.float32)
            R = np.empty((self.batch, W), np.float32)
            S = np.empty(self.batch, np.int64)
            K = np.empty(self.batch, np.int64)
            i = 0
            while i < self.batch:
                k = int(rng.choice(len(names), p=w))
                p = sample_pair(pool, names[k], rng)
                if p is None:
                    continue
                Q[i], R[i], S[i] = p
                K[i] = k
                i += 1
            N = np.empty((self.negs, W), np.float32)
            NS = np.empty(self.negs, np.int64)
            for j in range(self.negs):
                N[j], NS[j] = sample_negative(pool, rng)
            yield torch.from_numpy(Q), torch.from_numpy(R), torch.from_numpy(S), torch.from_numpy(N), torch.from_numpy(NS), torch.from_numpy(K)
