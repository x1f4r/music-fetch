"""Retrieval evaluation: encoder alone and in union with 97f-2's retrieval candidates.

Usage: python -m melenc.evaluate <model.pt> <refs-prefix> <eval.pkl> [--hop 20] [--qhop 20]
       [--tempos 1.0] [--meta per-query.csv.gz] [--out result.json]

The index holds one embedding per reference window (8 s, every --hop frames, >= 30 % voiced);
a query is cut into 8 s windows every --qhop frames (optionally resampled at --tempos), and a
song's score is its best window similarity over all query windows ("max") or the sum over query
windows of each window's best similarity for the song ("sum"). Brute-force search on the GPU.
"""

import argparse
import csv
import gzip
import json
import pickle

import numpy as np
import torch

from melenc.model import Encoder, featurize
from melenc.refs import Refs
from melenc.sim import W, ref_window

KS = (1, 10, 100, 500)


def load_model(path, dev):
    ck = torch.load(path, map_location=dev)
    a = ck["args"]
    m = Encoder(d=a["d"], layers=a["layers"], dim=a["dim"]).to(dev)
    m.load_state_dict(ck["model"])
    m.eval()
    return m


@torch.no_grad()
def embed(model, x, dev, bs=8192):
    out = []
    for i in range(0, len(x), bs):
        xb = torch.from_numpy(np.ascontiguousarray(x[i : i + bs])).to(dev)
        with torch.autocast("cuda", dtype=torch.bfloat16):
            out.append(model(featurize(xb)).float().half())
    return torch.cat(out)


def build_index(model, refs: Refs, dev, hop=20, min_voiced=0.3, chunk=200_000):
    songs, tracks, starts = [], [], []
    buf = []
    embs = []
    for t in range(refs.n_tracks):
        tr = refs.track(t)
        for s in range(0, max(1, len(tr) - 40), hop):
            r = ref_window(tr, s)
            if np.isfinite(r).mean() >= min_voiced:
                buf.append(r)
                songs.append(int(refs.song[t]))
                tracks.append(t)
                starts.append(s)
        if len(buf) >= chunk:
            embs.append(embed(model, np.stack(buf), dev))
            buf = []
    if buf:
        embs.append(embed(model, np.stack(buf), dev))
    return torch.cat(embs), np.array(songs), np.array(tracks), np.array(starts)


def resample(c, f):
    """Resample a query contour by tempo factor f (>1 = stretch)."""
    if f == 1.0:
        return c
    n = int(round(len(c) * f))
    idx = np.minimum((np.arange(n) / f).astype(int), len(c) - 1)
    return c[idx]


def query_windows(c, qhop=20, tempos=(1.0,), min_voiced=0.3):
    out = []
    v = np.flatnonzero(np.isfinite(c))
    if len(v) == 0:
        return out
    c = c[v[0] : v[-1] + 1]
    for f in tempos:
        cc = resample(c, f)
        if len(cc) <= W:
            w = np.full(W, np.nan, np.float32)
            w[: len(cc)] = cc
            out.append(w)
            continue
        last = len(cc) - W
        starts = list(range(0, last + 1, qhop))
        if starts[-1] != last:
            starts.append(last)
        for s in starts:
            w = cc[s : s + W]
            if np.isfinite(w).mean() >= min_voiced:
                out.append(w.astype(np.float32))
    return out


@torch.no_grad()
def search(model, index, rows, dev, qhop, tempos, n_songs, top=500):
    Z, wsong = index
    wsong_t = torch.from_numpy(wsong).to(dev)
    res = []
    for r in rows:
        ws = query_windows(r["contour"], qhop, tempos)
        if not ws:
            res.append(dict(max=None, sum=None, top_max=[], top_sum=[]))
            continue
        zq = embed(model, np.stack(ws), dev)
        sim = (zq @ Z.T).float()  # (nq, nwin)
        per = torch.full((len(ws), n_songs), -2.0, device=dev)
        per.scatter_reduce_(1, wsong_t.expand(len(ws), -1), sim, reduce="amax")
        smax = per.max(0).values
        ssum = per.clamp(min=0).sum(0)
        out = {}
        for name, s in (("max", smax), ("sum", ssum)):
            e = r["expected"]
            out[name] = int((s > s[e]).sum().item()) + 1 if e is not None else None
            out["top_" + name] = torch.topk(s, top).indices.tolist()
        res.append(out)
    return res


def summarize(rows, res, mode, subset=None):
    idx = [i for i in range(len(rows)) if subset is None or subset[i]]
    n = len(idx)
    if n == 0:
        return {}
    enc = np.array([res[i][mode] if res[i][mode] is not None else 10**9 for i in idx])
    ret = np.array([rows[i]["retrieval"] if rows[i]["retrieval"] is not None else 10**9 for i in idx])
    in97 = ret < 10**9
    out = {"n": n, "97f2_retrieved": float(in97.mean()), "97f2_top1": float(np.mean([rows[i]["rank"] == 1 for i in idx]))}
    for k in KS:
        out[f"enc@{k}"] = float((enc <= k).mean())
        out[f"97f2_ret@{k}"] = float((ret <= k).mean())
        out[f"union@{k}"] = float((in97 | (enc <= k)).mean())
        out[f"gain@{k}"] = out[f"union@{k}"] - out["97f2_retrieved"]
        out[f"rescued@{k}"] = float(((enc <= k) & ~in97).sum() / max(1, (~in97).sum()))
        out[f"union_ret@{k}"] = float(((ret <= k) | (enc <= k)).mean())
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("model")
    ap.add_argument("refs")
    ap.add_argument("evals", nargs="+")
    ap.add_argument("--hop", type=int, default=20)
    ap.add_argument("--qhop", type=int, default=20)
    ap.add_argument("--tempos", default="1.0")
    ap.add_argument("--meta")
    ap.add_argument("--out")
    ap.add_argument("--save-index")
    a = ap.parse_args()
    dev = "cuda"
    model = load_model(a.model, dev)
    refs = Refs(a.refs)
    Z, wsong, wtrack, wstart = build_index(model, refs, dev, hop=a.hop)
    print(f"index: {len(Z)} windows over {len(set(wsong.tolist()))} songs (hop {a.hop})", flush=True)
    if a.save_index:
        np.savez(a.save_index, emb=Z.cpu().numpy(), song=wsong, track=wtrack, start=wstart)
    tempos = tuple(float(x) for x in a.tempos.split(","))
    meta = {}
    if a.meta:
        with gzip.open(a.meta, "rt") as f:
            for m in csv.DictReader(f):
                meta[m["wav"]] = m
    report = {"hop": a.hop, "qhop": a.qhop, "tempos": tempos, "windows": len(Z)}
    for ev in a.evals:
        rows = pickle.load(open(ev, "rb"))
        res = search(model, (Z, wsong), rows, dev, a.qhop, tempos, len(refs.songs))
        name = ev.rsplit("/", 1)[-1].replace(".pkl", "")
        rep = {}
        for mode in ("max", "sum"):
            rep[mode] = {"all": summarize(rows, res, mode)}
            if meta:
                def m_of(r):
                    k = r["wav"].split("/chad/", 1)[-1]
                    return meta.get(k)
                ms = [m_of(r) for r in rows]
                cov = [bool(m) and (m["midi"] == "True" or float(m["melody_in_preview"] or 0) >= 0.5) for m in ms]
                unc = [bool(m) and not c and m["cov_class"] != "unknown" for m, c in zip(ms, cov)]
                fid = [bool(m) and c and float(m["fidelity"] or 0) >= 0.92 for m, c in zip(ms, cov)]
                rep[mode]["covered"] = summarize(rows, res, mode, cov)
                rep[mode]["uncovered"] = summarize(rows, res, mode, unc)
                rep[mode]["covered_fid92"] = summarize(rows, res, mode, fid)
        report[name] = rep
        s = rep["max"]["all"]
        print(name, json.dumps({k: round(v, 4) if isinstance(v, float) else v for k, v in s.items()}), flush=True)
        s = rep["sum"]["all"]
        print(name, "(sum)", json.dumps({k: round(v, 4) if isinstance(v, float) else v for k, v in s.items()}), flush=True)
        ranks_path = (a.out.removesuffix(".json") + f"-{name}-ranks.json") if a.out else ev.replace(".pkl", "") + "-enc-ranks.json"
        with open(ranks_path, "w") as f:
            json.dump([{"wav": r["wav"], "max": x["max"], "sum": x["sum"], "top_max": x["top_max"][:100]} for r, x in zip(rows, res)], f)
    if a.out:
        json.dump(report, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
