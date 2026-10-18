"""Where compressed-index recall goes: index hop, neighbour cap, quantization, IVF probing.

Usage: python -m melenc.ann_diag <model.pt> <index.npz> <eval.pkl> [--limit N] [--out diag.json]
       [--variants flat-h4,pq32-h4,pq32-h2-top20000,...] [--songs refs-songs.json]

All variants score a song by its best window similarity over the query's windows (as
`evaluate` does) and report recall@{1,10,100,500} alone and in union with 97f-2's retrieval.
  flat-h1      exact, every 1 s window (the brute-force baseline)
  flat-h2      exact, every second window (2 s hop)
  capK-hX      exact, but only each query window's K nearest windows count (ANN semantics)
  pqM-hX       product quantization (M bytes per window, exhaustive ADC), K nearest per window
  sq8-hX       8-bit scalar quantization, K nearest per window
  ivfN-pqM-hX  IVF with N lists (nprobe P) + PQ, K nearest per window
With --variants, only the named ones run: <flat|pqM|opqM|sq8|ivfpqM|pcaDpqM|pqMx4fs>-h<hop s>[-top<N songs by popularity>].
"""

import argparse
import json
import pickle

import faiss
import numpy as np
import torch

from melenc.evaluate import KS, embed, load_model, query_windows


def recall(ranks, in97):
    out = {}
    for k in KS:
        out[f"enc@{k}"] = round(float((ranks <= k).mean()), 4)
        out[f"union@{k}"] = round(float((in97 | (ranks <= k)).mean()), 4)
    out["gain@500"] = round(out["union@500"] - float(in97.mean()), 4)
    return out


def ranks_from_hits(D, I, owner, qid, nq, expected):
    """Best similarity per (query, song) among the hits; rank of the expected song."""
    best = [dict() for _ in range(nq)]
    for row in range(len(D)):
        b = best[qid[row]]
        for d, i in zip(D[row], I[row]):
            if i < 0:
                continue
            s = int(owner[i])
            if d > b.get(s, -9.0):
                b[s] = float(d)
    out = np.full(nq, 10**9)
    for j, e in enumerate(expected):
        b = best[j]
        if e in b:
            out[j] = 1 + sum(1 for v in b.values() if v > b[e])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("model")
    ap.add_argument("index")
    ap.add_argument("eval")
    ap.add_argument("--limit", type=int, default=1200)
    ap.add_argument("--k", type=int, default=1000)
    ap.add_argument("--qhop", type=int, default=20)
    ap.add_argument("--out")
    ap.add_argument("--variants")
    ap.add_argument("--songs", help="songs.json (popularity) for -topN variants")
    a = ap.parse_args()
    dev = "cuda"
    model = load_model(a.model, dev)
    z = np.load(a.index)
    emb = z["emb"].astype(np.float32)
    dim = emb.shape[1]
    song = z["song"]
    start = z["start"]
    rows = pickle.load(open(a.eval, "rb"))
    rng = np.random.default_rng(0)
    rows = [rows[i] for i in sorted(rng.choice(len(rows), min(a.limit, len(rows)), replace=False))]
    in97 = np.array([r["retrieval"] is not None for r in rows])
    expected = [r["expected"] for r in rows]
    Q, qid = [], []
    for j, r in enumerate(rows):
        ws = query_windows(r["contour"], a.qhop)
        Q += ws
        qid += [j] * len(ws)
    qid = np.array(qid)
    zq = embed(model, np.stack(Q), dev).float().cpu().numpy()
    nq = len(rows)
    print(f"{nq} queries, {len(Q)} query windows, 97f-2 retrieved {in97.mean():.4f}", flush=True)
    report = {"queries": nq, "query_windows": len(Q), "97f2_retrieved": float(in97.mean())}

    def exact(mask, cap=None):
        Z = torch.from_numpy(emb[mask]).to(dev).half()
        own = torch.from_numpy(song[mask]).to(dev).long()
        n_songs = int(song.max()) + 1
        out = np.full(nq, 10**9)
        for j in range(nq):
            sel = np.flatnonzero(qid == j)
            if len(sel) == 0:
                continue
            sim = (torch.from_numpy(zq[sel]).to(dev).half() @ Z.T).float()
            if cap:
                v, i = torch.topk(sim, cap, dim=1)
                per = torch.full((len(sel), n_songs), -2.0, device=dev)
                per.scatter_reduce_(1, own[i], v, reduce="amax")
            else:
                per = torch.full((len(sel), n_songs), -2.0, device=dev)
                per.scatter_reduce_(1, own.expand(len(sel), -1), sim, reduce="amax")
            s = per.max(0).values
            e = expected[j]
            if s[e] > -2:
                out[j] = int((s > s[e]).sum().item()) + 1
        return out

    h1 = np.ones(len(emb), bool)
    h2 = (start // 20) % 2 == 0
    if a.variants:
        pop_rank = None
        if a.songs:
            pop = np.array([s_["popularity"] for s_ in json.load(open(a.songs))])
            pop_rank = np.empty(len(pop), int)
            pop_rank[np.argsort(-pop, kind="stable")] = np.arange(len(pop))
        faiss.omp_set_num_threads(8)
        for v in a.variants.split(","):
            parts = v.split("-")
            kind, hop = parts[0], int(parts[1][1:])
            mask = (start // 20) % hop == 0
            if len(parts) > 2:
                mask &= pop_rank[song] < int(parts[2][3:])
            report[v + "-windows"] = int(mask.sum())
            if kind == "flat":
                r = recall(exact(mask, a.k), in97)
            else:
                X = np.ascontiguousarray(emb[mask])
                owner = song[mask]
                train = X[rng.choice(len(X), min(len(X), 300_000), replace=False)]
                if kind.startswith("pca"):
                    d_, m = kind[3:].split("pq")
                    idx = faiss.index_factory(dim, f"PCA{d_},PQ{m}", faiss.METRIC_INNER_PRODUCT)
                elif kind.endswith("fs"):
                    idx = faiss.index_factory(dim, "PQ" + kind[2:], faiss.METRIC_INNER_PRODUCT)  # pq64x4fs -> PQ64x4fs
                elif kind.startswith("opq"):
                    m = int(kind[3:])
                    idx = faiss.index_factory(dim, f"OPQ{m},PQ{m}", faiss.METRIC_INNER_PRODUCT)
                elif kind.startswith("ivfpq"):
                    m = int(kind[5:])
                    idx = faiss.IndexIVFPQ(faiss.IndexFlatIP(dim), dim, 2048, m, 8, faiss.METRIC_INNER_PRODUCT)
                elif kind == "sq8":
                    idx = faiss.IndexScalarQuantizer(dim, faiss.ScalarQuantizer.QT_8bit, faiss.METRIC_INNER_PRODUCT)
                else:
                    idx = faiss.IndexPQ(dim, int(kind[2:]), 8, faiss.METRIC_INNER_PRODUCT)
                idx.train(train)
                idx.add(X)
                if kind.startswith("ivfpq"):
                    idx.nprobe = 256
                D, I = idx.search(zq, a.k)
                r = recall(ranks_from_hits(D, I, owner, qid, nq, expected), in97)
            r["windows"] = int(mask.sum())
            report[v] = r
            print(v, json.dumps(r), flush=True)
        if a.out:
            json.dump(report, open(a.out, "w"), indent=1)
        return
    for name, mask, cap in [("flat-h1", h1, None), ("flat-h2", h2, None), (f"cap{a.k}-h1", h1, a.k), (f"cap{a.k}-h2", h2, a.k), ("cap4000-h2", h2, 4000)]:
        r = recall(exact(mask, cap), in97)
        report[name] = r
        print(name, json.dumps(r), flush=True)

    faiss.omp_set_num_threads(8)
    for hop, mask in (("h2", h2), ("h1", h1)):
        X = np.ascontiguousarray(emb[mask])
        owner = song[mask]
        train = X[rng.choice(len(X), min(len(X), 300_000), replace=False)]
        variants = [("pq16", lambda: faiss.IndexPQ(dim, 16, 8, faiss.METRIC_INNER_PRODUCT)),
                    ("pq32", lambda: faiss.IndexPQ(dim, 32, 8, faiss.METRIC_INNER_PRODUCT)),
                    ("opq16", lambda: faiss.index_factory(dim, "OPQ16,PQ16", faiss.METRIC_INNER_PRODUCT))]
        if hop == "h2":
            variants.append(("sq8", lambda: faiss.IndexScalarQuantizer(dim, faiss.ScalarQuantizer.QT_8bit, faiss.METRIC_INNER_PRODUCT)))
            variants.append(("pq8", lambda: faiss.IndexPQ(dim, 8, 8, faiss.METRIC_INNER_PRODUCT)))
        for vname, make in variants:
            idx = make()
            idx.train(train)
            idx.add(X)
            D, I = idx.search(zq, a.k)
            r = recall(ranks_from_hits(D, I, owner, qid, nq, expected), in97)
            report[f"{vname}-{hop}"] = r
            print(f"{vname}-{hop}", json.dumps(r), flush=True)
        for nprobe in (32, 128):
            ivf = faiss.IndexIVFPQ(faiss.IndexFlatIP(dim), dim, 4096, 16, 8, faiss.METRIC_INNER_PRODUCT)
            ivf.train(train)
            ivf.add(X)
            ivf.nprobe = nprobe
            D, I = ivf.search(zq, a.k)
            r = recall(ranks_from_hits(D, I, owner, qid, nq, expected), in97)
            report[f"ivf4096p{nprobe}-pq16-{hop}"] = r
            print(f"ivf4096p{nprobe}-pq16-{hop}", json.dumps(r), flush=True)
    if a.out:
        json.dump(report, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
