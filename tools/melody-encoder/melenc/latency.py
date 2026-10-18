"""CPU cost and compressed-index recall: ONNX encoder + faiss IVF-PQ, one thread per query.

Usage: python -m melenc.latency <index.npz> <model.onnx> <eval.pkl> [--m 16] [--nlist 4096]
       [--nprobe 16,32,64] [--k 1000] [--qhop 20] [--limit N] [--hop-keep 1]

<index.npz> comes from `evaluate --save-index` (unit embeddings of every reference window).
--hop-keep 2 keeps every second window of each track (index hop 2 s from a 1 s build).
Song scores are the best window similarity among the k nearest windows of any query window;
songs outside every window's k nearest are not retrieved.
"""

import argparse
import json
import pickle
import time

import faiss
import numpy as np
import onnxruntime as ort

from melenc.evaluate import KS, query_windows


def featurize_np(x):
    """numpy mirror of model.featurize (and the spec for the Rust port)."""
    B, n = x.shape
    v = np.isfinite(x)
    idx = np.where(v, np.arange(n)[None, :], 0)
    idx = np.maximum.accumulate(idx, axis=1)
    filled = np.take_along_axis(np.nan_to_num(x, nan=0.0), idx, axis=1)
    med = np.zeros((B, 1), np.float32)
    for i in range(B):
        if v[i].any():
            vals = np.sort(x[i][v[i]])
            med[i, 0] = vals[(len(vals) - 1) // 2]  # torch.nanmedian takes the lower middle
    before = np.maximum.accumulate(v, axis=1)
    filled = np.where(before, filled, med)
    rel = np.clip((filled - med) / 6.0, -2, 2)
    d = np.zeros_like(filled)
    d[:, 1:] = filled[:, 1:] - filled[:, :-1]
    d = np.clip(d / 3.0, -2, 2)
    return np.stack([rel, v.astype(np.float32), d], axis=1).astype(np.float32)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("index")
    ap.add_argument("onnx")
    ap.add_argument("eval")
    ap.add_argument("--m", type=int, default=16)
    ap.add_argument("--nlist", type=int, default=4096)
    ap.add_argument("--nprobe", default="16,32,64")
    ap.add_argument("--k", type=int, default=1000)
    ap.add_argument("--qhop", type=int, default=20)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--hop-keep", type=int, default=1)
    ap.add_argument("--out")
    a = ap.parse_args()

    z = np.load(a.index)
    emb = z["emb"].astype(np.float32)
    song = z["song"]
    start = z["start"]
    if a.hop_keep > 1:
        keep = (start // 20) % a.hop_keep == 0
        emb, song, start = emb[keep], song[keep], start[keep]
    N, dim = emb.shape
    faiss.omp_set_num_threads(12)
    q = faiss.IndexFlatIP(dim)
    ivf = faiss.IndexIVFPQ(q, dim, a.nlist, a.m, 8, faiss.METRIC_INNER_PRODUCT)
    rng = np.random.default_rng(0)
    t = time.time()
    ivf.train(emb[rng.choice(N, min(N, 300_000), replace=False)])
    ivf.add(emb)
    print(f"IVF{a.nlist},PQ{a.m}: {N} windows, built in {time.time() - t:.0f}s", flush=True)
    size = {
        "codes_MB": N * a.m / 1e6,
        "ids_MB (song u32 + start u16)": N * 6 / 1e6,
        "centroids_MB (f16)": a.nlist * dim * 2 / 1e6,
        "pq_codebooks_MB": a.m * 256 * (dim // a.m) * 4 / 1e6,
    }
    size["total_MB"] = sum(size.values())
    print("size", json.dumps({k: round(v, 2) for k, v in size.items()}), flush=True)

    so = ort.SessionOptions()
    so.intra_op_num_threads = 1
    so.inter_op_num_threads = 1
    sess = ort.InferenceSession(a.onnx, so, providers=["CPUExecutionProvider"])
    faiss.omp_set_num_threads(1)
    rows = pickle.load(open(a.eval, "rb"))
    if a.limit:
        rows = rows[: a.limit]
    report = {"windows": N, "size": size, "runs": {}}
    for nprobe in [int(x) for x in a.nprobe.split(",")]:
        ivf.nprobe = nprobe
        enc_ms, search_ms, ranks, nwin = [], [], [], []
        for r in rows:
            ws = query_windows(r["contour"], a.qhop)
            if not ws:
                ranks.append(10**9)
                continue
            t0 = time.perf_counter()
            zq = sess.run(None, {"features": featurize_np(np.stack(ws))})[0]
            t1 = time.perf_counter()
            D, I = ivf.search(np.ascontiguousarray(zq, dtype=np.float32), a.k)
            best = {}
            for d_row, i_row in zip(D, I):
                for d, i in zip(d_row, i_row):
                    if i < 0:
                        continue
                    s = int(song[i])
                    if d > best.get(s, -9):
                        best[s] = d
            t2 = time.perf_counter()
            e = r["expected"]
            if e in best:
                ranks.append(1 + sum(1 for v in best.values() if v > best[e]))
            else:
                ranks.append(10**9)
            enc_ms.append((t1 - t0) * 1000)
            search_ms.append((t2 - t1) * 1000)
            nwin.append(len(ws))
        ranks = np.array(ranks)
        in97 = np.array([r["retrieval"] is not None for r in rows])
        out = {"query_windows_p50": float(np.median(nwin)),
               "encode_ms_p50": float(np.median(enc_ms)), "encode_ms_p95": float(np.percentile(enc_ms, 95)),
               "search_ms_p50": float(np.median(search_ms)), "search_ms_p95": float(np.percentile(search_ms, 95)),
               "97f2_retrieved": float(in97.mean())}
        for k in KS:
            out[f"enc@{k}"] = float((ranks <= k).mean())
            out[f"union@{k}"] = float((in97 | (ranks <= k)).mean())
            out[f"gain@{k}"] = out[f"union@{k}"] - out["97f2_retrieved"]
        report["runs"][nprobe] = out
        print(f"nprobe {nprobe}:", json.dumps({k: round(v, 4) for k, v in out.items()}), flush=True)
    if a.out:
        json.dump(report, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
