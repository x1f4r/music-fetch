"""Checkpoint selection on real hums (song-disjoint splits, see `splits`).

Usage: python -m melenc.select <out.jsonl> <name>=<model.pt> [<name>=<model.pt> ...]
       [--refs data/refs-notes] [--hop 40] [--qhop 40]

Every checkpoint is evaluated with exact search over reference windows every --hop frames of
all library songs plus the 48 MIR-QBSH ground-truth melodies, query windows every --qhop
frames (the app's 2 s). Sets: CHAD-V and CHAD-T (the two halves), MIR-QBSH validation songs
(hums never trained on with --qbsh-holdout), synthetic and the private takes. The rule is fixed
before looking: the best CHAD-V union@500 (sum pooling) wins, ties go to CHAD-V enc@10.
"""

import argparse
import json
import pickle
import time

import numpy as np
import torch

from melenc.evaluate import build_index, embed, load_model, search, summarize
from melenc.refs import DATA, Refs
from melenc.sim import ref_window
from melenc.splits import chad_half, qbsh_val


def qbsh_rows(n_songs):
    q = pickle.load(open(f"{DATA}/qbsh.pkl", "rb"))
    rows = [
        {"wav": it["wav"], "contour": it["contour"], "expected": n_songs + it["ref"], "retrieval": None, "rank": None}
        for it in q["queries"]
        if qbsh_val(it["ref"])
    ]
    return q["refs"], rows


def qbsh_windows(model, refs, dev, n_songs, hop):
    ws, songs = [], []
    for i, tr in enumerate(refs):
        tr = np.asarray(tr, np.float32)
        for s in range(0, max(1, len(tr) - 40), hop):
            r = ref_window(tr, s)
            if np.isfinite(r).mean() >= 0.3:
                ws.append(r)
                songs.append(n_songs + i)
    return embed(model, np.stack(ws), dev), np.array(songs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("models", nargs="+")
    ap.add_argument("--refs", default=f"{DATA}/refs-notes")
    ap.add_argument("--hop", type=int, default=40)
    ap.add_argument("--qhop", type=int, default=40)
    ap.add_argument("--gpu-frac", type=float, default=0.45)
    a = ap.parse_args()
    dev = "cuda"
    torch.cuda.set_per_process_memory_fraction(a.gpu_frac)
    refs = Refs(a.refs)
    n = len(refs.songs)
    chad = pickle.load(open(f"{DATA}/eval-chad.pkl", "rb"))
    half = [chad_half(refs.songs[r["expected"]]["title"]) for r in chad]
    sets = {
        "chad_v": [r for r, h in zip(chad, half) if h == "V"],
        "chad_t": [r for r, h in zip(chad, half) if h == "T"],
    }
    qrefs, sets["qbsh_val"] = qbsh_rows(n)
    sets["synth"] = pickle.load(open(f"{DATA}/eval-synth.pkl", "rb"))
    sets["user"] = pickle.load(open(f"{DATA}/eval-user.pkl", "rb"))
    info = {k: {"queries": len(v), "songs": len({r["expected"] for r in v})} for k, v in sets.items()}
    print("sets", json.dumps(info), flush=True)
    with open(a.out, "a") as w:
        for spec in a.models:
            name, path = spec.split("=", 1)
            t = time.time()
            model = load_model(path, dev)
            Z, wsong, _, _ = build_index(model, refs, dev, hop=a.hop)
            Zq, qsong = qbsh_windows(model, qrefs, dev, n, a.hop)
            Z = torch.cat([Z, Zq])
            wsong = np.concatenate([wsong, qsong])
            rep = {"name": name, "path": path, "hop": a.hop, "qhop": a.qhop, "windows": len(Z)}
            for k, rows in sets.items():
                res = search(model, (Z, wsong), rows, dev, a.qhop, (1.0,), n + len(qrefs))
                rep[k] = {m: summarize(rows, res, m) for m in ("sum", "max")}
            rep["seconds"] = round(time.time() - t, 1)
            w.write(json.dumps(rep) + "\n")
            w.flush()
            s = lambda k, m, key: round(100 * rep[k][m].get(key, float("nan")), 1)  # noqa: E731
            print(
                name,
                f"V u500 {s('chad_v', 'sum', 'union@500')} e10 {s('chad_v', 'sum', 'enc@10')} e1 {s('chad_v', 'sum', 'enc@1')}",
                f"| T u500 {s('chad_t', 'sum', 'union@500')} e10 {s('chad_t', 'sum', 'enc@10')}",
                f"| qbsh e1 {s('qbsh_val', 'sum', 'enc@1')} e10 {s('qbsh_val', 'sum', 'enc@10')}",
                f"| synth e1 {s('synth', 'sum', 'enc@1')} | user e10 {s('user', 'sum', 'enc@10')} u500 {s('user', 'sum', 'union@500')}",
                f"({rep['seconds']} s)",
                flush=True,
            )
            del Z, model
            torch.cuda.empty_cache()


if __name__ == "__main__":
    main()
