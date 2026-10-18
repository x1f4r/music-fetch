"""MIR-QBSH real hums as training pairs.

Usage: python -m melenc.qbsh_prep <qbsh index.mfi> <queries.tsv> <export.jsonl> <out.pkl>

References are the ground-truth tracks decoded from the QBSH evaluation index; each exported
query contour (trimmed to its first voiced frame) is placed on its reference by a brute-force
search over tempo (x0.6-1.7) and onset (0-6 s) with a clipped L1 distance at the median key.
"""

import json
import pickle
import sys

import numpy as np

from melenc.mfi import Index

TEMPOS = np.geomspace(0.6, 1.7, 31)


def place(c, R):
    best = (1e9, 0.0, 1.0)
    vq = np.isfinite(c)
    for tau in TEMPOS:
        for off in range(0, 120, 2):
            pos = off + np.arange(len(c)) / tau
            idx = np.round(pos).astype(int)
            ok = vq & (idx < len(R))
            if ok.sum() < 0.6 * vq.sum():
                continue
            r = R[idx[ok]]
            m = np.isfinite(r)
            if m.sum() < 30:
                continue
            d = c[ok][m] - r[m]
            d = d - np.median(d)
            cost = np.minimum(np.abs(d), 3).mean() + 1.5 * (1 - m.mean())
            if cost < best[0]:
                best = (float(cost), float(off), float(tau))
    return best


def main():
    mfi, tsv, export, out = sys.argv[1:5]
    idx = Index(mfi)
    by_title = {s["title"]: s["id"] for s in idx.songs}
    first = {}
    for t, rec in enumerate(idx.tracks):
        first.setdefault(int(rec["song"]), t)
    titles = {}
    for line in open(tsv):
        wav, title = line.rstrip("\n").split("\t")[:2]
        titles[wav] = title
    refs, ref_of = [], {}
    queries = []
    costs = []
    for line in open(export):
        q = json.loads(line)
        title = titles.get(q["wav"])
        if title not in by_title or not q.get("contour"):
            continue
        song = by_title[title]
        if song not in ref_of:
            ref_of[song] = len(refs)
            refs.append(idx.contour(first[song]))
        R = refs[ref_of[song]]
        c = np.array([np.nan if v is None else v for v in q["contour"]], np.float32)
        v = np.flatnonzero(np.isfinite(c))
        if len(v) < 40:
            continue
        c = c[v[0] : v[-1] + 1]
        cost, off, tau = place(c, R)
        costs.append(cost)
        queries.append(dict(wav=q["wav"], contour=c, ref=ref_of[song], offset=off, tempo=tau, cost=cost))
    keep = [q for q in queries if q["cost"] < 1.2]
    pickle.dump({"refs": refs, "queries": keep}, open(out, "wb"))
    print(f"{len(queries)} queries on {len(refs)} refs; cost p10/50/90 {np.round(np.percentile(costs, [10, 50, 90]), 3)}; kept {len(keep)}")


if __name__ == "__main__":
    main()
