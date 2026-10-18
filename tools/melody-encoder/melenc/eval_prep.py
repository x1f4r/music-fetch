"""Join a query export (`mf queries`) with a 97f-2 evaluation run (`mf eval qbsh --out`).

Usage: python -m melenc.eval_prep <queries-export.jsonl> <eval-results.jsonl> <out.pkl>

Per query: wav, 20 fps contour (NaN = unvoiced), expected song id, the 97f-2 trace
(retrieval / linear / reranked ranks of the expected song, None = not reached), its final
rank, and the reranked candidate songs (best first).
"""

import json
import pickle
import sys

import numpy as np


def main():
    qpath, rpath, out = sys.argv[1:4]
    contours = {}
    with open(qpath) as f:
        for line in f:
            q = json.loads(line)
            if not q.get("contour"):
                continue
            c = np.array([np.nan if v is None else v for v in q["contour"]], dtype=np.float32)
            contours[q["wav"]] = c
    rows = []
    with open(rpath) as f:
        for line in f:
            r = json.loads(line)
            if r["wav"] not in contours:
                continue
            seen = []
            for c in r.get("candidates") or []:
                if c["song"] not in seen:
                    seen.append(c["song"])
            tr = r.get("trace") or [None, None, None]
            rows.append(
                dict(
                    wav=r["wav"],
                    contour=contours[r["wav"]],
                    expected=r["expected"],
                    retrieval=tr[0],
                    linear=tr[1],
                    reranked=tr[2],
                    rank=r.get("rank"),
                    candidates=seen,
                    voiced_ms=r.get("voiced_ms"),
                )
            )
    with open(out, "wb") as f:
        pickle.dump(rows, f)
    n = len(rows)
    ret = sum(r["retrieval"] is not None for r in rows) / max(n, 1)
    top1 = sum(r["rank"] == 1 for r in rows) / max(n, 1)
    print(f"{n} queries, expected known {sum(r['expected'] is not None for r in rows)}, 97f-2 retrieved {ret:.3f}, top-1 {top1:.3f}")


if __name__ == "__main__":
    main()
