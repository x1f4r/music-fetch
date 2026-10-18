"""How many hums the encoder's extra candidates could turn into top-1 / top-10 answers.

Usage: python -m melenc.conversion <97f-2 eval.jsonl (--trace)> <encoder ranks.json> [--ks 100,500]

For a hum 97f-2 never retrieves but the encoder ranks within k, the true song would enter the
rerank. `oracle_score` (97f-2's `score_song`: the song's own index votes plus exhaustive
placements, scored against "none" alone, popularity prior applied) is what it would score
there. Each of 97f-2's candidates is put on the same scale from its features (utility against
"none" alone, Platt scaling, prior), and the true song's rank among them is counted.

This is an upper bound in one respect: the encoder's other k - 1 songs also enter the
rerank and are not scored here, so a distractor could still outrank the true song.
"""

import argparse
import json
import math

import numpy as np

# core/crates/mfcore/src/melody/matcher.rs
WEIGHTS = [20.7026, -4.1774, -7.6168, -4.0570, -3.7445, -12.2397, -1.9081, 0.2353]
NONE = -15.5734
CAL = (-0.4713, 0.9644)
POP_W = 0.3


def utility(c):
    x = [
        min(c["votes"], 4.0),
        min(c["linear"], 3.0),
        min(c["dtw"], 4.0),
        c["coverage"],
        min(c["tempo_dev"], 2.0),
        min(c["align"], 2.0),
        min(c["edit"], 2.0),
        min(c["note_votes"], 6.0),
    ]
    return sum(a * w for a, w in zip(x, WEIGHTS))


def solo_logit(c):
    """logit of the prior-adjusted calibrated score, candidate against "none" alone."""
    return CAL[0] + CAL[1] * (utility(c) - NONE) + POP_W * math.log((c["popularity"] + 0.05) / 0.55)


def logit(p):
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("ranks")
    ap.add_argument("--ks", default="100,500")
    ap.add_argument("--mode", default="max")
    a = ap.parse_args()
    enc = {r["wav"]: r[a.mode] for r in json.load(open(a.ranks))}
    rows = []
    with open(a.results) as f:
        for line in f:
            r = json.loads(line)
            if r["wav"] in enc and r.get("expected") is not None:
                rows.append(r)
    n = len(rows)
    base1 = np.mean([r["rank"] == 1 for r in rows])
    base10 = np.mean([r["rank"] is not None and r["rank"] <= 10 for r in rows])
    print(f"{n} queries; 97f-2 top-1 {base1:.4f}, top-10 {base10:.4f}")
    for k in [int(x) for x in a.ks.split(",")]:
        add1 = add10 = rescued = no_oracle = 0
        for r in rows:
            tr = r.get("trace") or [None]
            e = enc[r["wav"]]
            if tr[0] is not None or e is None or e > k:
                continue
            rescued += 1
            o = r.get("oracle_score")
            if o is None:
                no_oracle += 1
                continue
            mine = logit(o)
            others = sorted((solo_logit(c) for c in r["candidates"] if c["song"] != r["expected"]), reverse=True)
            rank = 1 + sum(1 for s in others if s > mine)
            add1 += rank == 1
            add10 += rank <= 10
        print(
            f"k={k}: rescued {rescued} ({rescued / n:.4f}); no score_song placement {no_oracle}; "
            f"would rank 1st {add1} (+{add1 / n:.4f} top-1 -> {base1 + add1 / n:.4f}), "
            f"top-10 {add10} (+{add10 / n:.4f} -> {base10 + add10 / n:.4f})"
        )


if __name__ == "__main__":
    main()
