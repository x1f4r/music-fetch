"""Fit the matcher's score model on `mf eval qbsh --out` results.

Usage:
    python fit_fusion.py [options] NAME=results.jsonl [NAME=results.jsonl ...]

The model is a conditional logit. Each candidate gets the utility w·x over its
features (votes, linear, dtw, coverage, tempo_dev, align, edit, note_votes, the
reference length and the popularity prior); a "none of these" option gets
c0 + c·q over query traits (log voiced length, pitch range, pitch changes), so
short, narrow or simple hums are less conclusive. The query's candidates and
"none" share a softmax; Platt scaling of the top-1 probability, fitted on the
sets named by --calibrate (real hums), makes scores read as P(correct).

Cross-validation is song-disjoint: 2 folds by song (or by the group a --groups
JSON file assigns to each recording, e.g. the song of a CHAD hum), shared by all
inputs, so one song's hums never sit on both sides even when two inputs search
the same hums against different indexes. Prints cross-validated metrics per
input (top-1, top-10, tier precision and share, answers at 90 % precision), a
reliability table, and the constants for matcher.rs fitted on everything.

Options:
    --groups FILE        JSON object: recording path -> group (default: expected song)
    --calibrate A,B      inputs whose hums calibrate the scores (default: the first)
    --weights A=3,B=1    relative weight of each input (each input's total weight
                         is otherwise equal)
    --cand / --query     feature subsets (default: all)
    --prefixes           also fit on prefix rows (`--prefix-ms`) and report them
    --prefix-weight W    weight of a prefix row relative to a full one (0: report only)
    --dump FILE          cross-validated per-query predictions (JSONL)
Only candidates in the results can be re-ranked: queries whose song is missing
there stay misses.
"""

import argparse
import json

import numpy as np
from scipy.optimize import minimize

CAND = ["votes", "linear", "dtw", "coverage", "tempo_dev", "align", "edit", "note_votes", "reference", "prior"]
CAPS = {"votes": 4.0, "linear": 3.0, "dtw": 4.0, "coverage": 1.0, "tempo_dev": 2.0, "align": 2.0, "edit": 2.0,
        "note_votes": 6.0}
QUERY = ["voiced", "range", "steps"]
TIERS = (0.75, 0.35)


def cand_x(c, use):
    out = []
    for n in use:
        if n == "reference":
            out.append(np.log(max(c.get("ref_frames", 600), 20) / 600.0))
        elif n == "prior":
            out.append(np.log((c.get("popularity", 0.5) + 0.05) / 0.55))
        else:
            out.append(min(c.get(n, 0.0), CAPS[n]))
    return out


def query_x(r, use):
    out = []
    for n in use:
        if n == "voiced":
            out.append(np.log(max(r.get("voiced_ms") or 1, 500) / 8000.0))
        elif n == "range":
            out.append(min(r.get("range", 8.0), 24.0) / 8.0 - 1.0)
        elif n == "steps":
            out.append(min(r.get("steps", 20), 60) / 20.0 - 1.0)
    return out


def load(name, path, cuse, quse, groups, prefixes):
    items = []
    for line in open(path):
        r = json.loads(line)
        if r.get("prefix_ms") is not None and not prefixes:
            continue
        if not r.get("voiced_ms"):
            continue
        c = r.get("candidates") or []
        x = np.array([cand_x(a, cuse) for a in c], float) if c else np.zeros((0, len(cuse)))
        hit = [i for i, a in enumerate(c) if a["label"]]
        group = groups.get(r["wav"], f"song:{r.get('expected')}")
        items.append(dict(set=name, group=group, x=x, y=hit[0] if hit else -1, q=np.array(query_x(r, quse), float),
                          wav=r["wav"], prefix=r.get("prefix_ms"), voiced=r.get("voiced_ms"),
                          songs=[a["song"] for a in c]))
    return items


def split(p, d, dq):
    return p[:d], p[d], p[d + 1:d + 1 + dq]


def nll(p, data, d, dq, l2):
    w, c0, cq = split(p, d, dq)
    total = 0.0
    g = np.zeros_like(p)
    for it, wt in data:
        x, y, q = it["x"], it["y"], it["q"]
        z = x @ w if len(x) else np.zeros(0)
        cn = c0 + q @ cq
        m = max(z.max() if len(z) else cn, cn)
        e = np.exp(z - m)
        en = np.exp(cn - m)
        s = e.sum() + en
        pz, pn = e / s, en / s
        if y >= 0:
            total -= wt * (z[y] - m - np.log(s))
            g[:d] -= wt * (x[y] - pz @ x)
            g[d] += wt * pn
            g[d + 1:] += wt * pn * q
        else:
            total -= wt * (cn - m - np.log(s))
            if len(x):
                g[:d] += wt * (pz @ x)
            g[d] -= wt * (1 - pn)
            g[d + 1:] -= wt * (1 - pn) * q
    total += 0.5 * l2 * (p @ p)
    g += l2 * p
    return total, g


def fit(data, d, dq, l2):
    return minimize(nll, np.zeros(d + 1 + dq), args=(data, d, dq, l2), jac=True, method="L-BFGS-B").x


def probs(p, it, d, dq):
    w, c0, cq = split(p, d, dq)
    z = it["x"] @ w if len(it["x"]) else np.zeros(0)
    cn = c0 + it["q"] @ cq
    m = max(z.max() if len(z) else cn, cn)
    e = np.exp(z - m)
    return e / (e.sum() + np.exp(cn - m))


def logit(p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def platt(p, y):
    x = logit(np.asarray(p))
    y = np.asarray(y, float)

    def loss(ab):
        q = 1 / (1 + np.exp(-(ab[0] + ab[1] * x)))
        return -np.sum(y * np.log(q + 1e-9) + (1 - y) * np.log(1 - q + 1e-9))

    return minimize(loss, np.array([0.0, 1.0]), method="Nelder-Mead").x


def answers_at(s, ok, precision):
    """Share of queries that can be answered at this precision, best scores first."""
    if not len(s):
        return 0.0
    o = np.argsort(-s)
    cum = np.cumsum(ok[o]) / np.arange(1, len(o) + 1)
    k = np.flatnonzero(cum >= precision)
    return (k.max() + 1) / len(o) if len(k) else 0.0


def report(name, sub):
    n = max(1, len(sub))
    top = lambda k: sum(1 for it in sub if it["rank"] and it["rank"] <= k) / n
    s = np.array([it["score"] for it in sub])
    ok = np.array([it["ok"] for it in sub])
    parts = [f"{name:12s} n={len(sub):5d} top1={top(1):.4f} top10={top(10):.4f}"]
    for th in TIERS:
        m = s >= th
        parts.append(f">={th}: prec {ok[m].mean() if m.any() else float('nan'):.3f} share {m.mean():.3f}")
    parts.append(f"answers at 90% {answers_at(s, ok, 0.9):.3f}")
    print(" | ".join(parts))


def reliability(items):
    s = np.array([it["score"] for it in items])
    c = np.array([it["ok"] for it in items], float)
    print("reliability of the calibration sets (cross-validated): bin, n, mean score, share right")
    ece = 0.0
    for lo, hi in [(0, .05), (.05, .1), (.1, .2), (.2, .35), (.35, .5), (.5, .65), (.65, .75), (.75, .85), (.85, .9),
                   (.9, 1.01)]:
        m = (s >= lo) & (s < hi)
        if m.any():
            ece += abs(s[m].mean() - c[m].mean()) * m.sum()
            print(f"  {lo:.2f}-{min(hi, 1):.2f} {m.sum():6d} {s[m].mean():.3f} {c[m].mean():.3f}")
    print(f"  ECE {ece / max(1, len(s)):.4f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("--groups")
    ap.add_argument("--calibrate")
    ap.add_argument("--weights", default="")
    ap.add_argument("--cand", default=",".join(CAND))
    ap.add_argument("--query", default=",".join(QUERY))
    ap.add_argument("--prefixes", action="store_true")
    ap.add_argument("--prefix-weight", type=float, default=1.0, help="weight of prefix rows relative to full ones")
    ap.add_argument("--l2", type=float, default=0.05)
    ap.add_argument("--dump")
    a = ap.parse_args()
    cuse = [x for x in a.cand.split(",") if x]
    quse = [x for x in a.query.split(",") if x]
    d, dq = len(cuse), len(quse)
    groups = json.load(open(a.groups)) if a.groups else {}
    weights = {k: float(v) for k, v in (x.split("=") for x in a.weights.split(",") if x)}
    names, items = [], []
    for inp in a.inputs:
        name, path = inp.split("=", 1)
        names.append(name)
        items += load(name, path, cuse, quse, groups, a.prefixes)
    calibrate = (a.calibrate or names[0]).split(",")
    counts = {n: sum(1 for it in items if it["set"] == n) for n in names}
    mean = np.mean(list(counts.values()))
    wmap = {n: weights.get(n, 1.0) * mean / max(1, counts[n]) for n in names}
    weight = lambda it: wmap[it["set"]] * (a.prefix_weight if it["prefix"] is not None else 1.0)
    # Folds by group, shared across inputs.
    keys = sorted(set(it["group"] for it in items))
    perm = np.random.default_rng(11).permutation(len(keys))
    fold = {k: int(perm[i] % 2) for i, k in enumerate(keys)}
    for it in items:
        it["fold"] = fold[it["group"]]
    for f in (0, 1):
        p = fit([(it, weight(it)) for it in items if it["fold"] != f and weight(it) > 0], d, dq, a.l2)
        for it in items:
            if it["fold"] != f:
                continue
            pr = probs(p, it, d, dq)
            if len(pr):
                o = np.argsort(-pr)
                it["rank"] = int(np.where(o == it["y"])[0][0]) + 1 if it["y"] >= 0 else None
                it["p1"] = float(pr[o[0]])
                it["p2"] = float(pr[o[1]]) if len(o) > 1 else 0.0
                it["top"] = it["songs"][o[0]]
                it["ok"] = bool(o[0] == it["y"])
            else:
                it["rank"], it["p1"], it["p2"], it["top"], it["ok"] = None, 0.0, 0.0, None, False
    cal = [it for it in items if it["set"] in calibrate and it["prefix"] is None]
    ab = {f: platt([it["p1"] for it in cal if it["fold"] != f], [it["ok"] for it in cal if it["fold"] != f])
          for f in (0, 1)}
    for it in items:
        A, B = ab[it["fold"]]
        it["score"] = float(1 / (1 + np.exp(-(A + B * logit(it["p1"])))))
        it["score2"] = float(1 / (1 + np.exp(-(A + B * logit(it["p2"]))))) if it["p2"] > 0 else 0.0
    print("candidate features", cuse, "| query traits", quse, "| weights", weights or "equal")
    for name in names:
        report(name, [it for it in items if it["set"] == name and it["prefix"] is None])
        if a.prefixes:
            for pre in sorted(set(it["prefix"] for it in items if it["set"] == name and it["prefix"] is not None)):
                report(f"  first {pre / 1000:.0f}s",
                       [it for it in items if it["set"] == name and it["prefix"] == pre])
    reliability(cal)
    if a.dump:
        with open(a.dump, "w") as fh:
            for it in items:
                fh.write(json.dumps({k: it[k] for k in ("set", "wav", "prefix", "voiced", "rank", "p1", "ok",
                                                        "score", "score2", "top")}) + "\n")
    p = fit([(it, weight(it)) for it in items if weight(it) > 0], d, dq, a.l2)
    for it in items:
        pr = probs(p, it, d, dq)
        it["p1"] = float(pr.max()) if len(pr) else 0.0
        it["ok"] = bool(len(pr) and int(np.argmax(pr)) == it["y"])
    A, B = platt([it["p1"] for it in cal], [it["ok"] for it in cal])
    w, c0, cq = split(p, d, dq)
    fmt = lambda v: ", ".join(f"{x:.4f}" for x in v)
    print("\nmatcher.rs (fitted on everything):")
    print(f"// candidate features: {', '.join(cuse)}; query traits: {', '.join(quse)}")
    print(f"const MODEL_WEIGHTS: [f32; {d}] = [{fmt(w)}];")
    print(f"const MODEL_NONE: f32 = {c0:.4f};")
    print(f"const MODEL_NONE_QUERY: [f32; {dq}] = [{fmt(cq)}];")
    print(f"const CALIBRATION: (f32, f32) = ({A:.4f}, {B:.4f});")


if __name__ == "__main__":
    main()
