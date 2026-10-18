"""Fit the matcher's encoder terms on `mf eval qbsh --encoder --out` results.

Usage:
    python fit_encoder_terms.py --folds folds.json [options] NAME=results.jsonl [...]

The score model (fit_fusion.py) stays as shipped: its candidate weights, its
"none of these" option and its calibration are read from matcher.rs and kept
fixed, so searches without the encoder score exactly as before. Searches with
the encoder add, per candidate, w · [enc_best, enc_hit] (the encoder's best
window cosine for the song and whether it ranked the song among its first 200
hits), shift "none" by a constant and get their own Platt scaling. Only those
four numbers (plus the two of the calibration) are fitted, on real hums.

Cross-validation uses the fixed folds of --folds (wav -> 0 or 1, e.g. CHAD's
song-disjoint halves V and T): each fold is scored by the model fitted on the
other one, and those held-out numbers are what the report shows. The constants
printed for matcher.rs are fitted on both folds.

Options:
    --folds FILE         JSON object: recording path -> fold (0 or 1); rows of other
                         recordings are ignored
    --matcher FILE       matcher.rs to read the fixed model from (default: the repo's)
    --calibrate A,B      inputs whose rows calibrate the scores (default: all)
    --prefixes           also fit on prefix rows (`--prefix-ms`, >= 4 s voiced)
    --prefix-weight W    weight of a prefix row relative to a full one (default 0.5)
    --features A,B       encoder features (default enc_best,enc_hit; also enc_score,
                         enc_logrank)
    --hit-rank N         enc_hit counts hits up to this rank (default 200)
    --dump FILE          held-out per-query predictions (JSONL)
"""

import argparse
import json
import os
import re
import sys

import numpy as np
from scipy.optimize import minimize

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # fit_fusion sits next to this file (also under python -I)
import fit_fusion as ff  # noqa: E402
DEFAULT_MATCHER = os.path.join(HERE, "..", "..", "core", "crates", "mfcore", "src", "melody", "matcher.rs")


def floats(text):
    return [float(x) for x in re.findall(r"-?\d+(?:\.\d+)?(?:e-?\d+)?", text)]


def read_model(path):
    src = open(path).read()
    grab = lambda name: re.search(rf"const {name}: [^=]+=\s*([^;]+);", src, re.S).group(1)
    weights = floats(grab("MODEL_WEIGHTS").split("=")[-1])[-10:]
    none = floats(grab("MODEL_NONE"))[0]
    nq = floats(grab("MODEL_NONE_QUERY").split("=")[-1])[-3:]
    cal = floats(grab("CALIBRATION"))
    return np.array(weights), none, np.array(nq), tuple(cal[:2])


def enc_x(c, use, hit_rank):
    out = []
    for n in use:
        rank = c.get("enc_rank") or 0
        if n == "enc_best":
            out.append(max(c.get("enc_best", 0.0), 0.0) if 0 < rank <= hit_rank else 0.0)
        elif n == "enc_hit":
            out.append(1.0 if 0 < rank <= hit_rank else 0.0)
        elif n == "enc_score":
            out.append(min(c.get("enc_score", 0.0), 8.0) / 4.0)
        elif n == "enc_logrank":
            out.append(np.log(hit_rank / rank) / np.log(hit_rank) if 0 < rank <= hit_rank else 0.0)
        else:
            raise SystemExit(f"unknown feature {n}")
    return out


def load(name, path, folds, use, hit_rank, base_w, none0, nq, prefixes, min_voiced=4000):
    items = []
    for line in open(path):
        r = json.loads(line)
        if r["wav"] not in folds or not r.get("voiced_ms"):
            continue
        prefix = r.get("prefix_ms")
        if prefix is not None and (not prefixes or r["voiced_ms"] < min_voiced):
            continue
        c = r.get("candidates") or []
        base = np.array([ff.cand_x(a, ff.CAND) for a in c], float) if c else np.zeros((0, 10))
        x = np.array([enc_x(a, use, hit_rank) for a in c], float) if c else np.zeros((0, len(use)))
        hit = [i for i, a in enumerate(c) if a["label"]]
        q = np.array(ff.query_x(r, ff.QUERY), float)
        items.append(dict(set=name, wav=r["wav"], fold=folds[r["wav"]], prefix=prefix, voiced=r["voiced_ms"],
                          u0=base @ base_w if len(c) else np.zeros(0), x=x, y=hit[0] if hit else -1,
                          n0=none0 + q @ nq, songs=[a["song"] for a in c]))
    return items


def nll(p, data, l2):
    w, delta = p[:-1], p[-1]
    total, g = 0.0, np.zeros_like(p)
    for it, wt in data:
        z = it["u0"] + (it["x"] @ w if len(it["x"]) else 0.0)
        cn = it["n0"] + delta
        m = max(z.max() if len(z) else cn, cn)
        e, en = np.exp(z - m), np.exp(cn - m)
        s = e.sum() + en
        pz, pn = e / s, en / s
        if it["y"] >= 0:
            total -= wt * (z[it["y"]] - m - np.log(s))
            g[:-1] -= wt * (it["x"][it["y"]] - pz @ it["x"])
            g[-1] += wt * pn
        else:
            total -= wt * (cn - m - np.log(s))
            if len(z):
                g[:-1] += wt * (pz @ it["x"])
            g[-1] -= wt * (1 - pn)
    total += 0.5 * l2 * (p @ p)
    g += l2 * p
    return total, g


def fit(data, d, l2):
    return minimize(nll, np.zeros(d + 1), args=(data, l2), jac=True, method="L-BFGS-B").x


def predict(p, it):
    w, delta = p[:-1], p[-1]
    z = it["u0"] + (it["x"] @ w if len(it["x"]) else 0.0)
    cn = it["n0"] + delta
    if not len(z):
        return None, 0.0
    m = max(z.max(), cn)
    e = np.exp(z - m)
    pr = e / (e.sum() + np.exp(cn - m))
    return np.argsort(-pr), pr


def score(items, p, ab):
    for it in items:
        order, pr = predict(p, it)
        if order is None:
            it.update(rank=None, p1=0.0, ok=False, top=None)
        else:
            it["rank"] = int(np.where(order == it["y"])[0][0]) + 1 if it["y"] >= 0 else None
            it["p1"] = float(pr[order[0]])
            it["ok"] = bool(order[0] == it["y"])
            it["top"] = it["songs"][order[0]]
        A, B = ab
        it["score"] = float(1 / (1 + np.exp(-(A + B * ff.logit(it["p1"]))))) if it["p1"] > 0 else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("--folds", required=True)
    ap.add_argument("--matcher", default=DEFAULT_MATCHER)
    ap.add_argument("--calibrate")
    ap.add_argument("--prefixes", action="store_true")
    ap.add_argument("--prefix-weight", type=float, default=0.5)
    ap.add_argument("--features", default="enc_best,enc_hit")
    ap.add_argument("--hit-rank", type=int, default=200)
    ap.add_argument("--l2", type=float, default=0.05)
    ap.add_argument("--dump")
    a = ap.parse_args()
    use = [x for x in a.features.split(",") if x]
    base_w, none0, nq, base_cal = read_model(a.matcher)
    folds = {k: int(v) for k, v in json.load(open(a.folds)).items()}
    names, items = [], []
    for inp in a.inputs:
        name, path = inp.split("=", 1)
        names.append(name)
        items += load(name, path, folds, use, a.hit_rank, base_w, none0, nq, a.prefixes)
    calibrate = (a.calibrate or ",".join(names)).split(",")
    weight = lambda it: a.prefix_weight if it["prefix"] is not None else 1.0
    cal = lambda it: it["set"] in calibrate

    print(f"fixed model from {os.path.relpath(a.matcher)}; encoder features {use}; "
          f"{len(items)} rows ({sum(1 for it in items if it['prefix'] is None)} whole takes)")
    # Today's model on the same rows: encoder terms zero, the shipped calibration.
    score(items, np.zeros(len(use) + 1), base_cal)
    held = {}
    for f in (0, 1):
        train = [it for it in items if it["fold"] != f]
        p = fit([(it, weight(it)) for it in train], len(use), a.l2)
        score(train, p, (0.0, 1.0))
        ab = ff.platt([it["p1"] for it in train if cal(it)], [it["ok"] for it in train if cal(it)])
        test = [dict(it) for it in items if it["fold"] == f]
        before = [dict(it) for it in test]
        score(test, p, ab)
        held[f] = (p, ab, test, before)
        print(f"\nfold {f} held out (fitted on fold {1 - f}): w {np.round(p[:-1], 4).tolist()} "
              f"none {p[-1]:+.4f} calibration ({ab[0]:.4f}, {ab[1]:.4f})")
        for name in names:
            for label, rows in (("encoder terms 0", before), ("fitted", test)):
                sub = [it for it in rows if it["set"] == name and it["prefix"] is None]
                ff.report(f"{name} {label}", sub)
            pre = [it for it in test if it["set"] == name and it["prefix"] is not None]
            if pre:
                ff.report(f"{name} prefixes", pre)
    both = [it for f in (0, 1) for it in held[f][2]]
    print()
    for name in names:
        ff.report(f"{name} held-out", [it for it in both if it["set"] == name and it["prefix"] is None])
    ff.reliability([it for it in both if cal(it)])
    if a.dump:
        with open(a.dump, "w") as fh:
            for it in both:
                fh.write(json.dumps({k: it[k] for k in ("set", "wav", "fold", "prefix", "voiced", "rank", "p1", "ok",
                                                        "score", "top")}) + "\n")
    p = fit([(it, weight(it)) for it in items], len(use), a.l2)
    score(items, p, (0.0, 1.0))
    A, B = ff.platt([it["p1"] for it in items if cal(it)], [it["ok"] for it in items if cal(it)])
    print("\nmatcher.rs (fitted on both folds):")
    print(f"// encoder features: {', '.join(use)} (hits up to rank {a.hit_rank})")
    print(f"const ENC_WEIGHTS: [f32; {len(use)}] = [{', '.join(f'{x:.4f}' for x in p[:-1])}];")
    print(f"const ENC_NONE: f32 = {p[-1]:.4f};")
    print(f"const CALIBRATION_ENCODER: (f32, f32) = ({A:.4f}, {B:.4f});")
    print(f"mf eval qbsh --encoder-model {','.join(f'{x:.4f}' for x in p[:-1])},{p[-1]:.4f},{A:.4f},{B:.4f}")


if __name__ == "__main__":
    main()
