"""Fit the matcher's score model on `mf eval qbsh --out` results.

Usage: python fit_fusion.py results.jsonl [results.jsonl ...]

The model is a conditional logit: each candidate gets z = w·x, and a query's
candidates plus a "none of these" option with logit c share a softmax. The
probability of a candidate is then calibrated against its competitors and the
chance that the right song is not in the list at all. Fitted by maximum
likelihood with 5-fold cross-validation grouped by query. Prints re-ranking
metrics per input, a reliability table for the top-1 probability, and the
constants for matcher.rs. Only the candidates in the results (top 50 per query)
can be re-ranked, so queries whose song is missing there stay misses.
"""

import json
import sys

import numpy as np
from scipy.optimize import minimize

NAMES = ["votes", "align", "linear", "dtw", "edit"]


def features(c):
    return [min(c["votes"], 4.0), min(c.get("align", 1.0), 2.0), min(c["linear"], 3.0), min(c["dtw"], 4.0), min(c["edit"], 2.0)]


def load(paths):
    queries = []
    for k, path in enumerate(paths):
        for line in open(path):
            r = json.loads(line)
            cands = r.get("candidates") or []
            if not cands:
                queries.append((k, None, -1))
                continue
            x = np.array([features(c) for c in cands], dtype=np.float64)
            hit = [i for i, c in enumerate(cands) if c["label"]]
            queries.append((k, x, hit[0] if hit else -1))
    return queries


def nll(params, data, l2):
    w, c = params[:-1], params[-1]
    total = 0.0
    grad = np.zeros_like(params)
    for x, y in data:
        z = x @ w
        m = max(z.max(), c)
        e = np.exp(z - m)
        en = np.exp(c - m)
        s = e.sum() + en
        p = e / s
        pn = en / s
        if y >= 0:
            total -= z[y] - m - np.log(s)
            grad[:-1] -= x[y] - p @ x
            grad[-1] += pn
        else:
            total -= c - m - np.log(s)
            grad[:-1] += p @ x
            grad[-1] -= 1 - pn
    total += 0.5 * l2 * (w @ w)
    grad[:-1] += l2 * w
    return total, grad


def fit(data, l2=1e-2):
    x0 = np.zeros(len(NAMES) + 1)
    r = minimize(nll, x0, args=(data, l2), jac=True, method="L-BFGS-B")
    return r.x


def probs(params, x):
    w, c = params[:-1], params[-1]
    z = x @ w
    m = max(z.max(), c)
    e = np.exp(z - m)
    return e / (e.sum() + np.exp(c - m))


def logit(p):
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def platt(p, y):
    """Fit P = σ(a + b·logit(p))."""
    x = logit(np.asarray(p))
    y = np.asarray(y, dtype=float)

    def loss(ab):
        z = ab[0] + ab[1] * x
        q = 1 / (1 + np.exp(-z))
        eps = 1e-9
        return -np.sum(y * np.log(q + eps) + (1 - y) * np.log(1 - q + eps))

    return minimize(loss, np.array([0.0, 1.0]), method="Nelder-Mead").x


def main(paths):
    queries = load(paths)
    rng = np.random.default_rng(7)
    folds = rng.integers(0, 5, len(queries))
    ranks = [None] * len(queries)
    top = [None] * len(queries)
    held = []  # (probability, correct) of each query's top 5, out of fold
    for f in range(5):
        train = [(q[1], q[2]) for q, fold in zip(queries, folds) if fold != f and q[1] is not None]
        params = fit(train)
        for i, (q, fold) in enumerate(zip(queries, folds)):
            if fold != f or q[1] is None:
                continue
            p = probs(params, q[1])
            order = np.argsort(-p)
            if q[2] >= 0:
                ranks[i] = int(np.where(order == q[2])[0][0]) + 1
            top[i] = (float(p[order[0]]), order[0] == q[2])
            held.extend((float(p[o]), o == q[2], i) for o in order[:5])
    # Platt scaling of the probabilities, itself cross-validated by query fold.
    calibrated = {}
    for f in range(5):
        fit_on = [(p, y) for p, y, i in held if folds[i] != f]
        a, b = platt([x[0] for x in fit_on], [x[1] for x in fit_on])
        for i, t in enumerate(top):
            if t is not None and folds[i] == f:
                calibrated[i] = (float(1 / (1 + np.exp(-(a + b * logit(t[0]))))), t[1])
    top = [calibrated.get(i, top[i]) for i in range(len(top))]
    a_final, b_final = platt([x[0] for x in held], [x[1] for x in held])
    for k, path in enumerate(paths):
        idx = [i for i, q in enumerate(queries) if q[0] == k]
        n = len(idx)
        t = lambda r: sum(1 for i in idx if ranks[i] is not None and ranks[i] <= r) / n
        mrr = sum(1 / ranks[i] for i in idx if ranks[i] is not None) / n
        print(f"{path}: {n} queries, cross-validated top-1 {t(1):.4f} top-3 {t(3):.4f} top-10 {t(10):.4f} MRR {mrr:.4f}")
    pairs = [x for x in top if x is not None]
    s = np.array([p[0] for p in pairs])
    c = np.array([p[1] for p in pairs], dtype=float)
    print("\nreliability of the top-1 probability (cross-validated, both corpora)")
    print(" score bin      n   mean score   accuracy")
    ece = 0.0
    for b in range(10):
        lo, hi = b / 10, (b + 1) / 10 + (1e-9 if b == 9 else 0)
        m = (s >= lo) & (s < hi)
        if m.any():
            ece += abs(s[m].mean() - c[m].mean()) * m.sum()
            print(f" {lo:.1f}-{min(hi, 1):.1f}  {m.sum():6d}   {s[m].mean():.3f}       {c[m].mean():.3f}")
    print(f" expected calibration error {ece / len(s):.4f}")
    params = fit([(q[1], q[2]) for q in queries if q[1] is not None])
    print("\n// features: " + ", ".join(NAMES))
    print("const MODEL_WEIGHTS: [f32; %d] = [%s];" % (len(NAMES), ", ".join("%.4f" % v for v in params[:-1])))
    print("const MODEL_NONE: f32 = %.4f;" % params[-1])
    print("const CALIBRATION: (f32, f32) = (%.4f, %.4f);" % (a_final, b_final))


if __name__ == "__main__":
    main(sys.argv[1:])
