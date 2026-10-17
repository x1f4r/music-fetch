"""Fit the matcher's score model on `mf eval qbsh --out` results.

Usage: python fit_fusion.py [--calibrate-on K,K,...] [--hold-out B.jsonl,...] results.jsonl [results.jsonl ...]

The model is a conditional logit: each candidate gets z = w·x, and a query's
candidates plus a "none of these" option with logit c share a softmax. The
probability of a candidate is then calibrated against its competitors and the
chance that the right song is not in the list at all. Fitted by maximum
likelihood with 5-fold cross-validation grouped by query; Platt scaling of the
top-1 probability is fitted on the inputs listed in --calibrate-on (indices into
the result files, e.g. the real hums only), so scores read as P(correct) on
real queries. Prints re-ranking metrics per input (cross-validated), metrics on
held-out result files, a reliability table and the constants for matcher.rs.
Only the candidates in the results can be re-ranked, so queries whose song is
missing there stay misses.
"""

import argparse
import json

import numpy as np
from scipy.optimize import minimize

NAMES = ["votes", "linear", "dtw", "coverage", "tempo_dev", "align", "edit", "note_votes"]
CAPS = [4.0, 3.0, 4.0, 1.0, 2.0, 2.0, 2.0, 6.0]


def features(c):
    return [min(c.get(n, 0.0), cap) for n, cap in zip(NAMES, CAPS)]


def load(path, k):
    queries = []
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
    for x, y, weight in data:
        z = x @ w
        m = max(z.max(), c)
        e = np.exp(z - m)
        en = np.exp(c - m)
        s = e.sum() + en
        p = e / s
        pn = en / s
        if y >= 0:
            total -= weight * (z[y] - m - np.log(s))
            grad[:-1] -= weight * (x[y] - p @ x)
            grad[-1] += weight * pn
        else:
            total -= weight * (c - m - np.log(s))
            grad[:-1] += weight * (p @ x)
            grad[-1] -= weight * (1 - pn)
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


def weights_for(queries):
    """Each input file weighs the same in the fit, whatever its size."""
    counts = {}
    for q in queries:
        counts[q[0]] = counts.get(q[0], 0) + 1
    mean = np.mean(list(counts.values()))
    return {k: mean / n for k, n in counts.items()}


def report(name, ranks, top_cal):
    n = len(ranks)
    t = lambda r: sum(1 for x in ranks if x is not None and x <= r) / max(1, n)
    mrr = sum(1 / x for x in ranks if x is not None) / max(1, n)
    print(f"{name}: {n} queries, top-1 {t(1):.4f} top-3 {t(3):.4f} top-10 {t(10):.4f} MRR {mrr:.4f}")


def reliability(pairs, title):
    s = np.array([p[0] for p in pairs])
    c = np.array([p[1] for p in pairs], dtype=float)
    print(f"\nreliability of the top-1 score ({title})")
    print(" score bin      n   mean score   accuracy")
    ece = 0.0
    for b in range(10):
        lo, hi = b / 10, (b + 1) / 10 + (1e-9 if b == 9 else 0)
        m = (s >= lo) & (s < hi)
        if m.any():
            ece += abs(s[m].mean() - c[m].mean()) * m.sum()
            print(f" {lo:.1f}-{min(hi, 1):.1f}  {m.sum():6d}   {s[m].mean():.3f}       {c[m].mean():.3f}")
    print(f" expected calibration error {ece / max(1, len(s)):.4f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", nargs="+")
    ap.add_argument("--calibrate-on", default="", help="comma-separated indices of the result files used for Platt scaling")
    ap.add_argument("--hold-out", default="", help="comma-separated result files evaluated with the final model")
    args = ap.parse_args()
    paths = args.results
    queries = [q for k, p in enumerate(paths) for q in load(p, k)]
    cal_on = {int(x) for x in args.calibrate_on.split(",") if x != ""} or set(range(len(paths)))
    wmap = weights_for(queries)
    rng = np.random.default_rng(7)
    folds = rng.integers(0, 5, len(queries))
    ranks = [None] * len(queries)
    top = [None] * len(queries)
    for f in range(5):
        train = [(q[1], q[2], wmap[q[0]]) for q, fold in zip(queries, folds) if fold != f and q[1] is not None]
        params = fit(train)
        for i, (q, fold) in enumerate(zip(queries, folds)):
            if fold != f or q[1] is None:
                continue
            p = probs(params, q[1])
            order = np.argsort(-p)
            if q[2] >= 0:
                ranks[i] = int(np.where(order == q[2])[0][0]) + 1
            top[i] = (float(p[order[0]]), bool(order[0] == q[2]))
    # Platt scaling of the top-1 probability, cross-validated by query fold.
    calibrated = {}
    for f in range(5):
        fit_on = [(top[i][0], top[i][1]) for i in range(len(queries)) if top[i] and folds[i] != f and queries[i][0] in cal_on]
        a, b = platt([x[0] for x in fit_on], [x[1] for x in fit_on])
        for i in range(len(queries)):
            if top[i] is not None and folds[i] == f:
                calibrated[i] = (float(1 / (1 + np.exp(-(a + b * logit(top[i][0]))))), top[i][1])
    print("cross-validated re-ranking:")
    for k, path in enumerate(paths):
        idx = [i for i, q in enumerate(queries) if q[0] == k]
        report(path, [ranks[i] for i in idx], None)
    reliability([calibrated[i] for i in calibrated if queries[i][0] in cal_on], "cross-validated, calibration inputs")
    params = fit([(q[1], q[2], wmap[q[0]]) for q in queries if q[1] is not None])
    a_final, b_final = platt([top[i][0] for i in range(len(queries)) if top[i] and queries[i][0] in cal_on],
                             [top[i][1] for i in range(len(queries)) if top[i] and queries[i][0] in cal_on])
    for path in [p for p in args.hold_out.split(",") if p]:
        held = load(path, -1)
        hr, ht = [], []
        for _, x, y in held:
            if x is None:
                hr.append(None)
                continue
            p = probs(params, x)
            order = np.argsort(-p)
            hr.append(int(np.where(order == y)[0][0]) + 1 if y >= 0 else None)
            ht.append((float(1 / (1 + np.exp(-(a_final + b_final * logit(p[order[0]]))))), bool(order[0] == y)))
        report("held out " + path, hr, None)
        reliability(ht, "held out " + path)
    print("\n// features: " + ", ".join(NAMES))
    print("const MODEL_WEIGHTS: [f32; %d] = [%s];" % (len(NAMES), ", ".join("%.4f" % v for v in params[:-1])))
    print("const MODEL_NONE: f32 = %.4f;" % params[-1])
    print("const CALIBRATION: (f32, f32) = (%.4f, %.4f);" % (a_final, b_final))


if __name__ == "__main__":
    main()
