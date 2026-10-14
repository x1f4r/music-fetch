"""Fit the impression detector's logistic model on `mf eval detect --out` results.

Usage: python fit_detector.py detect.jsonl

Uses the features each clip had after 4 s of sounding audio (or at its end).
Cross-validates with folds grouped by source file, picks the decision threshold
that keeps music → impression errors under 2%, an early-verdict confidence that
keeps early errors under 1%, prints the confusion matrices and the constants
for detect.rs.
"""

import json
import sys

import numpy as np

NAMES = ["voiced", "confidence", "jumps", "step", "flatness", "centroid_khz", "low_share", "onsets_per_s", "level_sd", "harmonicity"]
CHECKPOINTS = [2000, 2500, 3000, 3500, 4000]


def load(path):
    clips = []
    for line in open(path):
        r = json.loads(line)
        if "error" in r:
            continue
        snaps = {s["at"]: s["features"] for s in r["checkpoints"]}
        final = r["final"]["features"]
        seq = [snaps.get(c) for c in CHECKPOINTS]
        clips.append({"wav": r["wav"], "y": 1.0 if r["impression"] else 0.0, "seq": seq, "x4": snaps.get(4000, final)})
    return clips


def source(wav):
    for key in ("twinkle", "hum.wav", "MIR-QBSH", "IOACAS", "whistle"):
        if key in wav:
            return key
    return "music"


def weights(clips):
    """Equal total weight per class, and per source within the impression class."""
    counts = {}
    for c in clips:
        counts[source(c["wav"])] = counts.get(source(c["wav"]), 0) + 1
    sources = [s for s in counts if s != "music"]
    return np.array([0.5 / counts["music"] if c["y"] == 0 else 0.5 / len(sources) / counts[source(c["wav"])] for c in clips])


def fit(x, y, l2=1.0, iters=100, cw=None):
    xb = np.hstack([np.ones((len(x), 1)), x])
    w = np.zeros(xb.shape[1])
    # Balance the classes so the threshold, not the prior, sets the trade-off.
    if cw is None:
        cw = np.where(y == 1, 0.5 / y.mean(), 0.5 / (1 - y.mean()))
    cw = cw * len(y)
    for _ in range(iters):
        p = 1 / (1 + np.exp(-xb @ w))
        g = xb.T @ (cw * (p - y)) + l2 * np.r_[0, w[1:]]
        h = (xb * (cw * p * (1 - p))[:, None]).T @ xb + l2 * np.diag(np.r_[0, np.ones(len(w) - 1)])
        step = np.linalg.solve(h, g)
        w -= step
        if np.abs(step).max() < 1e-9:
            break
    return w


def prob(w, mean, scale, x):
    z = w[0] + ((np.asarray(x) - mean) / scale) @ w[1:]
    return 1 / (1 + np.exp(-z))


def decide(w, mean, scale, clip, early, threshold):
    """(verdict, checkpoint index) as the detector would decide."""
    for k, f in enumerate(clip["seq"]):
        if f is None:
            break
        p = prob(w, mean, scale, f)
        if p >= early:
            return 1.0, k
        if p <= 1 - early:
            return 0.0, k
    p = prob(w, mean, scale, clip["x4"])
    return (1.0 if p >= threshold else 0.0), len(CHECKPOINTS) - 1


def main(path):
    clips = load(path)
    files = sorted({c["wav"] for c in clips})
    rng = np.random.default_rng(3)
    fold_of = {f: int(rng.integers(0, 5)) for f in files}
    held = []
    for f in range(5):
        train = [c for c in clips if fold_of[c["wav"]] != f]
        x = np.array([c["x4"] for c in train])
        y = np.array([c["y"] for c in train])
        mean, scale = x.mean(0), x.std(0) + 1e-6
        w = fit((x - mean) / scale, y, cw=weights(train))
        for c in clips:
            if fold_of[c["wav"]] == f:
                held.append((c, w, mean, scale))
    # Threshold for < 2% music → impression at the deadline (cross-validated).
    music_p = np.sort([prob(w, m, s, c["x4"]) for c, w, m, s in held if c["y"] == 0])
    threshold = max(0.5, float(music_p[int(np.ceil(0.985 * len(music_p))) - 1]) + 1e-3) if len(music_p) else 0.5
    best_early = 0.99
    for early in [0.9, 0.93, 0.95, 0.97, 0.98, 0.99, 0.995]:
        errs = [c for c, w, m, s in held if decide(w, m, s, c, early, threshold)[0] != c["y"] and decide(w, m, s, c, early, threshold)[1] < len(CHECKPOINTS) - 1]
        if len(errs) <= 0.01 * len(held):
            best_early = early
            break
    print(f"threshold {threshold:.4f}, early confidence {best_early}")
    cm = np.zeros((2, 2), dtype=int)
    when = []
    for c, w, m, s in held:
        v, k = decide(w, m, s, c, best_early, threshold)
        cm[int(c["y"]), int(v)] += 1
        when.append(CHECKPOINTS[k])
    print("cross-validated confusion matrix (rows: truth, columns: verdict)")
    print("                  music  impression")
    print(f"music        {cm[0, 0]:10d}  {cm[0, 1]:10d}   ({cm[0, 1] / max(cm[0].sum(), 1):.2%} false impressions)")
    print(f"impression   {cm[1, 0]:10d}  {cm[1, 1]:10d}   ({cm[1, 1] / max(cm[1].sum(), 1):.2%} recall)")
    print(f"accuracy {np.trace(cm) / cm.sum():.4f}; verdicts at {np.median(when):.0f} ms median of sounding audio")
    by_source = {}
    for c, w_, m, s in held:
        v, _ = decide(w_, m, s, c, best_early, threshold)
        e = by_source.setdefault(source(c["wav"]), [0, 0])
        e[0] += 1
        e[1] += int(v != c["y"])
    print("errors by source: " + ", ".join(f"{k} {v[1]}/{v[0]}" for k, v in by_source.items()))
    x = np.array([c["x4"] for c in clips])
    y = np.array([c["y"] for c in clips])
    mean, scale = x.mean(0), x.std(0) + 1e-6
    w = fit((x - mean) / scale, y, cw=weights(clips))
    print("\n// features: " + ", ".join(NAMES))
    print("const BIAS: f32 = %.4f;" % w[0])
    print("const WEIGHTS: [f32; FEATURES] = [%s];" % ", ".join("%.4f" % v for v in w[1:]))
    print("const MEANS: [f32; FEATURES] = [%s];" % ", ".join("%.4f" % v for v in mean))
    print("const SCALES: [f32; FEATURES] = [%s];" % ", ".join("%.4f" % v for v in scale))
    print("const DECIDE_THRESHOLD: f32 = %.4f;" % threshold)
    print("const EARLY_CONFIDENCE: f32 = %.3f;" % best_early)


if __name__ == "__main__":
    main(sys.argv[1])
