"""Render simulated hums as audio, for pitch tracking by the app's own front end.

Step 1 (render): python -m melenc.synth_audio render <out dir> <n> [procs] [seed]
  Picks training pairs (as in data.py, tracker errors left to the real tracker), renders each
  12-15 s target melody as hum / "oo" / "la" / whistle audio at 16 kHz with jitter, shimmer,
  breath, background noise, a small room and a phone-like mic, and writes queries.tsv plus
  specs.pkl (ref track, song, per-frame reference position, target contour).
Step 2: mf queries --queries <out>/queries.tsv --out <out>/export.jsonl
Step 3 (collect): python -m melenc.synth_audio collect <out dir> <tracked.pkl>
  Aligns each exported contour to its target (small lag search) and keeps
  (contour, ref_pos, track, song) for training.
"""

import json
import os
import pickle
import sys
from multiprocessing import Pool as MPool

import numpy as np
import soundfile as sf
from scipy.ndimage import uniform_filter1d
from scipy.signal import fftconvolve, lfilter

from melenc import data as D
from melenc.refs import DATA, HOME
from melenc.sim import hold_fill, humanize, simulate

SR = 16000
FPS = 20


def _envelope(voiced, rng, n):
    """Per-sample amplitude from a 20 fps voicing mask with soft attack/release."""
    T = n * SR // FPS
    m = np.repeat(voiced.astype(np.float32), SR // FPS)[:T]
    k = int(SR * rng.uniform(0.015, 0.06))
    return uniform_filter1d(m, size=k, mode="constant")


def render(pitch, rng):
    n = len(pitch)
    T = n * SR // FPS
    voiced = np.isfinite(pitch)
    mode = rng.choice(["hum", "oo", "la", "whistle"], p=[0.6, 0.12, 0.18, 0.10])
    f = hold_fill(pitch)
    med = np.median(f[voiced]) if voiced.any() else 60.0
    if mode == "whistle":
        target = rng.uniform(74, 92)
    elif rng.random() < 0.5:
        target = rng.uniform(43, 53)  # low voice
    else:
        target = rng.uniform(53, 64)
    f = f - med + target
    t20 = (np.arange(n) + 0.5) / FPS
    ts = np.arange(T) / SR
    fs = np.interp(ts, t20, f)
    # jitter (fast pitch noise)
    jit = np.interp(ts, np.arange(0, n / FPS, 0.01), rng.normal(0, 1, int(n / FPS / 0.01) + 1)[: len(np.arange(0, n / FPS, 0.01))])
    fs = fs + rng.uniform(0.02, 0.12) * jit
    hz = 440.0 * 2 ** ((fs - 69) / 12)
    phase = 2 * np.pi * np.cumsum(hz) / SR
    amp = _envelope(voiced, rng, n)
    if mode == "la":  # re-articulate each note
        on = np.flatnonzero(np.abs(np.diff(np.round(np.where(voiced, f, 0) * 2))) > 0) + 1
        for i in on:
            a = i * SR // FPS
            L = int(SR * rng.uniform(0.03, 0.08))
            amp[a : a + L] *= np.linspace(rng.uniform(0.1, 0.4), 1, len(amp[a : a + L]))
    shimmer = 1 + rng.uniform(0.02, 0.1) * np.interp(ts, np.arange(0, n / FPS, 0.02), rng.normal(0, 1, len(np.arange(0, n / FPS, 0.02))))
    amp = amp * shimmer
    if mode == "whistle":
        y = np.sin(phase) + rng.uniform(0.0, 0.05) * np.sin(2 * phase)
        breath = rng.normal(0, 1, T)
        breath = breath - lfilter([1], [1, -0.9], breath) * 0.1
        y = y * amp + breath * amp * 10 ** (rng.uniform(-30, -18) / 20)
    else:
        if mode == "hum":
            formants = [(rng.uniform(220, 320), rng.uniform(60, 120), 1.0), (rng.uniform(900, 1300), 150, 10 ** (-rng.uniform(15, 25) / 20))]
            tilt = rng.uniform(1.4, 2.4)
        elif mode == "oo":
            formants = [(rng.uniform(280, 380), 80, 1.0), (rng.uniform(750, 950), 100, 0.4), (2400, 200, 0.05)]
            tilt = rng.uniform(1.0, 1.6)
        else:
            formants = [(rng.uniform(600, 800), 100, 1.0), (rng.uniform(1050, 1350), 120, 0.6), (rng.uniform(2300, 2700), 200, 0.15)]
            tilt = rng.uniform(0.7, 1.2)
        K = int(min(14, 4000 / max(hz.max(), 50)))
        dec = 32  # harmonic amplitudes at 500 Hz control rate
        hzc = hz[::dec]
        y = np.zeros(T)
        for k in range(1, K + 1):
            fk = k * hzc
            env = k ** (-tilt) * (0.05 + sum(g * np.exp(-((fk - F) ** 2) / (2 * B**2)) for F, B, g in formants))
            env = np.where(fk < SR / 2 - 100, env, 0)
            y += np.repeat(env, dec)[:T] * np.sin(k * phase)
        y = y / (np.abs(y).max() + 1e-9) * amp
        y = y + rng.normal(0, 1, T) * amp * 10 ** (rng.uniform(-40, -25) / 20)
    # room
    if rng.random() < 0.4:
        rt = rng.uniform(0.1, 0.5)
        L = int(SR * rt)
        ir = rng.normal(0, 1, L) * np.exp(-6.9 * np.arange(L) / L)
        ir[0] = 1.0 / rng.uniform(0.05, 0.3)
        y = fftconvolve(y, ir / np.abs(ir).sum() * 4, mode="full")[:T]
    # background noise (white / pink / mains hum)
    y = y / (np.sqrt(np.mean(y**2)) + 1e-9)
    snr = rng.uniform(8, 40)
    noise = rng.normal(0, 1, T)
    if rng.random() < 0.5:
        noise = lfilter([1], [1, -0.97], noise)
        noise /= noise.std() + 1e-9
    if rng.random() < 0.2:
        noise = noise + 2 * np.sin(2 * np.pi * rng.choice([50, 60]) * ts)
    y = y + noise * 10 ** (-snr / 20)
    # mic: high-pass, sometimes low-pass
    fc = rng.uniform(60, 250)
    a1 = np.exp(-2 * np.pi * fc / SR)
    y = lfilter([1, -1], [1, -a1], y)
    if rng.random() < 0.5:
        fl = rng.uniform(3000, 7000)
        b = 1 - np.exp(-2 * np.pi * fl / SR)
        y = lfilter([b], [1, b - 1], y)
    y = y / (np.abs(y).max() + 1e-9) * rng.uniform(0.05, 0.8)
    return y.astype(np.float32), mode


_pool = None


def _init(args):
    global _pool
    _pool = D.Pool(**args)


def _spec(job):
    i, seed, out = job
    rng = np.random.default_rng(seed)
    pool = _pool
    kind = rng.choice(["pself", "m2p", "mself", "aself"], p=[0.5, 0.25, 0.2, 0.05])
    L = int(rng.integers(220, 300))
    for _ in range(30):
        if kind == "m2p":
            m = pool.m2p
            j = int(rng.integers(len(m["song"])))
            track = int(m["preview"][j])
            P = pool.track(track)
            M = pool.track(int(m["midi"][j]))
            s, lag = float(m["scale"][j]), int(m["lag"][j])
            start = float(rng.uniform(0, max(1, len(P) - 120)))
            q, pos = simulate(M, lag * s, s, start, rng, length=L, tracker=False)
            song = int(m["song"][j])
        else:
            ids = {"pself": pool.pv, "mself": pool.md, "aself": pool.au}[kind]
            track = int(ids[rng.integers(len(ids))])
            tr = pool.track(track)
            start = float(rng.uniform(0, max(1, len(tr) - 120)))
            a0 = max(0, int(start) - 10)
            seg = tr[a0 : int(start) + 460]
            if kind != "mself" and rng.random() < 0.85:
                seg = humanize(seg, rng)
            q, pos = simulate(seg, -a0, 1.0, start, rng, length=L, tracker=False)
            song = int(pool.refs.song[track])
        if np.isfinite(q).mean() >= 0.5:
            break
    lead = int(rng.integers(2, 12))
    q = np.concatenate([np.full(lead, np.nan, np.float32), q, np.full(int(rng.integers(4, 16)), np.nan, np.float32)])
    pos = np.concatenate([pos[0] - (np.arange(lead, 0, -1)), pos, pos[-1] + np.arange(1, len(q) - lead - len(pos) + 1)])
    y, mode = render(q, rng)
    path = os.path.join(out, "wav", f"{i:06d}.wav")
    sf.write(path, y, SR, subtype="PCM_16")
    return dict(i=i, wav=path, kind=str(kind), mode=str(mode), track=track, song=song, target=q, ref_pos=pos.astype(np.float32))


def cmd_render(out, n, procs, seed, split_path=os.path.join(HOME, "runs", "a", "split.json")):
    """Training songs only: the hold-out and validation songs of a training run's split."""
    os.makedirs(os.path.join(out, "wav"), exist_ok=True)
    split = json.load(open(split_path))
    hold = set(split["holdout"]) | set(split["val"])
    n_songs = len(json.load(open(os.path.join(DATA, "refs-full-songs.json"))))
    train = set(range(n_songs)) - hold
    args = dict(prefix=os.path.join(DATA, "refs-full"), train_songs=train, m2p_path=os.path.join(DATA, "m2p.npz"))
    jobs = [(i, seed * 1_000_003 + i, out) for i in range(n)]
    with MPool(procs, initializer=_init, initargs=(args,)) as p:
        specs = list(p.imap(_spec, jobs, chunksize=16))
    pickle.dump(specs, open(os.path.join(out, "specs.pkl"), "wb"))
    with open(os.path.join(out, "queries.tsv"), "w") as f:
        for s in specs:
            f.write(f"{s['wav']}\tsynth-{s['i']}\tsynth\n")
    print(len(specs), "rendered")


def cmd_collect(out, dst):
    specs = {s["wav"]: s for s in pickle.load(open(os.path.join(out, "specs.pkl"), "rb"))}
    items = []
    lags = []
    agree = []
    with open(os.path.join(out, "export.jsonl")) as f:
        for line in f:
            q = json.loads(line)
            s = specs.get(q["wav"])
            if s is None or not q.get("contour"):
                continue
            c = np.array([np.nan if v is None else v for v in q["contour"]], np.float32)
            t = s["target"]
            best = (-1, 0)
            for lag in range(-30, 11):
                a = c[max(0, lag) :]
                b = t[max(0, -lag) :]
                m = min(len(a), len(b))
                a, b = a[:m], b[:m]
                ok = np.isfinite(a) & np.isfinite(b)
                if ok.sum() < 20:
                    continue
                d = a[ok] - b[ok]
                d = d - np.median(d)
                sc = (np.abs(d) < 1).mean() * ok.sum()
                if sc > best[0]:
                    best = (sc, lag)
            lag = best[1]
            # contour frame i <-> target frame i - lag
            idx = np.clip(np.arange(len(c)) - lag, 0, len(t) - 1)
            ok = np.isfinite(c) & np.isfinite(t[idx])
            if ok.sum() < 40:
                continue
            d = c[ok] - t[idx][ok]
            agree.append(float((np.abs(d - np.median(d)) < 1).mean()))
            lags.append(lag)
            items.append(dict(contour=c, ref_pos=s["ref_pos"][idx], track=s["track"], song=s["song"], kind=s["kind"], mode=s["mode"], agree=agree[-1]))
    pickle.dump(items, open(dst, "wb"))
    print(len(items), "collected; lag p5/50/95", np.percentile(lags, [5, 50, 95]), "agreement p10/50/90", np.round(np.percentile(agree, [10, 50, 90]), 3))
    by = {}
    for it in items:
        by.setdefault(it["mode"], []).append(it["agree"])
    print({k: (len(v), round(float(np.median(v)), 3)) for k, v in by.items()})


if __name__ == "__main__":
    if sys.argv[1] == "render":
        cmd_render(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]) if len(sys.argv) > 4 else 4, int(sys.argv[5]) if len(sys.argv) > 5 else 0)
    else:
        cmd_collect(sys.argv[2], sys.argv[3])
