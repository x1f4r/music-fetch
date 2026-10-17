"""Contrastive training of the melody window encoder.

Usage: python -m melenc.train --out <run dir> [--steps N] [--tracked tracked.pkl] [--qbsh qbsh.pkl] ...

Loss: symmetric InfoNCE. Each query window's positive is its reference window; the other
positives in the batch plus `--negs` random reference windows are negatives; pairs of the same
song are masked out (a repeated chorus is not a negative). Validation uses simulated hums of a
song split never trained on (CHAD and the other evaluation songs are held out entirely).
"""

import argparse
import json
import math
import os
import time

import numpy as np
import torch
import torch.nn.functional as F

from melenc.data import KINDS, PairStream, Pool, sample_pair
from melenc.model import Encoder, featurize, params
from melenc.refs import DATA, Refs, holdout_songs
from melenc.sim import W, ref_window

D = DATA


def windows_of(track, hop=20, min_voiced=0.3):
    out, starts = [], []
    for s in range(0, max(1, len(track) - 40), hop):
        r = ref_window(track, s)
        if np.isfinite(r).mean() >= min_voiced:
            out.append(r)
            starts.append(s)
    return out, starts


@torch.no_grad()
def embed(model, x, dev, bs=4096):
    model.eval()
    outs = []
    for i in range(0, len(x), bs):
        xb = torch.from_numpy(np.asarray(x[i : i + bs])).to(dev)
        with torch.autocast("cuda", dtype=torch.bfloat16):
            outs.append(model(featurize(xb)).float())
    model.train()
    return torch.cat(outs)


def build_val(prefix, val_songs, m2p, n=1500, seed=123):
    pool = Pool(prefix, val_songs, m2p_path=m2p)
    rng = np.random.default_rng(seed)
    Q, S, K = [], [], []
    kinds = ["pself", "pself", "m2p", "mself"]
    while len(Q) < n:
        k = kinds[len(Q) % len(kinds)]
        p = sample_pair(pool, k, rng)
        if p is None:
            continue
        Q.append(p[0])
        S.append(p[2])
        K.append(k)
    R, RS = [], []
    for t in np.flatnonzero(np.isin(pool.refs.song, list(val_songs))):
        w, _ = windows_of(pool.track(int(t)))
        R += w
        RS += [int(pool.refs.song[t])] * len(w)
    return np.stack(Q), np.array(S), K, np.stack(R), np.array(RS)


def val_metrics(model, val, dev):
    Q, S, K, R, RS = val
    zq = embed(model, Q, dev)
    zr = embed(model, R, dev)
    sim = zq @ zr.T
    rs = torch.from_numpy(RS).to(dev)
    songs = torch.unique(rs)
    # best window per song
    best = torch.full((len(Q), len(songs)), -2.0, device=dev)
    col = torch.searchsorted(songs, rs)
    best.scatter_reduce_(1, col.expand(len(Q), -1), sim, reduce="amax")
    truth = torch.searchsorted(songs, torch.from_numpy(S).to(dev))
    rank = (best > best.gather(1, truth[:, None])).sum(1) + 1
    out = {"r1": (rank <= 1).float().mean().item(), "r10": (rank <= 10).float().mean().item()}
    for k in set(K):
        m = torch.tensor([x == k for x in K], device=dev)
        out[f"r1_{k}"] = (rank[m] <= 1).float().mean().item()
    out["songs"] = len(songs)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--refs", default=f"{D}/refs-full")
    ap.add_argument("--m2p", default=f"{D}/m2p.npz")
    ap.add_argument("--tracked")
    ap.add_argument("--qbsh")
    ap.add_argument("--steps", type=int, default=20000)
    ap.add_argument("--batch", type=int, default=1024)
    ap.add_argument("--negs", type=int, default=1024)
    ap.add_argument("--lr", type=float, default=2e-3)
    ap.add_argument("--warmup", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--d", type=int, default=128)
    ap.add_argument("--layers", type=int, default=3)
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--init")
    ap.add_argument("--kinds", help="JSON overriding pair-kind weights")
    ap.add_argument("--val-every", type=int, default=1000)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    dev = "cuda"
    torch.manual_seed(0)

    refs = Refs(a.refs)
    hold = holdout_songs(refs, [f"{D}/eval-chad.pkl", f"{D}/eval-chadfull.pkl", f"{D}/eval-synth.pkl"])
    rest = np.array(sorted(set(range(len(refs.songs))) - hold))
    rng = np.random.default_rng(7)
    val_songs = set(int(x) for x in rng.choice(rest, 600, replace=False))
    train_songs = set(int(x) for x in rest) - val_songs
    del refs
    print(f"hold-out {len(hold)} songs, val {len(val_songs)}, train {len(train_songs)}", flush=True)
    json.dump({"holdout": sorted(hold), "val": sorted(val_songs)}, open(f"{a.out}/split.json", "w"))

    val = build_val(a.refs, val_songs, a.m2p)
    print(f"val: {len(val[0])} queries, {len(val[3])} windows", flush=True)

    kinds = dict(KINDS)
    if a.kinds:
        kinds.update(json.loads(a.kinds))
    pool_args = dict(prefix=a.refs, train_songs=train_songs, m2p_path=a.m2p, tracked=a.tracked, qbsh=a.qbsh)
    ds = PairStream(pool_args, a.batch, a.negs, kinds=kinds, seed=1)
    dl = torch.utils.data.DataLoader(ds, batch_size=None, num_workers=a.workers, prefetch_factor=4, persistent_workers=True)

    model = Encoder(d=a.d, layers=a.layers, dim=a.dim).to(dev)
    if a.init:
        model.load_state_dict(torch.load(a.init, map_location=dev)["model"])
    print(f"params {params(model):,}", flush=True)
    scale = torch.nn.Parameter(torch.tensor(math.log(1 / 0.07), device=dev))
    opt = torch.optim.AdamW([{"params": model.parameters()}, {"params": [scale], "weight_decay": 0}], lr=a.lr, weight_decay=0.02)

    def lr_at(s):
        if s < a.warmup:
            return a.lr * s / a.warmup
        p = (s - a.warmup) / max(1, a.steps - a.warmup)
        return a.lr * 0.5 * (1 + math.cos(math.pi * p))

    t0 = time.time()
    hist = []
    for step, (Q, R, S, N, NS, K) in enumerate(dl, start=1):
        for g in opt.param_groups:
            g["lr"] = lr_at(step)
        Q, R, N = Q.to(dev, non_blocking=True), R.to(dev, non_blocking=True), N.to(dev, non_blocking=True)
        S, NS = S.to(dev), NS.to(dev)
        with torch.autocast("cuda", dtype=torch.bfloat16):
            z = model(featurize(torch.cat([Q, R, N])))
        z = z.float()
        B = len(Q)
        zq, zr, zn = z[:B], z[B : 2 * B], z[2 * B :]
        ls = scale.exp().clamp(max=100)
        keys = torch.cat([zr, zn])
        ksong = torch.cat([S, NS])
        lq = zq @ keys.T * ls
        same = S[:, None] == ksong[None, :]
        same[:, :B] &= ~torch.eye(B, dtype=torch.bool, device=dev)
        lq = lq.masked_fill(same, -1e4)
        tgt = torch.arange(B, device=dev)
        lr_ = zr @ zq.T * ls
        same2 = (S[:, None] == S[None, :]) & ~torch.eye(B, dtype=torch.bool, device=dev)
        lr_ = lr_.masked_fill(same2, -1e4)
        loss = 0.5 * (F.cross_entropy(lq, tgt) + F.cross_entropy(lr_, tgt))
        opt.zero_grad(set_to_none=True)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 2.0)
        opt.step()
        acc = (lq.argmax(1) == tgt).float().mean().item()
        hist.append((loss.item(), acc))
        if step % 100 == 0:
            l = np.mean(hist[-100:], axis=0)
            print(f"step {step} loss {l[0]:.3f} acc {l[1]:.3f} T {1 / ls.item():.4f} lr {lr_at(step):.2e} {time.time() - t0:.0f}s", flush=True)
        if step % a.val_every == 0 or step == a.steps:
            m = val_metrics(model, val, dev)
            print("val", json.dumps({k: round(v, 4) if isinstance(v, float) else v for k, v in m.items()}), flush=True)
            ck = {"model": model.state_dict(), "args": vars(a), "step": step, "val": m}
            torch.save(ck, f"{a.out}/model.pt")
            torch.save(ck, f"{a.out}/model-{step}.pt")
        if step >= a.steps:
            break
    print("done", flush=True)
    os._exit(0)


if __name__ == "__main__":
    main()
