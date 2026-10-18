"""Test vectors for the Rust port (core/crates/mfcore/src/melody/encoder/fixtures).

Usage: python -m melenc.fixtures <out dir>

Writes, from synthetic contours only (no evaluation data):
  tiny.onnx        the encoder architecture at toy size with seeded random weights
  windows.bin      f32 LE (n, 160) model windows, NaN = unvoiced
  features.bin     f32 LE (n, 3, 160) = latency.featurize_np(windows), the featurizer spec
  embeddings.bin   f32 LE (n, dim) = onnxruntime output of tiny.onnx on features.bin
  query.bin        f32 LE (len,) a long query contour with rests
  query-windows.bin f32 LE (k, 160) = evaluate.query_windows(query, qhop=40)
  fixtures.json    shapes, plus the reference-window starts of query.bin (hop 40, as build_index)
"""

import json
import os
import sys

import numpy as np
import onnxruntime as ort
import torch

from melenc.evaluate import query_windows
from melenc.latency import featurize_np
from melenc.model import W, Encoder, featurize
from melenc.sim import ref_window


def contours(rng):
    out = []
    # A stepwise melody, fully voiced.
    notes = np.repeat(60 + np.cumsum(rng.integers(-3, 4, 40)), 4).astype(np.float32)[:W]
    out.append(notes)
    # Leading and trailing rests, quarter-semitone values as in the index.
    x = np.round((62 + 3 * np.sin(np.arange(W) / 9)) * 4) / 4
    x[:23] = np.nan
    x[140:] = np.nan
    out.append(x.astype(np.float32))
    # Phrase breaks and big jumps (clipping of both channels).
    x = 55 + rng.normal(0, 0.3, W).cumsum()
    x[40:52] = np.nan
    x[90:91] = np.nan
    x[100:] += 19
    x[130:150] = np.nan
    out.append(x.astype(np.float32))
    # Sparse voicing.
    x = 70 + rng.normal(0, 2, W)
    x[rng.random(W) < 0.6] = np.nan
    out.append(x.astype(np.float32))
    # Nothing voiced.
    out.append(np.full(W, np.nan, np.float32))
    # One voiced frame, an even number of voiced values, a short query padded with rests.
    x = np.full(W, np.nan, np.float32)
    x[77] = 64.5
    out.append(x)
    x = np.full(W, np.nan, np.float32)
    x[10:20] = np.arange(10, dtype=np.float32) + 50.25
    out.append(x)
    x = np.full(W, np.nan, np.float32)
    x[:63] = (58 + rng.normal(0, 1.5, 63)).astype(np.float32)
    out.append(x)
    # Random hums.
    for _ in range(8):
        x = (60 + rng.normal(0, 1, W).cumsum() * 0.7).astype(np.float32)
        x[rng.random(W) < 0.15] = np.nan
        out.append(x)
    return np.stack(out).astype(np.float32)


def main():
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    rng = np.random.default_rng(20261018)
    torch.manual_seed(20261018)
    model = Encoder(d=16, layers=1, heads=2, ff=32, dim=16, c0=8)
    # A few contrastive steps on random walks (no data), so that embeddings tell windows apart.
    opt = torch.optim.Adam(model.parameters(), 3e-3)
    for _ in range(300):
        walk = 60 + torch.randn(64, W).cumsum(1) * 0.7
        walk[torch.rand(64, W) < 0.15] = float("nan")
        noisy = walk + 0.15 * torch.randn(64, W) + torch.randint(-6, 7, (64, 1))
        zq, zr = model(featurize(noisy)), model(featurize(walk))
        loss = torch.nn.functional.cross_entropy(zq @ zr.T / 0.1, torch.arange(64))
        opt.zero_grad()
        loss.backward()
        opt.step()
    model.eval()
    x = torch.randn(2, 3, W)
    path = os.path.join(out, "tiny.onnx")
    torch.onnx.export(
        model,
        (x,),
        path,
        input_names=["features"],
        output_names=["embedding"],
        dynamic_axes={"features": {0: "batch"}, "embedding": {0: "batch"}},
        opset_version=17,
        dynamo=False,
    )
    win = contours(rng)
    feats = featurize_np(win)
    sess = ort.InferenceSession(path, providers=["CPUExecutionProvider"])
    emb = sess.run(None, {"features": feats})[0].astype(np.float32)
    win.astype("<f4").tofile(os.path.join(out, "windows.bin"))
    feats.astype("<f4").tofile(os.path.join(out, "features.bin"))
    emb.astype("<f4").tofile(os.path.join(out, "embeddings.bin"))

    q = (60 + rng.normal(0, 1, 700).cumsum() * 0.5).astype(np.float32)
    q[:17] = np.nan
    q[200:260] = np.nan
    q[300:420][rng.random(120) < 0.8] = np.nan
    q[650:] = np.nan
    q.astype("<f4").tofile(os.path.join(out, "query.bin"))
    qw = np.stack(query_windows(q, 40))
    qw.astype("<f4").tofile(os.path.join(out, "query-windows.bin"))
    starts = [s for s in range(0, max(1, len(q) - 40), 40) if np.isfinite(ref_window(q, s)).mean() >= 0.3]
    meta = {"windows": len(win), "dim": int(emb.shape[1]), "query_len": len(q), "query_windows": len(qw), "ref_starts": starts}
    zw = sess.run(None, {"features": featurize_np(np.stack([ref_window(q, s) for s in starts]))})[0]
    print("mean cosine between reference windows", float((zw @ zw.T)[np.triu_indices(len(zw), 1)].mean()))
    json.dump(meta, open(os.path.join(out, "fixtures.json"), "w"))
    print(json.dumps(meta), os.path.getsize(path), "bytes of ONNX")


if __name__ == "__main__":
    main()
