"""Export the encoder (features -> embedding) to ONNX and check it against PyTorch.

Usage: python -m melenc.export_onnx <model.pt> <out.onnx> [eval.pkl]

Input `features` (batch, 3, 160) as produced by `model.featurize` (done in Rust in the app),
output `embedding` (batch, 128), unit length. With an evaluation set, real query windows are
featurized by the numpy port (`latency.featurize_np`, the spec for Rust), checked against the
torch features, and written with onnxruntime's embeddings as `<out>.features.bin` /
`<out>.embeddings.bin` (f32 LE) for `tract-bench`.
"""

import pickle
import sys
import time

import numpy as np
import onnxruntime as ort
import torch

from melenc.evaluate import load_model, query_windows
from melenc.latency import featurize_np
from melenc.model import featurize


def main():
    src, dst = sys.argv[1], sys.argv[2]
    model = load_model(src, "cpu").float().eval()
    x = torch.randn(4, 160) * 3 + 60
    x[:, 50:60] = float("nan")
    feats = featurize(x)
    torch.onnx.export(
        model,
        (feats,),
        dst,
        input_names=["features"],
        output_names=["embedding"],
        dynamic_axes={"features": {0: "batch"}, "embedding": {0: "batch"}},
        opset_version=17,
        dynamo=False,
    )
    so = ort.SessionOptions()
    so.intra_op_num_threads = 1
    so.inter_op_num_threads = 1
    sess = ort.InferenceSession(dst, so, providers=["CPUExecutionProvider"])
    with torch.no_grad():
        ref = model(feats).numpy()
    out = sess.run(None, {"features": feats.numpy()})[0]
    print("max abs diff vs torch", float(np.abs(out - ref).max()))
    for b in (1, 4, 8):
        f = np.random.randn(b, 3, 160).astype(np.float32)
        sess.run(None, {"features": f})
        t = time.perf_counter()
        for _ in range(50):
            sess.run(None, {"features": f})
        dt = (time.perf_counter() - t) / 50
        print(f"onnxruntime 1 thread: batch {b}: {dt * 1000:.2f} ms ({dt / b * 1000:.2f} ms/window)")
    if len(sys.argv) > 3:
        rows = pickle.load(open(sys.argv[3], "rb"))
        ws = []
        for r in rows[::40]:
            ws += query_windows(r["contour"])[:2]
        x = np.stack(ws[:64]).astype(np.float32)
        f_np = featurize_np(x)
        f_t = featurize(torch.from_numpy(x)).numpy()
        print(f"{len(x)} real windows; numpy vs torch features max abs diff {float(np.abs(f_np - f_t).max()):.2e}")
        emb = sess.run(None, {"features": f_np})[0]
        with torch.no_grad():
            emb_t = model(torch.from_numpy(f_np)).numpy()
        print(f"onnxruntime vs torch on real windows max abs diff {float(np.abs(emb - emb_t).max()):.2e}")
        f_np.astype("<f4").tofile(dst + ".features.bin")
        emb.astype("<f4").tofile(dst + ".embeddings.bin")
        print("batch", len(x))


if __name__ == "__main__":
    main()
