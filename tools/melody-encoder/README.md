# melody-encoder

A small learned encoder for hum retrieval: 8 s of the matcher's 20 fps pitch contour in,
a 128-d unit vector out (836k parameters, 3.4 MB as f32 ONNX). Reference windows are
embedded offline; a query's windows are embedded on the device and searched with
inner-product ANN. The candidates it finds are meant to join the 97f-2 matcher's own
retrieval before the linear-scaling/DTW rerank (`core/crates/mfcore/src/melody/matcher.rs`).

No data lives here. Everything reads and writes `$MELENC_HOME` (default `~/mf/encoder`):
`data/` for inputs, `runs/<name>/` for checkpoints and results.

## Setup

```sh
uv sync            # CUDA torch on Linux, the default wheels elsewhere
```

## Pipeline

| step | command | output |
|---|---|---|
| decode references from a format-2 index | `python -m melenc.prep_refs <index.mfi> data/refs-notes` | `refs-notes.npz`, `refs-notes-songs.json` |
| place previews inside MIDI melodies | `python -m melenc.align_midi data/refs-full data/m2p.npz 12` | clean MIDI <-> extracted preview pairs |
| evaluation sets (query export joined with a 97f-2 `--trace` run) | `python -m melenc.eval_prep <mf queries export> <mf eval output> data/eval-chad.pkl` | contour, expected song, 97f-2 trace per query |
| synthetic hums through the app's pitch tracker | `python -m melenc.synth_audio render <dir> 12000 6 1`, `mf queries --queries <dir>/queries.tsv --out <dir>/export.jsonl`, `python -m melenc.synth_audio collect <dir> data/tracked1.pkl` | tracked contours with their reference positions |
| real hums for training (MIR-QBSH) | `python -m melenc.qbsh_prep <qbsh.mfi> <mirqbsh.tsv> <export.jsonl> data/qbsh.pkl` | placed real hums on their MIDI |
| train | `python -m melenc.train --out runs/a --steps 24000 --batch 512 --negs 2048` | `runs/a/model.pt`, `split.json` |
| fine-tune with tracked and real hums | `python -m melenc.train --out runs/b --init runs/a/model.pt --tracked data/tracked1.pkl --qbsh data/qbsh.pkl ...` | |
| evaluate (brute force on the GPU) | `python -m melenc.evaluate runs/a/model.pt data/refs-notes data/eval-chad.pkl --meta per-query.csv.gz --out runs/a/eval-notes.json --save-index runs/a/index-notes.npz` | recall@1/10/100/500 alone and in union with 97f-2's retrieval |
| export | `python -m melenc.export_onnx runs/a/model.pt runs/a/encoder.onnx data/eval-chad.pkl` | ONNX plus test vectors for tract |
| compressed index, CPU cost | `python -m melenc.latency runs/a/index-notes.npz runs/a/encoder.onnx data/eval-chad.pkl --m 16 --hop-keep 2` | IVF-PQ recall, size, one-thread latency |
| tract check | `cargo run --release --manifest-path tract-bench/Cargo.toml -- runs/a/encoder.onnx runs/a/encoder.onnx.features.bin runs/a/encoder.onnx.embeddings.bin 64` | tract vs onnxruntime, one-thread timing |

## Data rules

- Evaluation songs never reach training. Every song of an evaluation set, every song that
  shares a normalized title with one, and the songs of the private acceptance recordings
  are held out (`refs.holdout_songs`); 600 more songs are the validation split.
- Real hums used for training come from MIR-QBSH only. CHAD, IOACAS and the private
  recordings are evaluation data.

## Features (mirrored in Rust)

Per frame of a 160-frame window: pitch relative to the window's voiced median (lower
middle value; rests hold the previous pitch, leading rests take the median), divided by 6
and clipped to ±2; the voicing flag; the held pitch change divided by 3 and clipped to ±2.
`latency.featurize_np` is the reference implementation.
