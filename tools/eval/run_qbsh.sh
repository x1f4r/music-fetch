#!/bin/bash
# End-to-end query-by-humming evaluation on MIR-QBSH and IOACAS (+ ThinkIT).
#
# Usage: tools/eval/run_qbsh.sh <data dir> <work dir>
#
#   <data dir>  holds qbsh/MIR-QBSH-corpus.tar.gz, qbsh/IOACAS_QBH_Corpus.tar.gz and
#               lmd/clean_midi.tar.gz (the Lakh MIDI Dataset's clean subset)
#   <work dir>  receives everything generated (several hundred MB); keep it outside
#               the repository
#
# Needs: python3 with mido, numpy and scipy (e.g. `uv venv` + `uv pip install mido
# numpy scipy`), and a release build of the CLI (`cargo build --release -p mf-cli`
# in core/). Steps:
#   1. unpack the corpora and the Lakh clean MIDIs
#   2. ground-truth songs (truth.jsonl) and query lists from the corpora
#   3. distractor melodies from Lakh (lakh.jsonl, about 9,000 songs)
#   4. build the index (ground truth + distractors) and run every query
#   5. fit the score model (prints constants for matcher.rs) and its reliability
set -euo pipefail

data="$1"
work="$2"
here="$(cd "$(dirname "$0")" && pwd)"
mf="$here/../../core/target/release/mf"
py="${PYTHON:-python3}"

mkdir -p "$work/qbsh" "$work/lmd" "$work/eval"
[ -d "$work/qbsh/MIR-QBSH-corpus" ] || tar xzf "$data/qbsh/MIR-QBSH-corpus.tar.gz" -C "$work/qbsh"
[ -d "$work/qbsh/IOACAS_QBH_Coprus" ] || tar xzf "$data/qbsh/IOACAS_QBH_Corpus.tar.gz" -C "$work/qbsh"
[ -d "$work/lmd/clean_midi" ] || tar xzf "$data/lmd/clean_midi.tar.gz" -C "$work/lmd"

cd "$work/eval"
"$py" -I "$here/qbsh_corpus.py" "$work/qbsh" .
[ -s lakh.jsonl ] || "$py" -I "$here/lakh_distractors.py" "$work/lmd/clean_midi" lakh.jsonl 1000000 8
cat truth.jsonl lakh.jsonl > full.jsonl
"$mf" index build full.jsonl full.mfi --version eval
"$mf" index info full.mfi

"$mf" eval qbsh --index full.mfi --queries mirqbsh.tsv --out results_mir.jsonl
"$mf" eval qbsh --index full.mfi --queries ioacas.tsv --out results_ioa.jsonl
"$py" -I "$here/fit_fusion.py" results_mir.jsonl results_ioa.jsonl
