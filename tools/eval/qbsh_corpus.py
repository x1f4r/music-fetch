"""Ground-truth songs and query lists for MIR-QBSH and IOACAS (+ ThinkIT).

Usage: python qbsh_corpus.py <extracted corpora dir> <out dir>

<extracted corpora dir> holds MIR-QBSH-corpus/ and IOACAS_QBH_Coprus/ as unpacked
from the MIREX archives. Writes:
  truth.jsonl       one song per ground-truth MIDI (melody-builder input)
  mirqbsh.tsv       wav path, expected title, expected artist (4431 queries)
  ioacas.tsv        the same for IOACAS + ThinkIT (759 queries)
Song titles are "<corpus>-<midi id>"; artists are the corpus names.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from midi_melody import melody_notes  # noqa: E402


def song(title, artist, notes):
    return {
        "title": title,
        "artist": artist,
        "isrc": None,
        "deezer_id": None,
        "year": None,
        "popularity": 0.5,
        "phrases": [{"source": "midi", "start_ms": 0, "notes": notes}],
    }


def main(root, out):
    os.makedirs(out, exist_ok=True)
    mir = os.path.join(root, "MIR-QBSH-corpus")
    ioa = os.path.join(root, "IOACAS_QBH_Coprus", "IOACAS_pt1")
    thk = os.path.join(root, "IOACAS_QBH_Coprus", "Thinkit.QBH.corpus")
    songs = []
    have = set()

    def add(title, artist, path):
        notes = melody_notes(path, pick_track=False)
        if len(notes) < 5:
            print("skipping", path, "with", len(notes), "notes", file=sys.stderr)
            return
        songs.append(song(title, artist, notes))
        have.add(title)

    for f in sorted(os.listdir(os.path.join(mir, "midiFile"))):
        if f.endswith(".mid"):
            add("qbsh-" + f[:-4], "MIR-QBSH", os.path.join(mir, "midiFile", f))
    for f in sorted(os.listdir(os.path.join(ioa, "midfile"))):
        if f.lower().endswith(".mid"):
            add("ioacas-" + f[:-4], "IOACAS", os.path.join(ioa, "midfile", f))
    for f in sorted(os.listdir(os.path.join(thk, "REFMIDI"))):
        if f.lower().endswith(".mid"):
            add("thinkit-" + f[:-4], "ThinkIT", os.path.join(thk, "REFMIDI", f))
    with open(os.path.join(out, "truth.jsonl"), "w") as w:
        for s in songs:
            w.write(json.dumps(s, ensure_ascii=False) + "\n")

    with open(os.path.join(out, "mirqbsh.tsv"), "w") as w:
        n = 0
        for dirpath, _, files in os.walk(os.path.join(mir, "waveFile")):
            for f in sorted(files):
                if f.endswith(".wav") and "qbsh-" + f[:-4] in have:
                    w.write(f"{os.path.join(dirpath, f)}\tqbsh-{f[:-4]}\tMIR-QBSH\n")
                    n += 1
        print("MIR-QBSH queries", n)

    with open(os.path.join(out, "ioacas.tsv"), "w") as w:
        n = 0
        for line in open(os.path.join(ioa, "query.list"), encoding="latin-1"):
            parts = line.split()
            if len(parts) >= 2:
                wav = os.path.join(ioa, *parts[0].replace("\\", "/").split("/"))
                title = "ioacas-" + parts[1]
                if title in have and os.path.exists(wav):
                    w.write(f"{wav}\t{title}\tIOACAS\n")
                    n += 1
                else:
                    print("missing", wav, title, file=sys.stderr)
        for line in open(os.path.join(thk, "query.list"), encoding="latin-1"):
            parts = line.split()
            if len(parts) >= 2:
                wav = os.path.join(thk, "Query", parts[0])
                title = "thinkit-" + parts[1]
                if title in have and os.path.exists(wav):
                    w.write(f"{wav}\t{title}\tThinkIT\n")
                    n += 1
                else:
                    print("missing", wav, title, file=sys.stderr)
        print("IOACAS + ThinkIT queries", n)
    print("ground-truth songs", len(songs))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
