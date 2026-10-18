#!/usr/bin/env python3
"""Compare hum parity runs (tests/hum_parity.rs output): the native run first, then any
number of browser runs. Prints, per run: queries, top-1 agreeing with native, top-10
lists identical, and top-1 accuracy against the expected song; exits 1 on a top-1
disagreement.

    hum-parity.py native.log chrome.log firefox.log
"""
import json
import re
import sys
import unicodedata


def norm(text):
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def read(path):
    rows, meta = {}, []
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.rstrip("\n")
        at = line.find("PARITY")
        if at < 0:
            continue
        parts = line[at:].split("\t")
        if parts[0] == "PARITY" and len(parts) >= 5:
            wav, expected, title, top = parts[1], parts[2], parts[3], parts[4]
            try:
                ranked = json.loads(top)
            except json.JSONDecodeError:
                ranked = []
            rows[wav] = {"expected": expected, "title": title, "ids": [r[0] for r in ranked], "scores": [r[1] for r in ranked]}
        elif parts[0] in ("PARITY-INDEX", "PARITY-DONE"):
            meta.append(" ".join(parts))
    return rows, meta


def correct(row):
    if not row["ids"]:
        return False
    want = norm(row["expected"].split("–")[-1])
    got = norm(row["title"].split("–")[-1])
    return bool(want) and (want == got or want in got or got in want)


def main(paths):
    base, base_meta = read(paths[0])
    print(f"{paths[0]}: {len(base)} queries, top-1 correct {sum(map(correct, base.values()))}; {' | '.join(base_meta)}")
    failed = False
    for path in paths[1:]:
        rows, meta = read(path)
        shared = [w for w in base if w in rows]
        top1 = sum(1 for w in shared if base[w]["ids"][:1] == rows[w]["ids"][:1])
        top10 = sum(1 for w in shared if base[w]["ids"] == rows[w]["ids"])
        score = max((abs(a - b) for w in shared for a, b in zip(base[w]["scores"], rows[w]["scores"])), default=0.0)
        print(
            f"{path}: {len(rows)} queries ({len(shared)} shared), top-1 same {top1}/{len(shared)}, "
            f"top-10 same {top10}/{len(shared)}, max score difference {score:.6f}, "
            f"top-1 correct {sum(correct(rows[w]) for w in shared)}; {' | '.join(meta)}"
        )
        for w in shared:
            if base[w]["ids"][:1] != rows[w]["ids"][:1]:
                print(f"  top-1 differs: {w}: native {base[w]['title']} / {path} {rows[w]['title']}")
                failed = True
        if len(shared) < len(base):
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
