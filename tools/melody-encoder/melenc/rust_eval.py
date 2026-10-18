"""Summarize `mf eval qbsh` runs with and without `--encoder`, CHAD split into its halves.

Usage: python -m melenc.rust_eval <songs.json> <name>=<eval.jsonl> [<name>=<eval.jsonl> ...] [--base NAME] [--json out.json]

<songs.json> is the index's song list (prep_refs' `-songs.json`) for the CHAD split; rows of
other sets (no CHAD wav path) are reported whole. With --base, each other run is compared
with it query by query (answers gained and lost at rank 1).
"""

import argparse
import json
import statistics

from melenc.splits import chad_half


def load(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def subsets(rows, songs):
    if not any("/chad/" in r.get("wav", "") for r in rows):
        return {"all": rows}
    out = {"all": rows, "V": [], "T": []}
    for r in rows:
        e = r.get("expected")
        if e is not None:
            out[chad_half(songs[e]["title"])].append(r)
    return out


def summary(rows):
    n = max(1, len(rows))
    rank = [r.get("rank") for r in rows]
    s = {
        "n": len(rows),
        "top1": sum(x == 1 for x in rank) / n,
        "top10": sum(x is not None and x <= 10 for x in rank) / n,
        "retrieved": sum((r.get("trace") or [None])[0] is not None for r in rows) / n,
    }
    enc = [r.get("encoder") for r in rows]
    if any(enc):
        er = [e.get("rank") if e else None for e in enc]
        k = max((e.get("hits", 0) for e in enc if e), default=0)
        for kk in (1, 10, 100, 200, 500):
            s[f"enc@{kk}"] = sum(x is not None and x <= kk for x in er) / n
        ret = [(r.get("trace") or [None])[0] is not None for r in rows]
        s["union"] = sum(a or x is not None for a, x in zip(ret, er)) / n
        for kk in (100, 200, 500):
            s[f"union@{kk}"] = sum(a or (x is not None and x <= kk) for a, x in zip(ret, er)) / n
        s["enc_songs_max"] = k
        win = [e for e in enc if e and e.get("windows")]
        if win:
            s["enc_windows_p50"] = statistics.median(e["windows"] for e in win)
            s["encode_ms_p50"] = statistics.median(e["encode_ms"] for e in win)
            s["scan_ms_p50"] = statistics.median(e["scan_ms"] for e in win)
    ms = [r["search_ms"] for r in rows if r.get("search_ms") is not None and r.get("voiced_ms")]
    if ms:
        s["search_ms_p50"] = statistics.median(ms)
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("songs")
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--base")
    ap.add_argument("--json")
    a = ap.parse_args()
    songs = json.load(open(a.songs))
    runs = {}
    for spec in a.runs:
        name, path = spec.split("=", 1)
        runs[name] = load(path)
    report = {}
    for name, rows in runs.items():
        report[name] = {k: summary(v) for k, v in subsets(rows, songs).items()}
        for k, s in report[name].items():
            keys = ["n", "top1", "top10", "retrieved", "enc@1", "enc@10", "enc@200", "enc@500", "union@200", "union@500", "search_ms_p50", "encode_ms_p50", "scan_ms_p50"]
            print(name, k, " ".join(f"{x}={round(s[x] * 100, 1) if isinstance(s[x], float) and x not in ('search_ms_p50', 'encode_ms_p50', 'scan_ms_p50') else round(s[x], 1)}" for x in keys if x in s))
    if a.base and a.base in runs:
        base = {r["wav"]: r for r in runs[a.base]}
        for name, rows in runs.items():
            if name == a.base:
                continue
            gained = [r["wav"] for r in rows if r.get("rank") == 1 and base.get(r["wav"], {}).get("rank") != 1]
            lost = [r["wav"] for r in rows if r.get("rank") != 1 and base.get(r["wav"], {}).get("rank") == 1]
            report[name]["vs_base"] = {"gained": len(gained), "lost": len(lost)}
            print(name, f"vs {a.base}: +{len(gained)} / -{len(lost)} at rank 1")
    if a.json:
        json.dump(report, open(a.json, "w"), indent=1)


if __name__ == "__main__":
    main()
