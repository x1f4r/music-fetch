"""Stage 5: the JSONL consumed by ``mf index build`` plus stats.json."""

from __future__ import annotations

import json
import logging
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from . import db as dbm
from .config import Paths
from .universe import percentiles

log = logging.getLogger("mfidx.export")

DEFAULT_MIN_AGREEMENT = 0.45


def _year(song_year: int | None, release_date: str | None) -> int | None:
    if song_year:
        return int(song_year)
    if release_date and release_date[:4].isdigit():
        y = int(release_date[:4])
        return y if 1900 <= y <= 2100 else None
    return None


def _clean_phrase(ph: dict[str, Any]) -> dict[str, Any]:
    return {"source": ph["source"], "start_ms": int(ph["start_ms"]), "notes": ph["notes"]}


def choose_midi(
    cands: list[dict[str, Any]],
    validation: dict[tuple[str, int], float | None],
    min_agreement: float,
) -> tuple[list[dict[str, Any]], str]:
    """MIDI phrases for one song: the best-validated track, else the most confident file.

    Returns (phrases, verdict) where verdict is one of validated / unvalidated / rejected / none.
    """
    if not cands:
        return [], "none"
    scored: list[tuple[float, str, int]] = []
    for c in cands:
        for track in {int(p.get("track", 0)) for p in c["phrases"]}:
            a = validation.get((c["md5"], track))
            if a is not None:
                scored.append((a, c["md5"], track))
    if scored:
        a, md5, track = max(scored)
        if a < min_agreement:
            return [], "rejected"
        c = next(c for c in cands if c["md5"] == md5)
        return [p for p in c["phrases"] if int(p.get("track", 0)) == track], "validated"
    best = max(cands, key=lambda c: (c["confidence"] or 0.0, c["n_notes"] or 0))
    return best["phrases"], "unvalidated"


def run(
    paths: Paths, date: str | None = None, min_agreement: float | None = None
) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    date = date or time.strftime("%Y-%m-%d")
    if min_agreement is None:
        row = conn.execute("SELECT info FROM stage_runs WHERE stage='validate'").fetchone()
        info = json.loads(row["info"]) if row and row["info"] else {}
        min_agreement = float(info.get("threshold") or DEFAULT_MIN_AGREEMENT)

    validation: dict[int, dict[tuple[str, int], float | None]] = defaultdict(dict)
    for r in conn.execute("SELECT song_id, md5, track, agreement FROM midi_validation"):
        validation[r["song_id"]][(r["md5"], r["track"])] = r["agreement"]
    midi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for r in conn.execute(
        "SELECT md5, song_id, phrases, confidence, n_notes FROM midi_melody WHERE status='ok'"
    ):
        midi[r["song_id"]].append(
            {
                "md5": r["md5"],
                "phrases": json.loads(r["phrases"]),
                "confidence": r["confidence"],
                "n_notes": r["n_notes"],
            }
        )

    rows = conn.execute(
        "SELECT s.id, s.title, s.artist, s.year, s.score, s.origin, d.status AS dz_status, d.deezer_id, d.isrc,"
        " d.release_date, d.has_preview, p.status AS p_status, p.phrases AS pp FROM songs s"
        " LEFT JOIN deezer d ON d.song_id=s.id LEFT JOIN preview_melody p ON p.song_id=s.id"
        " WHERE s.in_universe=1"
    ).fetchall()

    records: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    verdicts: dict[str, int] = defaultdict(int)
    universe_by_decade: dict[str, int] = defaultdict(int)
    for r in rows:
        year = _year(r["year"], r["release_date"])
        decade = f"{year // 10 * 10}s" if year else "unknown"
        universe_by_decade[decade] += 1
        preview = json.loads(r["pp"]) if r["p_status"] == "ok" and r["pp"] else []
        midi_phrases, verdict = choose_midi(
            midi.get(r["id"], []), validation.get(r["id"], {}), min_agreement
        )
        verdicts[verdict] += 1
        phrases = [_clean_phrase(p) for p in preview] + [_clean_phrase(p) for p in midi_phrases]
        if not phrases:
            continue
        rec = {
            "title": r["title"],
            "artist": r["artist"],
            "isrc": r["isrc"] if r["dz_status"] == "ok" else None,
            "deezer_id": int(r["deezer_id"]) if r["dz_status"] == "ok" and r["deezer_id"] else None,
            "year": year,
            "popularity": 0.0,
            "phrases": phrases,
        }
        meta = {
            "decade": decade,
            "preview": bool(preview),
            "midi": bool(midi_phrases),
            "origin": r["origin"],
        }
        records.append((r["score"], rec, meta))

    records.sort(key=lambda t: t[0], reverse=True)
    for (_, rec, _), p in zip(records, percentiles([t[0] for t in records]), strict=True):
        rec["popularity"] = round(p, 4)

    out = paths.out / f"melody-songs-{date}.jsonl"
    tmp = out.with_suffix(".jsonl.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        for _, rec, _ in records:
            fh.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
    tmp.replace(out)

    stats = build_stats(records, rows, universe_by_decade, verdicts, min_agreement, date, out)
    (paths.out / "stats.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False))
    log.info("export: %d songs -> %s", len(records), out)
    return stats


def build_stats(
    records, rows, universe_by_decade, verdicts, min_agreement, date, out: Path
) -> dict[str, Any]:
    notes_per_song = sorted(sum(len(p["notes"]) for p in rec["phrases"]) for _, rec, _ in records)
    phrases_by_source: dict[str, int] = defaultdict(int)
    notes_by_source: dict[str, int] = defaultdict(int)
    for _, rec, _ in records:
        for p in rec["phrases"]:
            phrases_by_source[p["source"]] += 1
            notes_by_source[p["source"]] += len(p["notes"])
    by_kind = {"preview_only": 0, "midi_only": 0, "both": 0}
    decade: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"universe": 0, "exported": 0, "preview": 0, "midi": 0}
    )
    for _, _, meta in records:
        kind = (
            "both"
            if meta["preview"] and meta["midi"]
            else "preview_only"
            if meta["preview"]
            else "midi_only"
        )
        by_kind[kind] += 1
        d = decade[meta["decade"]]
        d["exported"] += 1
        d["preview"] += meta["preview"]
        d["midi"] += meta["midi"]
    for k, v in universe_by_decade.items():
        decade[k]["universe"] = v
    for v in decade.values():
        v["coverage"] = round(v["exported"] / v["universe"], 3) if v["universe"] else None

    def pct(q: float) -> int:
        return (
            notes_per_song[min(len(notes_per_song) - 1, int(q * len(notes_per_song)))]
            if notes_per_song
            else 0
        )

    dz = defaultdict(int)
    pv = defaultdict(int)
    for r in rows:
        dz[r["dz_status"] or "pending"] += 1
        pv[r["p_status"] or "pending"] += 1
    return {
        "date": date,
        "file": out.name,
        "bytes": out.stat().st_size,
        "songs": len(records),
        "songs_by_source": by_kind,
        "songs_by_origin": {
            o: sum(1 for _, _, m in records if m["origin"] == o) for o in ("chart", "midi")
        },
        "phrases": sum(phrases_by_source.values()),
        "phrases_by_source": dict(phrases_by_source),
        "notes": sum(notes_by_source.values()),
        "notes_by_source": dict(notes_by_source),
        "notes_per_song": {
            "mean": round(statistics.fmean(notes_per_song), 1) if notes_per_song else 0,
            "median": pct(0.5),
            "p10": pct(0.1),
            "p90": pct(0.9),
        },
        "coverage_by_decade": dict(sorted(decade.items())),
        "universe": {"songs": len(rows), "deezer": dict(dz), "preview_melody": dict(pv)},
        "midi_choice": dict(verdicts),
        "midi_min_agreement": min_agreement,
    }
