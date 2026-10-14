"""Progress, throughput and ETA of every stage (``mfidx status``)."""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any

from . import db as dbm
from .config import Paths


def _counts(conn, table: str, where: str = "") -> dict[str, int]:
    q = f"SELECT status, COUNT(*) AS n FROM {table} {where} GROUP BY status"
    return {r["status"]: r["n"] for r in conn.execute(q)}


def _rate(conn, table: str, window: float) -> float:
    """Rows finished per minute over the last ``window`` seconds."""
    since = time.time() - window
    n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE updated_at>?", (since,)).fetchone()[0]
    return n / (window / 60.0)


def collect(paths: Paths, window: float = 600.0) -> dict[str, Any]:
    conn = dbm.connect(paths.db)
    universe = {
        r["origin"]: r["n"]
        for r in conn.execute(
            "SELECT origin, COUNT(*) AS n FROM songs WHERE in_universe=1 GROUP BY origin"
        )
    }
    total = sum(universe.values())
    resolve = _counts(conn, "deezer")
    resolved = sum(resolve.values())
    with_preview = conn.execute(
        "SELECT COUNT(*) FROM deezer WHERE status='ok' AND has_preview=1"
    ).fetchone()[0]
    deezer_ok = resolve.get("ok", 0)
    extract = _counts(conn, "preview_melody")
    extracted = sum(extract.values())
    midi = _counts(conn, "midi_melody")
    midi_files_linked = conn.execute(
        "SELECT COUNT(*) FROM midi_files WHERE key IS NOT NULL"
    ).fetchone()[0]
    validated = conn.execute("SELECT COUNT(*) FROM midi_validation").fetchone()[0]
    r_resolve = _rate(conn, "deezer", window)
    r_extract = _rate(conn, "preview_melody", window)
    stages = {
        r["stage"]: {
            "status": r["status"],
            "started": r["started_at"],
            "finished": r["finished_at"],
        }
        for r in conn.execute("SELECT * FROM stage_runs")
    }

    def eta(remaining: int, rate: float) -> str | None:
        if remaining <= 0:
            return "done"
        if rate <= 0:
            return None
        minutes = remaining / rate
        return f"{minutes / 60:.1f} h" if minutes > 90 else f"{minutes:.0f} min"

    return {
        "universe": {"songs": total, "by_origin": universe},
        "resolve": {
            "done": resolved,
            "of": total,
            "by_status": resolve,
            "songs_per_min": round(r_resolve, 1),
            "eta": eta(total - resolved, r_resolve),
        },
        "extract": {
            "done": extracted,
            "of": deezer_ok,
            "with_preview": with_preview,
            "by_status": extract,
            "songs_per_min": round(r_extract, 1),
            "eta": eta(deezer_ok + (total - resolved) - extracted, r_extract),
        },
        "midi": {
            "linked_files": midi_files_linked,
            "by_status": midi,
            "validated_pairs": validated,
        },
        "stages": stages,
        "window_s": window,
    }


def processes() -> list[str]:
    try:
        out = subprocess.run(
            ["pgrep", "-af", "mfidx|pipeline.sh"], capture_output=True, text=True, check=False
        ).stdout
    except OSError:
        return []
    return [line for line in out.splitlines() if "pgrep" not in line]


def show(paths: Paths) -> None:
    info = collect(paths)
    print(json.dumps(info, indent=2, default=str))
    procs = processes()
    print("\nrunning:" if procs else "\nrunning: none")
    for p in procs:
        print("  " + p[:160])
