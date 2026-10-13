"""Does vocal separation before RMVPE improve preview melodies?

For well-known songs with a MIDI melody, the preview is analysed twice (full mix
and the Mel-Band RoFormer vocal stem from audio-separator, run in its own venv)
and both results are scored against the MIDI melody with the DTW agreement.
Audio lives in /dev/shm only for the duration of the run.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np

from . import db as dbm
from .checks import FAMOUS, find_song
from .config import Paths
from .extract import decode_audio, phrases_for
from .net import DeezerClient, session
from .notes import NoteParams, voiced_ratio
from .resolve import Target, resolve_one
from .validate import _midi_tracks, agreement

log = logging.getLogger("mfidx.separation")


def _best_agreement(preview: list[dict[str, Any]], midis: list[Any]) -> float | None:
    best = None
    for m in midis:
        for phrases in _midi_tracks(json.loads(m["phrases"])).values():
            a = agreement(preview, phrases)["agreement"]
            if a is not None and (best is None or a > best):
                best = a
    return best


def _to_wav(data: bytes, path: Path, sr: int, channels: int) -> None:
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        "pipe:0",
        "-ar",
        str(sr),
        "-ac",
        str(channels),
        str(path),
    ]
    subprocess.run(cmd, input=data, check=True, capture_output=True, timeout=120)


def experiment(
    paths: Paths, n: int = 20, model: str = "vocals_mel_band_roformer.ckpt"
) -> dict[str, Any]:
    from .rmvpe import Rmvpe

    conn = dbm.connect(paths.db)
    client = DeezerClient(paths.state)
    http = session(4)
    work = Path("/dev/shm/mfidx-sep")
    shutil.rmtree(work, ignore_errors=True)
    (work / "in").mkdir(parents=True)
    picked: list[dict[str, Any]] = []
    try:
        for artist, title in FAMOUS:
            if len(picked) >= n:
                break
            s = find_song(conn, artist, title)
            if s is None:
                continue
            midis = conn.execute(
                "SELECT phrases FROM midi_melody WHERE song_id=? AND status='ok'", (s["id"],)
            ).fetchall()
            if not midis:
                continue
            res = resolve_one(client, Target(s["id"], s["title"], s["artist"], [], None))
            if res.get("status") != "ok" or not res.get("preview_url"):
                continue
            r = http.get(res["preview_url"], timeout=30)
            if r.status_code != 200:
                continue
            _to_wav(r.content, work / "in" / f"{s['id']}.wav", 44100, 2)
            picked.append(
                {"song_id": s["id"], "song": f"{s['artist']} - {s['title']}", "midis": midis}
            )

        sep_python = paths.root / "sepenv" / "bin" / "python"
        script = Path(__file__).resolve().parent.parent / "scripts" / "separate_vocals.py"
        t0 = time.time()
        proc = subprocess.run(
            [str(sep_python), str(script), str(work / "in"), str(work / "out"), model],
            capture_output=True,
            text=True,
            check=False,
            timeout=3600,
        )
        events = [json.loads(line) for line in proc.stdout.splitlines() if line.startswith("{")]
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr[-2000:])
        sep_times = [e["seconds"] for e in events if e.get("event") == "separated"]

        rmvpe = Rmvpe(paths.rmvpe_weights)
        prm = NoteParams()
        rows = []
        for item in picked:
            mix_path = work / "in" / f"{item['song_id']}.wav"
            vox = sorted((work / "out").glob(f"{item['song_id']}.vocals*.wav"))
            if not vox:
                continue
            mix = decode_audio(mix_path.read_bytes())
            voc = decode_audio(vox[0].read_bytes())
            out: dict[str, Any] = {"song": item["song"]}
            for name, audio in (("mix", mix), ("vocals", voc)):
                ((f0, conf),) = rmvpe.f0([audio], threshold=0.01)
                phrases = phrases_for(f0, conf, prm)
                out[f"{name}_agreement"] = _best_agreement(phrases, item["midis"])
                out[f"{name}_notes"] = sum(len(p["notes"]) for p in phrases)
                out[f"{name}_voiced"] = round(voiced_ratio(f0, conf, prm.voicing), 3)
            rows.append(out)

        def mean(key: str) -> float | None:
            vals = [r[key] for r in rows if r.get(key) is not None]
            return round(float(np.mean(vals)), 4) if vals else None

        return {
            "model": model,
            "songs": rows,
            "mean_mix_agreement": mean("mix_agreement"),
            "mean_vocals_agreement": mean("vocals_agreement"),
            "vocals_better": sum(
                1 for r in rows if (r["vocals_agreement"] or 0) > (r["mix_agreement"] or 0)
            ),
            "separation_seconds_per_clip": round(float(np.median(sep_times)), 2)
            if sep_times
            else None,
            "wall_seconds": round(time.time() - t0, 1),
        }
    finally:
        shutil.rmtree(work, ignore_errors=True)
