"""Stage 6: evaluation sets.

``synth``: hum-like renderings of random reference excerpts with known ground
truth (key shift, tempo, jitter, dropped notes, room noise).
``chad``: the CHAD real-humming subset mapped to songs in the JSONL.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf
from scipy.signal import fftconvolve, lfilter

from .config import Paths
from .net import PoliteFetcher
from .normalize import artist_similarity, loose_title_key, match_keys, song_key, title_similarity

log = logging.getLogger("mfidx.eval")

SR = 16000
CHAD_CSV_URL = "https://raw.githubusercontent.com/amanteur/CHAD/main/metadata/dataset.csv"


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def expected_key(rec: dict[str, Any]) -> str:
    """Stable song identity for manifests: Deezer id when known, else the normalized key."""
    return (
        f"deezer:{rec['deezer_id']}"
        if rec.get("deezer_id")
        else f"key:{song_key(rec['artist'], rec['title'])}"
    )


# ------------------------------------------------------------------------- synthesis


@dataclass
class HumParams:
    key_shift: float
    tempo: float
    jitter_cents: float
    drop_prob: float
    waveform: str
    vibrato_rate: float
    vibrato_cents: float
    snr_db: float
    rt60: float
    octave: int


def excerpt(
    rec: dict[str, Any], rng: np.random.Generator, min_s: float = 8.0, max_s: float = 14.0
) -> tuple[dict[str, Any], list[tuple[float, int, int]]] | None:
    """A contiguous run of notes (absolute ms) of 8-14 s from one source of a song."""
    target_ms = rng.uniform(min_s, max_s) * 1000
    by_source: dict[str, list[dict[str, Any]]] = {}
    for ph in rec["phrases"]:
        by_source.setdefault(ph["source"], []).append(ph)
    sources = list(by_source)
    rng.shuffle(sources)
    for src in sources:
        phrases = sorted(by_source[src], key=lambda p: p["start_ms"])
        notes = [(p, ph["start_ms"] + o, d) for ph in phrases for p, o, d in ph["notes"]]
        notes.sort(key=lambda n: n[1])
        if not notes or notes[-1][1] + notes[-1][2] - notes[0][1] < min_s * 1000:
            continue
        starts = [i for i, n in enumerate(notes) if notes[-1][1] + notes[-1][2] - n[1] >= target_ms]
        if not starts:
            continue
        i0 = int(rng.choice(starts))
        t0 = notes[i0][1]
        chosen = [n for n in notes[i0:] if n[1] + n[2] - t0 <= target_ms]
        if len(chosen) < 6:
            continue
        return {"source": src, "start_ms": int(t0), "target_ms": int(target_ms)}, chosen
    return None


def _pink(n: int, rng: np.random.Generator) -> np.ndarray:
    white = rng.standard_normal(n)
    b = [0.049922035, -0.095993537, 0.050612699, -0.004408786]
    a = [1.0, -2.494956002, 2.017265875, -0.522189400]
    return lfilter(b, a, white)


def _room_ir(rt60: float, rng: np.random.Generator) -> np.ndarray:
    n = int(SR * rt60)
    t = np.arange(n) / SR
    ir = rng.standard_normal(n) * np.exp(-6.9 * t / rt60)
    ir[0] = 1.0
    return ir / np.sqrt(np.sum(ir**2))


def render(
    notes: list[tuple[float, float, float]], prm: HumParams, rng: np.random.Generator
) -> np.ndarray:
    """``notes``: (midi pitch, onset s, dur s), already transposed and time-scaled."""
    lead, tail = 0.4, 0.6
    total = lead + max(o + d for _, o, d in notes) + tail
    n = int(total * SR)
    f = np.zeros(n)
    env = np.zeros(n)
    for idx, (p, o, d) in enumerate(notes):
        a, b = int((lead + o) * SR), min(n, int((lead + o + d) * SR))
        if b <= a:
            continue
        seg_t = np.arange(b - a) / SR
        cents = prm.vibrato_cents * np.sin(
            2 * np.pi * prm.vibrato_rate * seg_t + rng.uniform(0, 2 * np.pi)
        )
        cents *= np.clip((seg_t - 0.15) / 0.2, 0, 1)
        freq = 440.0 * 2 ** ((p - 69) / 12 + cents / 1200)
        # Glide in from the previous note when they touch.
        if idx > 0 and o - (notes[idx - 1][1] + notes[idx - 1][2]) < 0.05:
            g = min(len(freq), int(0.05 * SR))
            prev = 440.0 * 2 ** ((notes[idx - 1][0] - 69) / 12)
            freq[:g] = prev + (freq[:g] - prev) * np.linspace(0, 1, g)
        f[a:b] = freq
        att, rel = rng.uniform(0.02, 0.06), rng.uniform(0.04, 0.09)
        e = np.ones(b - a) * rng.uniform(0.7, 1.0)
        na, nr = min(len(e), int(att * SR)), min(len(e), int(rel * SR))
        e[:na] *= np.linspace(0, 1, na)
        if nr:
            e[-nr:] *= np.linspace(1, 0, nr)
        e *= np.exp(-0.25 * seg_t)
        env[a:b] = np.maximum(env[a:b], e)
    # Hold frequency through unvoiced gaps so the phase stays continuous.
    held = np.where(f > 0, np.arange(n), 0)
    np.maximum.accumulate(held, out=held)
    f = f[held]
    phase = 2 * np.pi * np.cumsum(f) / SR
    if prm.waveform == "sine":
        wave = np.sin(phase) + 0.12 * np.sin(2 * phase) + 0.04 * np.sin(3 * phase)
    else:
        tri = 2 / np.pi * np.arcsin(np.sin(phase))
        wave = 0.6 * tri + 0.4 * np.sin(phase)
    dry = wave * env
    wet = fftconvolve(dry, _room_ir(prm.rt60, rng))[:n]
    sig = 0.75 * dry + 0.25 * wet
    voiced = env > 0.05
    p_sig = np.mean(sig[voiced] ** 2) if voiced.any() else np.mean(sig**2)
    noise = _pink(n, rng)
    noise *= np.sqrt(p_sig / (np.mean(noise**2) * 10 ** (prm.snr_db / 10)))
    out = sig + noise
    peak = np.max(np.abs(out)) or 1.0
    return (out / peak * 10 ** (rng.uniform(-12, -3) / 20)).astype(np.float32)


def synth(paths: Paths, jsonl: Path, n: int = 300, seed: int = 20261013) -> dict[str, Any]:
    songs = load_jsonl(jsonl)
    rng = np.random.default_rng(seed)
    out_dir = paths.eval / "synthetic"
    wav_dir = out_dir / "wav"
    if wav_dir.exists():
        shutil.rmtree(wav_dir)
    wav_dir.mkdir(parents=True)
    order = rng.permutation(len(songs))
    truth = []
    for line in order:
        if len(truth) >= n:
            break
        rec = songs[int(line)]
        ex = excerpt(rec, rng)
        if ex is None:
            continue
        meta, notes = ex
        pitches = np.array([p for p, _, _ in notes])
        octave = int(np.round((57 - np.median(pitches)) / 12))
        prm = HumParams(
            key_shift=round(float(rng.uniform(-6, 6)), 2),
            tempo=round(float(rng.uniform(0.75, 1.3)), 3),
            jitter_cents=30.0,
            drop_prob=round(float(rng.uniform(0.0, 0.12)), 3),
            waveform=str(rng.choice(["sine", "triangle"])),
            vibrato_rate=round(float(rng.uniform(4.5, 6.5)), 2),
            vibrato_cents=round(float(rng.uniform(10, 45)), 1),
            snr_db=round(float(rng.uniform(15, 25)), 1),
            rt60=round(float(rng.uniform(0.15, 0.5)), 2),
            octave=octave,
        )
        t0 = notes[0][1]
        rendered = []
        dropped = 0
        for p, o, d in notes:
            if rng.random() < prm.drop_prob and len(notes) - dropped > 6:
                dropped += 1
                continue
            jitter = rng.uniform(-prm.jitter_cents, prm.jitter_cents) / 100
            rendered.append(
                (
                    p + 12 * octave + prm.key_shift + jitter,
                    (o - t0) / 1000 / prm.tempo,
                    max(0.06, d / 1000 / prm.tempo),
                )
            )
        audio = render(rendered, prm, rng)
        name = f"hum_{len(truth):04d}.wav"
        sf.write(wav_dir / name, audio, SR, subtype="PCM_16")
        truth.append(
            {
                "wav": f"wav/{name}",
                "line": int(line),
                "song_key": expected_key(rec),
                "title": rec["title"],
                "artist": rec["artist"],
                "deezer_id": rec.get("deezer_id"),
                "isrc": rec.get("isrc"),
                "source": meta["source"],
                "ref_start_ms": meta["start_ms"],
                "ref_dur_ms": int(notes[-1][1] + notes[-1][2] - t0),
                "notes": len(notes),
                "dropped_notes": dropped,
                "duration_s": round(len(audio) / SR, 2),
                "transpose": round(12 * octave + prm.key_shift, 2),
                **{k: v for k, v in prm.__dict__.items() if k != "octave"},
            }
        )
    (out_dir / "ground-truth.json").write_text(
        json.dumps(
            {"jsonl": jsonl.name, "seed": seed, "sample_rate": SR, "items": truth},
            indent=1,
            ensure_ascii=False,
        )
    )
    log.info("synthetic eval: %d hums in %s", len(truth), out_dir)
    return {"items": len(truth), "dir": str(out_dir)}


# ------------------------------------------------------------------------------ CHAD

_YT_NOISE_RE = re.compile(
    r"\s*[\(\[][^\)\]]*(official|video|audio|lyric|lyrics|visuali[sz]er|hd|hq|4k|remaster|live|mv|m/v|clip)"
    r"[^\)\]]*[\)\]]",
    re.I,
)
_YT_TAIL_RE = re.compile(
    r"\s*[|/]\s*.*$|\s+(official\s+)?(music\s+)?video\s*$|\s+lyrics?\s*$", re.I
)


def parse_youtube_title(title: str, author: str) -> tuple[str, str]:
    """Best-effort (artist, song) from a YouTube title and channel name."""
    t = _YT_NOISE_RE.sub("", title or "").strip()
    t = _YT_TAIL_RE.sub("", t).strip().strip('"“”')
    for sep in (" - ", " – ", " — ", " -- ", ": "):
        if sep in t:
            a, s = t.split(sep, 1)
            return a.strip(), s.strip().strip('"“”')
    a = re.sub(r"\s*-\s*Topic$|VEVO$|\s+Official$", "", author or "", flags=re.I).strip()
    return a, t


def _oembed(fetcher: PoliteFetcher, video_id: str) -> dict[str, Any] | None:
    url = (
        f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    )
    try:
        r = fetcher.get(url)
    except Exception:  # noqa: BLE001
        return None
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def chad(paths: Paths, jsonl: Path) -> dict[str, Any]:
    songs = load_jsonl(jsonl)
    index: dict[tuple[str, str, str], int] = {}
    by_loose: dict[str, list[int]] = {}
    for i, rec in enumerate(songs):
        for k in match_keys(rec["artist"], rec["title"]):
            index.setdefault(k, i)
        by_loose.setdefault(loose_title_key(rec["title"]), []).append(i)

    cache = paths.cache / "chad"
    cache.mkdir(parents=True, exist_ok=True)
    csv_path = cache / "dataset.csv"
    fetcher = PoliteFetcher(min_interval=1.0)
    if not csv_path.exists():
        r = fetcher.get(CHAD_CSV_URL)
        r.raise_for_status()
        csv_path.write_bytes(r.content)
    originals: dict[str, dict[str, Any]] = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["audio_type"] == "original" and row["group_id"] not in originals:
                originals[row["group_id"]] = row
    intervals: dict[tuple[str, str], str] = {}
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["audio_type"] == "original":
                intervals[(row["group_id"], row["fragment_id"])] = row["interval"]

    root = next(
        (p for p in [paths.chad / "chad_hummings_subset", paths.chad] if p.is_dir()), paths.chad
    )
    wavs = sorted(root.rglob("*.wav"))
    groups = sorted(
        {w.relative_to(root).parts[0] for w in wavs if len(w.relative_to(root).parts) >= 3}
    )
    oembed_path = cache / "oembed.json"
    oembed = json.loads(oembed_path.read_text()) if oembed_path.exists() else {}
    mapping: dict[str, dict[str, Any]] = {}
    for g in groups:
        orig = originals.get(g)
        info: dict[str, Any] = {"group_id": g, "youtube_id": orig["youtube_id"] if orig else None}
        if orig:
            vid = orig["youtube_id"]
            if vid not in oembed:
                oembed[vid] = _oembed(fetcher, vid)
                oembed_path.write_text(json.dumps(oembed, ensure_ascii=False))
            meta = oembed.get(vid) or {}
            artist, title = parse_youtube_title(
                meta.get("title") or "", meta.get("author_name") or ""
            )
            info.update(
                yt_title=meta.get("title"),
                yt_author=meta.get("author_name"),
                artist=artist,
                title=title,
            )
            line = None
            for k in match_keys(artist, title):
                if k in index:
                    line = index[k]
                    break
            if line is None and title:
                for i in by_loose.get(loose_title_key(title), []):
                    if artist_similarity(artist, songs[i]["artist"]) >= 0.6 or (
                        artist_similarity(meta.get("author_name") or "", songs[i]["artist"]) >= 0.7
                    ):
                        line = i
                        break
            if line is None and title:
                # Titles like "Artist Song" without a separator: match on the song title alone.
                for i, rec in enumerate(songs):
                    if title_similarity(title, rec["title"]) >= 0.95 and (
                        artist_similarity(meta.get("author_name") or "", rec["artist"]) >= 0.7
                        or rec["artist"].lower() in (meta.get("title") or "").lower()
                    ):
                        line = i
                        break
            if line is not None:
                rec = songs[line]
                info.update(
                    line=line,
                    song_key=expected_key(rec),
                    song_title=rec["title"],
                    song_artist=rec["artist"],
                    deezer_id=rec.get("deezer_id"),
                )
        mapping[g] = info

    out_dir = paths.eval / "chad"
    wav_out = out_dir / "wav"
    if wav_out.exists():
        shutil.rmtree(wav_out)
    wav_out.mkdir(parents=True)
    manifest = []
    for w in wavs:
        rel = w.relative_to(root)
        if len(rel.parts) < 3:
            continue
        g, frag = rel.parts[0], rel.parts[1]
        info = mapping.get(g) or {}
        if info.get("line") is None:
            continue
        dest = wav_out / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(w, dest)
        except OSError:
            shutil.copy2(w, dest)
        manifest.append(
            {
                "wav": f"wav/{rel.as_posix()}",
                "song_key": info["song_key"],
                "line": info["line"],
                "title": info["song_title"],
                "artist": info["song_artist"],
                "deezer_id": info["deezer_id"],
                "group_id": g,
                "fragment_id": frag,
                "original_interval_s": intervals.get((g, frag)),
            }
        )
    (out_dir / "manifest.json").write_text(
        json.dumps(
            {"jsonl": jsonl.name, "licence": "CC BY-NC 4.0 (CHAD)", "items": manifest},
            indent=1,
            ensure_ascii=False,
        )
    )
    (out_dir / "groups.json").write_text(json.dumps(mapping, indent=1, ensure_ascii=False))
    matched = sum(1 for v in mapping.values() if v.get("line") is not None)
    info = {
        "groups": len(groups),
        "groups_matched": matched,
        "wavs": len(wavs),
        "manifest_items": len(manifest),
    }
    log.info("chad: %s", info)
    return info
