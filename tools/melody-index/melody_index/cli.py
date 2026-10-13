"""``mfidx`` command line: one subcommand per stage plus checks and status."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import paths, setup_logging


def _latest_jsonl(out: Path) -> Path:
    files = sorted(out.glob("melody-songs-*.jsonl"))
    if not files:
        raise SystemExit("no melody-songs-*.jsonl in out/; run `mfidx export` first")
    return files[-1]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="mfidx", description="Music Fetch melody-index builder")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("universe", help="stage 1: chart sources -> song universe")
    p.add_argument("--refresh", action="store_true", help="refetch cached chart pages")
    p.add_argument("--max-songs", type=int, default=60000)

    p = sub.add_parser("resolve", help="stage 2: Deezer track per song")
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--limit", type=int)
    p.add_argument(
        "--retry-not-found", action="store_true", help="search again for unmatched songs"
    )

    p = sub.add_parser("extract", help="stage 3: previews -> RMVPE -> phrases (GPU)")
    p.add_argument("--limit", type=int)
    p.add_argument("--follow", action="store_true", help="keep running until stage 2 has finished")
    p.add_argument("--batch", type=int, default=8)
    p.add_argument("--fetchers", type=int, default=12)
    p.add_argument("--half", action="store_true", help="fp16 inference")

    sub.add_parser("renote", help="recompute preview phrases from stored f0")

    p = sub.add_parser("midi", help="stage 4: Lakh MIDI melodies")
    p.add_argument("--step", choices=["all", "catalog", "link", "extract"], default="all")
    p.add_argument("--workers", type=int)
    p.add_argument("--limit", type=int)

    p = sub.add_parser("validate", help="MIDI vs preview DTW agreement for all songs with both")
    p.add_argument("--workers", type=int, default=12)

    p = sub.add_parser("famous", help="DTW agreement report for well-known songs")
    p.add_argument("--n", type=int, default=20)

    sub.add_parser("sanity", help="RMVPE on a synthetic voice with a known melody")

    p = sub.add_parser(
        "separation", help="experiment: RMVPE on mix vs separated vocals, scored against MIDI"
    )
    p.add_argument("--n", type=int, default=20)
    p.add_argument("--model", default="vocals_mel_band_roformer.ckpt")

    p = sub.add_parser("show", help="print a song's resolution and notes")
    p.add_argument("artist")
    p.add_argument("title")

    p = sub.add_parser("export", help="stage 5: JSONL + stats.json")
    p.add_argument("--date")
    p.add_argument("--min-agreement", type=float)

    p = sub.add_parser("eval-synth", help="stage 6a: synthetic hummed queries")
    p.add_argument("--n", type=int, default=300)
    p.add_argument("--seed", type=int, default=20261013)
    p.add_argument("--jsonl", type=Path)

    p = sub.add_parser("eval-chad", help="stage 6b: CHAD humming manifest")
    p.add_argument("--jsonl", type=Path)

    sub.add_parser("status", help="progress, throughput, ETA")

    args = ap.parse_args(argv)
    P = paths()
    log = setup_logging(args.cmd if args.cmd not in ("status", "show") else "cli", args.verbose)

    if args.cmd == "universe":
        from . import universe

        out = universe.run(P, refresh=args.refresh, max_songs=args.max_songs)
    elif args.cmd == "resolve":
        from . import resolve

        out = resolve.run(
            P, workers=args.workers, limit=args.limit, retry_not_found=args.retry_not_found
        )
    elif args.cmd == "extract":
        from .extract import Extractor

        out = Extractor(P, batch=args.batch, fetchers=args.fetchers, half=args.half).run(
            limit=args.limit, follow=args.follow
        )
    elif args.cmd == "renote":
        from .extract import renote

        out = renote(P)
    elif args.cmd == "midi":
        from . import midi

        out = {}
        if args.step in ("all", "catalog"):
            out["catalog"] = midi.catalog(P)
        if args.step in ("all", "link"):
            out["link"] = midi.link(P)
        if args.step in ("all", "extract"):
            out["extract"] = midi.extract(P, workers=args.workers, limit=args.limit)
    elif args.cmd == "validate":
        from . import validate

        out = validate.run(P, workers=args.workers)
    elif args.cmd == "famous":
        from . import checks

        out = checks.famous(P, want=args.n)
    elif args.cmd == "sanity":
        from . import checks

        out = checks.rmvpe_synthetic(P)
    elif args.cmd == "separation":
        from . import separation

        out = separation.experiment(P, n=args.n, model=args.model)
    elif args.cmd == "show":
        from . import checks

        print(checks.show_song(P, args.artist, args.title))
        return 0
    elif args.cmd == "export":
        from . import export

        out = export.run(P, date=args.date, min_agreement=args.min_agreement)
    elif args.cmd == "eval-synth":
        from . import evalset

        out = evalset.synth(P, args.jsonl or _latest_jsonl(P.out), n=args.n, seed=args.seed)
    elif args.cmd == "eval-chad":
        from . import evalset

        out = evalset.chad(P, args.jsonl or _latest_jsonl(P.out))
    elif args.cmd == "status":
        from . import status

        status.show(P)
        return 0
    else:  # pragma: no cover - argparse enforces the choices
        ap.error(f"unknown command {args.cmd}")
        return 2
    log.info("%s done", args.cmd)
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
