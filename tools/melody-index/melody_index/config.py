"""Paths and constants shared by all stages."""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

USER_AGENT = "MusicFetch-index-builder/1.0"


@dataclass(frozen=True)
class Paths:
    """Layout of the build directory (``$MFIDX_ROOT``, default ``~/melody-build``)."""

    root: Path

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def cache(self) -> Path:
        return self.root / "cache"

    @property
    def state(self) -> Path:
        return self.root / "state"

    @property
    def db(self) -> Path:
        return self.state / "build.sqlite3"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def out(self) -> Path:
        return self.root / "out"

    @property
    def eval(self) -> Path:
        return self.root / "eval"

    @property
    def billboard(self) -> Path:
        return self.data / "billboard" / "all.json"

    @property
    def lmd(self) -> Path:
        return self.data / "lmd"

    @property
    def rmvpe_weights(self) -> Path:
        return self.data / "models" / "rmvpe.pt"

    @property
    def chad(self) -> Path:
        return self.data / "chad"

    def ensure(self) -> None:
        for p in (self.data, self.cache, self.state, self.logs, self.out, self.eval):
            p.mkdir(parents=True, exist_ok=True)


def paths() -> Paths:
    root = Path(os.environ.get("MFIDX_ROOT", str(Path.home() / "melody-build"))).expanduser()
    p = Paths(root)
    p.ensure()
    return p


def setup_logging(name: str, verbose: bool = False) -> logging.Logger:
    """Log to stderr and to ``logs/<name>.log`` (appending)."""
    p = paths()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)
    file = logging.FileHandler(p.logs / f"{name}.log")
    file.setFormatter(fmt)
    root.addHandler(file)
    for noisy in ("urllib3", "requests", "matplotlib", "numba"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(f"mfidx.{name}")
