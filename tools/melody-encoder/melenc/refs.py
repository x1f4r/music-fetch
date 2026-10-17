"""Reference bundles (from prep_refs) and the evaluation hold-out."""

import json
import os
import pickle
import re
import unicodedata

import numpy as np

PREVIEW, AUDIO, MIDI = 0, 1, 2

# Working directory with data/ and runs/ (MELENC_HOME, default ~/mf/encoder).
HOME = os.path.expanduser(os.environ.get("MELENC_HOME", "~/mf/encoder"))
DATA = os.path.join(HOME, "data")

# Songs of the private acceptance recordings; never trained on.
USER_TITLES = [
    "never gonna give you up",
    "shape of you",
    "shake it off",
    "billie jean",
    "blinding lights",
    "99 luftballons",
    "viva la vida",
    "wake me up",
    "megalovania",
    "my heart will go on",
    "twinkle twinkle little star",
    "toss a coin to your witcher",
    "africa",
]


def norm_title(t: str) -> str:
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"\(.*?\)|\[.*?\]", " ", t)
    t = re.sub(r"\s-\s.*$", " ", t)
    t = re.sub(r"feat\..*$", " ", t)
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


class Refs:
    def __init__(self, prefix: str):
        z = np.load(prefix + ".npz")
        self.contours = z["contours"].astype(np.float32)
        self.off = z["track_off"]
        self.song = z["track_song"]
        self.source = z["track_source"]
        self.songs = json.load(open(prefix + "-songs.json"))
        self.n_tracks = len(self.song)

    def track(self, t: int) -> np.ndarray:
        return self.contours[self.off[t] : self.off[t + 1]]

    def frames(self, t: int) -> int:
        return int(self.off[t + 1] - self.off[t])


def holdout_songs(refs: Refs, eval_pkls: list[str]) -> set[int]:
    """Song ids never used for training: every evaluation song, every song sharing a
    normalized title with one, and the user's test songs."""
    ids = set()
    for p in eval_pkls:
        for r in pickle.load(open(p, "rb")):
            if r["expected"] is not None:
                ids.add(int(r["expected"]))
    titles = {norm_title(refs.songs[i]["title"]) for i in ids}
    titles |= set(USER_TITLES)
    titles.discard("")
    for s in refs.songs:
        if norm_title(s["title"]) in titles:
            ids.add(s["id"])
    return ids
