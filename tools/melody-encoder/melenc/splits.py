"""Song-disjoint splits of the real-hum sets, for checkpoint selection.

CHAD is CC BY-NC: it never reaches training. Its songs are split in two halves by a hash of
the normalized title (songs sharing a title stay together): "V" selects checkpoints, "T" is
reported. MIR-QBSH ground-truth songs whose hash falls in a quarter are held out of training
and serve as a second validation set (the hums of the other 36 songs may be trained on).
"""

import hashlib

from melenc.refs import norm_title


def _h(s: str) -> int:
    return hashlib.sha1(s.encode()).digest()[0]


def chad_half(title: str) -> str:
    """'V' (validation) or 'T' (test) for a CHAD song title."""
    return "V" if _h("chad:" + norm_title(title)) % 2 == 0 else "T"


def qbsh_val(ref: int) -> bool:
    """True for MIR-QBSH ground-truth MIDI indices held out for validation (about 12 of 48)."""
    return _h(f"qbsh:{ref}") % 4 == 0
