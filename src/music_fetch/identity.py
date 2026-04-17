"""Track-identity normalization and fuzzy-merge helpers.

The central ``TrackMatch.normalized_key()`` comparison used by fusion and
timeline-stitching code is too strict: it collapses exact ``artist::title``
matches only, so a provider returning ``"CADMIUM, Chris Linton"`` for one probe
and ``"Chris Linton & CADMIUM"`` for an adjacent probe produces two "different"
tracks that never merge. This module provides:

- ``fuzzy_key(artist, title)`` — a canonical string derived from both fields,
  robust to common metadata drift (feat./ft., "(Remastered)" / "- Remaster
  2011" suffixes, artist-list ordering, "The " prefixes, diacritics).
- ``tiered_identity(track)`` — a layered identity that prefers ISRC, then
  provider-native IDs, and falls back to ``fuzzy_key``. Emits a ``(tier, key)``
  tuple so callers can distinguish strong-evidence matches (ISRC) from weak
  fuzzy ones.
- ``merges_with(a, b)`` — the merge predicate used by timeline stitching and
  fusion. Two tracks merge when their tiered identities agree, EXCEPT when
  both have ISRCs that disagree: an explicit ISRC is a strong "these are
  distinct recordings" signal and vetoes any fuzzy overlap.

All helpers are pure and side-effect-free.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Iterable


# Suffix patterns removed from a title before fuzzy comparison. Matches parentheses
# or brackets, optionally preceded by whitespace, containing any of these tokens
# (case-insensitive). We also strip trailing "- Remastered 2011" style dashes.
_PARENS_SUFFIX_RE = re.compile(
    r"\s*[\(\[][^)\]]*\b(remaster(?:ed)?|remix|edit|radio\s+edit|extended|"
    r"bonus\s+track|live|explicit|clean|version|acoustic|mono|stereo|deluxe|"
    r"re[\- ]?record(?:ed)?)\b[^)\]]*[\)\]]",
    re.IGNORECASE,
)

_TRAILING_DASH_RE = re.compile(
    r"\s[-–—]\s.*\b(remaster(?:ed)?|edit|version|mix|acoustic|re[\- ]?record(?:ed)?)\b.*$",
    re.IGNORECASE,
)

# ``feat.`` / ``ft.`` / ``featuring`` cut off both the artist list AND the title
# (providers sometimes put "feat. X" in either field).
_FEAT_RE = re.compile(
    r"\s*(?:[\(\[])?\s*\b(?:feat(?:\.|uring)?|ft\.?)\b.*?(?:[\)\]]|$)",
    re.IGNORECASE,
)

# Separators we accept between multiple artists. The list is intentionally broad
# because providers vary wildly: Shazam uses "&", ACRCloud often uses ",",
# AudD sometimes uses " / " or " + ".
_ARTIST_SEPARATORS = re.compile(r"\s*(?:,|&|\band\b|/|\+|;|×|\bx\b)\s*", re.IGNORECASE)


def _strip_diacritics(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _clean_token(text: str) -> str:
    """Lowercase, strip diacritics, drop anything that isn't ``[a-z0-9]``."""
    lowered = _strip_diacritics(text).lower()
    return re.sub(r"[^a-z0-9]+", "", lowered)


def _normalize_title(title: str | None) -> str:
    if not title:
        return ""
    working = title.strip()
    # Drop "feat. X" first — it can appear before the parenthesized suffix.
    working = _FEAT_RE.sub("", working)
    # Then drop "(Remastered)" / "[Live]" style suffixes, possibly multiple times.
    previous = None
    while previous != working:
        previous = working
        working = _PARENS_SUFFIX_RE.sub("", working)
    # Then drop " - Remastered 2011" style trailing clauses.
    working = _TRAILING_DASH_RE.sub("", working)
    # Then the "The " prefix (English stop-word that often differs across providers).
    working = re.sub(r"^the\s+", "", working, flags=re.IGNORECASE)
    return _clean_token(working)


def _normalize_artists(artist: str | None) -> str:
    if not artist:
        return ""
    working = artist.strip()
    working = _FEAT_RE.sub("", working)
    # Drop leading "The " on each artist component after splitting.
    components = [part for part in _ARTIST_SEPARATORS.split(working) if part.strip()]
    cleaned: list[str] = []
    for component in components:
        component = re.sub(r"^the\s+", "", component.strip(), flags=re.IGNORECASE)
        token = _clean_token(component)
        if token:
            cleaned.append(token)
    # Sort to make "A & B" and "B, A" collapse to the same canonical form.
    cleaned.sort()
    # De-duplicate (providers sometimes duplicate an artist).
    seen: list[str] = []
    for token in cleaned:
        if token not in seen:
            seen.append(token)
    return ";".join(seen)


def fuzzy_key(artist: str | None, title: str | None) -> str:
    """Return a canonical string identity for ``(artist, title)``.

    Two tracks that should be treated as the same song by human judgment should
    map to the same ``fuzzy_key``. Two tracks that are clearly different (even
    slightly) should map to different keys. Examples treated as the SAME:

    >>> fuzzy_key("Prince", "Purple Rain")
    >>> fuzzy_key("Prince", "Purple Rain (Remastered 2015)")
    >>> fuzzy_key("Prince", "Purple Rain - Remastered 2015")
    >>> fuzzy_key("CADMIUM, Chris Linton", "Slow Down")
    >>> fuzzy_key("Chris Linton & CADMIUM", "Slow Down")
    >>> fuzzy_key("Beyoncé", "Halo")
    >>> fuzzy_key("Beyonce", "Halo")
    """
    return f"{_normalize_artists(artist)}::{_normalize_title(title)}"


PROVIDER_ID_PREFIXES: list[tuple[str, str]] = [
    # Order matters: we prefer the most trustworthy/specific IDs first.
    ("acrcloud", "acr"),
    ("audd", "audd"),
    ("shazam", "shz"),
    ("catalog", "cat"),
]


def tiered_identity(
    isrc: str | None,
    provider_ids: dict[str, str] | None,
    artist: str | None,
    title: str | None,
) -> tuple[str, str]:
    """Return ``(tier, key)`` — see module docstring."""
    if isrc:
        trimmed = isrc.strip().upper()
        if trimmed:
            return ("isrc", f"isrc::{trimmed}")
    for pid_key, prefix in PROVIDER_ID_PREFIXES:
        value = (provider_ids or {}).get(pid_key)
        if value and value.strip():
            return ("provider_id", f"{prefix}::{value.strip().lower()}")
    return ("fuzzy", f"fuzzy::{fuzzy_key(artist, title)}")


def identity_tier(key: str) -> str:
    """Recover the tier label from an identity key produced by ``tiered_identity``.

    Useful for observability so we can SQL for "what fraction of merges were
    tier-C fuzzy?"
    """
    if key.startswith("isrc::"):
        return "isrc"
    for _pid_key, prefix in PROVIDER_ID_PREFIXES:
        if key.startswith(f"{prefix}::"):
            return "provider_id"
    if key.startswith("fuzzy::"):
        return "fuzzy"
    return "unknown"


def merges_with(
    left_isrc: str | None,
    left_provider_ids: dict[str, str] | None,
    left_artist: str | None,
    left_title: str | None,
    right_isrc: str | None,
    right_provider_ids: dict[str, str] | None,
    right_artist: str | None,
    right_title: str | None,
) -> bool:
    """Return True if the two track descriptors should be treated as one song.

    - If both sides have an ISRC and they differ, always return False (ISRC
      veto). This is the key protection against fuzzy keys collapsing two
      distinct recordings that happen to share a title + artist string.
    - Otherwise: return True iff their tiered identities agree.
    """
    if left_isrc and right_isrc:
        if left_isrc.strip().upper() != right_isrc.strip().upper():
            return False
        return True
    left_tier, left_key = tiered_identity(left_isrc, left_provider_ids, left_artist, left_title)
    right_tier, right_key = tiered_identity(right_isrc, right_provider_ids, right_artist, right_title)
    return left_key == right_key


def all_identity_keys(tracks: Iterable[tuple[str | None, dict[str, str] | None, str | None, str | None]]) -> set[str]:
    """Utility: collect the tiered-identity key for every ``(isrc, pids, artist, title)``."""
    return {tiered_identity(isrc, pids, art, ttl)[1] for isrc, pids, art, ttl in tracks}
