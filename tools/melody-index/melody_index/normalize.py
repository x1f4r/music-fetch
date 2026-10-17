"""Artist/title normalization and the matching keys used to merge sources.

Keys are Unicode-aware (Japanese, Korean, Hindi titles survive) and robust to
the usual metadata drift: "feat."/"with" credits, "(Remastered 2011)",
" - Radio Edit" suffixes, "&"/"and", "The " prefixes, accents and case.
"""

from __future__ import annotations

import re
import unicodedata

from rapidfuzz import fuzz

_FEAT = r"(?:feat\.?|featuring|ft\.?|with|w/|prod\.?(?:\s+by)?)"
_FEAT_GROUP_RE = re.compile(rf"\s*[\(\[]\s*{_FEAT}\s+[^\)\]]*[\)\]]", re.I)
_FEAT_TAIL_RE = re.compile(r"\s+(?:feat\.?|featuring|ft\.?)\s+.*$", re.I)
_VERSION_WORDS = (
    r"remaster(?:ed)?|re-?master|remix|mix|edit|version|versión|live|mono|stereo|acoustic|"
    r"demo|instrumental|explicit|clean|radio|extended|single|album|bonus|deluxe|original|"
    r"re-?recorded|from|soundtrack|ost|theme|sped\s+up|slowed|unplugged|session|"
    r"anniversary|edition|remake|cover|reprise|ao\s+vivo|en\s+vivo|en\s+directo|\d{4}"
)
_VERSION_GROUP_RE = re.compile(rf"\s*[\(\[][^\)\]]*\b(?:{_VERSION_WORDS})\b[^\)\]]*[\)\]]", re.I)
_DASH_TAIL_RE = re.compile(r"\s+[-–—]\s+.*$")
_ANY_GROUP_RE = re.compile(r"\s*[\(\[][^\)\]]*[\)\]]")
_NON_WORD_RE = re.compile(r"[\W_]+", re.UNICODE)

# Separators between credited artists. "&"/"and"/"," also split real band names
# ("Simon & Garfunkel"); that is fine because every source is split the same way.
_ARTIST_FEAT_RE = re.compile(
    r"\s+(?:feat\.?|featuring|ft\.?|with|duet\s+with|x|vs\.?|versus|presents|pres\.)\s+.*$", re.I
)
_ARTIST_SPLIT_RE = re.compile(
    r"\s*(?:,|;|/|\+|&|×|:|\(|\)|\b(?:and|featuring|feat|ft|with|x|vs|versus)\b\.?)\s*", re.I
)
_THE_SUFFIX_RE = re.compile(r"^(.*),\s*the$", re.I)
_NICKNAME_RE = re.compile(r"\s*[\"“”][^\"“”]+[\"“”]\s*")  # Missy "Misdemeanor" Elliott

# Words that mark a Deezer candidate as not the original recording.
NON_ORIGINAL_RE = re.compile(
    r"\b(?:karaoke|tribute|made famous|in the style of|covers?|instrumental|8[- ]?bit|lullaby|"
    r"music box|piano version|backing track|workout|originally performed|re-?recorded by)\b",
    re.I,
)


def fold(text: str) -> str:
    """Casefold, strip accents, compatibility-normalize (full-width -> ASCII)."""
    t = unicodedata.normalize("NFKC", text).casefold()
    t = "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c))
    return unicodedata.normalize("NFC", t)


def compact(text: str) -> str:
    """Folded text with every non-letter/digit removed; "&" counts as "and"."""
    t = fold(text).replace("&", " and ")
    return _NON_WORD_RE.sub("", t)


def title_core(title: str) -> str:
    """Title without featured artists and version/remaster decorations (display case kept)."""
    t = unicodedata.normalize("NFKC", title or "").strip()
    t = _FEAT_GROUP_RE.sub("", t)
    prev = None
    while prev != t:
        prev = t
        t = _VERSION_GROUP_RE.sub("", t)
    if " - " in t or " – " in t or " — " in t:
        head = _DASH_TAIL_RE.sub("", t)
        if head.strip():
            t = head
    t = _FEAT_TAIL_RE.sub("", t)
    return t.strip() or (title or "").strip()


_LEADING_THE_RE = re.compile(r"^the\s+", re.I)
_PART_RE = re.compile(r"\b(part|pt\.?|no\.?)\s+(iv|v|iii|ii|i)\b", re.I)
_ROMAN = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5"}


def _key_text(text: str) -> str:
    t = _LEADING_THE_RE.sub("", text.strip())
    return _PART_RE.sub(lambda m: f"{m.group(1)} {_ROMAN[m.group(2).lower()]}", t)


def title_key(title: str) -> str:
    core = title_core(title)
    return compact(_key_text(core)) or compact(core)


def loose_title_key(title: str) -> str:
    """Like ``title_key`` but every bracketed group is dropped ("(I Can't Get No) Satisfaction")."""
    core = _ANY_GROUP_RE.sub("", title_core(title)).strip()
    return compact(_key_text(core)) or title_key(title)


def _unthe(name: str) -> str:
    n = name.strip()
    m = _THE_SUFFIX_RE.match(n)
    if m:
        n = m.group(1)
    return re.sub(r"^the\s+", "", n, flags=re.I)


def _clean_artist(artist: str) -> str:
    a = unicodedata.normalize("NFKC", artist or "").strip()
    return _NICKNAME_RE.sub(" ", a).strip()


def primary_artist(artist: str) -> str:
    """First credited artist ("Drake Featuring Rihanna" -> "Drake")."""
    a = _ANY_GROUP_RE.sub("", _clean_artist(artist)).strip() or _clean_artist(artist)
    a = _ARTIST_FEAT_RE.sub("", a)
    parts = [p for p in _ARTIST_SPLIT_RE.split(a) if p and p.strip()]
    return parts[0].strip() if parts else a


def artist_parts(artist: str) -> list[str]:
    """Compact keys of every credited artist, in credit order, without duplicates."""
    a = _clean_artist(artist)
    m = _THE_SUFFIX_RE.match(a)
    if m:
        a = "The " + m.group(1)
    out: list[str] = []
    for part in _ARTIST_SPLIT_RE.split(a):
        if not part or not part.strip():
            continue
        k = compact(_unthe(part))
        if k and k not in out and k not in ("the", "and"):
            out.append(k)
    if not out:
        k = compact(a)
        if k:
            out.append(k)
    return out


def artist_key(artist: str) -> str:
    return compact(_unthe(primary_artist(artist)))


def song_key(artist: str, title: str) -> str:
    """Canonical identity used for the ``songs.key`` column."""
    return f"{artist_key(artist)}::{title_key(title)}"


def title_variants(title: str) -> list[str]:
    """The title plus each side of a double A-side ("It's Too Late/I Feel The Earth Move")."""
    out = [title]
    if "/" in title:
        out += [p.strip() for p in title.split("/") if len(p.strip()) >= 3]
    return out


def match_keys(artist: str, title: str) -> set[tuple[str, str, str]]:
    """Bucket keys: two entries sharing any bucket are the same song."""
    keys: set[tuple[str, str, str]] = set()
    parts = artist_parts(artist)
    for t in title_variants(title):
        tk = title_key(t)
        lk = loose_title_key(t)
        for part in parts:
            if tk:
                keys.add(("t", tk, part))
            if lk:
                keys.add(("l", lk, part))
    return keys


def title_similarity(a: str, b: str) -> float:
    """0..1 similarity of two titles after normalization."""
    ka, kb = title_key(a), title_key(b)
    if not ka or not kb:
        return 0.0
    if ka == kb or loose_title_key(a) == loose_title_key(b):
        return 1.0
    words_a, words_b = fold(title_core(a)), fold(title_core(b))
    return max(fuzz.ratio(ka, kb), fuzz.token_sort_ratio(words_a, words_b)) / 100.0


def artist_similarity(a: str, b: str) -> float:
    """0..1: best match between any credited artist on either side."""
    pa, pb = artist_parts(a), artist_parts(b)
    if not pa or not pb:
        return 0.0
    if set(pa) & set(pb):
        return 1.0
    best = max(fuzz.ratio(x, y) for x in pa for y in pb)
    whole = fuzz.ratio(compact(a), compact(b))
    # Renamed or shortened acts ("Lady Antebellum" -> "Lady A"): a prefix/infix match
    # counts, but never as a full match.
    partial = max(
        (fuzz.partial_ratio(x, y) * 0.85 for x in pa for y in pb if min(len(x), len(y)) >= 4),
        default=0.0,
    )
    return max(best, whole, partial) / 100.0


_UPLOAD_FEAT_RE = re.compile(
    r"[(\[]\s*(?:feat\.?|ft\.?|featuring|with)\s+([^()\[\]]+?)\s*[)\]]"  # "(feat. X)", "[with X]"
    r"|(?:^|\s)(?:feat\.?|ft\.?|featuring)\s+([^()\[\]|]+?)(?=\s*[(\[|]|\s+-\s|$)",  # "ft. X - ..."
    re.I,
)


def foreign_featured_artist(upload_title: str, artist: str) -> str | None:
    """A featured artist that an upload's title credits but the song's credit lacks, if any.

    Re-uploads such as "24kGoldn - Mood ft. NCK" keep the original hook but replace a verse;
    the song credit "24kGoldn Featuring iann dior" does not name NCK.
    """
    credited = artist_parts(artist)
    folded = compact(artist)
    for m in _UPLOAD_FEAT_RE.finditer(_clean_artist(upload_title)):
        for name in _ARTIST_SPLIT_RE.split(m.group(1) or m.group(2) or ""):
            k = compact(_unthe(name or ""))
            if not k or k in folded or any(k in c or c in k for c in credited):
                continue
            if max((artist_similarity(name, c) for c in artist_parts(artist)), default=0.0) < 0.6:
                return name.strip()
    return None
