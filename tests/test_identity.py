"""Tiered-identity + fuzzy-merge tests (T1.1)."""
from __future__ import annotations

import pytest

from music_fetch.identity import fuzzy_key, merges_with, tiered_identity
from music_fetch.models import TrackMatch


# ---------------------------------------------------------------------------
# fuzzy_key table: pairs that should collapse to the same key.
# ---------------------------------------------------------------------------

SAME_FUZZY_PAIRS = [
    # "(Remastered)" suffix should be stripped.
    (("Prince", "Purple Rain"), ("Prince", "Purple Rain (Remastered 2015)")),
    # "- Remaster 2011" dash clause too.
    (("The Beatles", "Yesterday"), ("The Beatles", "Yesterday - Remastered 2015")),
    # Artist list reordered / different separator.
    (("CADMIUM, Chris Linton", "Slow Down"), ("Chris Linton & CADMIUM", "Slow Down")),
    (("A & B", "Anthem"), ("B and A", "Anthem")),
    # Diacritics are normalized away.
    (("Beyoncé", "Halo"), ("Beyonce", "Halo")),
    # Leading "The " prefix is stripped.
    (("The Beatles", "Hey Jude"), ("Beatles", "Hey Jude")),
    # "feat." / "ft." is dropped from both fields.
    (("Drake ft. Rihanna", "What's My Name"), ("Drake", "What's My Name")),
    (("Drake", "What's My Name (feat. Rihanna)"), ("Drake", "What's My Name")),
    # Case and whitespace.
    (("  Drake  ", "  whats my name  "), ("drake", "Whats My Name")),
]


DIFFERENT_FUZZY_PAIRS = [
    # Different artist entirely.
    (("The Beatles", "Yesterday"), ("Boyz II Men", "Yesterday")),
    # Different title.
    (("Prince", "Purple Rain"), ("Prince", "Raspberry Beret")),
    # Similar titles but different artist.
    (("Ed Sheeran", "Halo"), ("Beyoncé", "Halo")),
    # "The The" band shouldn't collapse into nothing.
    (("The The", "This Is the Day"), ("Unknown", "This Is the Day")),
]


@pytest.mark.parametrize("left,right", SAME_FUZZY_PAIRS)
def test_fuzzy_key_collapses_equivalents(left, right):
    assert fuzzy_key(*left) == fuzzy_key(*right), f"should merge: {left} / {right}"


@pytest.mark.parametrize("left,right", DIFFERENT_FUZZY_PAIRS)
def test_fuzzy_key_separates_distinct_tracks(left, right):
    assert fuzzy_key(*left) != fuzzy_key(*right), f"should stay separate: {left} / {right}"


# ---------------------------------------------------------------------------
# tiered_identity: tier A > B > C order.
# ---------------------------------------------------------------------------


def test_tiered_identity_prefers_isrc():
    tier, key = tiered_identity("USRC17607839", {"shazam": "abc"}, "Some Artist", "Some Title")
    assert tier == "isrc"
    assert key == "isrc::USRC17607839"


def test_tiered_identity_falls_back_to_provider_id():
    tier, key = tiered_identity(None, {"acrcloud": "ACR-42"}, "Some Artist", "Some Title")
    assert tier == "provider_id"
    assert key.startswith("acr::")


def test_tiered_identity_falls_back_to_fuzzy():
    tier, key = tiered_identity(None, {}, "Prince", "Purple Rain (Remastered 2015)")
    assert tier == "fuzzy"
    assert key.startswith("fuzzy::")
    # Fuzzy-normalized: remaster stripped.
    other_tier, other_key = tiered_identity(None, {}, "Prince", "Purple Rain")
    assert key == other_key


# ---------------------------------------------------------------------------
# merges_with: ISRC veto & tiered agreement.
# ---------------------------------------------------------------------------


def test_merges_with_collapses_remaster_and_live_variants():
    a = TrackMatch(title="Purple Rain", artist="Prince")
    b = TrackMatch(title="Purple Rain (Remastered 2015)", artist="Prince")
    assert a.merges_with(b)


def test_merges_with_vetoes_distinct_isrcs_even_when_fuzzy_key_matches():
    # Different ISRCs → must not merge, regardless of fuzzy identity.
    a = TrackMatch(title="Purple Rain", artist="Prince", isrc="USRC17607839")
    b = TrackMatch(title="Purple Rain", artist="Prince", isrc="USRC12345678")
    assert not a.merges_with(b)


def test_merges_with_agrees_on_shared_isrc():
    a = TrackMatch(title="Purple Rain", artist="Prince", isrc="USRC17607839")
    b = TrackMatch(title="Purple Rain (Remastered)", artist="Prince", isrc="usrc17607839")
    assert a.merges_with(b)


def test_merges_with_agrees_on_provider_id_when_isrc_missing():
    a = TrackMatch(title="Track", artist="Artist", provider_ids={"acrcloud": "acr-1"})
    b = TrackMatch(title="different title", artist="Artist", provider_ids={"acrcloud": "acr-1"})
    assert a.merges_with(b)


def test_merges_with_separates_different_songs():
    a = TrackMatch(title="Purple Rain", artist="Prince")
    b = TrackMatch(title="Raspberry Beret", artist="Prince")
    assert not a.merges_with(b)


def test_normalized_key_is_tiered():
    assert TrackMatch(title="X", artist="Y", isrc="USRC000").normalized_key().startswith("isrc::")
    assert (
        TrackMatch(title="X", artist="Y", provider_ids={"acrcloud": "42"}).normalized_key().startswith("acr::")
    )
    assert TrackMatch(title="X", artist="Y").normalized_key().startswith("fuzzy::")


def test_identity_tier_reports_tier():
    assert TrackMatch(title="X", isrc="USRC000").identity_tier() == "isrc"
    assert TrackMatch(title="X", provider_ids={"shazam": "s"}).identity_tier() == "provider_id"
    assert TrackMatch(title="X", artist="Y").identity_tier() == "fuzzy"
