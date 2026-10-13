from melody_index.resolve import Target, pick, queries, score


def cand(title, artist, rank=500000, preview="https://x", version="", album="Album", **kw):
    return {
        "id": kw.get("id", 1),
        "title": title,
        "title_short": title,
        "title_version": version,
        "artist": {"name": artist},
        "album": {"title": album},
        "rank": rank,
        "preview": preview,
    }


T = Target(1, "Hello", "Adele", [], None)


def test_queries_plain_text_and_title_fallback():
    qs = queries(Target(1, "Stay (with Justin Bieber)", "The Kid LAROI & Justin Bieber", [], None))
    assert qs[0] == "The Kid LAROI Stay"
    assert "Stay" in qs


def test_score_accepts_original_rejects_covers_and_others():
    assert score(T, cand("Hello", "Adele")) >= 0.95
    assert score(T, cand("Hello", "Lionel Richie")) == 0.0
    assert score(T, cand("Hello (Karaoke Version)", "Karaoke Hits")) < 0.8
    assert score(T, cand("Hello", "Adele", album="Hello (Made Famous by Adele)")) < score(
        T, cand("Hello", "Adele")
    )
    assert score(T, cand("Goodbye", "Adele")) == 0.0


def test_pick_prefers_previews_and_popularity():
    a = cand("Hello", "Adele", rank=900000, preview="", id=1)
    b = cand("Hello", "Adele", rank=800000, preview="https://p", id=2)
    c = cand("Hello (Live)", "Adele", rank=950000, version="(Live)", id=3)
    best, s = pick(T, [a, b, c])
    assert best["id"] == 2 and s >= 0.95


def test_aliases_rescue_native_script_titles():
    t = Target(1, "Ao to Natsu", "Mrs. GREEN APPLE", [("Mrs. GREEN APPLE", "青と夏")], None)
    assert score(t, cand("青と夏", "Mrs. GREEN APPLE")) >= 0.95
