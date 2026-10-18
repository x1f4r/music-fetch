from melody_index.export import _assemble, apply_tier, estimate_bytes


def ph(n, source="preview", start=0):
    return {"source": source, "start_ms": start, "notes": [[60, i * 100, 90] for i in range(n)]}


def record(i, preview=100, full=0, window=0):
    parts = {
        "preview": [ph(preview)] if preview else [],
        "full": [ph(full, "audio")] if full else [],
        "midi": [],
        "window": [ph(window, start=60000)] if window else [],
        "symbolic": [],
    }
    parts["base"] = parts["preview"] + parts["midi"] + parts["symbolic"]
    return (1.0 - i / 1000, {"title": f"s{i}", "phrases": []}, {"parts": parts})


def test_full_tracks_go_to_the_most_popular_songs_that_fit():
    recs = [record(i, full=600, window=80 if i == 2 else 0) for i in range(5)]
    base = estimate_bytes(5 * 100 + 80, 5)
    # Room for two full tracks: the two most popular songs get them.
    budget = (base + 2 * estimate_bytes(0, 0, 600) + 1) / 1e6
    kept, info = apply_tier(recs, None, budget)
    assert len(kept) == 5 and info["full_tracks_included"] == 2 and info["full_tracks_left_out"] == 3
    for _, rec, meta in kept:
        _assemble(rec, meta)
    sources = [[p["source"] for p in rec["phrases"]] for _, rec, _ in kept]
    assert sources[0] == ["preview", "audio"]
    assert sources[1] == ["preview", "audio"]
    assert sources[2] == ["preview", "preview"]  # its second window comes back without a full track
    assert sources[3] == ["preview"]


def test_max_songs_and_a_budget_too_small_for_the_songs_themselves():
    recs = [record(i, full=600) for i in range(10)]
    kept, info = apply_tier(recs, 6, estimate_bytes(4 * 100, 4) / 1e6)
    assert len(kept) == 4 and info["songs_dropped_for_budget"] == 2 and info["full_tracks_included"] == 0
    kept, info = apply_tier(recs, 3, None)
    assert len(kept) == 3 and info["full_tracks_included"] == 3


def test_estimate_matches_the_measured_builds():
    # mf index build, 2026-10-18: android 32,816,843 B; mac 63,888,548 B.
    assert abs(estimate_bytes(3_801_759 - 583_660, 24_982, 583_660) - 32_816_843) < 0.01 * 32_816_843
    assert abs(estimate_bytes(7_759_215 - 583_660, 58_288, 583_660) - 63_888_548) < 0.01 * 63_888_548
