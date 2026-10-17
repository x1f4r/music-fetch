from melody_index.evergreen import FAME_SCORE, load, phrases


def test_bundled_list_is_valid_and_every_transcription_gives_phrases():
    items = load()
    assert len(items) >= 300
    assert len({it["id"] for it in items}) == len(items)
    for it in items:
        assert it["licence"] and int(it["fame"]) in FAME_SCORE
        if it["kind"] in ("symbolic", "both"):
            ph = phrases(it["abc"])
            assert ph, it["title"]
            assert all(p["source"] == "midi" and len(p["notes"]) >= 5 for p in ph), it["title"]
            notes = [n for p in ph for n in p["notes"]]
            assert all(30 <= n[0] <= 100 and n[2] > 0 for n in notes), it["title"]


def test_known_tunes_have_their_intervals():
    by_title = {it["title"]: it for it in load()}

    def steps(title, n):
        notes = [x for p in phrases(by_title[title]["abc"]) for x in p["notes"]][: n + 1]
        return [round(b[0] - a[0]) for a, b in zip(notes, notes[1:])]

    assert steps("Ode to Joy", 7) == [0, 1, 2, 0, -2, -1, -2]
    assert steps("Für Elise", 8) == [-1, 1, -1, 1, -5, 3, -2, -3]
    assert steps("Twinkle, Twinkle, Little Star", 6) == [0, 7, 0, 2, 0, -2]
    assert steps("Korobeiniki (Tetris Theme)", 7) == [-5, 1, 2, -2, -1, -2, 0]
