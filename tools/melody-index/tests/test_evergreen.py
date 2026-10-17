from melody_index.evergreen import FAME_SCORE, load, phrases


def test_bundled_list_is_valid_and_every_transcription_gives_phrases():
    items = load()
    assert len(items) >= 300
    assert all(isinstance(it.get("deezer", 1), int) for it in items)
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


def test_pins_clear_stale_matches_and_skip_transcription_only_items(tmp_path):
    from melody_index import db as dbm
    from melody_index.config import Paths
    from melody_index.evergreen import run
    from melody_index.resolve import pending

    paths = Paths(tmp_path)
    toml = tmp_path / "e.toml"

    def write(pin_a, pin_b):
        toml.write_text(
            "[[group]]\ncategory='x'\nkind='audio'\nlicence='l'\nitems=[\n"
            f"  {{title='Theme A', artist='Composer', fame=2, artists=['Orchestra'], deezer={pin_a}}},\n"
            f"  {{title='Theme B', artist='Composer', fame=3, deezer={pin_b}}},\n"
            "  ['Theme C', 'Composer', 1],\n]\n"
        )

    write(111, 0)
    run(paths, toml)
    conn = dbm.connect(paths.db)
    ids = {r["title"]: r["id"] for r in conn.execute("SELECT id, title FROM songs")}
    todo = {t.title: t for t in pending(conn, 100)}
    assert todo["Theme A"].pinned and todo["Theme A"].deezer_hint == 111
    assert ("Orchestra", "Theme A") in todo["Theme A"].aliases
    assert "Theme B" not in todo  # transcription only: resolve leaves it alone
    assert not todo["Theme C"].pinned
    # Resolved to the pinned track: a second run keeps it; a new pin clears the song's rows.
    conn.execute("INSERT INTO deezer(song_id, status, deezer_id) VALUES (?, 'ok', 111)", (ids["Theme A"],))
    conn.execute("INSERT INTO preview_melody(song_id, status) VALUES (?, 'ok')", (ids["Theme A"],))
    run(paths, toml)
    assert conn.execute("SELECT COUNT(*) FROM deezer WHERE song_id=?", (ids["Theme A"],)).fetchone()[0] == 1
    write(222, 0)
    out = run(paths, toml)
    assert out.get("repinned") == 1
    assert conn.execute("SELECT COUNT(*) FROM deezer WHERE song_id=?", (ids["Theme A"],)).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM preview_melody WHERE song_id=?", (ids["Theme A"],)).fetchone()[0] == 0
    # An item taken off the list leaves the universe.
    toml.write_text("[[group]]\ncategory='x'\nkind='audio'\nlicence='l'\nitems=[['Theme C', 'Composer', 1]]\n")
    assert run(paths, toml)["removed"] == 2
    left = {r[0] for r in conn.execute("SELECT title FROM songs WHERE in_universe=1")}
    assert left == {"Theme C"}
