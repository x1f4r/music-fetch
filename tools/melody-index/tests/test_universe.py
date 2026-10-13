import json

from melody_index.universe import (
    ChartEntry,
    combine,
    components,
    load_billboard,
    merge,
    parse_kworb,
    percentiles,
)


def test_billboard_aggregation(tmp_path):
    charts = [
        {"date": "1990-01-06", "data": [{"song": "A", "artist": "X", "this_week": 10}]},
        {
            "date": "1990-01-13",
            "data": [
                {"song": "A", "artist": "X", "this_week": 1},
                {"song": "B", "artist": "Y", "this_week": 100},
            ],
        },
    ]
    p = tmp_path / "all.json"
    p.write_text(json.dumps(charts))
    rows = {(e.artist, e.title): e for e in load_billboard(p)}
    a = rows[("X", "A")]
    assert (a.weeks, a.peak, a.points, a.first_date, a.last_date, a.year) == (
        2,
        1,
        191.0,
        "1990-01-06",
        "1990-01-13",
        1990,
    )
    assert rows[("Y", "B")].points == 1.0


def test_popularity_is_monotone_in_each_signal():
    base = [
        ChartEntry(
            "billboard",
            "hot100",
            "X",
            "A",
            peak=5,
            weeks=10,
            points=600,
            first_date="2000-01-01",
            last_date="2000-03-01",
            year=2000,
        )
    ]
    more_weeks = [
        ChartEntry(
            "billboard",
            "hot100",
            "X",
            "A",
            peak=5,
            weeks=20,
            points=1200,
            first_date="2000-01-01",
            last_date="2000-03-01",
            year=2000,
        )
    ]
    newer = [
        ChartEntry(
            "billboard",
            "hot100",
            "X",
            "A",
            peak=5,
            weeks=10,
            points=600,
            first_date="2020-01-01",
            last_date="2020-03-01",
            year=2020,
        )
    ]
    s0 = combine(components(base, {}))
    assert combine(components(more_weeks, {})) > s0
    assert combine(components(newer, {})) > s0
    streams = base + [ChartEntry("kworb", "global", "X", "A", streams=500_000_000)]
    assert combine(components(streams, {})) > s0
    apple_hi = base + [ChartEntry("apple", "us", "X", "A", rank=1)]
    apple_lo = base + [ChartEntry("apple", "us", "X", "A", rank=90)]
    assert combine(components(apple_hi, {})) > combine(components(apple_lo, {})) > s0


def test_percentiles():
    assert percentiles([3.0, 1.0, 2.0]) == [1.0, 1 / 3, 2 / 3]
    assert percentiles([1.0, 1.0]) == [0.75, 0.75]


def test_merge_dedupes_variants_across_sources():
    entries = [
        ChartEntry(
            "billboard",
            "hot100",
            "Lady Gaga & Bruno Mars",
            "Die With A Smile",
            peak=1,
            weeks=30,
            points=2500,
            first_date="2024-08-31",
            last_date="2025-06-01",
            year=2024,
        ),
        ChartEntry("kworb", "global", "Lady Gaga", "Die With A Smile", streams=3_990_000_000),
        ChartEntry("apple", "de", "Lady Gaga & Bruno Mars", "Die With A Smile", rank=3, year=2024),
        ChartEntry(
            "deezer", "chart:0", "Lady Gaga", "Die With A Smile (Remastered)", rank=7, deezer_id=42
        ),
        ChartEntry(
            "billboard",
            "hot100",
            "Adele",
            "Hello",
            peak=1,
            weeks=20,
            points=1800,
            first_date="2015-11-14",
            last_date="2016-04-01",
            year=2015,
        ),
        ChartEntry(
            "billboard",
            "hot100",
            "Lionel Richie",
            "Hello",
            peak=1,
            weeks=15,
            points=1200,
            first_date="1984-03-24",
            last_date="1984-07-01",
            year=1984,
        ),
    ]
    songs = {s.key: s for s in merge(entries)}
    assert len(songs) == 3
    gaga = songs["ladygaga::diewithasmile"]
    assert gaga.title == "Die With A Smile" and gaga.artist == "Lady Gaga & Bruno Mars"
    assert gaga.deezer_hint == 42 and gaga.year == 2024
    assert set(gaga.signals) == {"bb", "sp", "am", "dz"}
    assert gaga.score > songs["adele::hello"].score


def test_parse_kworb_both_layouts():
    weekly = """<table><thead><tr><th>Artist and Title</th><th>Wks</th><th>T10</th><th>Pk</th><th>(x?)</th>
    <th>PkStreams</th><th>Total</th></tr></thead><tbody><tr><td class="text mp"><div>
    <a href="../artist/1.html">The Weeknd</a> - <a href="../track/2.html">Blinding Lights</a></div></td>
    <td>358</td><td>64</td><td>1</td><td>(x13)</td><td>52,375,259</td><td>5,550,906,172</td></tr></tbody></table>"""
    rows = parse_kworb(weekly, "global")
    assert len(rows) == 1
    r = rows[0]
    assert (r.artist, r.title, r.weeks, r.peak, r.streams) == (
        "The Weeknd",
        "Blinding Lights",
        358,
        1,
        5550906172,
    )
    alltime = """<table><thead><tr><th>Artist and Title</th><th>Streams</th><th>Daily</th></tr></thead><tbody>
    <tr><td class="text"><div>Post Malone - Sunflower - Spider-Man</div></td><td>4,509,092,576</td><td>1</td></tr>
    </tbody></table>"""
    r = parse_kworb(alltime, "alltime")[0]
    assert (r.artist, r.title, r.streams) == ("Post Malone", "Sunflower - Spider-Man", 4509092576)
