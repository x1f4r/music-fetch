import numpy as np
import pretty_midi

from melody_index.midi import analyze_file, melody_score, skyline, track_features


def _melody_notes(n=64, base=67):
    pattern = [0, 2, 4, 5, 7, 5, 4, 2]
    return [(i * 0.4, i * 0.4 + 0.35, base + pattern[i % 8]) for i in range(n)]


def _chord_notes(n=32):
    out = []
    for i in range(n):
        t = i * 0.8
        out += [(t, t + 0.8, 48), (t, t + 0.8, 52), (t, t + 0.8, 55)]
    return out


def test_features_separate_melody_from_chords():
    syl = np.array([n[0] for n in _melody_notes()][:40])
    mel = track_features(0, "Melody", 73, _melody_notes(), 30.0, syl)
    chords = track_features(1, "Strings", 48, _chord_notes(), 30.0, syl)
    assert mel.mono == 1.0 and chords.mono == 0.0
    assert mel.lyric_match == 1.0 and chords.lyric_match <= 0.5
    assert melody_score(mel) > melody_score(chords) + 3


def test_bass_and_name_penalties():
    bass = track_features(
        0, "Bass", 33, [(a, b, p - 36) for a, b, p in _melody_notes()], 30.0, np.zeros(0)
    )
    lead = track_features(1, "", 81, _melody_notes(), 30.0, np.zeros(0))
    assert melody_score(lead) > melody_score(bass)


def test_skyline_cuts_overlaps():
    line = skyline([(0.0, 1.0, 60), (0.0, 1.0, 64), (0.5, 1.0, 62)])
    assert [(n.pitch, n.onset_ms, n.dur_ms) for n in line] == [(64, 0, 500), (62, 500, 500)]


def test_analyze_file_picks_the_melody(tmp_path):
    pm = pretty_midi.PrettyMIDI(initial_tempo=120)
    chords = pretty_midi.Instrument(program=48, name="Strings")
    for s, e, p in _chord_notes(40):
        chords.notes.append(pretty_midi.Note(velocity=80, pitch=p, start=s, end=e))
    drums = pretty_midi.Instrument(program=0, is_drum=True, name="Drums")
    for i in range(64):
        drums.notes.append(
            pretty_midi.Note(velocity=80, pitch=36, start=i * 0.5, end=i * 0.5 + 0.1)
        )
    lead = pretty_midi.Instrument(program=65, name="Lead Vocal")
    for s, e, p in _melody_notes(48):
        lead.notes.append(pretty_midi.Note(velocity=90, pitch=p, start=s, end=e))
    # A rest of 2 s splits the line into two phrases.
    for s, e, p in _melody_notes(16):
        lead.notes.append(pretty_midi.Note(velocity=90, pitch=p - 2, start=s + 21.2, end=e + 21.2))
    pm.instruments += [chords, drums, lead]
    path = tmp_path / "song.mid"
    pm.write(str(path))
    res = analyze_file(str(path))
    assert res["status"] == "ok"
    assert res["tracks"][0]["name"] == "Lead Vocal"
    track0 = [p for p in res["phrases"] if p["track"] == 0]
    assert len(track0) == 2
    assert track0[0]["start_ms"] == 0 and track0[1]["start_ms"] == 21200
    assert track0[0]["notes"][1] == [69.0, 400, 350]


def test_analyze_file_rejects_garbage(tmp_path):
    p = tmp_path / "bad.mid"
    p.write_bytes(b"not a midi file")
    assert analyze_file(str(p))["status"] == "error"


def test_arpeggio_loses_to_stepwise_melody():
    arp_pattern = [56, 63, 70, 63, 72, 70, 63, 70]
    arp = [(i * 0.25, i * 0.25 + 0.24, arp_pattern[i % 8]) for i in range(240)]
    mel = [
        (1.0 + i * 0.5, 1.0 + i * 0.5 + 0.45, 67 + [0, 2, 4, 2, 0, -1, 0, 2][i % 8])
        for i in range(60)
    ]
    fa = track_features(0, "Muted Guitar", 28, arp, 60.0, np.zeros(0))
    fm = track_features(1, "", 73, mel, 60.0, np.zeros(0))
    assert fa.stepwise < 0.2 and fm.stepwise > 0.8
    assert melody_score(fm) > melody_score(fa) + 1
