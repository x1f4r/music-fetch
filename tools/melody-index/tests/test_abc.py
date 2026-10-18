from melody_index.abc import key_signature, parse, to_notes


def pitches(abc):
    return [p for p, _, _ in to_notes(abc)]


def test_pitches_octaves_accidentals_and_keys():
    assert pitches("L:1/4\nK:C\nC D E F G A B c c' C, z ^F _B =B") == [60, 62, 64, 65, 67, 69, 71, 72, 84, 48, 66, 70, 71]
    assert key_signature("G")["F"] == 1 and key_signature("F")["B"] == -1
    assert key_signature("Em") == key_signature("G")
    assert key_signature("Bb")["E"] == -1 and key_signature("Bb")["A"] == 0
    # Accidentals hold until the bar line; the key signature comes back after it.
    assert pitches("L:1/4\nK:G\nF ^F =F F | F") == [66, 66, 65, 65, 66]


def test_lengths_tempo_ties_broken_rhythm_and_triplets():
    notes = to_notes("L:1/8\nQ:1/4=120\nK:C\nC2 D E/F/ G>A B<c (3cde z2 f4-f")
    onsets = [o for _, o, _ in notes]
    durs = [d for _, _, d in notes]
    # quarter = 500 ms, eighth = 250 ms
    assert onsets[:4] == [0, 500, 750, 875]
    assert durs[:4] == [500, 250, 125, 125]
    assert durs[4:8] == [375, 125, 125, 375]  # G>A, B<c
    assert [round(d) for d in durs[8:11]] == [167, 167, 167]  # (3 cde in the time of two eighths
    assert durs[-1] == 1250 and onsets[-1] == onsets[-2] + durs[-2] + 500  # tie f4-f, after a rest z2


def test_chords_keep_the_top_note_and_decorations_are_ignored():
    notes, fields = parse('T:x\nL:1/4\nK:D\n"D"[DFA] !trill!B {g}A |]')
    assert [n.pitch for n in notes] == [69, 71, 69] and fields["T"] == "x"
