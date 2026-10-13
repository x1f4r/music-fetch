from melody_index.normalize import (
    artist_parts,
    artist_similarity,
    loose_title_key,
    match_keys,
    primary_artist,
    song_key,
    title_core,
    title_key,
    title_similarity,
)


def test_title_versions_and_features_are_stripped():
    assert title_key("Purple Rain") == title_key("Purple Rain - Remastered 2015")
    assert title_key("Purple Rain") == title_key("Purple Rain (2015 Remaster)")
    assert title_key("Stay") == title_key("Stay (with Justin Bieber)")
    assert title_key("Dracula") == title_key("Dracula (with JENNIE)")
    assert title_key("Sunflower") == title_key("Sunflower - Spider-Man: Into the Spider-Verse")
    assert title_key("Shape of You") == title_key("Shape Of You (feat. Someone) [Live]")
    assert title_core("Hey Jude - Remastered 2015") == "Hey Jude"


def test_title_keys_keep_meaningful_parentheses():
    assert title_key("(I Can't Get No) Satisfaction") == "icantgetnosatisfaction"
    assert loose_title_key("(I Can't Get No) Satisfaction") == "satisfaction"
    assert loose_title_key("(I Can't Get No) Satisfaction - Mono Version") == "satisfaction"


def test_unicode_titles_survive():
    assert title_key("点描の唄") != ""
    assert title_key("青と夏") == title_key("青と夏 (Remastered)")
    assert title_key("Beyoncé") == title_key("Beyonce")
    assert title_key("ＡＢＣ") == "abc"


def test_ampersand_equals_and():
    assert title_key("Rock & Roll") == title_key("Rock and Roll")


def test_artist_credits():
    assert primary_artist("Drake Featuring Rihanna") == "Drake"
    assert primary_artist("Calvin Harris x Dua Lipa") == "Calvin Harris"
    assert primary_artist("Elvis Presley With The Jordanaires") == "Elvis Presley"
    assert artist_parts("Simon & Garfunkel") == ["simon", "garfunkel"]
    assert artist_parts("Beatles, The") == ["beatles"]
    assert artist_parts("The Beatles") == ["beatles"]
    assert "rihanna" in artist_parts("Drake Featuring Rihanna")


def test_song_key_is_stable_across_sources():
    assert song_key("The Weeknd", "Blinding Lights") == song_key(
        "Weeknd", "Blinding Lights (Remastered)"
    )
    assert song_key("Beyoncé", "Halo") == song_key("Beyonce", "Halo")


def test_match_keys_merge_collaborations():
    a = match_keys("Lady Gaga & Bruno Mars", "Die With A Smile")
    b = match_keys("Lady Gaga", "Die With A Smile")
    c = match_keys("Bruno Mars", "Die With A Smile")
    assert a & b and a & c
    assert not (match_keys("Adele", "Hello") & match_keys("Lionel Richie", "Hello"))


def test_similarities():
    assert title_similarity("Hello", "Hello") == 1.0
    assert title_similarity("Livin' On A Prayer", "Livin' on a Prayer") == 1.0
    assert title_similarity("Hello", "Goodbye") < 0.5
    assert artist_similarity("Simon & Garfunkel", "Simon & Garfunkel") == 1.0
    assert artist_similarity("Drake Featuring Rihanna", "Rihanna") == 1.0
    assert artist_similarity("Adele", "Coldplay") < 0.5


def test_leading_the_and_double_a_sides():
    assert title_key("The House of the Rising Sun") == title_key("House Of The Rising Sun")
    assert match_keys("Carole King", "It's Too Late/I Feel The Earth Move") & match_keys(
        "Carole King", "It's Too Late"
    )
    assert title_key("Another Brick in the Wall, Part 2") == title_key(
        "Another Brick In The Wall (Part II)"
    )


def test_artist_edge_cases():
    assert "brunomars" in artist_parts("Silk Sonic (Bruno Mars & Anderson .Paak)")
    assert primary_artist("Silk Sonic (Bruno Mars & Anderson .Paak)") == "Silk Sonic"
    assert artist_parts('Missy "Misdemeanor" Elliott') == ["missyelliott"]
    assert "willow" in artist_parts("THE ANXIETY: WILLOW & Tyler Cole")
    assert 0.8 <= artist_similarity("Lady Antebellum", "Lady A") < 1.0
    assert artist_similarity("Adele", "Coldplay") < 0.5
    assert (
        title_key("Saudade Burra - Ao Vivo")
        == title_key("Saudade Burra (Ao Vivo)")
        == "saudadeburra"
    )
