use super::*;
use crate::pipeline::testutil::{FakeServices, TempPcm, noise, play, track};

async fn survey(services: &FakeServices, pcm: &PcmFile, prior: &Prior) -> Survey {
    run(services, pcm, prior, Priority::Interactive).await.expect("survey")
}

fn title(verdict: &Verdict) -> Option<&str> {
    match verdict {
        Verdict::Single { track, .. } => Some(track.title.as_str()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_prior_costs_one_probe() {
    let song = track("Artist", "Song", 240);
    let audio = noise(240, 11);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, vec![play(0, 240, &song, 0)]);
    let prior = Prior { track: Some(song.clone()), strong: true };
    let s = survey(&services, &media.pcm, &prior).await;
    assert_eq!(title(&s.verdict), Some("Song"));
    assert_eq!(services.calls(), 1);
    let Verdict::Single { evidence, .. } = &s.verdict else { unreachable!() };
    assert_eq!(crate::pipeline::stitch::grade(evidence).0, crate::model::Tier::Confirmed);
}

#[tokio::test(flavor = "multi_thread")]
async fn loops_and_repeats_stay_one_song() {
    // A 9-minute extended track whose position jumps (loops): offsets disagree, the song does not.
    let song = track("Artist", "Extended", 300);
    let mut second = play(270, 540, &song, 60);
    second.offset_ms = 60_000;
    let audio = noise(540, 12);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, vec![play(0, 270, &song, 0), second]);
    let s = survey(&services, &media.pcm, &Prior::default()).await;
    assert_eq!(title(&s.verdict), Some("Extended"));
    assert_eq!(services.calls(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stray_answer_does_not_split_a_song() {
    // One probe hears a sampled record instead of the song.
    let song = track("Artist", "Song", 600);
    let sample = track("Other", "Sampled Record", 200);
    let audio = noise(600, 13);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, vec![play(0, 600, &song, 0)]);
    services.overrides.push((100_000, 130_000, Some(sample)));
    let s = survey(&services, &media.pcm, &Prior::default()).await;
    assert_eq!(title(&s.verdict), Some("Song"), "{:?}", s.log);
    let Verdict::Single { alternates, .. } = &s.verdict else { unreachable!() };
    assert_eq!(alternates.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn versions_of_one_song_are_one_song() {
    // A live recording that matches the studio version at one point.
    let live = track("Artist", "Song (Live)", 420);
    let studio = track("Artist", "Song", 400);
    let audio = noise(420, 14);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, vec![play(0, 420, &live, 0)]);
    services.overrides.push((300_000, 340_000, Some(studio)));
    let prior = Prior { track: Some(live.clone()), strong: false };
    let s = survey(&services, &media.pcm, &prior).await;
    assert_eq!(title(&s.verdict), Some("Song (Live)"));
}

#[tokio::test(flavor = "multi_thread")]
async fn two_songs_in_a_row_are_a_mix() {
    let a = track("A", "First", 200);
    let b = track("B", "Second", 200);
    let audio = noise(400, 15);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, vec![play(0, 200, &a, 0), play(200, 400, &b, 0)]);
    let s = survey(&services, &media.pcm, &Prior::default()).await;
    let Verdict::Mix { probes } = &s.verdict else { panic!("{:?}", s.verdict) };
    assert!(probes.len() >= 3);
    assert!(probes.windows(2).all(|w| w[0].at_ms <= w[1].at_ms));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_in_the_recognizer_catalog_does_not_beat_the_page() {
    // The recognizer keeps naming a re-upload; the page's song preview is in the audio.
    let mut song = track("Artist", "Real Song", 240);
    song.ids.insert("deezer".into(), "1".into());
    let copy = track("Uploader", "Copy", 240);
    let audio = noise(240, 17);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, vec![play(0, 240, &song, 0)]).with_preview_of(&song, 60, 30);
    services.overrides.push((0, 240_000, Some(copy)));
    let prior = Prior { track: Some(song.clone()), strong: true };
    let s = survey(&services, &media.pcm, &prior).await;
    assert_eq!(title(&s.verdict), Some("Copy"));
    let page = page_song_heard(&services, &media.pcm, &prior).await.unwrap().expect("preview found");
    let verdict = prefer_page_song(s.verdict, page);
    assert_eq!(title(&verdict), Some("Real Song"));
    let Verdict::Single { alternates, .. } = &verdict else { unreachable!() };
    assert_eq!(alternates[0].title, "Copy");
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_heard_falls_back_to_the_page() {
    let audio = noise(200, 16);
    let media = TempPcm::new(&audio);
    let services = FakeServices::new(audio, Vec::new());
    let hint = track("Page", "Title", 200);
    let s = survey(&services, &media.pcm, &Prior { track: Some(hint), strong: false }).await;
    let Verdict::Nothing { hint } = &s.verdict else { panic!("{:?}", s.verdict) };
    assert_eq!(hint.as_ref().map(|t| t.title.as_str()), Some("Title"));
    assert_eq!(services.calls(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn alternating_answers_are_one_recording() {
    // A remix the recognizer also knows inside a DJ set: the answers alternate
    // between the two names, which consecutive songs never do.
    let remix = track("Avicii", "Levels (Some Remix)", 190);
    let set = track("Some DJ", "Live Set 2019", 3600);
    let audio = noise(190, 18);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, vec![play(0, 190, &remix, 0)]);
    // The windows at 20 %, 50 % and 65 % land in the set; 35 % and 80 % in the remix.
    services.overrides.push((30_000, 46_000, Some(set.clone())));
    services.overrides.push((85_000, 100_000, Some(set.clone())));
    services.overrides.push((110_000, 125_000, Some(set)));
    let prior = Prior { track: Some(remix.clone()), strong: false };
    let s = survey(&services, &media.pcm, &prior).await;
    assert_eq!(title(&s.verdict), Some("Levels (Some Remix)"), "{:?}", s.log);
}

#[test]
fn interleaving_is_detected() {
    let probe = |at_s: u64| Probe { at_ms: at_s * 1000, len_ms: PROBE_MS, hit: None };
    let (a1, a2, b1, b2) = (probe(10), probe(30), probe(20), probe(40));
    assert!(interleaved(&[vec![&a1, &a2], vec![&b1, &b2]]));
    let (c1, c2) = (probe(50), probe(60));
    assert!(!interleaved(&[vec![&a1, &a2], vec![&c1, &c2]]));
    assert!(!interleaved(&[vec![&c1, &c2], vec![&a1, &a2]]));
}

fn single_with(t: &Track, votes: usize) -> Verdict {
    let evidence = (0..votes)
        .map(|k| Evidence { source: EvidenceSource::Shazam, at_ms: k as u64 * 1000, score: 0.72, detail: None })
        .collect();
    Verdict::Single { track: t.clone(), evidence, alternates: Vec::new() }
}

#[test]
fn the_page_outweighs_one_unconfirmed_answer() {
    let page = track("Coldplay", "Charlie Brown (Live in Madrid 2011)", 274);
    let prior = Prior { track: Some(page.clone()), strong: false };
    let other = track("Somebody", "Unrelated Song", 200);
    // One answer: the page wins and the answer becomes an alternate.
    let Verdict::Single { track: t, alternates, evidence } = weigh_page(single_with(&other, 1), &prior, true) else {
        unreachable!()
    };
    assert_eq!(t.title, page.title);
    assert_eq!(alternates[0].title, "Unrelated Song");
    assert_eq!(crate::pipeline::stitch::grade(&evidence).0, crate::model::Tier::Likely);
    // Two agreeing answers: the audio wins.
    let Verdict::Single { track: t, .. } = weigh_page(single_with(&other, 2), &prior, true) else { unreachable!() };
    assert_eq!(t.title, "Unrelated Song");
    // A cover of the page's song by someone else does not beat the artist's own upload...
    let cover = track("Arksey", "Charlie Brown (Live)", 270);
    let Verdict::Single { track: t, .. } = weigh_page(single_with(&cover, 3), &prior, true) else { unreachable!() };
    assert_eq!(t.artist, "Coldplay");
    // ...but does beat someone else's upload when heard more than once.
    let Verdict::Single { track: t, .. } = weigh_page(single_with(&cover, 3), &prior, false) else { unreachable!() };
    assert_eq!(t.artist, "Arksey");
}

#[tokio::test(flavor = "multi_thread")]
async fn disagreeing_versions_give_way_to_the_page() {
    // The upload is a remix the recognizer does not know; it names two other remixes once each.
    let page = track("Avicii", "Levels (Thrizzo Trap Remix)", 263);
    let audio = noise(263, 19);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, vec![play(0, 263, &track("Avicii", "Levels", 200), 0)]);
    services.overrides.push((40_000, 60_000, Some(track("Avicii", "Levels (Eff3cts Trap Remix)", 210))));
    services.overrides.push((195_000, 215_000, Some(track("Avicii", "Levels (Clockwork Remix)", 380))));
    let prior = Prior { track: Some(page.clone()), strong: false };
    let s = survey(&services, &media.pcm, &prior).await;
    assert_eq!(title(&s.verdict), Some("Levels (Thrizzo Trap Remix)"), "{:?}", s.log);
}

#[tokio::test(flavor = "multi_thread")]
async fn another_version_heard_at_unrelated_offsets_is_not_the_recording() {
    // Twice the same other remix, but placed at offsets that do not line up:
    // parts of it inside a different remix, not that remix itself.
    let page = track("Avicii", "Levels (Drankenstein Trap Remix)", 310);
    let audio = noise(310, 20);
    let media = TempPcm::new(&audio);
    let mut services = FakeServices::new(audio, vec![play(0, 310, &track("Avicii", "Levels", 200), 0)]);
    let other = track("Avicii", "Levels (Eff3cts Trap Remix)", 210);
    services.overrides.push((140_000, 160_000, Some(other.clone())));
    services.overrides.push((230_000, 250_000, Some(other)));
    let prior = Prior { track: Some(page.clone()), strong: false };
    let s = survey(&services, &media.pcm, &prior).await;
    assert_eq!(title(&s.verdict), Some("Levels (Drankenstein Trap Remix)"), "{:?}", s.log);
}
