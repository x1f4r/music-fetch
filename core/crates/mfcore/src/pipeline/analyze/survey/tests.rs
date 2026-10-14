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
