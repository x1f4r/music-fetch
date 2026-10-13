//! Live canaries for the link resolvers and the catalog. They hit real services,
//! so they are ignored by default:
//!
//! `cargo test -p mfcore --test links_live -- --ignored --test-threads 1`

use mfcore::links::{self, Resolution};
use mfcore::util::cancel::CancelToken;
use mfcore::{Service, Track, catalog};

fn http() -> reqwest::Client {
    links::http_client(&mfcore::core_version()).expect("http client")
}

async fn resolve(url: &str) -> Resolution {
    links::resolve(&http(), url, "us", &CancelToken::new()).await.unwrap_or_else(|e| panic!("{url}: {e}"))
}

fn tracks(resolution: Resolution) -> Vec<Track> {
    match resolution {
        Resolution::Tracks { tracks, .. } => tracks,
        other => panic!("expected tracks, got {other:?}"),
    }
}

fn hints(resolution: &Resolution) -> &[Track] {
    &resolution.source().hints
}

#[tokio::test]
#[ignore = "live network"]
async fn spotify_track_album_playlist() {
    let t = tracks(resolve("https://open.spotify.com/track/4cOdK2wGLETKBW3PvgPWqT?si=x").await);
    assert_eq!(t[0].artist, "Rick Astley");
    assert!(t[0].preview_url.is_some());
    assert!(tracks(resolve("https://open.spotify.com/album/4aawyAB9vmqN3uQ7FjRGTy").await).len() > 10);
    assert!(tracks(resolve("https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M").await).len() >= 20);
}

#[tokio::test]
#[ignore = "live network"]
async fn apple_deezer_tidal_amazon() {
    let t = tracks(resolve("https://music.apple.com/us/album/never-gonna-give-you-up/1559885420?i=1559885421").await);
    assert_eq!((t.len(), t[0].title.as_str()), (1, "Never Gonna Give You Up"));
    let t = tracks(resolve("https://www.deezer.com/en/track/3135556").await);
    assert_eq!(t[0].isrc.as_deref(), Some("GBDUW0000059"));
    let t = tracks(resolve("https://tidal.com/browse/track/1781887").await);
    assert_eq!(t[0].isrc.as_deref(), Some("USSM19902991"));
    let t = tracks(resolve("https://music.amazon.com/tracks/B000TDFHJW").await);
    assert_eq!(t[0].artist, "Daft Punk");
}

#[tokio::test]
#[ignore = "live network"]
async fn youtube_and_tiktok_hints() {
    let r = resolve("https://youtu.be/dQw4w9WgXcQ").await;
    assert!(hints(&r).iter().any(|h| h.artist == "Rick Astley"));
    let r = resolve("https://www.youtube.com/shorts/zN0e7v7AQmA").await;
    assert!(hints(&r).iter().any(|h| h.title == "Hootie Frutti"));
    let r = resolve("https://www.tiktok.com/@katseyeworld/video/7402793375398645022").await;
    let hint = &hints(&r)[0];
    assert_eq!(hint.title, "Touch");
    assert!(hint.links.iter().any(|l| l.service == Service::Spotify));
}

#[tokio::test]
#[ignore = "live network"]
async fn generic_page_and_short_link() {
    match resolve("https://www.jamendo.com/track/1886257/alone").await {
        Resolution::Media { direct, .. } => assert!(direct),
        other => panic!("expected media, got {other:?}"),
    }
    let r = resolve("https://spotify.link/N2zPz9qjoDb").await;
    assert_eq!(r.source().service, Some(Service::Spotify));
}

#[tokio::test]
#[ignore = "live network"]
async fn catalog_enrichment() {
    let track =
        Track { title: "Harder, Better, Faster, Stronger".into(), artist: "Daft Punk".into(), ..Track::default() };
    let t = catalog::enrich(&http(), track, "us", &CancelToken::new()).await;
    assert_eq!(t.isrc.as_deref(), Some("GBDUW0000059"));
    assert!(t.links.iter().any(|l| l.service == Service::Deezer && !l.is_search));
    assert!(t.artwork_url.is_some());
}
