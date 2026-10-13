//! Which site a canonical link belongs to.

use crate::model::Service;

/// Sites with dedicated handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Site {
    Spotify,
    AppleMusic,
    Deezer,
    Tidal,
    AmazonMusic,
    YouTube,
    YouTubeMusic,
    SoundCloud,
    TikTok,
    Instagram,
    X,
    Reddit,
    Twitch,
    Bandcamp,
    Mixcloud,
    Vimeo,
    Facebook,
    /// Any other website.
    Other,
}

impl Site {
    /// Classifies a lowercase host name.
    pub fn of_host(host: &str) -> Site {
        let host = host.trim_end_matches('.');
        let is = |domain: &str| host == domain || host.ends_with(&format!(".{domain}"));
        match host {
            "music.youtube.com" => return Site::YouTubeMusic,
            "music.apple.com" | "itunes.apple.com" => return Site::AppleMusic,
            "youtu.be" => return Site::YouTube,
            "redd.it" => return Site::Reddit,
            "x.com" | "www.x.com" | "mobile.x.com" | "twitter.com" | "www.twitter.com"
            | "mobile.twitter.com" => return Site::X,
            _ => {}
        }
        if host.starts_with("music.amazon.") {
            Site::AmazonMusic
        } else if is("spotify.com") {
            Site::Spotify
        } else if is("deezer.com") {
            Site::Deezer
        } else if is("tidal.com") {
            Site::Tidal
        } else if is("youtube.com") || is("youtube-nocookie.com") {
            Site::YouTube
        } else if is("soundcloud.com") {
            Site::SoundCloud
        } else if is("tiktok.com") {
            Site::TikTok
        } else if is("instagram.com") {
            Site::Instagram
        } else if is("reddit.com") {
            Site::Reddit
        } else if is("twitch.tv") {
            Site::Twitch
        } else if is("bandcamp.com") {
            Site::Bandcamp
        } else if is("mixcloud.com") {
            Site::Mixcloud
        } else if is("vimeo.com") {
            Site::Vimeo
        } else if is("facebook.com") || host == "fb.watch" {
            Site::Facebook
        } else {
            Site::Other
        }
    }

    /// The `Service` used in `SourceInfo.service`.
    pub fn service(self) -> Service {
        match self {
            Site::Spotify => Service::Spotify,
            Site::AppleMusic => Service::AppleMusic,
            Site::Deezer => Service::Deezer,
            Site::Tidal => Service::Tidal,
            Site::AmazonMusic => Service::AmazonMusic,
            Site::YouTube => Service::YouTube,
            Site::YouTubeMusic => Service::YouTubeMusic,
            Site::SoundCloud => Service::SoundCloud,
            Site::TikTok => Service::TikTok,
            Site::Instagram => Service::Instagram,
            Site::Bandcamp => Service::Bandcamp,
            Site::X
            | Site::Reddit
            | Site::Twitch
            | Site::Mixcloud
            | Site::Vimeo
            | Site::Facebook
            | Site::Other => Service::Web,
        }
    }

    /// Display name; unknown sites show their host without "www.".
    pub fn display_name(self, host: &str) -> String {
        let name = match self {
            Site::Spotify => "Spotify",
            Site::AppleMusic => "Apple Music",
            Site::Deezer => "Deezer",
            Site::Tidal => "TIDAL",
            Site::AmazonMusic => "Amazon Music",
            Site::YouTube => "YouTube",
            Site::YouTubeMusic => "YouTube Music",
            Site::SoundCloud => "SoundCloud",
            Site::TikTok => "TikTok",
            Site::Instagram => "Instagram",
            Site::X => "X",
            Site::Reddit => "Reddit",
            Site::Twitch => "Twitch",
            Site::Bandcamp => "Bandcamp",
            Site::Mixcloud => "Mixcloud",
            Site::Vimeo => "Vimeo",
            Site::Facebook => "Facebook",
            Site::Other => return host.trim_start_matches("www.").to_string(),
        };
        name.to_string()
    }

    /// Streaming services whose links resolve to track metadata.
    pub fn is_streaming_service(self) -> bool {
        matches!(
            self,
            Site::Spotify | Site::AppleMusic | Site::Deezer | Site::Tidal | Site::AmazonMusic
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_hosts() {
        assert_eq!(Site::of_host("open.spotify.com"), Site::Spotify);
        assert_eq!(Site::of_host("music.apple.com"), Site::AppleMusic);
        assert_eq!(Site::of_host("www.deezer.com"), Site::Deezer);
        assert_eq!(Site::of_host("tidal.com"), Site::Tidal);
        assert_eq!(Site::of_host("music.amazon.de"), Site::AmazonMusic);
        assert_eq!(Site::of_host("www.youtube.com"), Site::YouTube);
        assert_eq!(Site::of_host("music.youtube.com"), Site::YouTubeMusic);
        assert_eq!(Site::of_host("soundcloud.com"), Site::SoundCloud);
        assert_eq!(Site::of_host("www.tiktok.com"), Site::TikTok);
        assert_eq!(Site::of_host("artist.bandcamp.com"), Site::Bandcamp);
        assert_eq!(Site::of_host("www.mixcloud.com"), Site::Mixcloud);
        assert_eq!(Site::of_host("x.com"), Site::X);
        assert_eq!(Site::of_host("notspotify.com"), Site::Other);
        assert_eq!(Site::of_host("spotify.com.evil.example"), Site::Other);
        assert_eq!(Site::Other.display_name("www.jamendo.com"), "jamendo.com");
        assert_eq!(Site::Mixcloud.service(), Service::Web);
        assert!(Site::Tidal.is_streaming_service());
        assert!(!Site::YouTube.is_streaming_service());
    }
}
