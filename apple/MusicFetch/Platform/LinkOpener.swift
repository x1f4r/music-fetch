import AppKit
import Foundation
import MusicFetchCore

/// Opens provider links (docs/APPS.md, "Opening links").
nonisolated enum LinkOpener {
    /// The URL to open. Apple Music links use the `music://` form so they open in Music.
    static func url(for link: ProviderLink) -> URL? {
        guard var components = URLComponents(string: link.url) else { return nil }
        if link.provider == .service(.appleMusic), components.host?.hasSuffix("music.apple.com") == true,
            components.scheme == "https"
        {
            components.scheme = "music"
        }
        return components.url
    }

    /// A web search for the track, for tracks without any provider link.
    static func webSearchURL(for track: Track) -> URL? {
        var components = URLComponents(string: "https://www.google.com/search")
        components?.queryItems = [URLQueryItem(name: "q", value: "\(track.artist) \(track.title)")]
        return components?.url
    }

    /// Opens `link`; falls back to the https form if no app handles `music://`.
    @MainActor static func open(_ link: ProviderLink) {
        guard let url = url(for: link) else { return }
        if url.scheme == "music", NSWorkspace.shared.urlForApplication(toOpen: url) == nil,
            let web = URL(string: link.url)
        {
            NSWorkspace.shared.open(web)
            return
        }
        NSWorkspace.shared.open(url)
    }

    /// Opens `track` in the user's provider, or a web search when it has no links.
    @MainActor static func openPreferred(_ track: Track, preference: ProviderPreference) {
        if let link = TrackLinks(track: track, preference: preference).primary {
            open(link)
        } else if let url = webSearchURL(for: track) {
            NSWorkspace.shared.open(url)
        }
    }

    /// Opens a segment's deep link (e.g. YouTube `&t=905s`).
    @MainActor static func openSource(_ urlString: String?) {
        guard let urlString, let url = URL(string: urlString), url.scheme == "https" || url.scheme == "http" else { return }
        NSWorkspace.shared.open(url)
    }
}
