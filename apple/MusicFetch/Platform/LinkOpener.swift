import AppKit
import Foundation
import MusicFetchCore

/// Picks and opens service links (docs/APPS.md, "Opening links").
nonisolated enum LinkOpener {
    /// Services offered in "Open in" menus and Settings, in display order.
    static let menuOrder: [Service] = [
        .spotify, .appleMusic, .youTubeMusic, .youTube, .deezer, .tidal, .amazonMusic, .soundCloud, .shazam,
    ]

    /// Services a user can choose as the default ("Open songs in").
    static let defaultChoices: [Service] = [
        .spotify, .appleMusic, .youTubeMusic, .youTube, .deezer, .tidal, .amazonMusic, .soundCloud,
    ]

    /// The link the primary "Open in" action uses: the default service's link (exact
    /// before search), else the first exact link, else the first search link.
    static func preferredLink(in links: [ServiceLink], defaultService: Service) -> ServiceLink? {
        let ordered = sorted(links)
        if let exact = ordered.first(where: { $0.service == defaultService && !$0.isSearch }) { return exact }
        if let search = ordered.first(where: { $0.service == defaultService }) { return search }
        if let exact = ordered.first(where: { !$0.isSearch }) { return exact }
        return ordered.first
    }

    /// Links for the "Open in" menu, in `menuOrder`, one per service (exact preferred).
    static func menuLinks(_ links: [ServiceLink]) -> [ServiceLink] {
        var seen = Set<Service>()
        return sorted(links).filter { menuOrder.contains($0.service) && seen.insert($0.service).inserted }
    }

    /// Stable order: menu order first, then anything else; exact links before searches.
    static func sorted(_ links: [ServiceLink]) -> [ServiceLink] {
        func rank(_ service: Service) -> Int { menuOrder.firstIndex(of: service) ?? menuOrder.count }
        return links.enumerated().sorted { lhs, rhs in
            let (a, b) = (lhs.element, rhs.element)
            if rank(a.service) != rank(b.service) { return rank(a.service) < rank(b.service) }
            if a.isSearch != b.isSearch { return !a.isSearch }
            return lhs.offset < rhs.offset
        }.map(\.element)
    }

    /// The URL to open. Apple Music links use the `music://` form so they open in Music.
    static func url(for link: ServiceLink) -> URL? {
        guard var components = URLComponents(string: link.url) else { return nil }
        if link.service == .appleMusic, components.host?.hasSuffix("music.apple.com") == true, components.scheme == "https" {
            components.scheme = "music"
        }
        return components.url
    }

    /// A web search for the track, used by "Search the web".
    static func webSearchURL(for track: Track) -> URL? {
        var components = URLComponents(string: "https://www.google.com/search")
        components?.queryItems = [URLQueryItem(name: "q", value: "\(track.artist) \(track.title)")]
        return components?.url
    }

    /// Opens `link`; falls back to the https form if no app handles `music://`.
    @MainActor static func open(_ link: ServiceLink) {
        guard let url = url(for: link) else { return }
        if url.scheme == "music", NSWorkspace.shared.urlForApplication(toOpen: url) == nil,
            let web = URL(string: link.url)
        {
            NSWorkspace.shared.open(web)
            return
        }
        NSWorkspace.shared.open(url)
    }

    /// Opens the preferred link of `track`, or a web search when it has none.
    @MainActor static func openPreferred(_ track: Track, defaultService: Service) {
        if let link = preferredLink(in: track.links, defaultService: defaultService) {
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
