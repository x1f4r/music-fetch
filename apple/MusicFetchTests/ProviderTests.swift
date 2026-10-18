import Foundation
import MusicFetchCore
import Testing

@testable import MusicFetch

struct ProviderTests {
    private func track(links: [ServiceLink] = []) -> Track {
        Track(
            title: "Take On Me", artist: "a-ha", artists: ["a-ha"], album: nil, isrc: "NOA138500010", durationMs: nil,
            releaseYear: nil, genre: nil, artworkUrl: nil, previewUrl: nil, links: links, ids: [:], identityKey: "a-ha|take on me")
    }

    private let exactSpotify = ServiceLink(service: .spotify, url: "https://open.spotify.com/track/x", isSearch: false)
    private let exactYouTube = ServiceLink(service: .youTube, url: "https://www.youtube.com/watch?v=x", isSearch: false)
    private let validCustom = CustomProvider(name: "Radio Archive", template: "https://radio.example.org/find?q={query}")

    @Test func storageKeysRoundTrip() {
        let keys = Service.allCases.map(\.storageKey)
        #expect(Set(keys).count == keys.count)
        for service in Service.allCases {
            #expect(Service(storageKey: service.storageKey) == service)
            #expect(Provider(storageKey: Provider.service(service).storageKey) == .service(service))
        }
        #expect(Provider(storageKey: "custom") == .custom)
        #expect(Provider(storageKey: "nonsense") == nil)
        // Values written by earlier builds' "Open songs in" stay readable.
        #expect(Service(storageKey: "youTubeMusic") == .youTubeMusic)
    }

    @Test func providerListsAreConsistent() {
        #expect(Set(Providers.all).count == Providers.all.count)
        #expect(Providers.common.allSatisfy(Providers.all.contains))
        #expect(Providers.more.count + Providers.common.count == Providers.all.count)
    }

    @Test func oneLinkPerProviderInOrderWithExactLinksWinning() {
        let links = Providers.links(for: track(links: [exactYouTube, exactSpotify]), custom: validCustom)
        let services = links.compactMap { link -> Service? in
            if case .service(let service) = link.provider { return service }
            return nil
        }
        #expect(Set(services).count == services.count)
        #expect(services == Providers.all.filter(services.contains))
        #expect(links.first == ProviderLink(provider: .service(.spotify), url: exactSpotify.url, isSearch: false))
        #expect(links.contains(ProviderLink(provider: .service(.youTube), url: exactYouTube.url, isSearch: false)))
        #expect(links.last?.provider == .custom)
        #expect(links.last?.url == "https://radio.example.org/find?q=a%2Dha%20Take%20On%20Me")
        // The engine fills in searches for the providers without an exact link.
        #expect(links.contains { $0.provider == .service(.appleMusic) && $0.isSearch })
    }

    @Test func invalidCustomProviderIsLeftOut() {
        let links = Providers.links(for: track(), custom: CustomProvider(name: "Broken", template: "not a url {query}"))
        #expect(!links.contains { $0.provider == .custom })
    }

    @Test func primaryIsThePreferredProvider() {
        let links = Providers.links(for: track(links: [exactSpotify]), custom: validCustom)
        #expect(Providers.primary(in: links, preferred: .service(.appleMusic))?.provider == .service(.appleMusic))
        #expect(Providers.primary(in: links, preferred: .custom)?.provider == .custom)
        // A provider without a link falls back to the first exact link.
        let only = [ProviderLink(provider: .service(.deezer), url: "https://www.deezer.com/search/x", isSearch: true),
                    ProviderLink(provider: .service(.youTube), url: exactYouTube.url, isSearch: false)]
        #expect(Providers.primary(in: only, preferred: .service(.tidal))?.provider == .service(.youTube))
    }

    @Test func menuListsExactLinksFirstThenSearchesThenCustom() {
        let preference = ProviderPreference(key: "appleMusic", customName: validCustom.name, customTemplate: validCustom.template)
        let links = TrackLinks(track: track(links: [exactYouTube, exactSpotify]), preference: preference)
        #expect(links.primary?.provider == .service(.appleMusic))
        #expect(!links.others.contains { $0.provider == .service(.appleMusic) })
        let firstSearch = links.others.firstIndex { $0.isSearch } ?? links.others.endIndex
        #expect(links.others[..<firstSearch].map(\.provider) == [.service(.spotify), .service(.youTube)])
        #expect(links.others.last?.provider == .custom)
        let menu = links.menu
        #expect(menu.exact.map(\.provider) == [.service(.spotify), .service(.youTube)])
        #expect(menu.searches.allSatisfy { $0.isSearch })
        #expect(menu.custom.map(\.provider) == [.custom])
        #expect(Set(menu.all) == Set(links.others))
        #expect(menu.all.count == links.others.count)
        // Searches on the less common providers are tucked into "More…".
        #expect(!menu.searches.contains { $0.provider == .service(.qobuz) })
    }

    @Test func customTemplatesAreValidated() {
        #expect(validCustom.isValid)
        #expect(CustomProvider.isValidTemplate("https://example.com/isrc/{isrc}"))
        #expect(!CustomProvider.isValidTemplate("https://example.com/search"))
        #expect(!CustomProvider.isValidTemplate("javascript:alert({title})"))
        #expect(!CustomProvider.isValidTemplate("example.com/?q={query}"))
        #expect(!CustomProvider(name: "  ", template: validCustom.template).isValid)
    }

    @Test func preferenceIsUndecidedUntilStored() throws {
        let suite = "local.musicfetch.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }

        var preference = ProviderPreference.load(defaults)
        #expect(!preference.isDecided)
        #expect(preference.provider == .service(.spotify))

        ProviderPreference.store(.service(.tidal), in: defaults)
        preference = ProviderPreference.load(defaults)
        #expect(preference.isDecided)
        #expect(preference.provider == .service(.tidal))
        #expect(preference.name == "Tidal")

        ProviderPreference.store(.custom, custom: CustomProvider(name: " Radio Archive ", template: " https://radio.example.org/find?q={query} "),
                                 in: defaults)
        preference = ProviderPreference.load(defaults)
        #expect(preference.provider == .custom)
        #expect(preference.name == "Radio Archive")
        #expect(preference.custom.template == "https://radio.example.org/find?q={query}")

        // A custom provider that cannot be filled in opens in Spotify instead.
        defaults.set("https://radio.example.org/", forKey: PreferenceKey.customProviderTemplate)
        #expect(ProviderPreference.load(defaults).provider == .service(.spotify))
        #expect(ProviderPreference.load(defaults).isDecided)
    }

    @Test func earlierDefaultServiceIsMigrated() throws {
        let suite = "local.musicfetch.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }

        defaults.set("deezer", forKey: PreferenceKey.legacyDefaultService)
        ProviderPreference.migrate(defaults)
        #expect(ProviderPreference.load(defaults).provider == .service(.deezer))
        #expect(defaults.string(forKey: PreferenceKey.legacyDefaultService) == nil)

        // Nothing to migrate: still undecided.
        let fresh = "local.musicfetch.tests.\(UUID().uuidString)"
        let empty = try #require(UserDefaults(suiteName: fresh))
        defer { empty.removePersistentDomain(forName: fresh) }
        ProviderPreference.migrate(empty)
        #expect(!ProviderPreference.load(empty).isDecided)
    }

    @Test func installedThirdPartyAppsArePreferredOverMusic() {
        #expect(InstalledProviders.suggestion { $0 == .appleMusic } == .appleMusic)
        #expect(InstalledProviders.suggestion { [.appleMusic, .deezer].contains($0) } == .deezer)
        #expect(InstalledProviders.suggestion { [.appleMusic, .tidal, .spotify].contains($0) } == .spotify)
        #expect(InstalledProviders.suggestion { _ in false } == .appleMusic)
    }

    @Test func appleMusicOpensInMusic() {
        let apple = ProviderLink(provider: .service(.appleMusic), url: "https://music.apple.com/us/album/x?i=1", isSearch: false)
        #expect(LinkOpener.url(for: apple)?.scheme == "music")
        #expect(LinkOpener.url(for: ProviderLink(provider: .service(.youTube), url: exactYouTube.url, isSearch: false))?.scheme == "https")
    }
}
