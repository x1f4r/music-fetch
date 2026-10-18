import Foundation
import MusicFetchCore

/// User-facing strings (docs/design/DESIGN.md §7). The UI maps engine enums to these;
/// it never shows engine strings except a failure message without a mapping.
nonisolated enum Copy {
    // MARK: Listen stage

    static let idle = String(localized: "Identify what's playing")
    static let listening = String(localized: "Listening")
    static let moveCloser = String(localized: "Move closer to the sound")
    static let humStart = String(localized: "Hum, whistle or sing")
    static let keepGoing = String(localized: "Keep going")
    static let matching = String(localized: "Matching")
    static let resolving = String(localized: "Resolving link")
    static let downloading = String(localized: "Downloading")
    static let preparing = String(localized: "Preparing")
    static let drop = String(localized: "Drop to identify")
    static let captureSilent = String(localized: "This app blocks audio capture. Use the microphone instead.")
    static let offline = String(localized: "You're offline. Saved to identify later.")
    static let microphoneOff = String(localized: "Microphone access is off.")
    static let systemAudioPrePrompt = String(localized: "macOS will ask to allow system audio recording.")
    static let speechPrePrompt = String(localized: "To search the words you sing, macOS will ask to allow speech recognition.")

    static func rateLimited(seconds: Int) -> String {
        String(localized: "Too many requests. Trying again in \(seconds) s.")
    }

    static func noMatch(_ reason: NoMatchReason) -> String {
        switch reason {
        case .notFound: String(localized: "No match found")
        case .tooQuiet: String(localized: "Too quiet to hear")
        case .noisy: String(localized: "Too noisy to hear the music")
        case .tooShort: String(localized: "Too short to tell")
        case .mostlySpeech: String(localized: "Mostly talking, not music")
        case .captureSilent: captureSilent
        }
    }

    /// State-line text for a failure. Codes with fixed copy ignore `message`.
    static func failure(_ code: ErrorCode, message: String?) -> String {
        switch code {
        case .unsupportedLink: String(localized: "This link isn't supported.")
        case .mediaUnavailable: String(localized: "This video isn't available.")
        case .network: String(localized: "Couldn't reach the server.")
        case .rateLimited: String(localized: "Too many requests. Try again in a minute.")
        case .decode: String(localized: "Couldn't read this file.")
        case .indexUnavailable: String(localized: "The melody library isn't installed.")
        case .invalidInput:
            // The engine's validation messages are short user-facing sentences.
            message.flatMap { $0.isEmpty ? nil : $0 } ?? String(localized: "Something went wrong.")
        case .storage, .platform, .internal: String(localized: "Something went wrong.")
        }
    }

    // MARK: Actions

    static let tryAgain = String(localized: "Try again")
    static let humIt = String(localized: "Hum it")
    static let typeLyric = String(localized: "Type a lyric")
    static let useMicrophone = String(localized: "Use Microphone")
    static let continueAction = String(localized: "Continue")
    static let openSystemSettings = String(localized: "Open System Settings")
    static let stop = String(localized: "Stop")

    // MARK: Sources

    static let microphone = String(localized: "Microphone")
    static let thisMac = String(localized: "This Mac")
    static let hum = String(localized: "Hum")
    /// The Listen menus: one item per method.
    static let listenWithMicrophone = String(localized: "Listen with Microphone")
    static let listenToThisMac = String(localized: "Listen to This Mac")
    static let humWhistleOrSing = String(localized: "Hum, Whistle or Sing")

    // MARK: Results

    static func openIn(_ name: String) -> String {
        String(localized: "Open in \(name)")
    }

    /// A provider in the "Open in" menus; search links are marked.
    static func menuTitle(for link: ProviderLink, custom: CustomProvider) -> String {
        let name = name(of: link.provider, custom: custom)
        return link.isSearch ? String(localized: "\(name) (search)") : name
    }

    static let searchWeb = String(localized: "Search the web")

    static func contextLink(at cue: String, in title: String) -> String {
        String(localized: "At \(cue) in \(title)")
    }

    static func contextFile(_ name: String) -> String {
        String(localized: "From \(name)")
    }

    static func matchedBy(_ source: EvidenceSource, afterSeconds: Int?) -> String {
        switch source {
        case .shazam:
            if let afterSeconds { String(localized: "Shazam, after \(afterSeconds) s") } else { String(localized: "Shazam") }
        case .audD: String(localized: "AudD")
        case .acrCloud, .acrHumming: String(localized: "ACRCloud")
        case .melody: String(localized: "Melody")
        case .lyrics: String(localized: "Sung lyrics")
        case .metadata: String(localized: "Link metadata")
        case .pageHint: String(localized: "Page attribution only")
        case .user: String(localized: "You")
        }
    }

    static func matchedBy(_ sources: [CandidateSource]) -> String {
        var names: [String] = []
        for source in sources.sorted(by: { $0.score > $1.score }) {
            let name: String =
                switch source.kind {
                case .melody: String(localized: "Melody")
                case .lyrics: String(localized: "Sung lyrics")
                case .acrHumming: String(localized: "ACRCloud")
                case .fingerprint: String(localized: "Shazam")
                }
            if !names.contains(name) { names.append(name) }
        }
        return names.formatted(.list(type: .and))
    }

    // MARK: Tiers

    static func tier(_ tier: CandidateTier) -> String {
        switch tier {
        case .strong: String(localized: "Strong")
        case .possible: String(localized: "Possible")
        case .longShot: String(localized: "Long shot")
        }
    }

    static func tier(_ tier: Tier) -> String {
        switch tier {
        case .confirmed: String(localized: "Confirmed")
        case .likely: String(localized: "Likely")
        case .possible: String(localized: "Possible")
        case .unknown: String(localized: "ID")
        }
    }

    // MARK: Mix

    static func eta(milliseconds: UInt64?) -> String {
        guard let milliseconds else { return String(localized: "Analyzing") }
        if milliseconds < 60_000 { return String(localized: "Less than a minute left") }
        let minutes = Int((Double(milliseconds) / 60_000).rounded(.up))
        return String(localized: "About \(minutes) min left")
    }

    static func identified(_ identified: Int, of total: Int) -> String {
        String(localized: "\(identified) of \(total) identified")
    }

    static func identifiedOnly(_ count: Int) -> String {
        String(localized: "\(count) identified")
    }

    static func stopped(_ count: Int) -> String {
        String(localized: "Stopped · \(count) identified")
    }

    static func tracksSoFar(_ count: Int) -> String {
        String(localized: "\(count) tracks so far")
    }

    /// "Evidence" row of a mix segment. One loose-skew Shazam match (confidence 0.72)
    /// reads as pitch-shifted.
    static func evidence(_ evidence: [Evidence], confidence: Float) -> String {
        let shazam = evidence.filter { $0.source == .shazam }
        if shazam.count >= 2 { return String(localized: "\(shazam.count) Shazam matches") }
        if let acr = evidence.first(where: { $0.source == .acrCloud }) {
            return String(localized: "ACRCloud, score \(Int((acr.score * 100).rounded()))")
        }
        if shazam.count == 1 {
            return confidence >= 0.85 ? String(localized: "1 Shazam match") : String(localized: "1 Shazam match, pitch-shifted")
        }
        if let first = evidence.first { return matchedBy(first.source, afterSeconds: nil) }
        return String(localized: "Page attribution only")
    }

    static func openSource(site: String, cue: String) -> String {
        String(localized: "\(site) at \(cue)")
    }

    // MARK: Services

    /// A provider's display name; the custom provider shows the name the user gave it.
    static func name(of provider: Provider, custom: CustomProvider) -> String {
        switch provider {
        case .service(let service): name(of: service)
        case .custom: custom.trimmedName.isEmpty ? customProvider : custom.trimmedName
        }
    }

    static let customProvider = String(localized: "Custom")

    static func name(of service: Service) -> String {
        switch service {
        case .spotify: "Spotify"
        case .appleMusic: "Apple Music"
        case .youTubeMusic: "YouTube Music"
        case .youTube: "YouTube"
        case .deezer: "Deezer"
        case .tidal: "Tidal"
        case .amazonMusic: "Amazon Music"
        case .soundCloud: "SoundCloud"
        case .bandcamp: "Bandcamp"
        case .mixcloud: "Mixcloud"
        case .vimeo: "Vimeo"
        case .tikTok: "TikTok"
        case .instagram: "Instagram"
        case .facebook: "Facebook"
        case .x: "X"
        case .reddit: "Reddit"
        case .twitch: "Twitch"
        case .shazam: "Shazam"
        case .genius: "Genius"
        case .qobuz: "Qobuz"
        case .pandora: "Pandora"
        case .audiomack: "Audiomack"
        case .anghami: "Anghami"
        case .jioSaavn: "JioSaavn"
        case .boomplay: "Boomplay"
        case .kkbox: "KKBOX"
        case .yandexMusic: "Yandex Music"
        case .netEaseMusic: "NetEase Cloud Music"
        case .lastFm: "Last.fm"
        case .web: String(localized: "Web")
        }
    }

    // MARK: Accessibility

    static func found(_ track: Track) -> String {
        String(localized: "Found \(track.title) by \(track.artist)")
    }

    static func possibleMatches(_ candidates: [Candidate]) -> String {
        guard let best = candidates.first else { return noMatch(.notFound) }
        return String(
            localized: "\(candidates.count) possible matches. Best: \(best.track.title) by \(best.track.artist), \(tier(best.tier))")
    }
}
