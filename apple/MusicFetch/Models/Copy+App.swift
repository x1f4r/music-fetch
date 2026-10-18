import Foundation
import MusicFetchCore

// User-facing strings for app chrome, results, library and settings (DESIGN.md §7).

extension Copy {
    static let appName = "Music Fetch"
    static let listenTitle = String(localized: "Listen")
    static let libraryTitle = String(localized: "Library")
    static let listenAction = String(localized: "Listen")
    static let listenHint = String(localized: "Identifies what's playing")
    static let backToListen = String(localized: "Back to Listen")
    static let listenMethods = String(localized: "Other Ways to Identify")
    static let noSelection = String(localized: "No Selection")
    static let showAll = String(localized: "Show All")
    static let showInLibrary = String(localized: "Show in Library")

    /// The toolbar Listen button's help: what its main part does.
    static func listenHelp(_ source: ListenSource) -> String {
        switch source {
        case .microphone: String(localized: "Listen with the microphone")
        case .thisMac: String(localized: "Listen to audio playing on this Mac")
        case .hum: String(localized: "Hum, whistle or sing a tune")
        }
    }
    static let listenMenu = String(localized: "Listen")
    static let sessionMenu = String(localized: "Session")
    static let humming = String(localized: "Humming")
    static let idleValue = String(localized: "Idle")
    static let pasteLink = String(localized: "Paste a link")
    static let inspector = String(localized: "Inspector")
    static let showInspector = String(localized: "Show Inspector")
    static let hideInspector = String(localized: "Hide Inspector")
    static let pinnedSection = String(localized: "Pinned")
    static let search = String(localized: "Search")
    static let filter = String(localized: "Filter")
    nonisolated static let untitled = String(localized: "Untitled")

    /// Title for sessions without one (a live session that found nothing).
    nonisolated static func fallbackTitle(_ kind: SessionKind) -> String {
        switch kind {
        case .listen: microphone
        case .systemAudio: thisMac
        case .hum: hum
        case .lyrics: typeLyric
        case .link, .file: untitled
        }
    }
    static let engineUnavailable = String(localized: "Music Fetch couldn't open its library.")
    static let ok = String(localized: "OK")
    static let done = String(localized: "Done")
    static let cancel = String(localized: "Cancel")
    static let open = String(localized: "Open")
    static let source = String(localized: "Source")

    // MARK: Menus

    static let identifyLinkMenu = String(localized: "Identify Link…")
    static let openFileMenu = String(localized: "Open File…")
    static let exportEllipsis = String(localized: "Export…")
    static let cut = String(localized: "Cut")
    static let paste = String(localized: "Paste")
    static let selectAll = String(localized: "Select All")
    static let searchLibrary = String(localized: "Search Library")
    static let searchLyricsMenu = String(localized: "Type a Lyric…")
    static let zoomIn = String(localized: "Zoom In")
    static let zoomOut = String(localized: "Zoom Out")
    static let actualSize = String(localized: "Actual Size")
    static let openInMenu = String(localized: "Open in")
    static let openMusicFetch = String(localized: "Open Music Fetch")
    static let settingsTitle = String(localized: "Settings")
    static let serviceNoLink = String(localized: "There's no link or file to identify.")

    // MARK: Action row

    static func title(for action: ListenModel.Action) -> String {
        switch action {
        case .tryAgain: tryAgain
        case .humIt: humIt
        case .typeLyric: typeLyric
        case .useMicrophone: useMicrophone
        case .continueSystemAudio: continueAction
        case .openMicrophoneSettings: openSystemSettings
        case .stop: stop
        case .dismiss: ok
        }
    }

    static let tryAgainTitle = String(localized: "Try Again")

    // MARK: Prompts

    static let microphonePromptTitle = String(localized: "Allow the microphone?")
    static let microphonePromptMessage = String(
        localized: "Music Fetch listens through the microphone to identify the song that's playing or the tune you hum. macOS asks next.")
    static let melodyPromptTitle = String(localized: "Download the melody library?")
    static let melodyDownload = String(localized: "Download")
    static let notNow = String(localized: "Not Now")

    static func melodyPromptMessage(_ bytes: UInt64?) -> String {
        let size = Format.bytes(bytes ?? 48_000_000)
        return String(localized: "Humming needs a \(size) library that stays on this Mac.")
    }

    // MARK: Results

    static let share = String(localized: "Share")
    static let copy = String(localized: "Copy")
    static let copied = String(localized: "Copied")
    static let playPreview = String(localized: "Play Preview")
    static let pausePreview = String(localized: "Pause Preview")
    static let lyrics = String(localized: "Lyrics")
    static let album = String(localized: "Album")
    static let released = String(localized: "Released")
    static let length = String(localized: "Length")
    static let isrc = String(localized: "ISRC")
    static let heard = String(localized: "Heard")
    static let when = String(localized: "When")
    static let matchedByLabel = String(localized: "Matched by")
    static let whereLabel = String(localized: "Where")
    static let unknownTrack = String(localized: "ID")

    /// "Heard › Source": Microphone, This Mac, the site of a link, or a file.
    @MainActor static func sourceName(_ model: SessionModel) -> String {
        switch model.kind {
        case .listen: return microphone
        case .systemAudio: return thisMac
        case .hum: return model.origin == .thisMac ? thisMac : microphone
        case .link:
            return model.source?.site ?? Format.host(model.source?.url ?? model.entry?.sourceUrl) ?? String(localized: "Link")
        case .file: return String(localized: "File")
        case .lyrics: return String(localized: "Typed lyrics")
        }
    }

    /// "Heard › Matched by" for a single answer.
    @MainActor static func matchedBy(_ model: SessionModel) -> String? {
        if let confirmed = model.confirmedTrack {
            if let candidate = model.candidates.first(where: { $0.track.identityKey == confirmed.identityKey }),
                !candidate.sources.isEmpty
            {
                return matchedBy(candidate.sources)
            }
            return String(localized: "You")
        }
        let evidence = model.resultSegment?.evidence
        guard let best = evidence?.max(by: { $0.score < $1.score }) else { return nil }
        let live = model.kind == .listen || model.kind == .systemAudio
        let seconds = best.source == .shazam && live ? Int((Double(best.atMs) / 1000).rounded()) : nil
        return matchedBy(best.source, afterSeconds: seconds)
    }

    // MARK: Candidates

    static let humAgain = String(localized: "Hum again")
    static let thisIsIt = String(localized: "This is it")
    static let notHere = String(localized: "Not here?")
    static let legendYou = String(localized: "You")
    static let yourPitch = String(localized: "Your pitch")

    /// "Where" in the song a candidate matched, when a source says so. Melody details
    /// describe the transposition and tempo of the match, not a place, and are skipped.
    static func wherePhrase(_ sources: [CandidateSource]) -> String? {
        sources.lazy
            .filter { $0.kind != .melody && $0.kind != .lyrics }
            .compactMap(\.detail)
            .first { !$0.isEmpty }
    }

    static func yourHum(_ seconds: Int) -> String {
        String(localized: "Your hum · \(seconds) s")
    }

    static func overlayTitle(_ title: String) -> String {
        String(localized: "Your hum vs. \(title)")
    }

    static func secondsHummed(_ seconds: Int) -> String {
        String(localized: "\(seconds) seconds hummed")
    }

    /// VoiceOver summary of the overlay: how much of the hum follows the song.
    static func overlayAccessibility(title: String, alignment: MelodyAlignment) -> String {
        var voiced = 0
        var close = 0
        for (query, reference) in zip(alignment.query, alignment.reference) where query.isFinite {
            voiced += 1
            if reference.isFinite, abs(query - reference) <= 1.5 { close += 1 }
        }
        let percent = voiced > 0 ? Int((Double(close) / Double(voiced) * 100).rounded()) : 0
        return String(localized: "Your hum compared with \(title): \(percent) percent of it follows the song")
    }

    static func candidateAccessibility(_ candidate: Candidate, rank: Int) -> String {
        String(localized: "\(rank). \(candidate.track.title) by \(candidate.track.artist), \(tier(candidate.tier))")
    }

    static let lyricPrompt = String(localized: "Type a lyric you remember")
    static let lyricsEmpty = String(localized: "No songs with that lyric.")
    static let tryDifferentWords = String(localized: "Try different words")
    static let choose = String(localized: "Choose")

    // MARK: Mix

    static let confirm = String(localized: "Confirm")
    static let notThis = String(localized: "Not this")
    static let notThisMenu = String(localized: "Not This")
    static let humPart = String(localized: "Hum This Part")
    static let match = String(localized: "Match")
    static let evidenceLabel = String(localized: "Evidence")
    static let timeline = String(localized: "Timeline")
    static let timeAxis = String(localized: "Time")
    static let tracksSeries = String(localized: "Tracks")
    static let analyzing = String(localized: "Analyzing")
    static let stoppedPlain = String(localized: "Stopped")
    static let didNotFinish = String(localized: "Didn't finish")
    static let waitingForConnection = String(localized: "Waiting for connection")

    static func useAlternate(_ track: Track) -> String {
        String(localized: "Use “\(track.title)” by \(track.artist)")
    }

    static func playOnSite(_ site: String, cue: String) -> String {
        String(localized: "Play on \(site) at \(cue)")
    }

    static func trackCount(_ count: Int) -> String {
        String(localized: "\(count) tracks")
    }

    static func identifiedProgress(_ count: Int) -> String {
        String(localized: "\(count) identified")
    }

    static func segmentAccessibility(_ segment: Segment, showsIndex: Bool) -> String {
        let position = showsIndex ? "\(segment.itemIndex + 1)" : Format.cue(milliseconds: segment.startMs)
        guard let track = segment.track else { return "\(position), \(unknownTrack)" }
        return String(localized: "\(position), \(track.title) by \(track.artist), \(tier(segment.tier))")
    }

    // MARK: Export

    static let exportMenu = String(localized: "Export")
    static let copyTracklist = String(localized: "Copy Tracklist")
    static let exportFailed = String(localized: "Couldn't save the file.")
    static let formatLabel = String(localized: "Format:")

    static func exportTitle(_ format: ExportFormat) -> String {
        switch format {
        case .text: String(localized: "Export as Text…")
        case .csv: String(localized: "Export as CSV…")
        case .cue: String(localized: "Export as CUE…")
        case .json: String(localized: "Export as JSON…")
        }
    }

    static func formatName(_ format: ExportFormat) -> String {
        switch format {
        case .text: String(localized: "Text")
        case .csv: "CSV"
        case .cue: "CUE"
        case .json: "JSON"
        }
    }

    // MARK: Settings › Audio

    static let settingsAudio = String(localized: "Audio")
    static let microphoneOrder = String(localized: "Microphone order")
    static let microphoneOrderFooter = String(localized: "Music Fetch listens with the first connected microphone. Drag to reorder.")
    static let listenTo = String(localized: "Listen to")
    static let systemOutput = String(localized: "System output")
    static let notConnected = String(localized: "Not connected")
    static let outputFooter = String(localized: "When the chosen output isn't connected, the system output is used.")
    static let inUse = String(localized: "In use")

    static func moveUp(_ device: String) -> String {
        String(localized: "Move \(device) up")
    }

    static func moveDown(_ device: String) -> String {
        String(localized: "Move \(device) down")
    }

    static func forget(_ device: String) -> String {
        String(localized: "Forget \(device)")
    }

    // MARK: Library

    static let filterAll = String(localized: "All")
    static let filterHeard = String(localized: "Heard")
    static let filterHummed = String(localized: "Hummed")
    static let filterLinks = String(localized: "Links")
    static let filterMixes = String(localized: "Mixes")
    static let filterPinned = String(localized: "Pinned")
    static let libraryEmptyTitle = String(localized: "Nothing identified yet")
    static let libraryEmptyMessage = String(localized: "Songs you identify will show up here.")
    static let libraryEmptyFiltered = String(localized: "Nothing here yet")
    static let noPinnedTitle = String(localized: "Nothing pinned yet")
    static let noPinnedMessage = String(localized: "Pin songs and mixes to keep them here and in the sidebar.")
    static let pin = String(localized: "Pin")
    static let unpin = String(localized: "Unpin")
    static let pinned = String(localized: "Pinned")
    static let delete = String(localized: "Delete")
    static let deleteConfirmMessage = String(localized: "The tracklist and its saved audio will be removed.")

    static func deleteConfirmTitle(_ title: String) -> String {
        String(localized: "Delete “\(title)”?")
    }

    static func sessionsCount(_ count: Int) -> String {
        String(localized: "\(count) sessions")
    }

    static func candidatesCount(_ artist: String, _ count: Int) -> String {
        artist.isEmpty ? String(localized: "\(count) candidates") : String(localized: "\(artist) · \(count) candidates")
    }

    static func recentAccessibility(_ entry: LibraryEntry) -> String {
        let title = entry.track?.title ?? entry.title
        let artist = entry.track?.artist ?? entry.subtitle ?? ""
        return artist.isEmpty ? title : String(localized: "\(title) by \(artist)")
    }

    // MARK: Announcements and notifications

    static func noMatchAnnouncement(_ reason: NoMatchReason) -> String {
        reason == .notFound ? noMatch(.notFound) : String(localized: "No match found. \(noMatch(reason))")
    }

    static func notificationTitle(_ track: Track) -> String {
        track.artist.isEmpty ? track.title : "\(track.title) — \(track.artist)"
    }

    // MARK: Providers

    static let providerPanelTitle = String(localized: "Where do you listen?")
    static let providerPanelMessage = String(localized: "Songs you identify open there. You can change this in Settings.")
    static let moreProviders = String(localized: "More…")
    static let skip = String(localized: "Skip")
    static let providerService = String(localized: "Service")
    static let customProviderEllipsis = String(localized: "Custom…")
    static let providerName = String(localized: "Name")
    static let providerNamePrompt = String(localized: "Name of the app or site")
    static let providerAddress = String(localized: "Web address")
    static let providerAddressPrompt = "https://example.com/search?q={query}"
    static let providerAddressHelp = String(localized: "{query} becomes the artist and title. {artist}, {title} and {isrc} work too.")
    static let providerAddressInvalid = String(localized: "Enter a web address with {query}, {artist}, {title} or {isrc} in it.")

    // MARK: Settings

    static let settingsGeneral = String(localized: "General")
    static let settingsServices = String(localized: "Services")
    static let settingsMelody = String(localized: "Melody")
    static let settingsStorage = String(localized: "Storage")
    static let settingsAbout = String(localized: "About")
    static let openSongsIn = String(localized: "Open songs in")
    static let showInMenuBar = String(localized: "Show in menu bar")
    static let launchAtLogin = String(localized: "Launch at login")
    static let notifyInBackground = String(localized: "Notify when a result arrives in the background")
    static let audd = "AudD"
    static let apiToken = String(localized: "API token")
    static let auddFooter = String(localized: "Optional. Tried when the free match fails.")
    static let acrCloud = "ACRCloud"
    static let acrHumming = String(localized: "ACRCloud humming")
    static let host = String(localized: "Host")
    static let accessKey = String(localized: "Access key")
    static let accessSecret = String(localized: "Access secret")
    static let acrFooter = String(localized: "Optional. Used for recordings.")
    static let acrHummingFooter = String(localized: "Optional. A second opinion when you hum.")
    static let melodyLibrary = String(localized: "Melody library")
    static let updated = String(localized: "Updated")
    static let checkForUpdates = String(localized: "Check for Updates")
    static let remove = String(localized: "Remove")
    static let update = String(localized: "Update")
    static let melodyError = String(localized: "Couldn't download. Check your connection.")
    static let melodyFooter = String(localized: "Needed to recognize humming, whistling and singing. Stays on this Mac.")
    static let whileHumming = String(localized: "While humming")
    static let alsoSearchLyrics = String(localized: "Also search lyrics you sing")
    static let autoStopAfterPause = String(localized: "Stop automatically after a pause")
    static let onDisk = String(localized: "On disk")
    static let cachedAudio = String(localized: "Cached audio")
    static let clearCache = String(localized: "Clear Cache")
    static let location = String(localized: "Location")
    static let showInFinder = String(localized: "Show in Finder")
    static let licenses = String(localized: "Licenses")
    static let acknowledgements = String(localized: "Acknowledgements")
    static let aboutLine = String(localized: "Identifies songs from what you hear, hum or link.")

    static func melodyInstalled(songs: UInt32, bytes: UInt64) -> String {
        String(localized: "\(Format.number(songs)) songs · \(Format.bytes(bytes))")
    }

    static func melodyNotInstalled(_ bytes: UInt64?) -> String {
        guard let bytes else { return String(localized: "Not installed") }
        return String(localized: "Not installed · \(Format.bytes(bytes))")
    }

    static func updateAvailable(_ bytes: UInt64?) -> String {
        guard let bytes else { return String(localized: "Update available") }
        return String(localized: "Update available · \(Format.bytes(bytes))")
    }

    static func downloadProgress(fraction: Double, total: UInt64?) -> String {
        guard let total else { return fraction.formatted(.percent.precision(.fractionLength(0))) }
        let done = UInt64(Double(total) * min(max(fraction, 0), 1))
        return String(localized: "\(Format.bytes(done)) of \(Format.bytes(total))")
    }

    static func clearCacheConfirm(_ size: String) -> String {
        String(localized: "Clear \(size) of cached audio? Pinned sessions keep their audio.")
    }

    static func versionLine(version: String, build: String) -> String {
        String(localized: "Music Fetch \(version) (\(build))")
    }
}

extension LibraryEntry {
    /// The track title, the stored title, or a title from the session kind.
    nonisolated var displayTitle: String {
        if let title = track?.title, !title.isEmpty { return title }
        if !title.isEmpty { return title }
        return Copy.fallbackTitle(kind)
    }
}
