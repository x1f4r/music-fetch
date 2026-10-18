import Foundation
import MusicFetchCore

/// Sample data for SwiftUI previews and the demo engine. Artwork uses the
/// `mfdemo://artwork/<palette>` scheme, which `ArtworkView` renders locally.
/// Lyrics are invented placeholder lines.
nonisolated enum DemoFixtures {
    static let now = Date.now

    static func track(
        _ title: String, _ artist: String, album: String? = nil, year: UInt16? = nil, duration: UInt64? = nil,
        isrc: String? = nil, palette: String, preview: Bool = false, exactLinks: Bool = false
    ) -> Track {
        let key = "\(artist)|\(title)".lowercased()
        let query = "\(artist) \(title)".addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? ""
        let links: [ServiceLink] =
            exactLinks
            ? [
                ServiceLink(service: .spotify, url: "https://open.spotify.com/track/0000000000000000000000", isSearch: false),
                ServiceLink(service: .appleMusic, url: "https://music.apple.com/us/song/0000000000", isSearch: false),
                ServiceLink(service: .deezer, url: "https://www.deezer.com/track/0000000000", isSearch: false),
            ]
            : [
                ServiceLink(service: .spotify, url: "https://open.spotify.com/search/\(query)", isSearch: true),
                ServiceLink(service: .appleMusic, url: "https://music.apple.com/search?term=\(query)", isSearch: true),
                ServiceLink(service: .youTubeMusic, url: "https://music.youtube.com/search?q=\(query)", isSearch: true),
                ServiceLink(service: .deezer, url: "https://www.deezer.com/search/\(query)", isSearch: true),
            ]
        return Track(
            title: title,
            artist: artist,
            artists: artist.components(separatedBy: ", "),
            album: album,
            isrc: isrc,
            durationMs: duration,
            releaseYear: year,
            genre: nil,
            artworkUrl: "mfdemo://artwork/\(palette)",
            previewUrl: preview ? "mfdemo://preview/\(palette)" : nil,
            links: links,
            ids: [:],
            identityKey: key
        )
    }

    // MARK: Tracks

    static let houdini = track(
        "Houdini", "Dua Lipa", album: "Radical Optimism", year: 2024, duration: 185_000, isrc: "GBAHT2301226", palette: "ocean",
        preview: true, exactLinks: true)
    static let nights = track("Nights", "Frank Ocean", album: "Blonde", year: 2016, duration: 307_000, palette: "sage")
    static let blindingLights = track("Blinding Lights", "The Weeknd", album: "After Hours", year: 2020, palette: "ember")
    static let dreams = track("Dreams", "Fleetwood Mac", album: "Rumours", year: 1977, palette: "sand")
    static let espresso = track("Espresso", "Sabrina Carpenter", album: "Short n' Sweet", year: 2024, palette: "sky")

    static let candidateTracks: [Track] = [
        track("Take On Me", "a-ha", album: "Hunting High and Low", year: 1985, palette: "paper", preview: true),
        track("Dancing Queen", "ABBA", album: "Arrival", year: 1976, palette: "sunset", preview: true),
        track("Africa", "Toto", album: "Toto IV", year: 1982, palette: "dune", preview: true),
        track("Don't Stop Me Now", "Queen", album: "Jazz", year: 1978, palette: "night", preview: true),
    ]

    static let mixTracks: [(cue: UInt64, end: UInt64, track: Track?, tier: Tier)] = [
        (30_000, 250_000, track("Marea (we've lost dancing)", "Fred again.., The Blessed Madonna", palette: "navy"), .confirmed),
        (250_000, 468_000, track("Kyle (i found you)", "Fred again..", palette: "fog"), .confirmed),
        (468_000, 680_000, track("Jungle", "Fred again..", palette: "forest"), .likely),
        (680_000, 905_000, nil, .unknown),
        (905_000, 1_170_000, track("Lights Out", "Fred again.., Romy, HAAi", album: "Actual Life 3", palette: "violet"), .possible),
        (1_170_000, 1_435_000, track("Delilah (pull me out of this)", "Fred again..", palette: "peach"), .confirmed),
        (1_435_000, 1_720_000, track("Turn On The Lights again..", "Fred again.., Swedish House Mafia, Future", palette: "gold"),
         .confirmed),
        (1_720_000, 1_960_000, track("Rumble", "Skrillex, Fred again.., Flowdan", palette: "hazard"), .likely),
    ]

    static let collectionTitles = [
        "Taste", "Please Please Please", "Good Graces", "Sharpest Tool", "Coincidence", "Bed Chem", "Espresso",
        "Dumb & Poetic", "Slim Pickins", "Juno", "Lie to Girls", "Don't Smile",
    ]

    // MARK: Sessions

    static let resultID = "00000000-0000-4000-8000-000000000001"
    static let mixID = "00000000-0000-4000-8000-000000000002"
    static let humID = "00000000-0000-4000-8000-000000000003"
    static let nightsID = "00000000-0000-4000-8000-000000000004"
    static let espressoID = "00000000-0000-4000-8000-000000000005"
    static let blindingID = "00000000-0000-4000-8000-000000000006"
    static let collectionID = "00000000-0000-4000-8000-000000000007"
    static let dreamsID = "00000000-0000-4000-8000-000000000008"
    static let waitingID = "00000000-0000-4000-8000-000000000009"
    /// A hum where every answer is a long shot (screenshots only; not in the library).
    static let unsureHumID = "00000000-0000-4000-8000-000000000010"

    static func at(daysAgo: Int, hour: Int, minute: Int) -> Int64 {
        let calendar = Calendar.current
        let day = calendar.date(byAdding: .day, value: -daysAgo, to: calendar.startOfDay(for: now)) ?? now
        let date = calendar.date(bySettingHour: hour, minute: minute, second: 0, of: day) ?? day
        return Int64(date.timeIntervalSince1970 * 1000)
    }

    static func entry(
        _ id: String, _ kind: SessionKind, _ status: SessionStatus, _ created: Int64, title: String, subtitle: String?,
        artwork: String?, track: Track?, count: UInt32 = 0, duration: UInt64? = nil, url: String? = nil, pinned: Bool = false
    ) -> LibraryEntry {
        LibraryEntry(
            id: id, kind: kind, status: status, createdAtMs: created, title: title, subtitle: subtitle, artworkUrl: artwork,
            track: track, trackCount: count, durationMs: duration, sourceUrl: url, pinned: pinned)
    }

    static func segment(_ index: Int, _ item: (cue: UInt64, end: UInt64, track: Track?, tier: Tier)) -> Segment {
        let confidence: Float =
            switch item.tier {
            case .confirmed: 0.95
            case .likely: 0.88
            case .possible: 0.72
            case .unknown: 0
            }
        let evidence: [Evidence] =
            item.track == nil
            ? []
            : (item.tier == .confirmed
                ? [Evidence(source: .shazam, atMs: item.cue + 20_000, score: 0.95, detail: nil),
                   Evidence(source: .shazam, atMs: item.cue + 60_000, score: 0.95, detail: nil)]
                : [Evidence(source: .shazam, atMs: item.cue + 20_000, score: confidence, detail: nil)])
        return Segment(
            id: "seg-\(index)", itemIndex: 0, startMs: item.cue, endMs: item.end, kind: item.track == nil ? .unidentified : .track,
            track: item.track, confidence: confidence, tier: item.tier, evidence: evidence, alternates: [],
            sourceUrl: "https://www.youtube.com/watch?v=demo&t=\(item.cue / 1000)s")
    }

    static var mixSegments: [Segment] {
        mixTracks.enumerated().map { segment($0.offset, $0.element) }
    }

    static let mixSource = SourceInfo(
        url: "https://www.youtube.com/watch?v=demo", title: "Fred again.. | Boiler Room: London", uploader: "Boiler Room",
        thumbnailUrl: "mfdemo://artwork/stage", durationMs: 3_633_000, service: .youTube, site: "YouTube", itemCount: 1,
        hints: [])

    static let collectionSource = SourceInfo(
        url: "https://open.spotify.com/album/demo", title: "Short n' Sweet", uploader: "Sabrina Carpenter",
        thumbnailUrl: "mfdemo://artwork/sky", durationMs: nil, service: .spotify, site: "Spotify",
        itemCount: UInt32(collectionTitles.count), hints: [])

    static var collectionSegments: [Segment] {
        collectionTitles.enumerated().map { index, title in
            Segment(
                id: "track-\(index)", itemIndex: UInt32(index), startMs: 0, endMs: 0, kind: .track,
                track: track(title, "Sabrina Carpenter", album: "Short n' Sweet", year: 2024, palette: "sky"), confidence: 0.99,
                tier: .confirmed, evidence: [Evidence(source: .metadata, atMs: 0, score: 0.99, detail: nil)], alternates: [],
                sourceUrl: nil)
        }
    }

    /// A stepped melody in semitones around 0 with small wobble; NaN gaps between phrases.
    static func contour(seed: Int, frames: Int, shift: Float = 0) -> [Float] {
        let steps: [Float] = [0, 0, 2, 4, 4, 2, 0, -1, 0, 4, 7, 5, 4, 2, 4, 0]
        return (0..<frames).map { index in
            let phrase = index / 40
            if index % 40 >= 36 { return .nan }
            let note = steps[(phrase * 4 + (index % 40) / 10 + seed) % steps.count]
            let wobble = Float(sin(Double(index) * 0.7 + Double(seed))) * 0.18
            return note + wobble + shift
        }
    }

    static var candidates: [Candidate] {
        let tiers: [CandidateTier] = [.strong, .possible, .possible, .longShot]
        let scores: [Float] = [0.86, 0.58, 0.52, 0.31]
        return candidateTracks.enumerated().map { index, track in
            let query = contour(seed: 0, frames: 220)
            let reference = contour(seed: index, frames: 220, shift: index == 0 ? 0.1 : 0.4)
            return Candidate(
                track: track, score: scores[index], tier: tiers[index],
                sources: [CandidateSource(kind: .melody, score: scores[index], detail: index == 0 ? "transposed −2, tempo ×0.9" : nil)],
                alignment: MelodyAlignment(frameMs: 50, query: query, reference: reference))
        }
    }

    /// The progressive search's interim lists while humming: melody only, not enriched
    /// (no artwork), each replacing the last. The first has only long shots.
    static var liveLists: [[Candidate]] {
        [
            [live(2, 0.18, .longShot), live(0, 0.12, .longShot)],
            [live(0, 0.58, .possible), live(1, 0.41, .possible), live(2, 0.2, .longShot)],
            [live(0, 0.81, .strong), live(1, 0.37, .possible), live(2, 0.12, .longShot)],
        ]
    }

    private static func live(_ index: Int, _ score: Float, _ tier: CandidateTier) -> Candidate {
        var track = candidateTracks[index]
        track.artworkUrl = nil
        track.previewUrl = nil
        return Candidate(
            track: track, score: score, tier: tier, sources: [CandidateSource(kind: .melody, score: score, detail: nil)],
            alignment: nil)
    }

    /// Final candidates of a hum the matcher is not sure about.
    static var unsureCandidates: [Candidate] {
        let scores: [Float] = [0.22, 0.14, 0.09]
        return [candidateTracks[2], candidateTracks[3], dreams].enumerated().map { index, track in
            let reference = contour(seed: index + 3, frames: 220, shift: 1.2)
            return Candidate(
                track: track, score: scores[index], tier: .longShot,
                sources: [CandidateSource(kind: .melody, score: scores[index], detail: nil)],
                alignment: MelodyAlignment(frameMs: 50, query: contour(seed: 0, frames: 220), reference: reference))
        }
    }

    static var unsureEntry: LibraryEntry {
        entry(unsureHumID, .hum, .candidates, at(daysAgo: 0, hour: 15, minute: 2), title: "Africa", subtitle: "Toto",
              artwork: candidateTracks[2].artworkUrl, track: nil)
    }

    static var entries: [LibraryEntry] {
        [
            entry(resultID, .listen, .matched, at(daysAgo: 0, hour: 14, minute: 32), title: "Houdini", subtitle: "Dua Lipa",
                  artwork: houdini.artworkUrl, track: houdini),
            entry(mixID, .link, .running, at(daysAgo: 0, hour: 13, minute: 5), title: mixSource.title ?? "", subtitle: "Boiler Room",
                  artwork: mixSource.thumbnailUrl, track: nil, count: 8, duration: mixSource.durationMs, url: mixSource.url,
                  pinned: true),
            entry(humID, .hum, .candidates, at(daysAgo: 0, hour: 11, minute: 48), title: "Take On Me", subtitle: "a-ha",
                  artwork: candidateTracks[0].artworkUrl, track: nil),
            entry(waitingID, .listen, .waiting, at(daysAgo: 0, hour: 9, minute: 20), title: "", subtitle: nil, artwork: nil,
                  track: nil),
            entry(espressoID, .link, .completed, at(daysAgo: 1, hour: 21, minute: 10), title: "Espresso",
                  subtitle: "Sabrina Carpenter", artwork: espresso.artworkUrl, track: espresso, count: 1,
                  url: "https://www.tiktok.com/@demo/video/1"),
            entry(nightsID, .systemAudio, .matched, at(daysAgo: 1, hour: 18, minute: 22), title: "Nights", subtitle: "Frank Ocean",
                  artwork: nights.artworkUrl, track: nights, pinned: true),
            entry(blindingID, .listen, .matched, at(daysAgo: 3, hour: 16, minute: 40), title: "Blinding Lights",
                  subtitle: "The Weeknd", artwork: blindingLights.artworkUrl, track: blindingLights),
            entry(collectionID, .link, .completed, at(daysAgo: 3, hour: 9, minute: 15), title: "Short n' Sweet",
                  subtitle: "Sabrina Carpenter", artwork: collectionSource.thumbnailUrl, track: nil, count: 12,
                  url: collectionSource.url),
            entry(dreamsID, .file, .completed, at(daysAgo: 3, hour: 8, minute: 2), title: "Dreams", subtitle: "Fleetwood Mac",
                  artwork: dreams.artworkUrl, track: dreams, count: 1),
        ]
    }

    static func detail(_ id: String) -> SessionDetail? {
        if id == unsureHumID {
            return SessionDetail(entry: unsureEntry, source: nil, segments: [], candidates: unsureCandidates, transcript: nil)
        }
        guard let entry = entries.first(where: { $0.id == id }) else { return nil }
        switch id {
        case mixID:
            var finished = entry
            finished.status = .completed
            return SessionDetail(entry: finished, source: mixSource, segments: mixSegments, candidates: [], transcript: nil)
        case humID:
            return SessionDetail(entry: entry, source: nil, segments: [], candidates: candidates, transcript: nil)
        case collectionID:
            return SessionDetail(entry: entry, source: collectionSource, segments: collectionSegments, candidates: [], transcript: nil)
        default:
            let track = entry.track
            let evidence = [Evidence(source: .shazam, atMs: 4_000, score: 0.88, detail: nil)]
            let segments =
                track.map {
                    [Segment(id: "match", itemIndex: 0, startMs: 0, endMs: 6_000, kind: .track, track: $0, confidence: 0.88,
                             tier: .likely, evidence: evidence, alternates: [], sourceUrl: nil)]
                } ?? []
            let source: SourceInfo? =
                entry.kind == .file
                ? SourceInfo(url: nil, title: "rumours_side_a", uploader: nil, thumbnailUrl: nil, durationMs: 1_100_000,
                             service: nil, site: nil, itemCount: 1, hints: [])
                : entry.kind == .link
                    ? SourceInfo(url: entry.sourceUrl, title: "Espresso dance", uploader: nil, thumbnailUrl: nil,
                                 durationMs: 31_000, service: .tikTok, site: "TikTok", itemCount: 1, hints: [])
                    : nil
            return SessionDetail(entry: entry, source: source, segments: segments, candidates: [], transcript: nil)
        }
    }

    /// Invented placeholder lines for the lyrics section (never real lyrics).
    static let lyrics = Lyrics(
        plain: """
            Lyrics appear here
            line by line as the song plays
            synced when timing is known
            plain text otherwise

            A second verse follows
            with the same quiet styling
            scrolling under the details
            """,
        synced: [])

    static let melodyStatus = MelodyIndexStatus(
        installed: true, version: "2026.10.1", songCount: 41_234, sizeBytes: 48_000_000, availableVersion: nil, downloadBytes: nil)
}
