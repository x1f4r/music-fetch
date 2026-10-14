import MusicFetchCore
import SwiftUI
import Testing

@testable import MusicFetch

struct CopyTests {
    @Test func whereSkipsMelodyAndLyricDetails() {
        let melody = CandidateSource(kind: .melody, score: 0.8, detail: "transposed −2, tempo ×0.9")
        let lyrics = CandidateSource(kind: .lyrics, score: 0.6, detail: "a matched line")
        let humming = CandidateSource(kind: .acrHumming, score: 0.5, detail: "Chorus")
        #expect(Copy.wherePhrase([melody, lyrics]) == nil)
        #expect(Copy.wherePhrase([melody, humming]) == "Chorus")
    }

    @Test func untitledSessionsUseTheirSource() {
        var entry = DemoFixtures.entries[0]
        entry.track = nil
        entry.title = ""
        entry.kind = .systemAudio
        #expect(entry.displayTitle == "This Mac")
        entry.kind = .hum
        #expect(entry.displayTitle == "Hum")
        entry.title = "Stored"
        #expect(entry.displayTitle == "Stored")
    }

    @Test func matchedByListsDistinctSources() {
        let sources = [
            CandidateSource(kind: .lyrics, score: 0.4, detail: nil),
            CandidateSource(kind: .melody, score: 0.9, detail: nil),
            CandidateSource(kind: .melody, score: 0.3, detail: nil),
        ]
        #expect(Copy.matchedBy(sources) == "Melody and Sung lyrics")
    }

    @Test func downloadProgressReadsInBytes() {
        #expect(Copy.downloadProgress(fraction: 0.5, total: 48_000_000).contains("of"))
        #expect(Copy.melodyNotInstalled(nil) == "Not installed")
    }

    @Test func evidenceSummaries() {
        let two = [
            Evidence(source: .shazam, atMs: 0, score: 0.95, detail: nil),
            Evidence(source: .shazam, atMs: 30_000, score: 0.95, detail: nil),
        ]
        #expect(Copy.evidence(two, confidence: 0.95) == "2 Shazam matches")
        #expect(Copy.evidence([two[0]], confidence: 0.72) == "1 Shazam match, pitch-shifted")
        #expect(Copy.evidence([Evidence(source: .acrCloud, atMs: 0, score: 0.86, detail: nil)], confidence: 0.8) == "ACRCloud, score 86")
    }
}

struct ContourPathTests {
    @Test func outOfBandValuesBreakTheLine() {
        let values: [Float] = [60, 61, 40, 62, .nan, 63]
        let path = ContourPath.path(values, in: CGRect(x: 0, y: 0, width: 100, height: 48), median: 61)
        var moves = 0
        path.forEach { element in
            if case .move = element { moves += 1 }
        }
        // 60-61, then 62 alone, then 63 alone.
        #expect(moves == 3)
    }
}
