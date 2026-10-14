import Foundation
import MusicFetchCore
import Testing

@testable import MusicFetch

struct FormattingTests {
    @Test func cues() {
        #expect(Format.cue(milliseconds: 30_000) == "0:30")
        #expect(Format.cue(milliseconds: 905_000) == "15:05")
        #expect(Format.cue(milliseconds: 3_633_000) == "1:00:33")
    }

    @Test func etaCopy() {
        #expect(Copy.eta(milliseconds: nil) == "Analyzing")
        #expect(Copy.eta(milliseconds: 59_000) == "Less than a minute left")
        #expect(Copy.eta(milliseconds: 181_000) == "About 4 min left")
    }

    @Test func dayHeaders() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = try #require(TimeZone(identifier: "UTC"))
        let now = try #require(calendar.date(from: DateComponents(year: 2026, month: 10, day: 14, hour: 12)))
        let yesterday = try #require(calendar.date(byAdding: .day, value: -1, to: now))
        #expect(Format.dayHeader(now, now: now, calendar: calendar) == "Today")
        #expect(Format.dayHeader(yesterday, now: now, calendar: calendar) == "Yesterday")
    }

    @Test func indexVersionsReadAsDates() {
        #expect(Format.indexDate("2026.10.1")?.contains("2026") == true)
        #expect(Format.indexDate("beta") == "beta")
        #expect(Format.indexDate(nil) == nil)
    }

    @Test func hostsDropWWW() {
        #expect(Format.host("https://www.tiktok.com/@a/video/1") == "tiktok.com")
        #expect(Format.host("https://open.spotify.com/album/x") == "open.spotify.com")
        #expect(Format.host(nil) == nil)
    }

    @Test func exportFileNames() {
        #expect(AppModel.exportFileName("Fred again.. | Boiler Room: London", format: .cue) == "Fred again.. - Boiler Room- London.cue")
        #expect(AppModel.exportFileName("  ", format: .text) == "Tracklist.txt")
    }

    @Test func inputExtraction() {
        #expect(AppModel.extractInput(from: "listen https://youtu.be/abc?t=3 now") == "https://youtu.be/abc?t=3")
        #expect(AppModel.extractInput(from: "/tmp/song.m4a") == "/tmp/song.m4a")
        #expect(AppModel.extractInput(from: "file:///tmp/a%20b.mp3") == "/tmp/a b.mp3")
        #expect(AppModel.extractInput(from: "no link here") == nil)
        #expect(AppModel.extractInput(from: "ftp://example.com/x") == nil)
    }

    @Test func noticesParse() {
        let markdown = """
            # Third-party notices

            Intro.

            ## Component

            The `model.onnx` file, unmodified.

            ```
            MIT License
            Copyright (c) Someone
            ```
            """
        let entries = Licenses.parseNotices(markdown)
        #expect(entries.count == 1)
        #expect(entries.first?.title == "Component")
        #expect(entries.first?.summary == "The model.onnx file, unmodified.")
        #expect(entries.first?.text == "MIT License\nCopyright (c) Someone")
    }
}

@MainActor
struct LibraryGroupingTests {
    @Test func entriesGroupByDay() {
        let sections = LibraryModel.group(DemoFixtures.entries)
        #expect(sections.count == 3)
        #expect(sections[0].title == "Today")
        #expect(sections[0].entries.count == 3)
        #expect(sections[1].title == "Yesterday")
        #expect(sections[2].entries.count == 3)
    }

    @Test func filters() {
        let entries = DemoFixtures.entries
        #expect(entries.filter(LibraryModel.Filter.heard.includes).count == 3)
        #expect(entries.filter(LibraryModel.Filter.hummed.includes).count == 1)
        #expect(entries.filter(LibraryModel.Filter.mixes.includes).map(\.title) == ["Fred again.. | Boiler Room: London", "Short n' Sweet"])
    }
}

@MainActor
struct LiveSignalTests {
    @Test func levelIsSmoothed() {
        let signal = LiveSignal()
        let start = Date(timeIntervalSinceReferenceDate: 0)
        signal.push(level: 0, at: start)
        signal.push(level: 1, at: start.addingTimeInterval(0.1))
        #expect(abs(signal.level - (1 - exp(-1))) < 0.001)
        signal.freeze()
        signal.push(level: 0, at: start.addingTimeInterval(0.2))
        #expect(signal.level > 0.6)
    }

    @Test func framesScrollOutOfTheWindow() {
        let signal = LiveSignal()
        let frames = (0..<200).map { PitchFrame(tMs: UInt64($0) * 50, midi: $0 % 10 == 9 ? .nan : 60, confidence: 1) }
        signal.push(frames: frames)
        #expect(signal.frames.first?.tMs ?? 0 >= 9_950 - 8_500)
        #expect(signal.median == 60)
        #expect(signal.voicedSeconds == 180 * 0.05)
        #expect(signal.history.count == 200)
    }
}

struct LinkOpenerTests {
    private let links = [
        ServiceLink(service: .youTube, url: "https://www.youtube.com/watch?v=x", isSearch: false),
        ServiceLink(service: .spotify, url: "https://open.spotify.com/search/x", isSearch: true),
        ServiceLink(service: .appleMusic, url: "https://music.apple.com/us/album/x?i=1", isSearch: false),
        ServiceLink(service: .spotify, url: "https://open.spotify.com/track/x", isSearch: false),
    ]

    @Test func defaultServiceExactLinkFirst() {
        #expect(LinkOpener.preferredLink(in: links, defaultService: .spotify)?.url == "https://open.spotify.com/track/x")
        // No Deezer link: the first exact link in menu order.
        #expect(LinkOpener.preferredLink(in: links, defaultService: .deezer)?.url == "https://open.spotify.com/track/x")
        #expect(LinkOpener.preferredLink(in: [links[1]], defaultService: .deezer)?.isSearch == true)
    }

    @Test func menuIsOrderedAndDeduplicated() {
        #expect(LinkOpener.menuLinks(links).map(\.service) == [.spotify, .appleMusic, .youTube])
        #expect(LinkOpener.menuLinks(links).first?.isSearch == false)
    }

    @Test func appleMusicOpensInMusic() {
        #expect(LinkOpener.url(for: links[2])?.scheme == "music")
        #expect(LinkOpener.url(for: links[0])?.scheme == "https")
    }

    @Test func credentialsNeedEveryField() {
        var fields = CredentialFields()
        fields.auddToken = "  token "
        fields.acrHost = "identify-eu-west-1.acrcloud.com"
        fields.acrAccessKey = "key"
        let credentials = fields.credentials
        #expect(credentials.auddToken == "token")
        #expect(credentials.acrFingerprint == nil)
        #expect(credentials.acrHumming == nil)
    }
}
