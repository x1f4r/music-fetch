import Foundation
import MusicFetchCore
import Testing

@testable import MusicFetch

/// Settings › Melody with the index's encoder file: when it is offered, how big each
/// file is, and that an encoder-only download isn't called an index update.
@MainActor
struct MelodySettingsTests {
    private func status(
        installed: Bool = true, version: String? = "2026.10.1", available: String? = nil, download: UInt64? = nil,
        encoder: Bool
    ) -> MelodyIndexStatus {
        MelodyIndexStatus(
            installed: installed, version: installed ? version : nil, songCount: 41_234, sizeBytes: 61_000_000,
            availableVersion: available, downloadBytes: download, encoderInstalled: encoder)
    }

    @Test func encoderStatesFollowTheEngineStatus() {
        typealias State = SettingsModel.EncoderState
        #expect(SettingsModel.encoderState(status(encoder: true), encoderBytes: 13_000_000) == State.installed(bytes: 13_000_000))
        #expect(
            SettingsModel.encoderState(status(available: "2026.10.1", download: 13_000_000, encoder: false), encoderBytes: nil)
                == State.available(bytes: 13_000_000))
        #expect(
            SettingsModel.encoderState(status(available: "2026.10.1", download: 14_000_000, encoder: true), encoderBytes: 13_000_000)
                == State.updateAvailable(bytes: 14_000_000))
        // A published index without an encoder for this core: nothing to show.
        #expect(SettingsModel.encoderState(status(encoder: false), encoderBytes: nil) == State.none)
        // A newer index brings its encoder along: that's the index update row.
        #expect(
            SettingsModel.encoderState(status(available: "2026.11.1", download: 70_000_000, encoder: false), encoderBytes: nil)
                == State.none)
        #expect(SettingsModel.encoderState(status(installed: false, encoder: false), encoderBytes: nil) == State.none)
    }

    private func settings(with melody: MelodyIndexStatus) async throws -> SettingsModel {
        let app = AppModel(demo: DemoState(name: "tests"))
        await app.start()
        let engine = try #require(app.engine as? FakeEngine)
        engine.setMelodyStatus(melody)
        app.settings.refreshMelody()
        return app.settings
    }

    @Test func theLibrarySizeLeavesOutTheEncoder() async throws {
        let settings = try await settings(with: DemoFixtures.melodyStatus)
        #expect(settings.encoder == .installed(bytes: DemoFixtures.encoderBytes))
        #expect(settings.libraryBytes == 61_000_000 - DemoFixtures.encoderBytes)
        #expect(!settings.hasMelodyUpdate)
    }

    @Test func aMissingEncoderIsNotAnIndexUpdate() async throws {
        let settings = try await settings(with: DemoFixtures.melodyStatusWithoutEncoder)
        #expect(!settings.hasMelodyUpdate)
        #expect(settings.encoder == .available(bytes: DemoFixtures.encoderBytes))
        #expect(settings.libraryBytes == 48_000_000)

        settings.downloadMelody()
        #expect(settings.isDownloadingEncoderOnly)
        settings.cancelMelodyDownload()
    }

    @Test func aNewerIndexIsAnUpdate() async throws {
        var newer = DemoFixtures.melodyStatusWithoutEncoder
        newer.availableVersion = "2026.11.1"
        newer.downloadBytes = 70_000_000
        let settings = try await settings(with: newer)
        #expect(settings.hasMelodyUpdate)
        #expect(settings.encoder == .none)
        settings.downloadMelody()
        #expect(!settings.isDownloadingEncoderOnly)
        settings.cancelMelodyDownload()
    }
}
