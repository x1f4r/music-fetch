import Foundation
import MusicFetchCore
import Testing

@testable import MusicFetch

struct EngineOptionsTests {
    private let gib: UInt64 = 1 << 30

    @Test func weakMacsAndLowPowerModeUseLowPower() {
        // Intel MacBook Air 2020: 8 GB, 4 cores.
        #expect(LowPowerMonitor.shouldUseLowPower(memoryBytes: 8 * gib, cores: 4, lowPowerMode: false))
        #expect(LowPowerMonitor.shouldUseLowPower(memoryBytes: 4 * gib, cores: 8, lowPowerMode: false))
        #expect(LowPowerMonitor.shouldUseLowPower(memoryBytes: 16 * gib, cores: 8, lowPowerMode: true))
        #expect(!LowPowerMonitor.shouldUseLowPower(memoryBytes: 8 * gib, cores: 8, lowPowerMode: false))
        #expect(!LowPowerMonitor.shouldUseLowPower(memoryBytes: 128 * gib, cores: 16, lowPowerMode: false))
    }

    @Test func autoStopFollowsTheSetting() throws {
        let suite = "local.musicfetch.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }

        Preferences.registerDefaults(defaults)
        #expect(Preferences.listenOptions(defaults).autoStop)
        defaults.set(false, forKey: PreferenceKey.autoStopAfterPause)
        #expect(!Preferences.listenOptions(defaults).autoStop)
    }

    @Test func registeredDefaultsAreNotAProviderDecision() throws {
        let suite = "local.musicfetch.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        Preferences.registerDefaults(defaults)
        #expect(!ProviderPreference.load(defaults).isDecided)
    }
}
