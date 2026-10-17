import Foundation

/// Puts the engine in low-power mode on weaker Macs (less than 8 GB of memory or at
/// most four cores) and while Low Power Mode is on, and follows Low Power Mode changes.
/// Lives as long as the app.
final class LowPowerMonitor {
    private var engine: (any MusicEngine)?
    private var observer: NSObjectProtocol?
    private var current: Bool?

    /// The rule: weak hardware, or the system asks to save energy.
    nonisolated static func shouldUseLowPower(memoryBytes: UInt64, cores: Int, lowPowerMode: Bool) -> Bool {
        memoryBytes < 8 << 30 || cores <= 4 || lowPowerMode
    }

    func start(engine: any MusicEngine) {
        self.engine = engine
        apply()
        observer = NotificationCenter.default.addObserver(
            forName: .NSProcessInfoPowerStateDidChange, object: nil, queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated { self?.apply() }
        }
    }

    private func apply() {
        let info = ProcessInfo.processInfo
        let enabled = Self.shouldUseLowPower(
            memoryBytes: info.physicalMemory, cores: info.activeProcessorCount, lowPowerMode: info.isLowPowerModeEnabled)
        guard enabled != current else { return }
        current = enabled
        engine?.setLowPower(enabled)
        Log.engine.info("Low-power mode \(enabled ? "on" : "off", privacy: .public)")
    }
}
