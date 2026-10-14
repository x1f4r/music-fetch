import AVFoundation
import Foundation

/// Receives captured audio: interleaved f32 bytes, sample rate, channel count.
typealias PCMHandler = @Sendable (Data, UInt32, UInt32) -> Void

/// A source of live audio for a `LiveSessionHandle`.
nonisolated protocol AudioCapture: AnyObject, Sendable {
    func start(handler: @escaping PCMHandler) throws
    func stop()
}

nonisolated enum CaptureError: LocalizedError, Equatable {
    case noInputDevice
    case coreAudio(operation: String, status: OSStatus)

    var errorDescription: String? {
        switch self {
        case .noInputDevice: "No microphone is available."
        case .coreAudio(let operation, let status): "Couldn't \(operation) (\(status))."
        }
    }
}

/// Microphone capture with `AVAudioEngine`: an input tap in the hardware format, without
/// voice processing (AGC, noise suppression and echo cancellation hurt music). The engine
/// converts rate and channels itself. Follows device changes.
nonisolated final class MicrophoneCapture: AudioCapture, @unchecked Sendable {
    private let engine = AVAudioEngine()
    private let lock = NSLock()
    private var handler: PCMHandler?
    private var configurationObserver: NSObjectProtocol?

    func start(handler: @escaping PCMHandler) throws {
        lock.lock()
        defer { lock.unlock() }
        self.handler = handler
        let input = engine.inputNode
        if input.isVoiceProcessingEnabled {
            try input.setVoiceProcessingEnabled(false)
        }
        try installTapAndStart()
        configurationObserver = NotificationCenter.default.addObserver(
            forName: .AVAudioEngineConfigurationChange, object: engine, queue: nil
        ) { [weak self] _ in
            self?.restartAfterConfigurationChange()
        }
    }

    func stop() {
        lock.lock()
        defer { lock.unlock() }
        if let configurationObserver {
            NotificationCenter.default.removeObserver(configurationObserver)
        }
        configurationObserver = nil
        handler = nil
        engine.inputNode.removeTap(onBus: 0)
        engine.stop()
    }

    private func installTapAndStart() throws {
        let input = engine.inputNode
        let format = input.outputFormat(forBus: 0)
        guard format.sampleRate > 0, format.channelCount > 0 else { throw CaptureError.noInputDevice }
        let rate = UInt32(format.sampleRate.rounded())
        let channels = UInt32(format.channelCount)
        guard let handler else { return }
        input.installTap(onBus: 0, bufferSize: 4096, format: format) { buffer, _ in
            guard let data = PCMConversion.interleavedFloat32(buffer) else { return }
            handler(data, rate, channels)
        }
        engine.prepare()
        try engine.start()
    }

    /// The input device or its format changed: reinstall the tap with the new format.
    private func restartAfterConfigurationChange() {
        lock.lock()
        defer { lock.unlock() }
        guard handler != nil else { return }
        engine.inputNode.removeTap(onBus: 0)
        engine.stop()
        do {
            try installTapAndStart()
        } catch {
            Log.capture.error("Microphone restart failed: \(error.localizedDescription, privacy: .public)")
        }
    }

    /// Current microphone permission.
    static var authorization: AVAuthorizationStatus {
        AVCaptureDevice.authorizationStatus(for: .audio)
    }

    /// Asks for microphone access (shows the system prompt once).
    static func requestAccess() async -> Bool {
        await AVAudioApplication.requestRecordPermission()
    }
}
