@preconcurrency import AVFoundation
import CoreGraphics
import Foundation
import ScreenCaptureKit

final class MicrophoneRecorder: @unchecked Sendable {
    private var recorder: AVAudioRecorder?
    private var outputURL: URL?

    func start() async throws -> URL {
        let granted = await AVCaptureDevice.requestAccess(for: .audio)
        guard granted else {
            throw NSError(domain: "MusicFetchMac", code: 401, userInfo: [NSLocalizedDescriptionKey: "Microphone access was denied"])
        }

        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("music-fetch-mic-\(UUID().uuidString)")
            .appendingPathExtension("m4a")
        let settings: [String: Any] = [
            AVFormatIDKey: kAudioFormatMPEG4AAC,
            AVSampleRateKey: 44_100,
            AVNumberOfChannelsKey: 1,
            AVEncoderBitRateKey: 192_000,
        ]
        let recorder = try AVAudioRecorder(url: url, settings: settings)
        recorder.isMeteringEnabled = true
        guard recorder.record() else {
            throw NSError(domain: "MusicFetchMac", code: 500, userInfo: [NSLocalizedDescriptionKey: "Failed to start microphone recording"])
        }
        self.recorder = recorder
        self.outputURL = url
        return url
    }

    func stop() -> URL? {
        recorder?.stop()
        recorder = nil
        return outputURL
    }
}

final class SystemAudioRecorder: NSObject, SCStreamOutput, @unchecked Sendable {
    private var stream: SCStream?
    private let queue = DispatchQueue(label: "MusicFetchMac.SystemAudio")
    private var writer: AVAssetWriter?
    private var writerInput: AVAssetWriterInput?
    private var outputURL: URL?
    private var didStartSession = false
    private var requestedAuthorizationThisLaunch = false

    func start() async throws -> URL {
        try ensureScreenCaptureAuthorization()

        let content: SCShareableContent
        do {
            content = try await SCShareableContent.excludingDesktopWindows(false, onScreenWindowsOnly: true)
        } catch {
            throw wrapAuthorizationErrorIfNeeded(error)
        }
        guard let display = content.displays.first else {
            throw NSError(domain: "MusicFetchMac", code: 404, userInfo: [NSLocalizedDescriptionKey: "No display available for system audio capture"])
        }

        let outputURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("music-fetch-system-\(UUID().uuidString)")
            .appendingPathExtension("m4a")
        try? FileManager.default.removeItem(at: outputURL)

        let writer = try AVAssetWriter(outputURL: outputURL, fileType: .m4a)
        let writerInput = AVAssetWriterInput(
            mediaType: .audio,
            outputSettings: [
                AVFormatIDKey: kAudioFormatMPEG4AAC,
                AVSampleRateKey: 44_100,
                AVNumberOfChannelsKey: 2,
                AVEncoderBitRateKey: 192_000,
            ]
        )
        writerInput.expectsMediaDataInRealTime = true
        guard writer.canAdd(writerInput) else {
            throw NSError(domain: "MusicFetchMac", code: 500, userInfo: [NSLocalizedDescriptionKey: "Could not create system audio writer input"])
        }
        writer.add(writerInput)
        self.writer = writer
        self.writerInput = writerInput
        self.outputURL = outputURL
        self.didStartSession = false

        let filter = SCContentFilter(display: display, excludingApplications: [], exceptingWindows: [])
        let config = SCStreamConfiguration()
        config.capturesAudio = true
        config.width = 2
        config.height = 2
        config.minimumFrameInterval = CMTime(value: 1, timescale: 30)
        config.sampleRate = 44_100
        config.channelCount = 2

        let stream = SCStream(filter: filter, configuration: config, delegate: nil)
        do {
            try stream.addStreamOutput(self, type: .audio, sampleHandlerQueue: queue)
            try await stream.startCapture()
        } catch {
            throw wrapAuthorizationErrorIfNeeded(error)
        }
        self.stream = stream
        return outputURL
    }

    func stop() async throws -> URL? {
        if let stream {
            try await stream.stopCapture()
        }
        writerInput?.markAsFinished()
        let outputURL = self.outputURL
        if let writer {
            try await withCheckedThrowingContinuation { continuation in
                writer.finishWriting {
                    if writer.status == .completed || writer.status == .unknown {
                        continuation.resume(returning: ())
                    } else {
                        let error = writer.error ?? NSError(domain: "MusicFetchMac", code: 500, userInfo: [NSLocalizedDescriptionKey: "Failed to finalize system audio recording"])
                        continuation.resume(throwing: error)
                    }
                }
            }
        }
        self.stream = nil
        self.writer = nil
        self.writerInput = nil
        self.didStartSession = false
        return outputURL
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer sampleBuffer: CMSampleBuffer, of outputType: SCStreamOutputType) {
        guard outputType == .audio else { return }
        guard CMSampleBufferDataIsReady(sampleBuffer) else { return }
        guard let writer, let writerInput else { return }

        if !didStartSession {
            writer.startWriting()
            writer.startSession(atSourceTime: CMSampleBufferGetPresentationTimeStamp(sampleBuffer))
            didStartSession = true
        }

        if writerInput.isReadyForMoreMediaData {
            writerInput.append(sampleBuffer)
        }
    }

    private func ensureScreenCaptureAuthorization() throws {
        if CGPreflightScreenCaptureAccess() {
            return
        }
        if !requestedAuthorizationThisLaunch {
            requestedAuthorizationThisLaunch = true
            _ = CGRequestScreenCaptureAccess()
        }
        throw NSError(
            domain: "MusicFetchMac",
            code: 401,
            userInfo: [
                NSLocalizedDescriptionKey: "Allow Music Fetch in Privacy & Security > Screen & System Audio Recording, then relaunch the app."
            ]
        )
    }

    private func wrapAuthorizationErrorIfNeeded(_ error: Error) -> Error {
        let nsError = error as NSError
        let text = [nsError.localizedDescription, nsError.domain]
            .joined(separator: " ")
            .lowercased()
        if text.contains("not authorized") || text.contains("permission") || text.contains("screen capture") || text.contains("system audio") {
            return NSError(
                domain: "MusicFetchMac",
                code: 401,
                userInfo: [
                    NSLocalizedDescriptionKey: "Music Fetch needs Screen & System Audio Recording. If you just allowed it, fully relaunch the app."
                ]
            )
        }
        return error
    }
}
