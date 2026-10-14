import AVFoundation
import Foundation
import MusicFetchCore

/// `PlatformDecoder` for files the engine's own decoder can't read: decodes the first
/// audio track with `AVAssetReader` to 16-bit mono PCM at the requested rate and writes
/// a WAV file.
nonisolated final class AVFoundationDecoder: PlatformDecoder {
    func decodeToWav(inputPath: String, outputPath: String, sampleRate: UInt32, task: TaskHandle) throws {
        let output = URL(filePath: outputPath)
        do {
            try Self.decode(
                input: URL(filePath: inputPath),
                output: output,
                sampleRate: sampleRate,
                isCanceled: { task.isCanceled() },
                progress: { task.reportProgress(fraction: $0) }
            )
        } catch {
            try? FileManager.default.removeItem(at: output)
            throw Self.platformError(error)
        }
    }

    enum DecodeError: Error, Equatable {
        case noAudioTrack
        case unreadable
        case canceled
    }

    /// Decodes `input` into a 16-bit mono WAV at `sampleRate`.
    static func decode(
        input: URL,
        output: URL,
        sampleRate: UInt32,
        isCanceled: @escaping @Sendable () -> Bool,
        progress: (Float) -> Void
    ) throws {
        let asset = AVURLAsset(url: input)
        // AVAssetTrack isn't Sendable; it only crosses back to this (waiting) thread.
        let loaded = try BlockingBridge.run(timeout: .seconds(30), isCanceled: isCanceled) {
            let tracks = try await asset.loadTracks(withMediaType: .audio)
            guard let track = tracks.first else { throw DecodeError.noAudioTrack }
            let duration = try await asset.load(.duration)
            return UncheckedSendable((track, duration))
        }
        let (track, duration) = loaded.value

        let reader = try AVAssetReader(asset: asset)
        let settings: [String: Any] = [
            AVFormatIDKey: kAudioFormatLinearPCM,
            AVSampleRateKey: Double(sampleRate),
            AVNumberOfChannelsKey: 1,
            AVLinearPCMBitDepthKey: 16,
            AVLinearPCMIsFloatKey: false,
            AVLinearPCMIsBigEndianKey: false,
            AVLinearPCMIsNonInterleaved: false,
        ]
        let trackOutput = AVAssetReaderTrackOutput(track: track, outputSettings: settings)
        trackOutput.alwaysCopiesSampleData = false
        guard reader.canAdd(trackOutput) else { throw DecodeError.unreadable }
        reader.add(trackOutput)
        guard reader.startReading() else { throw DecodeError.unreadable }

        let writer = try WAVWriter(url: output, sampleRate: sampleRate, channels: 1)
        let totalSeconds = duration.isNumeric && duration.seconds > 0 ? duration.seconds : nil
        var lastReported: Float = -1

        while let sampleBuffer = trackOutput.copyNextSampleBuffer() {
            if isCanceled() {
                reader.cancelReading()
                throw DecodeError.canceled
            }
            if let blockBuffer = CMSampleBufferGetDataBuffer(sampleBuffer) {
                try append(blockBuffer, to: writer)
            }
            if let totalSeconds {
                let position = CMSampleBufferGetPresentationTimeStamp(sampleBuffer).seconds
                let fraction = Float(min(max(position / totalSeconds, 0), 1))
                if fraction - lastReported >= 0.01 {
                    lastReported = fraction
                    progress(fraction)
                }
            }
        }

        guard reader.status == .completed else {
            throw isCanceled() ? DecodeError.canceled : DecodeError.unreadable
        }
        guard writer.dataBytes > 0 else { throw DecodeError.noAudioTrack }
        try writer.finish()
        progress(1)
    }

    private static func append(_ blockBuffer: CMBlockBuffer, to writer: WAVWriter) throws {
        let length = CMBlockBufferGetDataLength(blockBuffer)
        guard length > 0 else { return }
        var pointer: UnsafeMutablePointer<CChar>?
        var contiguousLength = 0
        let status = CMBlockBufferGetDataPointer(
            blockBuffer, atOffset: 0, lengthAtOffsetOut: nil, totalLengthOut: &contiguousLength, dataPointerOut: &pointer)
        if status == kCMBlockBufferNoErr, let pointer, contiguousLength == length,
            CMBlockBufferIsRangeContiguous(blockBuffer, atOffset: 0, length: length)
        {
            try writer.append(UnsafeRawBufferPointer(start: pointer, count: length))
            return
        }
        var bytes = [UInt8](repeating: 0, count: length)
        let copyStatus = bytes.withUnsafeMutableBytes {
            CMBlockBufferCopyDataBytes(blockBuffer, atOffset: 0, dataLength: length, destination: $0.baseAddress!)
        }
        guard copyStatus == kCMBlockBufferNoErr else { throw DecodeError.unreadable }
        try bytes.withUnsafeBytes { try writer.append($0) }
    }

    private static func platformError(_ error: Error) -> PlatformError {
        switch error {
        case DecodeError.canceled, BlockingBridge.Failure.canceled:
            .Canceled
        case DecodeError.noAudioTrack:
            .Failed(message: "This file has no audio.")
        default:
            .Failed(message: "Couldn't read this file.")
        }
    }
}
