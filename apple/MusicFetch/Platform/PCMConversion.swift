import AVFAudio
import CoreAudio
import Foundation

/// Converts captured audio into the engine's input format: interleaved little-endian
/// f32 at the source rate and channel count.
nonisolated enum PCMConversion {
    /// Interleaved f32 bytes for `buffer` (float or 16/32-bit integer, interleaved or
    /// not). Nil for empty buffers and unsupported formats.
    static func interleavedFloat32(_ buffer: AVAudioPCMBuffer) -> Data? {
        let frames = Int(buffer.frameLength)
        let channels = Int(buffer.format.channelCount)
        guard frames > 0, channels > 0 else { return nil }

        if let floats = buffer.floatChannelData {
            if buffer.format.isInterleaved {
                return Data(bytes: floats[0], count: frames * channels * MemoryLayout<Float>.size)
            }
            let planes = (0..<channels).map { UnsafePointer(floats[$0]) }
            return interleave(planes: planes, frames: frames) { $0 }
        }
        if let ints = buffer.int16ChannelData {
            let scale = Float(1) / 32768
            if buffer.format.isInterleaved {
                return convertInterleaved(UnsafePointer(ints[0]), count: frames * channels) { Float($0) * scale }
            }
            let planes = (0..<channels).map { UnsafePointer(ints[$0]) }
            return interleave(planes: planes, frames: frames) { Float($0) * scale }
        }
        if let ints = buffer.int32ChannelData {
            let scale = Float(1) / 2_147_483_648
            if buffer.format.isInterleaved {
                return convertInterleaved(UnsafePointer(ints[0]), count: frames * channels) { Float($0) * scale }
            }
            let planes = (0..<channels).map { UnsafePointer(ints[$0]) }
            return interleave(planes: planes, frames: frames) { Float($0) * scale }
        }
        return nil
    }

    /// Interleaved f32 bytes for a Core Audio buffer list described by `format`. Handles
    /// one interleaved buffer or one buffer per channel, f32 or signed 16/32-bit integers.
    static func interleavedFloat32(
        _ list: UnsafePointer<AudioBufferList>,
        format: AudioStreamBasicDescription
    ) -> (data: Data, channels: UInt32)? {
        let buffers = UnsafeMutableAudioBufferListPointer(UnsafeMutablePointer(mutating: list))
        guard !buffers.isEmpty, format.mBitsPerChannel > 0 else { return nil }
        let isFloat = format.mFormatFlags & kAudioFormatFlagIsFloat != 0
        let bytesPerSample = Int(format.mBitsPerChannel / 8)
        guard (isFloat && bytesPerSample == 4) || (!isFloat && (bytesPerSample == 2 || bytesPerSample == 4)) else {
            return nil
        }

        if buffers.count == 1 {
            let buffer = buffers[0]
            guard let raw = buffer.mData, buffer.mDataByteSize > 0 else { return nil }
            let count = Int(buffer.mDataByteSize) / bytesPerSample
            let channels = max(buffer.mNumberChannels, 1)
            let data: Data? =
                switch (isFloat, bytesPerSample) {
                case (true, _): Data(bytes: raw, count: count * 4)
                case (false, 2):
                    convertInterleaved(raw.assumingMemoryBound(to: Int16.self), count: count) { Float($0) / 32768 }
                default:
                    convertInterleaved(raw.assumingMemoryBound(to: Int32.self), count: count) { Float($0) / 2_147_483_648 }
                }
            return data.map { ($0, channels) }
        }

        // One mono buffer per channel.
        let frames = buffers.map { Int($0.mDataByteSize) / bytesPerSample }.min() ?? 0
        guard frames > 0, buffers.allSatisfy({ $0.mData != nil }) else { return nil }
        let data: Data? =
            switch (isFloat, bytesPerSample) {
            case (true, _):
                interleave(planes: buffers.map { UnsafePointer($0.mData!.assumingMemoryBound(to: Float.self)) }, frames: frames) { $0 }
            case (false, 2):
                interleave(planes: buffers.map { UnsafePointer($0.mData!.assumingMemoryBound(to: Int16.self)) }, frames: frames) {
                    Float($0) / 32768
                }
            default:
                interleave(planes: buffers.map { UnsafePointer($0.mData!.assumingMemoryBound(to: Int32.self)) }, frames: frames) {
                    Float($0) / 2_147_483_648
                }
            }
        return data.map { ($0, UInt32(buffers.count)) }
    }

    /// A mono f32 buffer (the standard deinterleaved format) holding `samples`.
    static func monoBuffer(_ samples: [Float], rate: Double) -> AVAudioPCMBuffer? {
        guard !samples.isEmpty, let format = AVAudioFormat(standardFormatWithSampleRate: rate, channels: 1),
            let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: AVAudioFrameCount(samples.count)),
            let channel = buffer.floatChannelData?[0]
        else { return nil }
        buffer.frameLength = AVAudioFrameCount(samples.count)
        samples.withUnsafeBufferPointer { channel.update(from: $0.baseAddress!, count: samples.count) }
        return buffer
    }

    private static func interleave<S>(planes: [UnsafePointer<S>], frames: Int, convert: (S) -> Float) -> Data {
        let channels = planes.count
        var data = Data(count: frames * channels * MemoryLayout<Float>.size)
        data.withUnsafeMutableBytes { raw in
            let out = raw.bindMemory(to: Float.self)
            for frame in 0..<frames {
                for channel in 0..<channels {
                    out[frame * channels + channel] = convert(planes[channel][frame])
                }
            }
        }
        return data
    }

    private static func convertInterleaved<S>(_ samples: UnsafePointer<S>, count: Int, convert: (S) -> Float) -> Data {
        var data = Data(count: count * MemoryLayout<Float>.size)
        data.withUnsafeMutableBytes { raw in
            let out = raw.bindMemory(to: Float.self)
            for index in 0..<count {
                out[index] = convert(samples[index])
            }
        }
        return data
    }
}
