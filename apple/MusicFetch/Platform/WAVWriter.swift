import Foundation

/// Streams 16-bit PCM into a canonical RIFF/WAVE file and patches the sizes on finish.
nonisolated final class WAVWriter {
    enum WriteError: Error {
        case tooLarge
    }

    private let handle: FileHandle
    private let sampleRate: UInt32
    private let channels: UInt16
    private(set) var dataBytes: UInt64 = 0

    /// Creates (or truncates) `url` and writes a placeholder header.
    init(url: URL, sampleRate: UInt32, channels: UInt16) throws {
        FileManager.default.createFile(atPath: url.path, contents: nil)
        handle = try FileHandle(forWritingTo: url)
        self.sampleRate = sampleRate
        self.channels = channels
        try handle.write(contentsOf: Self.header(sampleRate: sampleRate, channels: channels, dataBytes: 0))
    }

    deinit {
        try? handle.close()
    }

    /// Appends little-endian 16-bit samples.
    func append(_ bytes: UnsafeRawBufferPointer) throws {
        guard !bytes.isEmpty else { return }
        dataBytes += UInt64(bytes.count)
        guard dataBytes <= UInt64(UInt32.max) - 36 else { throw WriteError.tooLarge }
        try handle.write(contentsOf: Data(bytes))
    }

    /// Writes the final sizes and closes the file.
    func finish() throws {
        try handle.seek(toOffset: 0)
        try handle.write(contentsOf: Self.header(sampleRate: sampleRate, channels: channels, dataBytes: UInt32(dataBytes)))
        try handle.close()
    }

    /// The 44-byte header of a 16-bit PCM WAVE file.
    static func header(sampleRate: UInt32, channels: UInt16, dataBytes: UInt32) -> Data {
        var data = Data(capacity: 44)
        func append<T: FixedWidthInteger>(_ value: T) {
            withUnsafeBytes(of: value.littleEndian) { data.append(contentsOf: $0) }
        }
        let bitsPerSample: UInt16 = 16
        let blockAlign = channels * bitsPerSample / 8
        data.append(contentsOf: Array("RIFF".utf8))
        append(UInt32(36) &+ dataBytes)
        data.append(contentsOf: Array("WAVE".utf8))
        data.append(contentsOf: Array("fmt ".utf8))
        append(UInt32(16))
        append(UInt16(1))  // PCM
        append(channels)
        append(sampleRate)
        append(sampleRate * UInt32(blockAlign))
        append(blockAlign)
        append(bitsPerSample)
        data.append(contentsOf: Array("data".utf8))
        append(dataBytes)
        return data
    }
}
