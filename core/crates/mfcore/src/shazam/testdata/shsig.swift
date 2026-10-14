// Test oracle: runs Apple's SHSignatureGenerator on a WAV file and writes the
// signature's dataRepresentation (Apple's 12-byte wrapper + inner signature).
//
// Usage: shsig <input.wav> <output.bin>

import AVFoundation
import Foundation
import ShazamKit

let args = CommandLine.arguments
guard args.count == 3 else {
    FileHandle.standardError.write(Data("usage: shsig <input.wav> <output.bin>\n".utf8))
    exit(2)
}

do {
    let file = try AVAudioFile(forReading: URL(fileURLWithPath: args[1]))
    let format = file.processingFormat
    guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: AVAudioFrameCount(file.length)) else {
        throw NSError(domain: "shsig", code: 1, userInfo: [NSLocalizedDescriptionKey: "cannot allocate buffer"])
    }
    try file.read(into: buffer)
    let generator = SHSignatureGenerator()
    try generator.append(buffer, at: nil)
    let signature = generator.signature()
    try signature.dataRepresentation.write(to: URL(fileURLWithPath: args[2]))
} catch {
    FileHandle.standardError.write(Data("shsig: \(error)\n".utf8))
    exit(1)
}
