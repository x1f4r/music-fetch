// Renders the macOS app icon set from the full-bleed master artwork.
//
//   swift assets/icon/make-macos-icon.swift <master-1024.png> <AppIcon.appiconset>
//
// macOS 26 places icons that do not follow the system shape into a gray container, so
// the artwork is masked to Apple's icon grid: an 824 × 824 body centered in the
// 1024 canvas with continuous corners, a subtle drop shadow and transparency outside.

import AppKit
import SwiftUI

let arguments = CommandLine.arguments
guard arguments.count == 3 else {
    FileHandle.standardError.write(Data("usage: make-macos-icon.swift <master.png> <AppIcon.appiconset>\n".utf8))
    exit(2)
}
let masterURL = URL(filePath: arguments[1])
let outputURL = URL(filePath: arguments[2], directoryHint: .isDirectory)

guard let source = NSImage(contentsOf: masterURL),
    let master = source.cgImage(forProposedRect: nil, context: nil, hints: nil)
else {
    FileHandle.standardError.write(Data("cannot read \(masterURL.lastPathComponent)\n".utf8))
    exit(1)
}

let canvas: CGFloat = 1024
let body: CGFloat = 824
let cornerRadius: CGFloat = 185.4
let bodyRect = CGRect(x: (canvas - body) / 2, y: (canvas - body) / 2, width: body, height: body)
let shape = RoundedRectangle(cornerRadius: cornerRadius, style: .continuous).path(in: bodyRect).cgPath

func render(pixels: Int) -> CGImage {
    let space = CGColorSpace(name: CGColorSpace.sRGB)!
    let context = CGContext(
        data: nil, width: pixels, height: pixels, bitsPerComponent: 8, bytesPerRow: 0, space: space,
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)!
    context.interpolationQuality = .high
    let scale = CGFloat(pixels) / canvas
    context.scaleBy(x: scale, y: scale)

    // Shadow: cast by the body shape only, then the artwork is clipped on top.
    context.saveGState()
    context.setShadow(offset: CGSize(width: 0, height: -10), blur: 22, color: CGColor(gray: 0, alpha: 0.32))
    context.addPath(shape)
    context.setFillColor(CGColor(gray: 0, alpha: 1))
    context.fillPath()
    context.restoreGState()

    context.saveGState()
    context.addPath(shape)
    context.clip()
    context.draw(master, in: bodyRect)
    context.restoreGState()
    return context.makeImage()!
}

func write(_ image: CGImage, to url: URL) throws {
    let rep = NSBitmapImageRep(cgImage: image)
    guard let data = rep.representation(using: .png, properties: [:]) else {
        throw CocoaError(.fileWriteUnknown)
    }
    try data.write(to: url)
}

try FileManager.default.createDirectory(at: outputURL, withIntermediateDirectories: true)

struct Entry {
    let points: Int
    let scale: Int
    var pixels: Int { points * scale }
    var filename: String { "icon_\(points)x\(points)\(scale == 2 ? "@2x" : "").png" }
}

let entries = [16, 32, 128, 256, 512].flatMap { [Entry(points: $0, scale: 1), Entry(points: $0, scale: 2)] }
var images: [[String: String]] = []
for entry in entries {
    try write(render(pixels: entry.pixels), to: outputURL.appending(path: entry.filename))
    images.append([
        "filename": entry.filename,
        "idiom": "mac",
        "scale": "\(entry.scale)x",
        "size": "\(entry.points)x\(entry.points)",
    ])
}

let contents: [String: Any] = ["images": images, "info": ["author": "xcode", "version": 1]]
let json = try JSONSerialization.data(withJSONObject: contents, options: [.prettyPrinted, .sortedKeys])
try json.write(to: outputURL.appending(path: "Contents.json"))
print("wrote \(entries.count) icons to \(outputURL.lastPathComponent)")
