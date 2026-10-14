import Foundation

/// Third-party licenses shown in Settings › About.
nonisolated enum Licenses {
    struct Entry: Identifiable, Equatable {
        var title: String
        var summary: String
        var text: String
        var id: String { title }
    }

    /// THIRD_PARTY_NOTICES.md, the bundled media runtime and the engine's Rust crates.
    static var all: [Entry] {
        var entries = notices(bundle: .main)
        entries.append(contentsOf: runtime)
        if let crates = crates(bundle: .main) { entries.append(crates) }
        return entries
    }

    static let acknowledgements = [
        "Shazam-compatible fingerprints are generated on this Mac by a clean-room implementation.",
        "Song metadata, artwork and previews come from Deezer and the iTunes Search API.",
        "Lyrics come from LRCLIB.",
        "Links are read with yt-dlp, run by the bundled Python and QuickJS.",
        "Pitch tracking uses the SwiftF0 model.",
    ]

    /// Sections of a THIRD_PARTY_NOTICES.md: "## Title", prose, then a fenced license.
    static func parseNotices(_ markdown: String) -> [Entry] {
        var entries: [Entry] = []
        var title: String?
        var prose: [String] = []
        var license: [String] = []
        var inFence = false
        let flush = {
            guard let title else { return }
            let summary = prose.joined(separator: " ").replacingOccurrences(of: "`", with: "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            entries.append(Entry(title: title, summary: summary, text: license.joined(separator: "\n")))
        }
        for line in markdown.components(separatedBy: .newlines) {
            if line.hasPrefix("```") {
                inFence.toggle()
                continue
            }
            if inFence {
                license.append(line)
            } else if line.hasPrefix("## ") {
                flush()
                title = String(line.dropFirst(3)).trimmingCharacters(in: .whitespaces)
                prose = []
                license = []
            } else if title != nil, !line.trimmingCharacters(in: .whitespaces).isEmpty {
                prose.append(line.trimmingCharacters(in: .whitespaces))
            }
        }
        flush()
        return entries
    }

    private static func notices(bundle: Bundle) -> [Entry] {
        guard let url = bundle.url(forResource: "THIRD_PARTY_NOTICES", withExtension: "md"),
            let text = try? String(contentsOf: url, encoding: .utf8)
        else { return [] }
        return parseNotices(text)
    }

    private static let runtime: [Entry] = [
        Entry(title: "Python", summary: "The bundled interpreter (python-build-standalone) is distributed under the Python Software Foundation License.", text: ""),
        Entry(title: "yt-dlp", summary: "Released into the public domain under the Unlicense.", text: ""),
        Entry(title: "QuickJS-ng", summary: "Distributed under the MIT License.", text: ""),
    ]

    /// `Contents/Resources/Licenses/rust-crates.json` written by the build script:
    /// `[{"name": "...", "version": "...", "license": "..."}]`.
    private static func crates(bundle: Bundle) -> Entry? {
        guard let url = bundle.resourceURL?.appending(path: "Licenses/rust-crates.json"),
            let data = try? Data(contentsOf: url),
            let list = try? JSONDecoder().decode([Crate].self, from: data), !list.isEmpty
        else { return nil }
        let lines = list.map { "\($0.name) \($0.version) — \($0.license ?? "see crate")" }
        return Entry(
            title: "Rust crates", summary: "The engine is built from these open-source crates.", text: lines.joined(separator: "\n"))
    }

    private struct Crate: Decodable {
        var name: String
        var version: String
        var license: String?
    }
}
