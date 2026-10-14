import Foundation
import MusicFetchCore

/// Formatting for times, sizes and dates.
nonisolated enum Format {
    /// "0:30", "15:05", "1:00:33".
    static func cue(milliseconds: UInt64) -> String {
        let total = Int(milliseconds / 1000)
        let hours = total / 3600
        let minutes = (total % 3600) / 60
        let seconds = total % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, seconds)
        }
        return String(format: "%d:%02d", minutes, seconds)
    }

    /// "15:05 – 19:30".
    static func cueRange(start: UInt64, end: UInt64) -> String {
        "\(cue(milliseconds: start)) – \(cue(milliseconds: end))"
    }

    /// "48 MB", "1.2 GB".
    static func bytes(_ count: UInt64) -> String {
        ByteCountFormatter.string(fromByteCount: Int64(clamping: count), countStyle: .file)
    }

    /// "41,234".
    static func number(_ value: UInt32) -> String {
        Int(value).formatted(.number)
    }

    static func date(milliseconds: Int64) -> Date {
        Date(timeIntervalSince1970: TimeInterval(milliseconds) / 1000)
    }

    /// "14:32" (locale time style).
    static func time(_ date: Date) -> String {
        date.formatted(date: .omitted, time: .shortened)
    }

    /// Library day header: "Today", "Yesterday", "Sunday, October 11" (year only when
    /// not the current year).
    static func dayHeader(_ day: Date, now: Date = .now, calendar: Calendar = .current) -> String {
        if calendar.isDate(day, inSameDayAs: now) { return String(localized: "Today") }
        if let yesterday = calendar.date(byAdding: .day, value: -1, to: now), calendar.isDate(day, inSameDayAs: yesterday) {
            return String(localized: "Yesterday")
        }
        if calendar.component(.year, from: day) == calendar.component(.year, from: now) {
            return day.formatted(.dateTime.weekday(.wide).month(.wide).day())
        }
        return day.formatted(date: .complete, time: .omitted)
    }

    /// Inspector "When": "Today at 14:32" or "Oct 11 at 16:40".
    static func when(_ date: Date, now: Date = .now, calendar: Calendar = .current) -> String {
        let time = time(date)
        if calendar.isDate(date, inSameDayAs: now) { return String(localized: "Today at \(time)") }
        if let yesterday = calendar.date(byAdding: .day, value: -1, to: now), calendar.isDate(date, inSameDayAs: yesterday) {
            return String(localized: "Yesterday at \(time)")
        }
        let day = date.formatted(.dateTime.month(.abbreviated).day())
        return String(localized: "\(day) at \(time)")
    }

    /// "Released" from a melody-index version like "2026.10.1" → "October 1, 2026".
    static func indexDate(_ version: String?) -> String? {
        guard let version else { return nil }
        let parts = version.split(separator: ".").compactMap { Int($0) }
        guard parts.count >= 3, (2000...2100).contains(parts[0]), (1...12).contains(parts[1]), (1...31).contains(parts[2])
        else { return version }
        var components = DateComponents()
        (components.year, components.month, components.day) = (parts[0], parts[1], parts[2])
        guard let date = Calendar(identifier: .gregorian).date(from: components) else { return version }
        return date.formatted(date: .long, time: .omitted)
    }

    /// Host without "www.", e.g. "tiktok.com".
    static func host(_ urlString: String?) -> String? {
        guard let urlString, let host = URL(string: urlString)?.host() else { return nil }
        return host.hasPrefix("www.") ? String(host.dropFirst(4)) : host
    }

    /// "Artist – Title" (what ⌘C copies).
    static func artistTitle(_ track: Track) -> String {
        track.artist.isEmpty ? track.title : "\(track.artist) – \(track.title)"
    }

    /// "Radical Optimism · 2024" (parts only when known).
    static func meta(_ track: Track) -> String? {
        let parts = [track.album, track.releaseYear.map(String.init)].compactMap { $0 }.filter { !$0.isEmpty }
        return parts.isEmpty ? nil : parts.joined(separator: " · ")
    }
}
