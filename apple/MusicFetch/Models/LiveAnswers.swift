import Foundation
import MusicFetchCore

/// What the Listen stage shows while someone hums: the progressive melody search's
/// provisional candidates (`Candidates { is_final: false }`, docs/CORE_API.md "Humming,
/// whistling, singing"), reduced to what is honest to show.
///
/// Only Strong and Possible candidates appear, at most two. Long shots are right about
/// one time in fourteen, so "Sounds like …" would mislead; until something better turns
/// up the stage shows nothing and the state line keeps saying "Keep going".
nonisolated struct LiveAnswers: Equatable, Sendable {
    /// Best first; one or two Strong or Possible candidates, or none.
    private(set) var candidates: [Candidate] = []

    static let limit = 2

    init() {}

    /// Keeps the leading candidates that are at least Possible.
    init(list: [Candidate]) {
        candidates = Array(list.prefix(while: { $0.tier != .longShot }).prefix(Self.limit))
    }

    var isEmpty: Bool { candidates.isEmpty }

    /// The candidate in a row, or nil when the row is empty.
    func candidate(at row: Int) -> Candidate? {
        row < candidates.count ? candidates[row] : nil
    }

    /// Whether `list` shows anything different from this one: the songs, their order or
    /// their tiers. Score changes alone don't redraw the rows.
    func differs(from other: LiveAnswers) -> Bool {
        candidates.map(\.liveKey) != other.candidates.map(\.liveKey)
    }
}

extension Candidate {
    /// Identity and tier: what the live rows show.
    nonisolated var liveKey: String {
        "\(track.identityKey)|\(tier)"
    }
}

/// How sure the final candidates are, for the candidates screen.
nonisolated enum CandidateConfidence: Equatable, Sendable {
    /// The best answer is Strong.
    case strong
    /// The best answer is Possible.
    case possible
    /// Every answer is a long shot: say so instead of offering a guess as an answer.
    case unsure

    init(_ candidates: [Candidate]) {
        switch candidates.first?.tier {
        case .strong: self = .strong
        case .possible: self = .possible
        case .longShot, nil: self = .unsure
        }
    }
}
