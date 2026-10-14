import AVFoundation
import Foundation
import Observation

/// Plays 30-second track previews, one at a time.
@Observable
final class PreviewPlayer {
    /// Identifies what is playing (a track identity key or a candidate id).
    private(set) var currentID: String?
    private(set) var isPlaying = false
    /// 0…1 through the preview.
    private(set) var progress: Double = 0

    @ObservationIgnored private var player: AVPlayer?
    @ObservationIgnored private var timeObserver: Any?
    @ObservationIgnored private var endObserver: NSObjectProtocol?

    func isPlaying(_ id: String) -> Bool {
        isPlaying && currentID == id
    }

    /// Plays `url` for `id`, or pauses/resumes when `id` is already loaded.
    func toggle(id: String, url: URL) {
        if currentID == id, let player {
            if isPlaying {
                player.pause()
                isPlaying = false
            } else {
                if progress >= 1 { player.seek(to: .zero) }
                player.play()
                isPlaying = true
            }
            return
        }
        stop()
        let item = AVPlayerItem(url: url)
        let player = AVPlayer(playerItem: item)
        self.player = player
        currentID = id
        progress = 0
        timeObserver = player.addPeriodicTimeObserver(forInterval: CMTime(value: 1, timescale: 10), queue: .main) {
            [weak self] time in
            MainActor.assumeIsolated {
                guard let self, let duration = self.player?.currentItem?.duration, duration.isNumeric, duration.seconds > 0
                else { return }
                self.progress = min(max(time.seconds / duration.seconds, 0), 1)
            }
        }
        endObserver = NotificationCenter.default.addObserver(
            forName: AVPlayerItem.didPlayToEndTimeNotification, object: item, queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.isPlaying = false
                self?.progress = 1
            }
        }
        player.play()
        isPlaying = true
    }

    func stop() {
        if let timeObserver { player?.removeTimeObserver(timeObserver) }
        if let endObserver { NotificationCenter.default.removeObserver(endObserver) }
        timeObserver = nil
        endObserver = nil
        player?.pause()
        player = nil
        currentID = nil
        isPlaying = false
        progress = 0
    }
}
