import AVFoundation
import AppKit
import Foundation
import MusicFetchCore
import Observation
import SwiftUI

/// The Listen stage: live sessions (microphone, this Mac, hum) and link or file
/// analyses until they have something to show (docs/design/DESIGN.md §6.1).
@Observable
final class ListenModel {
    enum Stage: Equatable {
        case idle
        /// First "This Mac": explain before the system prompt.
        case systemAudioPrePrompt
        case microphoneDenied
        /// Capturing.
        case listening
        /// Capture ended; the core is matching.
        case matching
        /// A link or file analysis without anything to list yet.
        case link
        case noMatch(NoMatchReason)
        case failed(ErrorCode, String?)
        /// No connection: the capture was saved and is identified once online.
        case offline
    }

    /// Transient state-line notices while capturing.
    enum StageNotice: Equatable {
        case moveCloser
        case keepGoing
        case captureSilent
        case rateLimited(until: Date)
    }

    /// Buttons in the action row (§5.4).
    enum Action: Hashable {
        case tryAgain, humIt, typeLyric, useMicrophone, continueSystemAudio, openMicrophoneSettings, stop, dismiss
    }

    private(set) var stage: Stage = .idle
    /// The selected source toggle (persisted).
    var source: ListenSource {
        didSet { if !app.isDemo { UserDefaults.standard.set(source.rawValue, forKey: PreferenceKey.listenSource) } }
    }
    /// The source of the running (or last) live session.
    private(set) var runningSource: ListenSource?
    private(set) var impression = false
    private(set) var notice: StageNotice?
    private(set) var startedAt: Date?
    /// Whole seconds of voiced pitch so far (coarse, for accessibility and the ring).
    private(set) var voicedSeconds = 0
    var isDropTargeted = false
    /// A link or file analysis shown on the stage.
    private(set) var linkSession: SessionModel?
    /// Link progress appears only when resolving takes longer than 300 ms.
    private(set) var linkRevealed = false
    /// The current live session.
    private(set) var liveSession: SessionModel?
    /// The result, candidates or mix that replaces the stage.
    var shown: SessionModel?

    /// Level and pitch for the drawing code (not observed).
    @ObservationIgnored let signal = LiveSignal()

    @ObservationIgnored private unowned let app: AppModel
    @ObservationIgnored private var handle: LiveSessionHandle?
    @ObservationIgnored private var capture: AudioCapture?
    @ObservationIgnored private var consumer: Task<Void, Never>?
    @ObservationIgnored private var lastInput: (input: String, mode: AnalyzeMode)?
    @ObservationIgnored private var lastAnnouncedSegments = 0

    init(app: AppModel) {
        self.app = app
        let stored = UserDefaults.standard.string(forKey: PreferenceKey.listenSource)
        source = stored.flatMap(ListenSource.init(rawValue:)) ?? .microphone
    }

    // MARK: - Derived state

    var isCapturing: Bool { stage == .listening }
    var isBusy: Bool { stage == .listening || stage == .matching || stage == .link }
    var isIdleStage: Bool { shown == nil && !isBusy }

    /// Text under the button (§7).
    var stateLine: String {
        switch stage {
        case .idle:
            return isDropTargeted ? Copy.drop : Copy.idle
        case .systemAudioPrePrompt:
            return Copy.systemAudioPrePrompt
        case .microphoneDenied:
            return Copy.microphoneOff
        case .listening:
            switch notice {
            case .captureSilent: return Copy.captureSilent
            case .rateLimited(let until): return Copy.rateLimited(seconds: max(Int(until.timeIntervalSinceNow.rounded(.up)), 0))
            case .moveCloser: return Copy.moveCloser
            case .keepGoing: return Copy.keepGoing
            case nil:
                if impression { return runningSource == .hum && voicedSeconds == 0 ? Copy.humStart : Copy.keepGoing }
                return Copy.listening
            }
        case .matching:
            return Copy.matching
        case .link:
            switch linkSession?.phase {
            case .resolving, nil: return Copy.resolving
            case .downloading: return Copy.downloading
            case .decoding, .segmenting: return Copy.preparing
            default: return Copy.matching
            }
        case .noMatch(let reason):
            return Copy.noMatch(reason)
        case .failed(let code, let message):
            return Copy.failure(code, message: message)
        case .offline:
            return Copy.offline
        }
    }

    var isStateLineActive: Bool {
        switch stage {
        case .idle: isDropTargeted
        default: true
        }
    }

    /// Whether the state line carries a small progress indicator.
    var showsSpinner: Bool {
        stage == .matching || (stage == .link && linkSession?.phase == .recognizing)
    }

    /// The action row, or nil for the source toggles.
    var actions: [Action]? {
        switch stage {
        case .idle, .matching:
            return nil
        case .listening:
            switch notice {
            case .captureSilent: return [.useMicrophone]
            case .rateLimited: return [.stop]
            default: return nil
            }
        case .systemAudioPrePrompt:
            return [.continueSystemAudio, .useMicrophone]
        case .microphoneDenied:
            return [.openMicrophoneSettings]
        case .link:
            return [.stop]
        case .noMatch(let reason):
            switch reason {
            case .notFound: return [.tryAgain, .humIt, .typeLyric]
            case .tooQuiet, .noisy, .mostlySpeech: return [.tryAgain, .humIt]
            case .tooShort: return [.tryAgain]
            case .captureSilent: return [.useMicrophone]
            }
        case .failed:
            return [.tryAgain]
        case .offline:
            return [.dismiss]
        }
    }

    /// Accessibility value of the listen button.
    var buttonValue: String {
        switch stage {
        case .listening: impression ? Copy.humming : Copy.listening
        case .matching, .link: Copy.matching
        default: Copy.idleValue
        }
    }

    /// Hum ring progress: voiced seconds over 12 (guidance only).
    var ringProgress: Double {
        min(signal.voicedSeconds / 12, 1)
    }

    // MARK: - Listening

    /// The big button and ⌘L: start with the selected source, or stop.
    func toggle() {
        switch stage {
        case .listening: stop()
        case .matching: break
        case .link: linkSession?.cancel()
        default: start(source)
        }
    }

    func start(_ source: ListenSource) {
        guard !isBusy || stage == .link else { return }
        self.source = source
        switch source {
        case .thisMac:
            if !UserDefaults.standard.bool(forKey: PreferenceKey.systemAudioExplained), !app.isDemo {
                shown = nil
                stage = .systemAudioPrePrompt
                return
            }
        case .microphone, .hum:
            if !app.isDemo {
                switch MicrophoneCapture.authorization {
                case .notDetermined:
                    app.presentMicrophonePrePrompt(for: source)
                    return
                case .denied, .restricted:
                    shown = nil
                    stage = .microphoneDenied
                    return
                default:
                    break
                }
            }
        }
        begin(source)
    }

    /// After the microphone pre-prompt: ask the system, then start or show the banner.
    func requestMicrophone(then source: ListenSource) async {
        if await MicrophoneCapture.requestAccess() {
            begin(source)
        } else {
            shown = nil
            stage = .microphoneDenied
        }
    }

    private func begin(_ source: ListenSource) {
        guard let engine = app.engine else { return }
        if stage == .link { detachLink() }
        app.preview.stop()
        let bridge = ObserverBridge()
        let handle: LiveSessionHandle
        do {
            handle = try engine.startListening(
                source: source.capture, mode: source.mode, options: Preferences.listenOptions(), observer: bridge)
        } catch {
            bridge.close()
            fail(error)
            return
        }
        let model = SessionModel(id: handle.sessionID, kind: source == .hum ? .hum : (source == .thisMac ? .systemAudio : .listen),
                                 isRunning: true)
        model.origin = source
        self.handle = handle
        liveSession = model
        shown = nil
        stage = .listening
        runningSource = source
        impression = source == .hum
        notice = nil
        voicedSeconds = 0
        startedAt = .now
        signal.reset()
        announce(source == .hum ? Copy.humStart : Copy.listening)

        consumer?.cancel()
        consumer = Task { [weak self] in
            for await event in bridge.events {
                guard let self else { return }
                model.apply(event)
                self.handleLive(event, model: model)
            }
        }

        let capture: AudioCapture =
            if app.isDemo { NullCapture() } else if source == .thisMac { SystemAudioCapture() } else { MicrophoneCapture() }
        self.capture = capture
        Task {
            do {
                try await offMain { try capture.start { data, rate, channels in handle.pushPcm(data, sampleRate: rate, channels: channels) } }
            } catch {
                Log.capture.error("Capture failed to start: \(error.localizedDescription, privacy: .public)")
                guard self.handle === handle else { return }
                capture.stop()
                handle.cancel()
                self.handle = nil
                self.stage = .failed(.platform, nil)
            }
        }
    }

    /// The user stopped capture: analyze what was captured.
    func stop() {
        guard stage == .listening else { return }
        stopCapture()
        handle?.finish()
        signal.freeze()
        stage = .matching
    }

    /// Abandons the live session (Esc while matching, leaving the stage).
    func cancel() {
        guard stage == .listening || stage == .matching else { return }
        stopCapture()
        handle?.cancel()
        handle = nil
        stage = .idle
    }

    /// Esc: stops capture like a second click; while matching it abandons the session.
    func escape() {
        switch stage {
        case .listening: stop()
        case .matching: cancel()
        default: break
        }
    }

    func retryAsHum() {
        start(.hum)
    }

    /// Continue after the system-audio explanation.
    func continueSystemAudio() {
        UserDefaults.standard.set(true, forKey: PreferenceKey.systemAudioExplained)
        begin(.thisMac)
    }

    func useMicrophone() {
        if stage == .listening { cancel() }
        stage = .idle
        source = .microphone
        start(.microphone)
    }

    func perform(_ action: Action) {
        switch action {
        case .tryAgain:
            if case .failed = stage, let lastInput, runningSource == nil {
                analyze(lastInput.input, mode: lastInput.mode)
            } else {
                start(runningSource ?? source)
            }
        case .humIt: retryAsHum()
        case .typeLyric: app.presentLyricSearch()
        case .useMicrophone: useMicrophone()
        case .continueSystemAudio: continueSystemAudio()
        case .openMicrophoneSettings: app.openPrivacySettings(.microphone)
        case .stop:
            if stage == .link { linkSession?.cancel() } else { cancel() }
        case .dismiss:
            stage = .idle
        }
    }

    /// Back to the idle stage (Esc on a result, ⌘1).
    func showStage() {
        app.preview.stop()
        shown = nil
        switch stage {
        case .noMatch, .failed, .offline, .systemAudioPrePrompt, .microphoneDenied: stage = .idle
        default: break
        }
    }

    /// Puts the stage into a static state (demo screenshots only).
    func showForDemo(_ stage: Stage) {
        shown = nil
        self.stage = stage
    }

    private func stopCapture() {
        capture?.stop()
        capture = nil
    }

    private func handleLive(_ event: SessionEvent, model: SessionModel) {
        switch event {
        case .level(let rms, _):
            signal.push(level: rms)
        case .pitch(let frames):
            signal.push(frames: frames)
            let seconds = Int(signal.voicedSeconds)
            if seconds != voicedSeconds { voicedSeconds = seconds }
        case .notice(let notice):
            handleNotice(notice)
        case .phase(let phase, _):
            if phase != .listening, stage == .listening {
                stopCapture()
                signal.freeze()
                stage = .matching
            }
        case .finished(let outcome):
            stopCapture()
            handle = nil
            model.humTrace = signal.history.map(\.midi)
            model.humSeconds = Int(signal.voicedSeconds.rounded())
            switch outcome {
            case .matched(let track):
                stage = .idle
                shown = model
                announce(Copy.found(track))
                app.sessionFinished(model)
            case .candidates:
                stage = .idle
                shown = model
                announce(Copy.possibleMatches(model.candidates))
                app.sessionFinished(model)
            case .noMatch(let reason):
                stage = .noMatch(reason)
                announce(Copy.noMatchAnnouncement(reason))
                app.sessionFinished(model)
            case .waiting:
                // Saved without a connection; WaitingSessions identifies it once online.
                stage = .offline
                announce(Copy.offline)
                app.sessionFinished(model)
            case .completed:
                stage = .idle
            }
        case .failed(let code, let message):
            stopCapture()
            handle = nil
            stage = .failed(code, message)
            announce(Copy.failure(code, message: message))
        case .canceled:
            stopCapture()
            handle = nil
            if stage == .listening || stage == .matching { stage = .idle }
        default:
            break
        }
    }

    private func handleNotice(_ notice: Notice) {
        switch notice {
        case .moveCloser:
            // Moving closer means nothing when listening to this Mac.
            guard runningSource != .thisMac else { return }
            setNotice(.moveCloser, announcement: Copy.moveCloser)
        case .impressionDetected:
            withAnimation(app.motion(.hero)) { impression = true }
            self.notice = nil
            announce(Copy.keepGoing)
        case .keepGoing:
            setNotice(.keepGoing, announcement: Copy.keepGoing)
        case .rateLimited(let retryInMs):
            setNotice(.rateLimited(until: .now.addingTimeInterval(Double(retryInMs) / 1000)), announcement: nil)
        case .captureSilent:
            setNotice(.captureSilent, announcement: Copy.captureSilent)
        case .melodyIndexMissing:
            app.promptForMelodyLibrary()
        case .providerUnavailable(let provider, _):
            Log.engine.info("Provider unavailable: \(provider, privacy: .public)")
        }
    }

    private func setNotice(_ notice: StageNotice, announcement: String?) {
        self.notice = notice
        if let announcement { announce(announcement) }
        if case .moveCloser = notice {
            // "Move closer" shows for 6 s.
            Task { [weak self] in
                try? await Task.sleep(for: .seconds(6))
                if self?.notice == .moveCloser { self?.notice = nil }
            }
        }
    }

    // MARK: - Links and files

    /// Identifies a link or file path on the stage.
    func analyze(_ input: String, mode: AnalyzeMode = .auto) {
        guard let engine = app.engine else { return }
        if stage == .listening || stage == .matching { cancel() }
        if stage == .link { detachLink() }
        app.preview.stop()
        lastInput = (input, mode)
        runningSource = nil
        let bridge = ObserverBridge()
        let handle: AnalysisSessionHandle
        do {
            handle = try engine.analyze(input: input, mode: mode, observer: bridge)
        } catch {
            bridge.close()
            shown = nil
            fail(error)
            return
        }
        let isFile = input.hasPrefix("/")
        let model = SessionModel(id: handle.sessionID, kind: isFile ? .file : .link, isRunning: true)
        model.mode = mode
        model.onEvent = { [weak self] model, event in self?.handleLink(event, model: model) }
        app.register(model)
        model.attach(handle, events: bridge.events)
        linkSession = model
        linkRevealed = false
        lastAnnouncedSegments = 0
        shown = nil
        stage = .link
        Task { [weak self] in
            try? await Task.sleep(for: .milliseconds(300))
            guard let self, self.linkSession === model else { return }
            self.linkRevealed = true
        }
    }

    /// Leaves a running analysis in the background (it lands in the library).
    private func detachLink() {
        linkSession?.onEvent = { [weak app] model, event in app?.backgroundEvent(event, model: model) }
        linkSession = nil
        stage = .idle
    }

    private func handleLink(_ event: SessionEvent, model: SessionModel) {
        guard linkSession === model else { return }
        switch model.presentation {
        case .timeline, .collection:
            if shown !== model {
                withAnimation(app.motion(.hero)) {
                    shown = model
                    stage = .idle
                }
            }
            announceProgress(model)
        default:
            break
        }
        switch event {
        case .finished:
            linkSession = nil
            switch model.presentation {
            case .result:
                stage = .idle
                withAnimation(app.motion(.hero)) { shown = model }
                if let track = model.track { announce(Copy.found(track)) }
            case .timeline, .collection:
                stage = .idle
                announce(model.statusText)
            default:
                stage = .noMatch(.notFound)
                announce(Copy.noMatchAnnouncement(.notFound))
            }
            app.sessionFinished(model)
        case .failed(let code, let message):
            linkSession = nil
            if shown !== model { stage = .failed(code, message) }
            announce(Copy.failure(code, message: message))
            app.sessionFinished(model)
        case .canceled:
            linkSession = nil
            if shown !== model { stage = .idle }
            app.sessionFinished(model)
        default:
            break
        }
    }

    /// "‹n› of ‹m› identified" every 5 new segments while a mix runs.
    private func announceProgress(_ model: SessionModel) {
        let count = model.identifiedCount
        guard model.isRunning, count >= lastAnnouncedSegments + 5 else { return }
        lastAnnouncedSegments = count
        announce(Copy.identifiedProgress(count))
    }

    private func fail(_ error: Error) {
        if let error = error as? MfError {
            stage = .failed(error.code, error.message)
        } else {
            stage = .failed(.internal, nil)
        }
        announce(stateLine)
    }

    private func announce(_ text: String) {
        AccessibilityNotification.Announcement(text).post()
    }
}

/// Capture stand-in for the demo engine: produces nothing.
nonisolated final class NullCapture: AudioCapture {
    func start(handler: @escaping PCMHandler) throws {}
    func stop() {}
}
