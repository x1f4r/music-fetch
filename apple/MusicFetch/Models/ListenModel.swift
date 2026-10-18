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
        /// First hum on macOS 15 with lyric search on: explain before macOS asks for
        /// speech recognition.
        case speechPrePrompt
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

    /// Buttons in the recovery row under the source toggles (§5.4).
    enum Action: Hashable {
        case tryAgain, humIt, typeLyric, useMicrophone, continueSystemAudio, openMicrophoneSettings, stop, dismiss
        case allowSpeech, skipSpeech
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
    /// Answers of the progressive melody search while someone hums (Strong or Possible
    /// only). Kept while matching; cleared when the session ends.
    private(set) var liveAnswers = LiveAnswers()

    /// Level and pitch for the drawing code (not observed).
    @ObservationIgnored let signal = LiveSignal()

    @ObservationIgnored private unowned let app: AppModel
    @ObservationIgnored private var handle: LiveSessionHandle?
    @ObservationIgnored private var capture: AudioCapture?
    /// On-device recognition of sung words for the running session, when it reads them.
    @ObservationIgnored private var words: SungWordsTranscriber?
    @ObservationIgnored private var consumer: Task<Void, Never>?
    @ObservationIgnored private var lastInput: (input: String, mode: AnalyzeMode)?
    @ObservationIgnored private var lastAnnouncedSegments = 0
    /// A live answer the user clicked: selected when the final list arrives.
    @ObservationIgnored private var chosenAnswer: String?

    init(app: AppModel) {
        self.app = app
        let stored = UserDefaults.standard.string(forKey: PreferenceKey.listenSource)
        source = stored.flatMap(ListenSource.init(rawValue:)) ?? .microphone
    }

    // MARK: - Derived state

    var isCapturing: Bool { stage == .listening }
    var isBusy: Bool { stage == .listening || stage == .matching || stage == .link }
    var isIdleStage: Bool { shown == nil && !isBusy }

    /// The live answers sit in the recovery row's place while humming; actions win.
    var showsLiveAnswers: Bool {
        !liveAnswers.isEmpty && (stage == .listening || stage == .matching) && recoveryActions.isEmpty
    }

    /// Text under the button (§7).
    var stateLine: String {
        switch stage {
        case .idle:
            return isDropTargeted ? Copy.drop : Copy.idle
        case .systemAudioPrePrompt:
            return Copy.systemAudioPrePrompt
        case .speechPrePrompt:
            return Copy.speechPrePrompt
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

    /// The recovery row: what helps after a no-match, an error or a prompt. Empty while
    /// idle; the source toggles above it never leave the stage (§5.4).
    var recoveryActions: [Action] {
        switch stage {
        case .idle, .matching:
            return []
        case .listening:
            switch notice {
            case .captureSilent: return [.useMicrophone]
            case .rateLimited: return [.stop]
            default: return []
            }
        case .systemAudioPrePrompt:
            return [.continueSystemAudio, .useMicrophone]
        case .speechPrePrompt:
            return [.allowSpeech, .skipSpeech]
        case .microphoneDenied:
            return [.openMicrophoneSettings]
        case .link:
            return [.stop]
        case .noMatch(let reason):
            // "Hum it" only offers something new when the last try wasn't a hum.
            let hum: [Action] = runningSource == .hum ? [] : [.humIt]
            switch reason {
            case .notFound: return [.tryAgain] + hum + [.typeLyric]
            case .tooQuiet, .noisy, .mostlySpeech: return [.tryAgain] + hum
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

    /// A source toggle: chooses the method without listening. A finished state (no
    /// match, an error, a prompt) gives way to the idle stage for the new method.
    func select(_ source: ListenSource) {
        guard !isBusy else { return }
        self.source = source
        switch stage {
        case .noMatch, .failed, .offline, .microphoneDenied, .systemAudioPrePrompt, .speechPrePrompt: stage = .idle
        default: break
        }
    }

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
        proceed(source)
    }

    /// After the microphone pre-prompt: ask the system, then start or show the banner.
    func requestMicrophone(then source: ListenSource) async {
        if await MicrophoneCapture.requestAccess() {
            proceed(source)
        } else {
            shown = nil
            stage = .microphoneDenied
        }
    }

    /// The microphone is allowed: explain speech recognition before the first hum when
    /// macOS will ask for it (macOS 15), else start.
    private func proceed(_ source: ListenSource) {
        if source == .hum, app.speech.asksBeforeHumming {
            shown = nil
            stage = .speechPrePrompt
            return
        }
        begin(source)
    }

    /// "Continue" on the speech explanation: macOS asks, then the hum starts either way.
    func allowSpeech() {
        guard stage == .speechPrePrompt else { return }
        Task {
            await app.speech.requestPermission()
            guard stage == .speechPrePrompt else { return }
            begin(.hum)
        }
    }

    /// "Not Now": hum without words; the explanation doesn't come back.
    func skipSpeech() {
        guard stage == .speechPrePrompt else { return }
        app.speech.declinePermission()
        begin(.hum)
    }

    private func begin(_ source: ListenSource) {
        guard let engine = app.engine else { return }
        if stage == .link { detachLink() }
        app.preview.stop()
        // Set before starting, so Try Again after a failed start listens again rather
        // than repeating an earlier link.
        runningSource = source
        let bridge = ObserverBridge()
        // Sung words: the microphone's audio also goes to the on-device recognizer, whose
        // text the engine searches as lyrics. System audio never reads words.
        let readsWords = source != .thisMac && engine.acceptsTranscripts && app.speech.transcribesLive
        if source == .hum, !readsWords { app.speech.installModelIfNeeded() }
        var options = Preferences.listenOptions()
        options.setTranscripts(readsWords)
        let handle: LiveSessionHandle
        do {
            handle = try engine.startListening(source: source.capture, mode: source.mode, options: options, observer: bridge)
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
        liveAnswers = LiveAnswers()
        chosenAnswer = nil
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

        words?.cancel()
        let words: SungWordsTranscriber? =
            readsWords
            ? SungWordsTranscriber(language: app.speech.language, lowPower: app.isLowPower) { [weak handle] heard in
                handle?.pushTranscript(heard)
            }
            : nil
        // A hum reads words from the start; a microphone session once it turns out to be
        // humming (the transcriber keeps the last seconds until then).
        if source == .hum { words?.activate() }
        self.words = words

        let capture: AudioCapture =
            if app.isDemo { NullCapture() } else if source == .thisMac { SystemAudioCapture() } else { MicrophoneCapture() }
        self.capture = capture
        Task {
            do {
                try await offMain {
                    try capture.start { data, rate, channels in
                        handle.pushPcm(data, sampleRate: rate, channels: channels)
                        words?.append(data, sampleRate: rate, channels: channels)
                    }
                }
            } catch {
                Log.capture.error("Capture failed to start: \(error.localizedDescription, privacy: .public)")
                guard self.handle === handle else { return }
                capture.stop()
                handle.cancel()
                self.handle = nil
                self.endWords(canceling: true)
                self.stage = .failed(.platform, nil)
            }
        }
    }

    /// The user stopped capture: analyze what was captured.
    func stop() {
        guard stage == .listening else { return }
        stopCapture()
        handle?.finish()
        endWords(canceling: false)
        signal.freeze()
        stage = .matching
    }

    /// Abandons the live session (Esc while matching, leaving the stage).
    func cancel() {
        guard stage == .listening || stage == .matching else { return }
        stopCapture()
        handle?.cancel()
        handle = nil
        endWords(canceling: true)
        liveAnswers = LiveAnswers()
        stage = .idle
    }

    /// A live answer clicked while humming: stop and select it among the final matches.
    func choose(_ answer: Candidate) {
        guard stage == .listening else { return }
        chosenAnswer = answer.track.identityKey
        stop()
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
        case .allowSpeech: allowSpeech()
        case .skipSpeech: skipSpeech()
        case .tryAgain:
            // A link or file that failed or found nothing is analyzed again; a live
            // session listens again with the same method.
            if runningSource == nil, let lastInput {
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

    /// Back to the idle stage (Esc or "‹" on a result, ⌘1). A mix still being analyzed
    /// carries on in the background and lands in the library.
    func showStage() {
        app.preview.stop()
        if let shown, shown === linkSession { detachLink() }
        shown = nil
        switch stage {
        case .noMatch, .failed, .offline, .systemAudioPrePrompt, .speechPrePrompt, .microphoneDenied: stage = .idle
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

    /// Capture ended: the recognizer's final words follow (the engine waits briefly for
    /// them). Canceling drops them; the session is over or abandoned.
    private func endWords(canceling: Bool) {
        if canceling {
            words?.cancel()
            words = nil
        } else {
            words?.finish()
        }
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
        case .candidates(let list, let isFinal):
            // Interim lists are a quiet preview: never a screen change or an announcement.
            guard !isFinal, stage == .listening || stage == .matching else { break }
            let answers = LiveAnswers(list: list)
            if answers.differs(from: liveAnswers) { liveAnswers = answers }
        case .phase(let phase, _):
            if phase != .listening, stage == .listening {
                // The engine stopped on its own (a clear answer, a pause, the time limit).
                stopCapture()
                endWords(canceling: false)
                signal.freeze()
                stage = .matching
            }
        case .finished(let outcome):
            stopCapture()
            handle = nil
            endWords(canceling: true)
            liveAnswers = LiveAnswers()
            model.humTrace = signal.history.map(\.midi)
            model.humSeconds = Int(signal.voicedSeconds.rounded())
            switch outcome {
            case .matched(let track):
                stage = .idle
                shown = model
                announce(Copy.found(track))
                app.sessionFinished(model)
            case .candidates:
                if let chosen = chosenAnswer, model.candidates.contains(where: { $0.track.identityKey == chosen }) {
                    model.selectedCandidateID = chosen
                }
                chosenAnswer = nil
                stage = .idle
                shown = model
                announce(Copy.candidatesAnnouncement(model.candidates, kind: model.kind))
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
            endWords(canceling: true)
            liveAnswers = LiveAnswers()
            stage = .failed(code, message)
            announce(Copy.failure(code, message: message))
        case .canceled:
            stopCapture()
            handle = nil
            endWords(canceling: true)
            liveAnswers = LiveAnswers()
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
            words?.activate()
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

extension ListenOptions {
    /// Placeholder until the engine's transcript input lands (hum-fusion).
    nonisolated mutating func setTranscripts(_ enabled: Bool) {}
}
