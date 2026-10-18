package app.musicfetch.capture

import android.Manifest
import android.app.Application
import android.app.ForegroundServiceStartNotAllowedException
import android.content.ComponentName
import android.content.Intent
import android.content.pm.PackageManager
import android.media.projection.MediaProjection
import android.media.projection.MediaProjectionManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.service.quicksettings.TileService
import android.util.Log
import androidx.core.content.ContextCompat
import app.musicfetch.appGraph
import app.musicfetch.core.Candidate
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ErrorCode
import app.musicfetch.core.ListenMode
import app.musicfetch.core.ListenOptions
import app.musicfetch.core.LiveSession
import app.musicfetch.core.MfException
import app.musicfetch.core.Notice
import app.musicfetch.core.Phase
import app.musicfetch.core.SessionEvent
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionObserver
import app.musicfetch.core.SpeechTranscript
import app.musicfetch.engine.EngineHost
import app.musicfetch.engine.EngineUnavailableException
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.DevFiles
import app.musicfetch.platform.SettingsStore
import app.musicfetch.platform.SungLyrics
import app.musicfetch.platform.SungTranscript
import app.musicfetch.widget.ListenWidget
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.nio.ByteBuffer

/** The MediaProjection consent for one phone-audio session (Android asks every time). */
class ProjectionGrant(val resultCode: Int, val data: Intent)

/**
 * Runs the app's one live session (docs/APPS.md, `ListenModel`): starts [CaptureService] so
 * the microphone or phone audio may be captured in the background, feeds the engine, turns
 * its events into [state] and [signal], and posts the result notification for listens started
 * from a tile or that finish while the app is in the background.
 *
 * The Listen screen, the capture notification and both Quick Settings tiles all drive and
 * observe this one object.
 */
class ListenController(
    private val app: Application,
    private val scope: CoroutineScope,
    private val engineHost: EngineHost,
    private val settings: SettingsStore,
    /** Sung words of hum and microphone listens (Android 13+ with an on-device recognizer). */
    private val sungLyrics: SungLyrics,
) {
    private val _state = MutableStateFlow(ListenState())
    val state: StateFlow<ListenState> = _state.asStateFlow()

    val signal = LiveSignal()

    private val lock = Any()
    private val main = Handler(Looper.getMainLooper())

    // Guarded by [lock]. [generation] identifies the current attempt so late events and
    // callbacks of an earlier session are ignored.
    private var generation = 0L
    private var pendingGrant: ProjectionGrant? = null
    private var session: LiveSession? = null
    private var capture: PcmSource? = null
    private var projection: MediaProjection? = null
    private var words: SungLyrics.Session? = null

    /** The recognizer finishing after capture; once the session ended, nothing waits for it. */
    private var finishingWords: SungLyrics.Session? = null
    private var service: CaptureService? = null
    private var userCanceled = false

    /** The Listen stage is on screen: tile and widget matches open there instead of notifying. */
    @Volatile
    var stageShown = false

    /** The current phone-audio session replays a development file (no MediaProjection). */
    @Volatile
    internal var replayingPhone = false
        private set

    /** Development builds: a phone-audio replay file is present (see [DevFiles.PHONE_CAPTURE_WAV]). */
    fun canReplayPhone(): Boolean = DevFiles.find(app, DevFiles.PHONE_CAPTURE_WAV) != null

    /**
     * Start listening. Returns false when a session is already running or the capture service
     * could not be started. [grant] is required for [CaptureSource.SYSTEM_AUDIO].
     */
    fun start(
        source: CaptureSource,
        mode: ListenMode,
        origin: ListenOrigin,
        grant: ProjectionGrant? = null,
    ): Boolean {
        val effectiveMode = if (source == CaptureSource.SYSTEM_AUDIO) ListenMode.RECORDING else mode
        synchronized(lock) {
            if (_state.value.isActive) return false
            if (ContextCompat.checkSelfPermission(app, Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
                return false
            }
            val replay = source == CaptureSource.SYSTEM_AUDIO && grant == null && canReplayPhone()
            if (source == CaptureSource.SYSTEM_AUDIO && grant == null && !replay) return false
            replayingPhone = replay
            generation++
            pendingGrant = grant
            userCanceled = false
            signal.reset()
            _state.value = ListenState(
                phase = ListenPhase.Starting,
                source = source,
                mode = effectiveMode,
                origin = origin,
                impression = effectiveMode == ListenMode.HUM,
                startedAtWallMs = System.currentTimeMillis(),
            )
        }
        val intent = Intent(app, CaptureService::class.java).setAction(CaptureService.ACTION_START)
        val started = try {
            ContextCompat.startForegroundService(app, intent)
            true
        } catch (e: IllegalStateException) {
            // ForegroundServiceStartNotAllowedException (API 31+) is an IllegalStateException.
            val notAllowed = Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && e is ForegroundServiceStartNotAllowedException
            Log.w(TAG, "capture service not started (background start allowed: ${!notAllowed})", e)
            false
        } catch (e: SecurityException) {
            Log.w(TAG, "capture service not started", e)
            false
        }
        if (!started) {
            fail(null, ErrorCode.PLATFORM, "Couldn't start listening.")
        }
        refreshTiles()
        return started
    }

    /** The user stopped capture: analyze what was heard. */
    fun stop() {
        val current = synchronized(lock) {
            if (!_state.value.isCapturing) return
            session
        }
        if (current == null) {
            // Still starting: nothing heard yet, so stopping means canceling.
            cancel()
            return
        }
        stopCapture()
        runCatching { current.finish() }
        _state.update { if (it.isCapturing) it.copy(phase = ListenPhase.Matching) else it }
        refreshTiles()
    }

    /** Abandon the session without a result (back gesture, Cancel). */
    fun cancel() {
        val current = synchronized(lock) {
            if (!_state.value.isActive) return
            userCanceled = true
            session
        }
        stopCapture(discardWords = true)
        if (current != null) {
            runCatching { current.cancel() }
            // The engine answers with Canceled, which resets the state.
        } else {
            synchronized(lock) { generation++ }
            finishCapture()
            _state.value = ListenState()
            refreshTiles()
        }
    }

    /** The UI consumed a finished result: back to idle. */
    fun reset() {
        _state.update { if (it.phase is ListenPhase.Done) ListenState(source = it.source, mode = it.mode) else it }
    }

    /** [CaptureService] is in the foreground: obtain the audio source and start the engine session. */
    internal fun onServiceForeground(svc: CaptureService) {
        val (gen, grant, source, mode) = synchronized(lock) {
            service = svc
            val s = _state.value
            if (s.phase !is ListenPhase.Starting) {
                // Canceled while the service was starting.
                main.post { svc.release() }
                return
            }
            Quad(generation, pendingGrant, s.source, s.mode)
        }
        scope.launch(Dispatchers.IO) { begin(gen, svc, grant, source, mode) }
    }

    /** The system stopped the service or the projection (e.g. the screen locked during phone-audio capture). */
    internal fun onServiceStopped(svc: CaptureService) {
        val stillOurs = synchronized(lock) {
            if (service !== svc) return
            service = null
            _state.value.isCapturing
        }
        if (stillOurs) stop()
    }

    private suspend fun begin(gen: Long, svc: CaptureService, grant: ProjectionGrant?, source: CaptureSource, mode: ListenMode) {
        val engine = try {
            engineHost.engine()
        } catch (e: EngineUnavailableException) {
            Log.e(TAG, "engine unavailable", e)
            fail(gen, ErrorCode.INTERNAL, "Music Fetch couldn't start. Restart the app.")
            return
        }
        val audio = try {
            openAudio(svc, source, grant)
        } catch (e: SecurityException) {
            Log.w(TAG, "capture not permitted", e)
            null
        }
        if (audio == null) {
            val message = if (source == CaptureSource.SYSTEM_AUDIO) {
                "Couldn't capture phone audio."
            } else {
                "The microphone is not available."
            }
            fail(gen, ErrorCode.PLATFORM, message)
            return
        }
        // Microphone listens may turn into humming: their progressive melody search shows its
        // answer so far under the listening UI (ListenState.preview), and the on-device recognizer
        // reads sung words from the same audio (docs/CORE_API.md, "Sung words"). Phone audio
        // never hums.
        val prefs = settings.current()
        val microphone = source == CaptureSource.MICROPHONE
        val language = sungLyrics.language()
        val listenForWords = microphone && prefs.sungLyrics && sungLyrics.isAvailable() && sungLyrics.mayHaveSpeechPack()
        val options = ListenOptions(autoStop = prefs.autoStop, liveCandidates = microphone, transcripts = listenForWords)
        val live = try {
            engine.startListeningWith(source, mode, options, Observer(gen))
        } catch (e: MfException) {
            Log.w(TAG, "startListeningWith failed", e)
            audio.stop()
            fail(gen, ErrorCode.INTERNAL, e.message ?: "Couldn't start listening.")
            return
        }
        // A hum is read from its first second; a microphone listen keeps its audio until it
        // turns out to be singing (Notice.ImpressionDetected).
        val tap = if (listenForWords) sungLyrics.prepare(language) { transcript -> pushWords(gen, live, transcript) } else null
        val keep = synchronized(lock) {
            if (gen != generation || !_state.value.isCapturing) {
                false
            } else {
                session = live
                capture = audio
                words = tap
                true
            }
        }
        if (!keep) {
            audio.stop()
            tap?.cancel()
            runCatching { live.cancel() }
            return
        }
        if (mode == ListenMode.HUM) tap?.begin()
        Log.i(TAG, "capturing ${audio.description} at ${audio.sampleRate} Hz × ${audio.channels}${if (tap != null) ", reading words" else ""}")
        audio.start(
            sink = { buffer: ByteBuffer, rate: Int, channels: Int ->
                live.pushPcm(buffer, rate.toUInt(), channels.toUInt())
                tap?.write(buffer, rate, channels)
            },
            onError = { message ->
                Log.w(TAG, "capture error: $message")
                // Analyze what was captured so far.
                stop()
            },
        )
    }

    private fun openAudio(svc: CaptureService, source: CaptureSource, grant: ProjectionGrant?): PcmSource? {
        val devFile = if (source == CaptureSource.MICROPHONE) DevFiles.CAPTURE_WAV else DevFiles.PHONE_CAPTURE_WAV
        DevFiles.find(app, devFile)?.let { replay -> WavReplay.open(replay)?.let { return it } }
        if (ContextCompat.checkSelfPermission(app, Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            return null
        }
        return when (source) {
            CaptureSource.MICROPHONE -> AudioCapture.microphone(svc)
            CaptureSource.SYSTEM_AUDIO -> {
                val g = grant ?: return null
                val manager = svc.getSystemService(MediaProjectionManager::class.java) ?: return null
                val mp = manager.getMediaProjection(g.resultCode, g.data) ?: return null
                mp.registerCallback(
                    object : MediaProjection.Callback() {
                        override fun onStop() {
                            Log.i(TAG, "media projection stopped")
                            stop()
                        }
                    },
                    main,
                )
                synchronized(lock) { projection = mp }
                AudioCapture.playback(mp) ?: run {
                    mp.stop()
                    synchronized(lock) { projection = null }
                    null
                }
            }
        }
    }

    private inner class Observer(private val gen: Long) : SessionObserver {
        override fun onEvent(event: SessionEvent) {
            if (synchronized(lock) { gen != generation }) return
            handle(gen, event)
        }
    }

    private fun handle(gen: Long, event: SessionEvent) {
        when (event) {
            is SessionEvent.Started -> {
                _state.update {
                    it.copy(
                        sessionId = event.sessionId,
                        phase = if (it.phase == ListenPhase.Starting) ListenPhase.Listening else it.phase,
                        impression = it.impression || event.kind == SessionKind.HUM,
                    )
                }
                refreshTiles()
            }
            is SessionEvent.Phase -> if (event.phase != Phase.LISTENING) {
                // The engine stopped capturing on its own (match pending, hum auto-stop, 30 s).
                stopCapture()
                _state.update { if (it.isCapturing) it.copy(phase = ListenPhase.Matching) else it }
                refreshTiles()
            }
            is SessionEvent.Level -> signal.onLevel(event.rms)
            is SessionEvent.Pitch -> signal.pitch.add(event.frames)
            is SessionEvent.Notice -> onNotice(event.notice)
            is SessionEvent.Finished -> {
                val result = listenResult(event.outcome, _state.value.sessionId, lastCandidates)
                end(gen, result)
                // Saved without a connection: identified once the phone is online again.
                if (result is ListenResult.Waiting) app.appGraph.waitingRetry.onWaitingSaved()
            }
            is SessionEvent.Candidates -> onCandidates(event.candidates, event.isFinal)
            is SessionEvent.Failed -> end(gen, ListenResult.Failed(_state.value.sessionId, event.code, event.message))
            is SessionEvent.Canceled -> end(gen, null)
            else -> Unit
        }
    }

    /** The final list, or until one arrives the latest interim list. */
    @Volatile
    private var lastCandidates: List<Candidate> = emptyList()

    @Volatile
    private var finalCandidates = false

    /**
     * Interim lists (`is_final = false`) preview the answer while the user hums and while the
     * final pass runs; each replaces the previous one and an empty list clears it. The final
     * list is the result (docs/CORE_API.md, "Humming, whistling, singing").
     */
    private fun onCandidates(candidates: List<Candidate>, isFinal: Boolean) {
        if (isFinal) {
            finalCandidates = true
            lastCandidates = candidates
            return
        }
        if (!finalCandidates) lastCandidates = candidates
        Log.i(TAG, "interim candidates: ${candidates.size}, top ${candidates.firstOrNull()?.tier}")
        _state.update { if (it.isActive) it.copy(preview = candidates) else it }
    }

    private fun onNotice(notice: Notice) {
        when (notice) {
            is Notice.ProviderUnavailable -> Log.i(TAG, "provider unavailable: ${notice.provider}")
            is Notice.MelodyIndexMissing -> _state.update { it.copy(melodyIndexMissing = true) }
            is Notice.ImpressionDetected -> {
                // Singing after all: read its words, from the start of the capture.
                synchronized(lock) { words }?.begin()
                _state.update { it.copy(impression = true, notice = notice) }
            }
            else -> _state.update { it.copy(notice = notice) }
        }
        updateNotification()
    }

    /**
     * Refresh the capture notification. Only while [CaptureService] is in the foreground, so a
     * late engine event can't leave an orphaned "Matching" notification behind.
     */
    private fun updateNotification() {
        synchronized(lock) {
            if (service == null) return
            Notifications.updateListening(app, _state.value)
        }
    }

    /** A terminal event: release capture, publish the result, notify if nobody is looking. */
    private fun end(gen: Long, result: ListenResult?) {
        val canceledByUser = synchronized(lock) {
            if (gen != generation) return
            generation++
            session = null
            userCanceled
        }
        stopCapture(discardWords = true)
        finishCapture()
        lastCandidates = emptyList()
        finalCandidates = false
        val previous = _state.value
        if (result == null) {
            _state.value = ListenState(source = previous.source, mode = previous.mode)
            if (!canceledByUser) Log.i(TAG, "session canceled by the engine")
        } else {
            _state.value = previous.copy(phase = ListenPhase.Done(result), notice = null, preview = emptyList())
            val elapsed = System.currentTimeMillis() - previous.startedAtWallMs
            Log.i(TAG, "${previous.mode} session ended (${result.javaClass.simpleName}) after $elapsed ms")
            onResult(previous, result)
        }
        app.appGraph.notifyLibraryChanged()
        refreshTiles()
    }

    private fun onResult(state: ListenState, result: ListenResult) {
        scope.launch {
            if (result is ListenResult.Matched || result is ListenResult.Candidates) {
                settings.recordSuccessfulListen()
            }
            val prefs = settings.current()
            val visible = app.appGraph.isAppVisible
            // A tile or widget match while the stage is in front opens on the stage instead.
            val answered = result is ListenResult.Matched || result is ListenResult.Candidates
            val shownInApp = visible && stageShown && answered
            val wanted = !shownInApp && (state.origin != ListenOrigin.App || (!visible && prefs.notifyInBackground))
            if (wanted) Notifications.postListenResult(app, state, result, settings.preference())
            if (result is ListenResult.Matched || result is ListenResult.Candidates) ListenWidget.refresh(app)
        }
    }

    private fun fail(gen: Long?, code: ErrorCode, message: String) {
        synchronized(lock) {
            if (gen != null && gen != generation) return
        }
        end(synchronized(lock) { generation }, ListenResult.Failed(_state.value.sessionId, code, message))
    }

    /**
     * Release the audio source. The word recognizer finishes what it heard and sends its final
     * transcript (the engine waits up to 2 s for it), unless [discardWords].
     */
    private fun stopCapture(discardWords: Boolean = false) {
        val (audio, mp, tap) = synchronized(lock) {
            val held = Triple(capture, projection, words)
            capture = null
            projection = null
            words = null
            if (discardWords) {
                finishingWords?.cancel()
                finishingWords = null
            } else if (held.third != null) {
                finishingWords = held.third
            }
            held
        }
        audio?.stop()
        mp?.let { runCatching { it.stop() } }
        if (discardWords) tap?.cancel() else tap?.end()
    }

    /** Hand the recognizer's latest transcript to the session it belongs to (main thread). */
    private fun pushWords(gen: Long, live: LiveSession, transcript: SungTranscript) {
        if (synchronized(lock) { gen != generation }) return
        if (transcript.isFinal) Log.i(TAG, "final transcript: ${transcript.text.split(' ').count { it.isNotBlank() }} words")
        runCatching {
            live.pushTranscript(
                SpeechTranscript(
                    text = transcript.text,
                    alternatives = transcript.alternatives,
                    language = transcript.language,
                    confidence = transcript.confidence,
                    isFinal = transcript.isFinal,
                ),
            )
        }.onFailure { Log.w(TAG, "transcript not taken", it) }
    }

    private fun finishCapture() {
        val svc = synchronized(lock) {
            pendingGrant = null
            service.also { service = null }
        }
        if (svc != null) main.post { svc.release() }
    }

    /** Ask both tiles to redraw (their state follows [state]). */
    private fun refreshTiles() {
        for (cls in listOf(SongTileService::class.java, PhoneAudioTileService::class.java)) {
            runCatching { TileService.requestListeningState(app, ComponentName(app, cls)) }
        }
        updateNotification()
    }

    private data class Quad(val gen: Long, val grant: ProjectionGrant?, val source: CaptureSource, val mode: ListenMode)

    private companion object {
        const val TAG = "ListenController"
    }
}
