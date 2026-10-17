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
import app.musicfetch.core.LiveSession
import app.musicfetch.core.MfException
import app.musicfetch.core.Notice
import app.musicfetch.core.Outcome
import app.musicfetch.core.Phase
import app.musicfetch.core.SessionEvent
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionObserver
import app.musicfetch.engine.EngineHost
import app.musicfetch.engine.EngineUnavailableException
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.DevFiles
import app.musicfetch.platform.SettingsStore
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
    private var service: CaptureService? = null
    private var userCanceled = false

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
            if (source == CaptureSource.SYSTEM_AUDIO && grant == null) return false
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
        stopCapture()
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
        val live = try {
            engine.startListening(source, mode, Observer(gen))
        } catch (e: MfException) {
            Log.w(TAG, "startListening failed", e)
            audio.stop()
            fail(gen, ErrorCode.INTERNAL, e.message ?: "Couldn't start listening.")
            return
        }
        val keep = synchronized(lock) {
            if (gen != generation || !_state.value.isCapturing) {
                false
            } else {
                session = live
                capture = audio
                true
            }
        }
        if (!keep) {
            audio.stop()
            runCatching { live.cancel() }
            return
        }
        Log.i(TAG, "capturing ${audio.description} at ${audio.sampleRate} Hz × ${audio.channels}")
        audio.start(
            sink = { buffer: ByteBuffer, rate: Int, channels: Int -> live.pushPcm(buffer, rate.toUInt(), channels.toUInt()) },
            onError = { message ->
                Log.w(TAG, "capture error: $message")
                // Analyze what was captured so far.
                stop()
            },
        )
    }

    private fun openAudio(svc: CaptureService, source: CaptureSource, grant: ProjectionGrant?): PcmSource? {
        if (source == CaptureSource.MICROPHONE) {
            DevFiles.find(app, DevFiles.CAPTURE_WAV)?.let { replay -> WavReplay.open(replay)?.let { return it } }
        }
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
                val id = _state.value.sessionId
                val result = when (val outcome = event.outcome) {
                    is Outcome.Matched -> if (id != null) ListenResult.Matched(id, outcome.track) else null
                    is Outcome.Candidates -> if (id != null) ListenResult.Candidates(id, lastCandidates) else null
                    is Outcome.NoMatch -> ListenResult.NoMatch(id, outcome.reason)
                    is Outcome.Completed -> null
                } ?: ListenResult.Failed(id, ErrorCode.INTERNAL, "Something went wrong.")
                end(gen, result)
            }
            is SessionEvent.Candidates -> lastCandidates = event.candidates
            is SessionEvent.Failed -> end(gen, ListenResult.Failed(_state.value.sessionId, event.code, event.message))
            is SessionEvent.Canceled -> end(gen, null)
            else -> Unit
        }
    }

    @Volatile
    private var lastCandidates: List<Candidate> = emptyList()

    private fun onNotice(notice: Notice) {
        when (notice) {
            is Notice.ProviderUnavailable -> Log.i(TAG, "provider unavailable: ${notice.provider}")
            is Notice.MelodyIndexMissing -> _state.update { it.copy(melodyIndexMissing = true) }
            is Notice.ImpressionDetected -> _state.update { it.copy(impression = true, notice = notice) }
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
        stopCapture()
        finishCapture()
        lastCandidates = emptyList()
        val previous = _state.value
        if (result == null) {
            _state.value = ListenState(source = previous.source, mode = previous.mode)
            if (!canceledByUser) Log.i(TAG, "session canceled by the engine")
        } else {
            _state.value = previous.copy(phase = ListenPhase.Done(result), notice = null)
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
            val wanted = state.origin != ListenOrigin.App || (!visible && prefs.notifyInBackground)
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

    private fun stopCapture() {
        val (audio, mp) = synchronized(lock) {
            val pair = capture to projection
            capture = null
            projection = null
            pair
        }
        audio?.stop()
        mp?.let { runCatching { it.stop() } }
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
