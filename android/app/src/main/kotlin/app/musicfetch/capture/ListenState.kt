package app.musicfetch.capture

import app.musicfetch.core.Candidate
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ErrorCode
import app.musicfetch.core.ListenMode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.Notice
import app.musicfetch.core.PitchFrame
import app.musicfetch.core.Track
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlin.math.exp

/** Where a listen was started; tiles and widgets get their result as a notification. */
enum class ListenOrigin { App, Tile, Widget }

/** Stage of the one live session the app runs at a time. */
sealed interface ListenPhase {
    data object Idle : ListenPhase

    /** Waiting for the capture service, the engine or the microphone. */
    data object Starting : ListenPhase

    data object Listening : ListenPhase

    /** Capture ended; the engine is matching what it heard. */
    data object Matching : ListenPhase

    data class Done(val result: ListenResult) : ListenPhase
}

/** How a live session ended. */
sealed interface ListenResult {
    val sessionId: String?

    data class Matched(override val sessionId: String, val track: Track) : ListenResult

    data class Candidates(override val sessionId: String, val candidates: List<Candidate>) : ListenResult

    data class NoMatch(override val sessionId: String?, val reason: NoMatchReason) : ListenResult

    data class Failed(override val sessionId: String?, val code: ErrorCode, val message: String) : ListenResult
}

/** Everything the Listen stage, the capture notification and the tiles show. */
data class ListenState(
    val phase: ListenPhase = ListenPhase.Idle,
    val source: CaptureSource = CaptureSource.MICROPHONE,
    val mode: ListenMode = ListenMode.AUTO,
    val origin: ListenOrigin = ListenOrigin.App,
    val sessionId: String? = null,
    /** Humming, whistling or singing detected, or Hum mode: the trace band and ring show. */
    val impression: Boolean = false,
    /** The latest notice that changes the state line. */
    val notice: Notice? = null,
    /** `System.currentTimeMillis()` when capture started (notification chronometer). */
    val startedAtWallMs: Long = 0,
    /** The engine reported that the melody library is missing during this session. */
    val melodyIndexMissing: Boolean = false,
) {
    val isActive: Boolean
        get() = phase is ListenPhase.Starting || phase is ListenPhase.Listening || phase is ListenPhase.Matching

    val isCapturing: Boolean
        get() = phase is ListenPhase.Starting || phase is ListenPhase.Listening
}

/**
 * The fast-changing part of a live session (input level ~30 Hz, pitch ~10 Hz), kept apart from
 * [ListenState] so only the views that draw it recompose.
 */
class LiveSignal {
    private val _level = MutableStateFlow(0f)

    /** Input level 0..1, smoothed with a 100 ms exponential moving average. */
    val level: StateFlow<Float> = _level.asStateFlow()

    /** The last [PitchRing.WINDOW_MS] of pitch frames. */
    val pitch = PitchRing()

    private var lastLevelNanos = 0L

    fun onLevel(rms: Float, nowNanos: Long = System.nanoTime()) {
        val target = rms.coerceIn(0f, 1f)
        val dtMs = if (lastLevelNanos == 0L) 33.0 else (nowNanos - lastLevelNanos) / 1e6
        lastLevelNanos = nowNanos
        val alpha = (1.0 - exp(-dtMs.coerceIn(1.0, 500.0) / LEVEL_EMA_MS)).toFloat()
        _level.value += (target - _level.value) * alpha
    }

    fun reset() {
        _level.value = 0f
        lastLevelNanos = 0L
        pitch.clear()
    }

    private companion object {
        const val LEVEL_EMA_MS = 100.0
    }
}

/**
 * Thread-safe ring of recent pitch frames, written by engine threads and read by the drawing
 * code each frame. Also counts voiced time for the hum ring.
 */
class PitchRing {
    private val lock = Any()
    private val times = LongArray(CAPACITY)
    private val midis = FloatArray(CAPACITY)
    private val confidences = FloatArray(CAPACITY)
    private var start = 0
    private var size = 0
    private var voicedMs = 0L
    private var lastT = -1L
    private var lastVoiced = false
    private var version = 0L

    fun add(frames: List<PitchFrame>) {
        synchronized(lock) {
            for (frame in frames) {
                val t = frame.tMs.toLong()
                val voiced = !frame.midi.isNaN()
                if (lastT >= 0 && voiced && lastVoiced) voicedMs += (t - lastT).coerceIn(0, MAX_STEP_MS)
                lastT = t
                lastVoiced = voiced
                val index = (start + size) % CAPACITY
                times[index] = t
                midis[index] = frame.midi
                confidences[index] = frame.confidence
                if (size < CAPACITY) size++ else start = (start + 1) % CAPACITY
            }
            // Keep only the drawing window.
            while (size > 0 && lastT - times[start] > WINDOW_MS) {
                start = (start + 1) % CAPACITY
                size--
            }
            version++
        }
    }

    fun clear() {
        synchronized(lock) {
            start = 0
            size = 0
            voicedMs = 0
            lastT = -1
            lastVoiced = false
            version++
        }
    }

    /** Changes whenever frames are added or cleared; lets readers skip unchanged windows. */
    fun version(): Long = synchronized(lock) { version }

    /** Seconds of voiced input so far in this session. */
    fun voicedSeconds(): Float = synchronized(lock) { voicedMs / 1000f }

    /** Copy of the window: frame times (ms since session start), MIDI pitch (NaN = unvoiced), confidence. */
    fun snapshot(): Snapshot = synchronized(lock) {
        val t = LongArray(size)
        val m = FloatArray(size)
        val c = FloatArray(size)
        for (i in 0 until size) {
            val index = (start + i) % CAPACITY
            t[i] = times[index]
            m[i] = midis[index]
            c[i] = confidences[index]
        }
        Snapshot(t, m, c, version)
    }

    class Snapshot(val times: LongArray, val midis: FloatArray, val confidences: FloatArray, val version: Long) {
        val isEmpty: Boolean get() = times.isEmpty()
    }

    companion object {
        /** The trace band shows the last 8 s (docs/design/DESIGN.md §5.6). */
        const val WINDOW_MS = 8_000L
        private const val CAPACITY = 512
        private const val MAX_STEP_MS = 120L
    }
}
