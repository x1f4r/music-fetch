package app.musicfetch.analysis

import android.app.Application
import android.util.Log
import androidx.core.content.ContextCompat
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.core.AnalysisSession
import app.musicfetch.core.AnalyzeMode
import app.musicfetch.core.Candidate
import app.musicfetch.core.ErrorCode
import app.musicfetch.core.MfException
import app.musicfetch.core.Notice
import app.musicfetch.core.Outcome
import app.musicfetch.core.Phase
import app.musicfetch.core.Segment
import app.musicfetch.core.SegmentKind
import app.musicfetch.core.SessionEvent
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionObserver
import app.musicfetch.core.SourceInfo
import app.musicfetch.engine.EngineHost
import app.musicfetch.engine.EngineUnavailableException
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.FileImports
import app.musicfetch.platform.SettingsStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** A playlist entry being processed. */
data class ItemProgress(val index: Int, val count: Int, val title: String?)

/** Live state of one link, file or lyric-search session (docs/APPS.md, `SessionModel`). */
data class AnalysisState(
    val input: String,
    val sessionId: String? = null,
    val kind: SessionKind? = null,
    val phase: Phase? = null,
    /** Download fraction while [phase] is `Downloading`. */
    val fraction: Float? = null,
    val analyzedMs: Long = 0,
    val totalMs: Long = 0,
    val etaMs: Long? = null,
    val source: SourceInfo? = null,
    val item: ItemProgress? = null,
    /** Ordered by playlist entry, then start time. */
    val segments: List<Segment> = emptyList(),
    val candidates: List<Candidate> = emptyList(),
    val candidatesFinal: Boolean = false,
    val notice: Notice? = null,
    val outcome: Outcome? = null,
    val error: AnalysisError? = null,
    val canceled: Boolean = false,
) {
    val isTerminal: Boolean get() = outcome != null || error != null || canceled

    /** Segments that name a track. */
    val identified: Int get() = segments.count { it.kind == SegmentKind.TRACK && it.track != null }

    /** Long media or several entries: shown as a mix/collection rather than a single result. */
    val isCollection: Boolean
        get() = (source?.itemCount?.toInt() ?: 1) > 1 || (source?.durationMs?.toLong() ?: 0) > MIX_THRESHOLD_MS ||
            segments.size > 1

    companion object {
        /** Media longer than 4 minutes is analyzed as a mix (docs/ARCHITECTURE.md). */
        const val MIX_THRESHOLD_MS = 4 * 60_000L
    }
}

data class AnalysisError(val code: ErrorCode, val message: String)

/** One running (or recently finished) analysis. */
class AnalysisRun internal constructor(input: String, val showsProgress: Boolean) {
    private val _state = MutableStateFlow(AnalysisState(input))
    val state: StateFlow<AnalysisState> = _state.asStateFlow()

    @Volatile
    internal var handle: AnalysisSession? = null

    internal fun update(transform: (AnalysisState) -> AnalysisState) = _state.update(transform)

    val sessionId: String? get() = _state.value.sessionId
}

/**
 * Starts and tracks link, file and lyric-search sessions. Link and file analyses run inside
 * [AnalysisService] (a foreground service with a progress notification) so they continue when
 * the share sheet closes or the app goes to the background; finished analyses land in the
 * library and, when nobody is looking, as a notification.
 */
class AnalysisController(
    private val app: Application,
    private val scope: CoroutineScope,
    private val engineHost: EngineHost,
    private val settings: SettingsStore,
) {
    private val _runs = MutableStateFlow<List<AnalysisRun>>(emptyList())

    /** Runs of this process, newest last. Finished runs stay until [forget]. */
    val runs: StateFlow<List<AnalysisRun>> = _runs.asStateFlow()

    /** The run for [sessionId], if it was started in this process. */
    fun run(sessionId: String): AnalysisRun? = _runs.value.lastOrNull { it.sessionId == sessionId }

    /** Analyze a link or an absolute file path. Throws [MfException] for invalid input. */
    suspend fun analyze(input: String, mode: AnalyzeMode = AnalyzeMode.AUTO): AnalysisRun =
        start(input, showsProgress = true) { engine, observer -> engine.analyze(input, mode, observer) }

    /** Search songs by typed lyrics. Throws [MfException] for empty text. */
    suspend fun searchLyrics(text: String): AnalysisRun =
        start(text, showsProgress = false) { engine, observer -> engine.searchLyrics(text, observer) }

    fun cancel(sessionId: String) {
        run(sessionId)?.handle?.let { runCatching { it.cancel() } }
    }

    fun cancelAll() {
        _runs.value.filterNot { it.state.value.isTerminal }.forEach { run -> run.handle?.let { runCatching { it.cancel() } } }
    }

    /** Drop a finished run (its session is read from the library from now on). */
    fun forget(run: AnalysisRun) {
        if (run.state.value.isTerminal) _runs.update { list -> list.filterNot { it === run } }
    }

    private suspend fun start(
        input: String,
        showsProgress: Boolean,
        open: (app.musicfetch.core.EngineInterface, SessionObserver) -> AnalysisSession,
    ): AnalysisRun {
        val engine = try {
            engineHost.engine()
        } catch (e: EngineUnavailableException) {
            throw MfException.Internal(e.message ?: "The engine is not available.")
        }
        val run = AnalysisRun(input, showsProgress)
        val observer = object : SessionObserver {
            override fun onEvent(event: SessionEvent) = handle(run, event)
        }
        val handle = withContext(Dispatchers.IO) { open(engine, observer) }
        run.handle = handle
        run.update { if (it.sessionId == null) it.copy(sessionId = handle.id()) else it }
        _runs.update { list -> list.filterNot { it.state.value.isTerminal && it.sessionId == run.sessionId } + run }
        if (showsProgress && !run.state.value.isTerminal) startService()
        return run
    }

    private fun startService() {
        try {
            ContextCompat.startForegroundService(app, AnalysisService.intent(app))
        } catch (e: IllegalStateException) {
            // Started from the background without an exemption; the analysis still runs while
            // the process lives.
            Log.w(TAG, "analysis service not started", e)
        } catch (e: SecurityException) {
            Log.w(TAG, "analysis service not started", e)
        }
    }

    private fun handle(run: AnalysisRun, event: SessionEvent) {
        when (event) {
            is SessionEvent.Started -> run.update { it.copy(sessionId = event.sessionId, kind = event.kind) }
            is SessionEvent.Phase -> run.update { it.copy(phase = event.phase, fraction = event.fraction) }
            is SessionEvent.Progress -> run.update {
                it.copy(
                    analyzedMs = maxOf(it.analyzedMs, event.analyzedMs.toLong()),
                    totalMs = event.totalMs.toLong(),
                    etaMs = event.etaMs?.toLong(),
                )
            }
            is SessionEvent.Source -> run.update { it.copy(source = event.info) }
            is SessionEvent.Item -> run.update {
                it.copy(item = ItemProgress(event.index.toInt(), event.count.toInt(), event.title))
            }
            is SessionEvent.Segment -> run.update { it.copy(segments = upsert(it.segments, event.segment)) }
            is SessionEvent.SegmentRemoved -> run.update { s -> s.copy(segments = s.segments.filterNot { it.id == event.id }) }
            is SessionEvent.Candidates -> run.update { it.copy(candidates = event.candidates, candidatesFinal = event.isFinal) }
            is SessionEvent.Notice -> if (event.notice !is Notice.ProviderUnavailable) run.update { it.copy(notice = event.notice) }
            is SessionEvent.Finished -> {
                run.update { it.copy(outcome = event.outcome, phase = null, notice = null) }
                onTerminal(run)
            }
            is SessionEvent.Failed -> {
                run.update { it.copy(error = AnalysisError(event.code, event.message), phase = null) }
                onTerminal(run)
            }
            SessionEvent.Canceled -> {
                run.update { it.copy(canceled = true, phase = null) }
                onTerminal(run)
            }
            is SessionEvent.Level, is SessionEvent.Pitch -> Unit
        }
    }

    private fun onTerminal(run: AnalysisRun) {
        app.appGraph.notifyLibraryChanged()
        // Screens read finished sessions from the library; keep the run only briefly.
        scope.launch {
            delay(FORGET_AFTER_MS)
            forget(run)
        }
        val state = run.state.value
        if (state.kind == SessionKind.FILE) FileImports.release(app, state.input)
        val id = state.sessionId ?: return
        if (!run.showsProgress || state.canceled || app.appGraph.isAppVisible) return
        scope.launch {
            val prefs = settings.current()
            if (!prefs.notifyInBackground) return@launch
            val single = state.segments.singleOrNull { it.track != null }?.track
            val (title, text) = when {
                state.error != null -> app.getString(R.string.state_no_match) to state.error.message
                single != null && !state.isCollection ->
                    app.getString(R.string.notif_result_title, single.title, single.artist) to state.source?.title
                state.identified > 0 -> (state.source?.title ?: state.input) to
                    app.resources.getQuantityString(R.plurals.notif_tracks_found, state.identified, state.identified)
                else -> app.getString(R.string.state_no_match) to (state.source?.title ?: state.input)
            }
            val track = if (!state.isCollection) single else null
            Notifications.postAnalysisResult(app, id, title, text, track, prefs.defaultService)
        }
    }

    private companion object {
        const val TAG = "AnalysisController"
        const val FORGET_AFTER_MS = 60_000L

        fun upsert(list: List<Segment>, segment: Segment): List<Segment> {
            val index = list.indexOfFirst { it.id == segment.id }
            val updated = if (index >= 0) list.toMutableList().also { it[index] = segment } else list + segment
            return updated.sortedWith(compareBy({ it.itemIndex }, { it.startMs }))
        }
    }
}
