package app.musicfetch.ui.session

import android.app.Application
import android.content.Context
import android.net.Uri
import android.util.Log
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.CreationExtras
import app.musicfetch.R
import app.musicfetch.analysis.AnalysisState
import app.musicfetch.analysis.SegmentFix
import app.musicfetch.appGraph
import app.musicfetch.core.Candidate
import app.musicfetch.core.EngineInterface
import app.musicfetch.core.ExportFormat
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.Lyrics
import app.musicfetch.core.MfException
import app.musicfetch.core.Segment
import app.musicfetch.core.SegmentKind
import app.musicfetch.core.Service
import app.musicfetch.core.SessionDetail
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionStatus
import app.musicfetch.core.SourceInfo
import app.musicfetch.core.Track
import app.musicfetch.platform.ProviderPreference
import app.musicfetch.ui.common.ArtworkLoader
import app.musicfetch.widget.ListenWidget
import com.materialkolor.ktx.themeColorOrNull
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** What a session screen shows, derived from the stored session or a running analysis. */
sealed interface SessionView {
    data object Loading : SessionView

    data object Missing : SessionView

    /** One identified track. [context] is "At 12:34 in ‹title›" / "From ‹file›" for links and files. */
    data class Result(val entry: LibraryEntry, val track: Track, val context: SessionContext?) : SessionView

    /** Ranked hum or lyric candidates. [humContour] is the user's hum (semitones), if known. */
    data class Candidates(
        val entry: LibraryEntry,
        val candidates: List<Candidate>,
        val humContour: List<Float>?,
        val humSeconds: Int,
        val final: Boolean,
    ) : SessionView

    /** A long recording with a timeline (`timed`) or a collection with numbered entries. */
    data class Mix(
        val entry: LibraryEntry?,
        val source: SourceInfo?,
        val segments: List<Segment>,
        val timed: Boolean,
        val running: Boolean,
        val canceled: Boolean,
        val analyzedMs: Long,
        val totalMs: Long,
        val etaMs: Long?,
        val itemCount: Int,
    ) : SessionView

    /** Nothing was found, or the session failed or stopped early. */
    data class Empty(val entry: LibraryEntry?, val message: String?, val failed: Boolean, val running: Boolean) : SessionView
}

/** Where a single result came from. */
sealed interface SessionContext {
    data class Link(val atMs: Long?, val title: String) : SessionContext

    data class File(val name: String) : SessionContext
}

/** State of a session screen (docs/APPS.md, `SessionModel`). */
class SessionViewModel(app: Application, private val id: String) : AndroidViewModel(app) {
    private val graph = app.appGraph
    private val _detail = MutableStateFlow<SessionDetail?>(null)
    private val _missing = MutableStateFlow(false)
    private val _live = MutableStateFlow<AnalysisState?>(null)

    private val _view = MutableStateFlow<SessionView>(SessionView.Loading)
    val view: StateFlow<SessionView> = _view.asStateFlow()

    private val _lyrics = MutableStateFlow<Lyrics?>(null)
    val lyrics: StateFlow<Lyrics?> = _lyrics.asStateFlow()

    private val _seed = MutableStateFlow<Color?>(null)

    /** Dominant artwork colour of the result, for the content-based scheme. */
    val seed: StateFlow<Color?> = _seed.asStateFlow()

    /** Where results open; null until the settings are read. */
    val preference: StateFlow<ProviderPreference?> = graph.preference

    private val _messages = MutableStateFlow<String?>(null)

    /** One-shot messages ("Copied", export results). */
    val messages: StateFlow<String?> = _messages.asStateFlow()

    private var lyricsFor: String? = null
    private var seedFor: String? = null
    private var liveJob: Job? = null

    init {
        val run = graph.analyses.run(id)
        if (run != null && !run.state.value.isTerminal) {
            liveJob = viewModelScope.launch {
                run.state.collect { state ->
                    _live.value = state
                    rebuild()
                    if (state.isTerminal) {
                        load()
                        _live.value = null
                        rebuild()
                        liveJob?.cancel()
                    }
                }
            }
        }
        viewModelScope.launch { load() }
        viewModelScope.launch { graph.libraryChanged.collect { if (_live.value == null) load() } }
    }

    private suspend fun load() {
        val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return
        val detail = withContext(Dispatchers.IO) {
            try {
                engine.session(id)
            } catch (e: MfException) {
                Log.w(TAG, "session $id not readable", e)
                null
            }
        }
        _detail.value = detail
        _missing.value = detail == null
        rebuild()
    }

    private fun rebuild() {
        val view = derive(_detail.value, _live.value, _missing.value)
        _view.value = view
        val track = (view as? SessionView.Result)?.track
        if (track != null) {
            loadLyrics(track)
            loadSeed(track.artworkUrl)
        }
    }

    private fun derive(detail: SessionDetail?, live: AnalysisState?, missing: Boolean): SessionView {
        if (live != null && !live.isTerminal) return liveView(detail?.entry, live)
        if (detail == null) return if (missing) SessionView.Missing else SessionView.Loading
        val entry = detail.entry
        val segments = detail.segments.sortedWith(compareBy({ it.itemIndex }, { it.startMs }))
        val identified = segments.filter { it.kind == SegmentKind.TRACK && it.track != null }
        val source = detail.source
        return when {
            entry.status == SessionStatus.MATCHED -> {
                val track = entry.track ?: identified.firstOrNull()?.track
                if (track != null) SessionView.Result(entry, track, null) else SessionView.Empty(entry, null, false, false)
            }
            entry.kind == SessionKind.HUM || entry.kind == SessionKind.LYRICS || entry.status == SessionStatus.CANDIDATES -> {
                val confirmed = entry.track
                when {
                    confirmed != null -> SessionView.Result(entry, confirmed, null)
                    detail.candidates.isNotEmpty() -> candidatesView(entry, detail.candidates, true)
                    else -> SessionView.Empty(entry, null, entry.status == SessionStatus.FAILED, entry.status == SessionStatus.RUNNING)
                }
            }
            // A single recording that is one track (even a long music video) is a result.
            identified.size == 1 && isSingleTrack(source, segments) ->
                SessionView.Result(entry, identified.first().track!!, contextFor(entry, source, identified.first()))
            isCollectionOrMix(source, segments) -> mixView(entry, source, segments, running = false, canceled = entry.status == SessionStatus.CANCELED)
            identified.size == 1 -> SessionView.Result(entry, identified.first().track!!, contextFor(entry, source, identified.first()))
            else -> SessionView.Empty(
                entry,
                null,
                failed = entry.status == SessionStatus.FAILED,
                running = entry.status == SessionStatus.RUNNING,
            )
        }
    }

    private fun liveView(entry: LibraryEntry?, live: AnalysisState): SessionView {
        if (live.candidates.isNotEmpty() && entry != null) return candidatesView(entry, live.candidates, live.candidatesFinal)
        if (live.isCollection || live.segments.isNotEmpty()) {
            return SessionView.Mix(
                entry = entry,
                source = live.source,
                segments = live.segments,
                timed = isTimed(live.segments, live.source),
                running = true,
                canceled = false,
                analyzedMs = live.analyzedMs,
                totalMs = live.totalMs.takeIf { it > 0 } ?: (live.source?.durationMs?.toLong() ?: 0L),
                etaMs = live.etaMs,
                itemCount = live.source?.itemCount?.toInt() ?: 1,
            )
        }
        return SessionView.Empty(entry, null, failed = false, running = true)
    }

    private fun mixView(entry: LibraryEntry, source: SourceInfo?, segments: List<Segment>, running: Boolean, canceled: Boolean) =
        SessionView.Mix(
            entry = entry,
            source = source,
            segments = segments,
            timed = isTimed(segments, source),
            running = running,
            canceled = canceled,
            analyzedMs = source?.durationMs?.toLong() ?: 0L,
            totalMs = source?.durationMs?.toLong() ?: entry.durationMs?.toLong() ?: segments.maxOfOrNull { it.endMs.toLong() } ?: 0L,
            etaMs = null,
            itemCount = source?.itemCount?.toInt() ?: 1,
        )

    private fun candidatesView(entry: LibraryEntry, candidates: List<Candidate>, final: Boolean): SessionView.Candidates {
        val alignment = candidates.firstNotNullOfOrNull { it.alignment }
        val seconds = alignment?.let { a -> (a.query.size * a.frameMs.toLong() / 1000).toInt() } ?: 0
        return SessionView.Candidates(entry, candidates, alignment?.query, seconds, final)
    }

    private fun isSingleTrack(source: SourceInfo?, segments: List<Segment>): Boolean =
        (source?.itemCount?.toInt() ?: 1) <= 1 &&
            segments.count { it.kind == SegmentKind.TRACK || it.kind == SegmentKind.UNIDENTIFIED } <= 1

    private fun isCollectionOrMix(source: SourceInfo?, segments: List<Segment>): Boolean =
        (source?.itemCount?.toInt() ?: 1) > 1 || segments.size > 1 ||
            (source?.durationMs?.toLong() ?: 0L) > AnalysisState.MIX_THRESHOLD_MS

    /** Collections from streaming services carry no times (start = end = 0). */
    private fun isTimed(segments: List<Segment>, source: SourceInfo?): Boolean =
        (source?.itemCount?.toInt() ?: 1) <= 1 &&
            (segments.any { it.endMs > it.startMs } || (source?.durationMs?.toLong() ?: 0L) > 0L)

    private fun contextFor(entry: LibraryEntry, source: SourceInfo?, segment: Segment): SessionContext? = when (entry.kind) {
        SessionKind.FILE -> SessionContext.File(entry.title)
        SessionKind.LINK -> {
            val title = source?.title ?: entry.title
            // A position only helps inside long media ("At 12:34 in ‹title›").
            val long = (source?.durationMs?.toLong() ?: 0L) > AnalysisState.MIX_THRESHOLD_MS && segment.startMs > 0u
            if (source?.service in streamingServices) null else SessionContext.Link(if (long) segment.startMs.toLong() else null, title)
        }
        else -> null
    }

    private fun loadLyrics(track: Track) {
        if (lyricsFor == track.identityKey) return
        lyricsFor = track.identityKey
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            _lyrics.value = runCatching { engine.fetchLyrics(track) }.getOrNull()?.takeIf { it.plain.isNotBlank() }
        }
    }

    private fun loadSeed(url: String?) {
        if (url == null || seedFor == url) return
        seedFor = url
        viewModelScope.launch(Dispatchers.Default) {
            val bitmap = ArtworkLoader.bitmap(getApplication(), url, sizePx = 128) ?: return@launch
            _seed.value = bitmap.asImageBitmap().themeColorOrNull()
        }
    }

    /** "This is it": record the pick and show it as the result. */
    fun confirm(candidate: Candidate) {
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            withContext(Dispatchers.IO) {
                try {
                    engine.confirmCandidate(id, candidate.track)
                    graph.pendingSegmentFix.value?.let { fix ->
                        engine.correctSegment(fix.sessionId, fix.segmentId, candidate.track)
                        graph.pendingSegmentFix.value = null
                    }
                } catch (e: MfException) {
                    Log.w(TAG, "confirm failed", e)
                }
            }
            graph.notifyLibraryChanged()
            ListenWidget.refresh(getApplication())
            load()
        }
    }

    /** Confirm a mix segment's track, or replace it with an alternate. */
    fun correct(segment: Segment, track: Track) = edit("correction") { engine -> engine.correctSegment(id, segment.id, track) }

    /** "Not this": the segment becomes unidentified and its track is kept as an alternate. */
    fun reject(segment: Segment) = edit("rejection") { engine -> engine.rejectSegment(id, segment.id) }

    private fun edit(what: String, change: (EngineInterface) -> Segment) {
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            val updated = withContext(Dispatchers.IO) {
                try {
                    change(engine)
                } catch (e: MfException) {
                    Log.w(TAG, "$what failed", e)
                    null
                }
            }
            if (updated == null) {
                _messages.value = getApplication<Application>().getString(R.string.error_generic)
                return@launch
            }
            _detail.update { d -> d?.copy(segments = d.segments.map { if (it.id == updated.id) updated else it }) }
            _live.update { l -> l?.copy(segments = l.segments.map { if (it.id == updated.id) updated else it }) }
            rebuild()
            graph.notifyLibraryChanged()
        }
    }

    /** Remember which mix segment a hum should fill in ("Hum it" on an ID row). */
    fun humForSegment(segment: Segment) {
        graph.pendingSegmentFix.value = SegmentFix(id, segment.id)
    }

    fun stop() = graph.analyses.cancel(id)

    /** Pin or unpin at once; the engine catches up, and a failure puts the toggle back. */
    fun setPinned(pinned: Boolean) {
        val before = _detail.value?.entry?.pinned ?: return
        if (before == pinned) return
        showPinned(pinned)
        viewModelScope.launch {
            val ok = withContext(Dispatchers.IO) {
                runCatching { graph.engineHost.engine().setPinned(id, pinned) }.isSuccess
            }
            if (!ok) {
                showPinned(before)
                _messages.value = getApplication<Application>().getString(R.string.error_generic)
            }
            graph.notifyLibraryChanged()
        }
    }

    private fun showPinned(pinned: Boolean) {
        _detail.update { d -> d?.copy(entry = d.entry.copy(pinned = pinned)) }
        rebuild()
    }

    /** Delete with Undo (heard, hummed and link sessions). */
    fun delete() = graph.deletions.schedule(listOf(id))

    /** Delete after the user confirmed (mixes and pinned sessions). */
    fun deleteNow() = graph.deletions.deleteNow(listOf(id))

    /** The session rendered for export or sharing. */
    suspend fun export(format: ExportFormat): String? {
        val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return null
        return withContext(Dispatchers.IO) {
            try {
                engine.exportSession(id, format)
            } catch (e: MfException) {
                Log.w(TAG, "export failed", e)
                null
            }
        }
    }

    /** Write an export to a document the user picked. */
    fun exportTo(context: Context, uri: Uri, format: ExportFormat, done: (Boolean) -> Unit) {
        viewModelScope.launch {
            val text = export(format)
            val ok = text != null && withContext(Dispatchers.IO) {
                runCatching {
                    context.contentResolver.openOutputStream(uri)?.use { it.write(text.toByteArray(Charsets.UTF_8)) } != null
                }.getOrDefault(false)
            }
            done(ok)
        }
    }

    fun message(text: String?) {
        _messages.value = text
    }

    class Factory(private val id: String) : ViewModelProvider.Factory {
        override fun <T : ViewModel> create(modelClass: Class<T>, extras: CreationExtras): T {
            val app = extras[ViewModelProvider.AndroidViewModelFactory.APPLICATION_KEY] as Application
            @Suppress("UNCHECKED_CAST") // Only SessionViewModel is requested from this factory.
            return SessionViewModel(app, id) as T
        }
    }

    private companion object {
        const val TAG = "SessionViewModel"
        val streamingServices = setOf(
            Service.SPOTIFY, Service.APPLE_MUSIC, Service.DEEZER, Service.TIDAL, Service.AMAZON_MUSIC, Service.YOU_TUBE_MUSIC,
        )
    }
}
