package app.musicfetch.ui.listen

import android.app.Application
import android.net.Uri
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import app.musicfetch.analysis.AnalysisRun
import app.musicfetch.analysis.AnalysisState
import app.musicfetch.appGraph
import app.musicfetch.capture.ListenOrigin
import app.musicfetch.capture.ListenPhase
import app.musicfetch.capture.ListenResult
import app.musicfetch.capture.ListenState
import app.musicfetch.capture.ProjectionGrant
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.LibraryQuery
import app.musicfetch.core.ListenMode
import app.musicfetch.core.MfException
import app.musicfetch.core.SessionStatus
import app.musicfetch.platform.AppSettings
import app.musicfetch.platform.FileImports
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.common.userMessage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** A link or file being identified on the Listen stage, or why it could not start. */
sealed interface LinkStage {
    data object None : LinkStage

    /** Copying a shared file or waiting for the engine. */
    data object Preparing : LinkStage

    data class Running(val run: AnalysisRun) : LinkStage

    data class Failed(val message: String) : LinkStage
}

/**
 * State of the Listen stage (docs/APPS.md, `ListenModel`): the shared live session, a link or
 * file analysis started from the link field, the most recent result and the app settings.
 */
class ListenViewModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph

    val listen: StateFlow<ListenState> = graph.listen.state
    val signal = graph.listen.signal

    val settings: StateFlow<AppSettings?> = graph.settings.settings
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    val melodyStatus = graph.melody.status
    val melodyDownload = graph.melody.download

    private val _recent = MutableStateFlow<LibraryEntry?>(null)

    /** The newest finished session with an answer, for the card at the bottom. */
    val recent: StateFlow<LibraryEntry?> = _recent.asStateFlow()

    private val _link = MutableStateFlow<LinkStage>(LinkStage.None)
    val link: StateFlow<LinkStage> = _link.asStateFlow()

    /** The live state of the stage's link analysis. */
    @OptIn(ExperimentalCoroutinesApi::class)
    val linkState: StateFlow<AnalysisState?> = _link
        .flatMapLatest { stage -> if (stage is LinkStage.Running) stage.run.state else flowOf(null) }
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    private val _openSession = Channel<String>(Channel.BUFFERED)

    /** Sessions to show (a result arrived, or a link turned out to be a mix). */
    val openSession: Flow<String> = _openSession.receiveAsFlow()

    init {
        viewModelScope.launch {
            graph.libraryChanged.onStart { emit(Unit) }.collect { loadRecent() }
        }
        viewModelScope.launch {
            listen.collect { state -> onListenState(state) }
        }
        viewModelScope.launch {
            linkState.collect { state -> if (state != null) onLinkState(state) }
        }
        graph.melody.refresh()
    }

    private suspend fun loadRecent() {
        val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return
        val entries = withContext(Dispatchers.IO) {
            runCatching { engine.library(LibraryQuery(kinds = emptyList(), search = null, pinnedOnly = false, limit = 12u, offset = 0u)) }
                .getOrDefault(emptyList())
        }
        _recent.value = entries.firstOrNull {
            it.status == SessionStatus.MATCHED || it.status == SessionStatus.CANDIDATES ||
                (it.status == SessionStatus.COMPLETED && it.trackCount > 0u)
        }
    }

    private fun onListenState(state: ListenState) {
        val done = state.phase as? ListenPhase.Done ?: return
        val result = done.result
        // Listens from tiles and widgets answer with a notification; the stage stays idle.
        if (state.origin != ListenOrigin.App) {
            graph.listen.reset()
            return
        }
        if (result is ListenResult.Matched || result is ListenResult.Candidates) {
            result.sessionId?.let { _openSession.trySend(it) }
            graph.listen.reset()
        }
    }

    private fun onLinkState(state: AnalysisState) {
        val id = state.sessionId ?: return
        val single = state.segments.singleOrNull()
        val navigate = when {
            state.error != null || state.canceled -> false
            state.isCollection -> true
            state.segments.isNotEmpty() && !state.isTerminal -> true
            state.isTerminal -> state.identified > 0 || single != null || state.candidates.isNotEmpty()
            else -> false
        }
        if (navigate) {
            _openSession.trySend(id)
            _link.value = LinkStage.None
            return
        }
        if (state.isTerminal) {
            _link.value = when {
                state.canceled -> LinkStage.None
                state.error != null -> LinkStage.Failed(state.error.message)
                else -> LinkStage.Failed(getApplication<Application>().getString(app.musicfetch.R.string.state_no_match))
            }
        }
    }

    /** Start listening with the selected source; phone audio needs [grant]. */
    fun start(source: ListenSource, grant: ProjectionGrant? = null): Boolean {
        clearLink()
        viewModelScope.launch { graph.settings.setListenSource(source) }
        return when (source) {
            ListenSource.Microphone -> graph.listen.start(CaptureSource.MICROPHONE, ListenMode.AUTO, ListenOrigin.App)
            ListenSource.Hum -> graph.listen.start(CaptureSource.MICROPHONE, ListenMode.HUM, ListenOrigin.App)
            ListenSource.Phone -> grant != null &&
                graph.listen.start(CaptureSource.SYSTEM_AUDIO, ListenMode.RECORDING, ListenOrigin.App, grant)
        }
    }

    fun selectSource(source: ListenSource) {
        graph.listen.reset()
        clearLink()
        viewModelScope.launch { graph.settings.setListenSource(source) }
    }

    fun stop() = graph.listen.stop()

    fun cancel() = graph.listen.cancel()

    /** Clear a finished no-match or failure from the stage. */
    fun dismissResult() {
        graph.listen.reset()
        clearLink()
    }

    fun clearLink() {
        if (_link.value !is LinkStage.Running) _link.value = LinkStage.None
    }

    fun phoneAudioExplained() {
        viewModelScope.launch { graph.settings.setPhoneAudioExplained() }
    }

    fun micAsked() {
        viewModelScope.launch { graph.settings.setMicExplained() }
    }

    fun declineMelody() {
        viewModelScope.launch { graph.settings.setMelodyPromptDeclined(true) }
    }

    fun downloadMelody() = graph.melody.install()

    fun tileOffered() {
        viewModelScope.launch { graph.settings.setTileOffered() }
    }

    /** Identify a pasted or shared link. */
    fun analyze(input: String) {
        val text = input.trim()
        if (text.isEmpty()) return
        graph.listen.reset()
        _link.value = LinkStage.Preparing
        viewModelScope.launch {
            _link.value = try {
                LinkStage.Running(graph.analyses.analyze(text))
            } catch (e: MfException) {
                LinkStage.Failed(e.userMessage(getApplication()))
            }
        }
    }

    /** Identify a local audio or video file picked by the user. */
    fun analyzeFile(uri: Uri) {
        graph.listen.reset()
        _link.value = LinkStage.Preparing
        viewModelScope.launch {
            val file = FileImports.copy(getApplication(), uri)
            if (file == null) {
                _link.value = LinkStage.Failed(getApplication<Application>().getString(app.musicfetch.R.string.error_decode))
                return@launch
            }
            _link.value = try {
                LinkStage.Running(graph.analyses.analyze(file.path))
            } catch (e: MfException) {
                Log.w(TAG, "file analysis failed to start", e)
                file.delete()
                LinkStage.Failed(e.userMessage(getApplication()))
            }
        }
    }

    /** Stop the stage's link analysis. */
    fun stopLink() {
        val stage = _link.value
        if (stage is LinkStage.Running) stage.run.sessionId?.let { graph.analyses.cancel(it) }
        _link.value = LinkStage.None
    }

    private companion object {
        const val TAG = "ListenViewModel"
    }
}
