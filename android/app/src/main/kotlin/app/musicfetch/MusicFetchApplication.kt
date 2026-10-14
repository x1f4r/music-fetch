package app.musicfetch

import android.app.Application
import android.content.Context
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.ProcessLifecycleOwner
import app.musicfetch.analysis.AnalysisController
import app.musicfetch.analysis.Deletions
import app.musicfetch.analysis.SegmentFix
import app.musicfetch.capture.ListenController
import app.musicfetch.engine.EngineHost
import app.musicfetch.engine.MelodyLibrary
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.CredentialStore
import app.musicfetch.platform.SettingsStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow

class MusicFetchApplication : Application() {
    /** Process-wide services; created in [onCreate]. */
    lateinit var graph: AppGraph
        private set

    override fun onCreate() {
        super.onCreate()
        graph = AppGraph(this)
        Notifications.createChannels(this)
        // Engine creation and the Python/yt-dlp import run off the main thread so they
        // don't delay the first frame.
        graph.engineHost.start()
        graph.engineHost.mediaBackend.warmUp()
    }
}

/**
 * The services every screen, service and tile shares. One instance per process, owned by
 * [MusicFetchApplication].
 */
class AppGraph(val app: Application) {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    val settings = SettingsStore(app)
    val credentials = CredentialStore(app)
    val engineHost = EngineHost(app, scope, credentials)
    val listen = ListenController(app, scope, engineHost, settings)
    val analyses = AnalysisController(app, scope, engineHost, settings)
    val melody = MelodyLibrary(scope, engineHost)
    val deletions = Deletions(scope, engineHost) { notifyLibraryChanged() }

    /** Set by "Hum it" on an unidentified mix segment; the next confirmed hum fills it in. */
    val pendingSegmentFix = MutableStateFlow<SegmentFix?>(null)

    private val _libraryChanged = MutableSharedFlow<Unit>(extraBufferCapacity = 8)

    /** Emits when a session finished or was edited, so lists can refresh. */
    val libraryChanged: SharedFlow<Unit> = _libraryChanged.asSharedFlow()

    fun notifyLibraryChanged() {
        _libraryChanged.tryEmit(Unit)
    }

    /** Whether any of the app's activities is visible. */
    val isAppVisible: Boolean
        get() = ProcessLifecycleOwner.get().lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED)
}

/** The process-wide [AppGraph]. */
val Context.appGraph: AppGraph
    get() = (applicationContext as MusicFetchApplication).graph
