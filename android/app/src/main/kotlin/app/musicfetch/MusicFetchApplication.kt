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
import app.musicfetch.engine.WaitingRetry
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.CredentialStore
import app.musicfetch.platform.LowPower
import app.musicfetch.platform.ProviderPreference
import app.musicfetch.platform.Providers
import app.musicfetch.platform.SettingsStore
import coil3.ImageLoader
import coil3.PlatformContext
import coil3.SingletonImageLoader
import coil3.disk.DiskCache
import coil3.memory.MemoryCache
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import okio.Path.Companion.toOkioPath

class MusicFetchApplication :
    Application(),
    SingletonImageLoader.Factory {
    /** Process-wide services; created in [onCreate]. */
    lateinit var graph: AppGraph
        private set

    override fun onCreate() {
        super.onCreate()
        graph = AppGraph(this)
        graph.lowPower.observe()
        Notifications.createChannels(this)
        // Engine creation runs off the main thread so it doesn't delay the first frame.
        graph.engineHost.start()
        // Python and yt-dlp take a few seconds and tens of MB to start. Phones with plenty of
        // headroom start them now so the first link is fast; on the low-power path they start
        // when a link is about to be identified.
        if (!graph.lowPower.enabled.value) graph.engineHost.mediaBackend.warmUp()
        graph.waitingRetry.start()
    }

    /** Artwork only: small caches, smaller still on weak phones. */
    override fun newImageLoader(context: PlatformContext): ImageLoader {
        val weak = graph.lowPower.weakHardware
        return ImageLoader.Builder(context)
            .memoryCache {
                MemoryCache.Builder().maxSizePercent(context, if (weak) 0.08 else 0.15).build()
            }
            .diskCache {
                DiskCache.Builder()
                    .directory(cacheDir.resolve("image_cache").toOkioPath())
                    .maxSizeBytes(if (weak) 32L * 1024 * 1024 else 96L * 1024 * 1024)
                    .build()
            }
            .build()
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
    val lowPower = LowPower(app)

    /** Settings, "Also search lyrics you sing", readable from any thread. */
    val sungLyricsEnabled: StateFlow<Boolean> = settings.settings.map { it.sungLyrics }
        .distinctUntilChanged()
        .stateIn(scope, SharingStarted.Eagerly, true)
    val engineHost = EngineHost(app, scope, credentials, lowPower) { sungLyricsEnabled.value }
    val listen = ListenController(app, scope, engineHost, settings)
    val analyses = AnalysisController(app, scope, engineHost, settings)
    val melody = MelodyLibrary(scope, engineHost)
    val deletions = Deletions(scope, engineHost) { notifyLibraryChanged() }
    val waitingRetry = WaitingRetry(app, scope, engineHost)

    /**
     * Where results open (docs/APPS.md, "Opening links"); before the user decided, what the
     * first-launch panel would pre-select. Null only until the settings are read.
     */
    val preference: StateFlow<ProviderPreference?> = settings.settings
        .map { it.preference ?: Providers.undecided(app) }
        .distinctUntilChanged()
        .stateIn(scope, SharingStarted.Eagerly, null)

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
