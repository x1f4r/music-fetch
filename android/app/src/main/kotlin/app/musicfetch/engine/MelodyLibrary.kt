package app.musicfetch.engine

import android.util.Log
import app.musicfetch.core.AnalysisSession
import app.musicfetch.core.MelodyIndexStatus
import app.musicfetch.core.MfException
import app.musicfetch.core.Phase
import app.musicfetch.core.SessionEvent
import app.musicfetch.core.SessionObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/** The melody-library download, as Settings and the Listen prompt show it. */
sealed interface MelodyDownload {
    data object Idle : MelodyDownload

    /** [fraction] is null until the size is known. */
    data class Running(val fraction: Float?) : MelodyDownload

    data class Failed(val message: String) : MelodyDownload
}

/**
 * Installs, updates and removes the downloadable melody index (docs/CORE_API.md, "Melody
 * index"). One instance per process so a download started from the Listen prompt shows its
 * progress in Settings too.
 */
class MelodyLibrary(private val scope: CoroutineScope, private val engineHost: EngineHost) {
    private val _status = MutableStateFlow<MelodyIndexStatus?>(null)

    /** Installed index and, after [check], the newest published one. Null until read. */
    val status: StateFlow<MelodyIndexStatus?> = _status.asStateFlow()

    private val _download = MutableStateFlow<MelodyDownload>(MelodyDownload.Idle)
    val download: StateFlow<MelodyDownload> = _download.asStateFlow()

    private val _checkFailed = MutableStateFlow(false)

    /** The last manifest check could not reach the server. */
    val checkFailed: StateFlow<Boolean> = _checkFailed.asStateFlow()

    @Volatile
    private var session: AnalysisSession? = null

    /** Re-read the installed index (no network). */
    fun refresh() {
        scope.launch(Dispatchers.IO) {
            val engine = runCatching { engineHost.engine() }.getOrNull() ?: return@launch
            _status.value = engine.melodyIndexStatus()
        }
    }

    /** Fetch the manifest: learns the download size and whether an update exists. */
    fun check() {
        scope.launch(Dispatchers.IO) {
            val engine = runCatching { engineHost.engine() }.getOrNull() ?: return@launch
            try {
                _status.value = engine.checkMelodyIndex()
                _checkFailed.value = false
            } catch (e: MfException) {
                Log.i(TAG, "manifest check failed: ${e.message}")
                _status.value = engine.melodyIndexStatus()
                _checkFailed.value = true
            }
        }
    }

    /** Download and install (or update) the index. */
    fun install() {
        if (_download.value is MelodyDownload.Running) return
        _download.value = MelodyDownload.Running(null)
        scope.launch(Dispatchers.IO) {
            val engine = try {
                engineHost.engine()
            } catch (e: EngineUnavailableException) {
                _download.value = MelodyDownload.Failed(e.message ?: "")
                return@launch
            }
            val observer = object : SessionObserver {
                override fun onEvent(event: SessionEvent) {
                    when (event) {
                        is SessionEvent.Phase -> if (event.phase == Phase.DOWNLOADING) {
                            _download.value = MelodyDownload.Running(event.fraction)
                        }
                        is SessionEvent.Finished -> {
                            session = null
                            _download.value = MelodyDownload.Idle
                            _status.value = engine.melodyIndexStatus()
                        }
                        is SessionEvent.Failed -> {
                            session = null
                            _download.value = MelodyDownload.Failed(event.message)
                            _status.value = engine.melodyIndexStatus()
                        }
                        SessionEvent.Canceled -> {
                            session = null
                            _download.value = MelodyDownload.Idle
                        }
                        else -> Unit
                    }
                }
            }
            try {
                session = engine.installMelodyIndex(observer)
            } catch (e: MfException) {
                Log.w(TAG, "install failed to start", e)
                _download.value = MelodyDownload.Failed(e.message ?: "")
            }
        }
    }

    fun cancel() {
        session?.let { runCatching { it.cancel() } }
    }

    fun remove() {
        scope.launch(Dispatchers.IO) {
            val engine = runCatching { engineHost.engine() }.getOrNull() ?: return@launch
            try {
                engine.removeMelodyIndex()
            } catch (e: MfException) {
                Log.w(TAG, "remove failed", e)
            }
            _status.value = engine.melodyIndexStatus()
        }
    }

    private companion object {
        const val TAG = "MelodyLibrary"
    }
}
