package app.musicfetch.engine

import android.content.Context
import android.os.Build
import android.util.Log
import app.musicfetch.BuildConfig
import app.musicfetch.core.Engine
import app.musicfetch.core.EngineConfig
import app.musicfetch.core.EngineInterface
import app.musicfetch.core.MfException
import app.musicfetch.platform.AndroidSpeechToText
import app.musicfetch.platform.ChaquopyMediaBackend
import app.musicfetch.platform.CredentialStore
import app.musicfetch.platform.MediaCodecDecoder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import java.io.File
import java.util.Locale

/** Lifecycle of the process-wide engine. */
sealed interface EngineState {
    data object Starting : EngineState
    data class Ready(val engine: EngineInterface) : EngineState
    data class Failed(val message: String) : EngineState
}

/** Thrown by [EngineHost.engine] when the engine could not be created. */
class EngineUnavailableException(message: String) : Exception(message)

/**
 * Owns the one [Engine] of the process (docs/APPS.md): builds the [EngineConfig], creates the
 * engine off the main thread with the platform services, and keeps its credentials in sync
 * with [CredentialStore].
 */
class EngineHost(
    context: Context,
    private val scope: CoroutineScope,
    private val credentials: CredentialStore,
) {
    private val appContext = context.applicationContext
    private val _state = MutableStateFlow<EngineState>(EngineState.Starting)

    val state: StateFlow<EngineState> = _state.asStateFlow()

    /** yt-dlp through Chaquopy; also the engine's `MediaBackend`. */
    val mediaBackend: ChaquopyMediaBackend by lazy { ChaquopyMediaBackend(appContext) }

    /** Create the engine in the background. Idempotent. */
    fun start() {
        if (started) return
        started = true
        scope.launch(Dispatchers.IO) {
            val result = runCatching { create() }
            val engine = result.getOrElse { error ->
                Log.e(TAG, "engine could not start", error)
                val message = (error as? MfException)?.message ?: error.javaClass.simpleName
                _state.value = EngineState.Failed(message)
                return@launch
            }
            runCatching { engine.setCredentials(credentials.current()) }
                .onFailure { Log.w(TAG, "credentials not applied", it) }
            _state.value = EngineState.Ready(engine)
            credentials.credentials.distinctUntilChanged().collect { engine.setCredentials(it) }
        }
    }

    @Volatile
    private var started = false

    /** The engine, waiting for it if it is still starting. */
    suspend fun engine(): EngineInterface {
        start()
        return when (val ready = state.filter { it !is EngineState.Starting }.first()) {
            is EngineState.Ready -> ready.engine
            is EngineState.Failed -> throw EngineUnavailableException(ready.message)
            EngineState.Starting -> error("unreachable")
        }
    }

    /** The engine if it is ready, without waiting. */
    fun engineOrNull(): EngineInterface? = (state.value as? EngineState.Ready)?.engine

    private fun create(): EngineInterface {
        val config = EngineConfig(
            dataDir = File(appContext.filesDir, "engine").path,
            cacheDir = File(appContext.cacheDir, "engine").path,
            country = Locale.getDefault().country.ifBlank { DEFAULT_COUNTRY },
            language = Locale.getDefault().toLanguageTag(),
            appVersion = BuildConfig.VERSION_NAME,
            mediaTools = null,
            melodyIndexManifestUrl = MELODY_MANIFEST_URL,
        )
        DebugEngines.override(appContext, config)?.let { return it }
        val speech = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) AndroidSpeechToText(appContext) else null
        return Engine(config, mediaBackend, MediaCodecDecoder(), speech)
    }

    companion object {
        private const val TAG = "EngineHost"
        private const val DEFAULT_COUNTRY = "US"

        /** Manifest of the downloadable melody index (docs/CORE_API.md, "Melody index"). */
        const val MELODY_MANIFEST_URL =
            "https://github.com/x1f4r/music-fetch/releases/download/melody-index-1/manifest.json"
    }
}
