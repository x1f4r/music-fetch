package app.musicfetch

import android.app.Application
import android.os.Bundle
import android.os.SystemClock
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewModelScope
import app.musicfetch.core.PlatformException
import app.musicfetch.core.coreVersion
import app.musicfetch.ui.theme.MusicFetchTheme
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import java.io.File

/**
 * Pipeline check: shows the engine version and probes a YouTube link through
 * Chaquopy + yt-dlp. Placeholder until the real UI lands.
 *
 * `adb shell am start -n app.musicfetch/.MainActivity --ez auto_probe true
 * [--es url <link>] [--es player_client web_safari] [--ez download true]` runs the probe on
 * launch, and with `download` also fetches the audio (to the cache, deleted afterwards).
 */
class MainActivity : ComponentActivity() {
    private val viewModel: PipelineCheckViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        if (savedInstanceState == null && intent.getBooleanExtra(EXTRA_AUTO_PROBE, false)) {
            viewModel.probe(
                url = intent.getStringExtra(EXTRA_URL) ?: TEST_URL,
                playerClient = intent.getStringExtra(EXTRA_PLAYER_CLIENT),
                download = intent.getBooleanExtra(EXTRA_DOWNLOAD, false),
            )
        }
        setContent {
            MusicFetchTheme {
                val state by viewModel.state.collectAsStateWithLifecycle()
                PipelineCheckScreen(state, onProbe = { viewModel.probe(TEST_URL, playerClient = null) })
            }
        }
    }

    private companion object {
        const val TEST_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        const val EXTRA_AUTO_PROBE = "auto_probe"
        const val EXTRA_URL = "url"
        const val EXTRA_PLAYER_CLIENT = "player_client"
        const val EXTRA_DOWNLOAD = "download"
    }
}

data class PipelineState(
    val coreVersion: String? = null,
    val runtime: String? = null,
    val probe: ProbeState = ProbeState.Idle,
)

sealed interface ProbeState {
    data object Idle : ProbeState
    data object Running : ProbeState
    data class Done(
        val title: String,
        val duration: String,
        val uploader: String?,
        val seconds: Double,
        val download: String? = null,
    ) : ProbeState
    data class Failed(val message: String) : ProbeState
}

class PipelineCheckViewModel(application: Application) : AndroidViewModel(application) {
    private val backend = (application as MusicFetchApplication).mediaBackend
    private val _state = MutableStateFlow(PipelineState())
    val state: StateFlow<PipelineState> = _state.asStateFlow()

    init {
        viewModelScope.launch(Dispatchers.IO) {
            val version = runCatching { coreVersion() }
                .onFailure { Log.e(TAG, "Loading mfcore failed", it) }
                .getOrDefault("unavailable")
            Log.i(TAG, "mfcore $version")
            _state.update { it.copy(coreVersion = version) }

            val runtime = try {
                val info = backend.runtimeInfo()
                listOfNotNull(
                    "Python ${info.optString("python")}",
                    "yt-dlp ${info.optString("yt_dlp")}",
                    info.optString("js_runtime").takeIf { info.optBoolean("js_ok") },
                ).joinToString(" · ")
            } catch (e: PlatformException) {
                "Python unavailable: ${e.describe()}"
            }
            Log.i(TAG, "runtime $runtime")
            _state.update { it.copy(runtime = runtime) }
        }
    }

    fun probe(url: String, playerClient: String?, download: Boolean = false) {
        if (_state.value.probe == ProbeState.Running) return
        _state.update { it.copy(probe = ProbeState.Running) }
        viewModelScope.launch(Dispatchers.IO) {
            val started = SystemClock.elapsedRealtime()
            val extractorArgs = playerClient?.let {
                JSONObject().put("youtube", JSONObject().put("player_client", JSONArray().put(it)))
            }
            val result = try {
                val info = JSONObject(backend.probe(url, isCanceled = { !isActive }, extractorArgs = extractorArgs))
                ProbeState.Done(
                    title = info.optString("title", "Untitled"),
                    duration = formatDuration(info.optDouble("duration")),
                    uploader = info.optString("uploader").ifBlank { null },
                    seconds = (SystemClock.elapsedRealtime() - started) / 1000.0,
                    download = if (download) downloadCheck(url) { !isActive } else null,
                )
            } catch (e: PlatformException) {
                ProbeState.Failed(e.describe())
            } catch (e: JSONException) {
                Log.e(TAG, "Unreadable probe output", e)
                ProbeState.Failed("The media info could not be read.")
            }
            Log.i(TAG, "probe $url -> $result")
            _state.update { it.copy(probe = result) }
        }
    }

    /** Downloads the audio into the cache, describes the result and deletes it again. */
    private fun downloadCheck(url: String, isCanceled: () -> Boolean): String {
        val dir = File(getApplication<Application>().cacheDir, "pipeline-check")
        dir.deleteRecursively()
        val started = SystemClock.elapsedRealtime()
        var updates = 0
        var last = 0f
        return try {
            val file = File(backend.downloadAudio(url, dir.path, isCanceled) { updates++; last = it })
            val seconds = (SystemClock.elapsedRealtime() - started) / 1000.0
            "%s %.1f MB in %.1f s".format(file.extension, file.length() / 1_048_576.0, seconds).also {
                Log.i(TAG, "download ${file.name}: $it, $updates progress updates, last $last")
            }
        } catch (e: PlatformException) {
            "Download failed: ${e.describe()}"
        } finally {
            dir.deleteRecursively()
        }
    }

    private companion object {
        const val TAG = "MusicFetch"
    }
}

private fun PlatformException.describe(): String = when (this) {
    is PlatformException.Failed -> reason
    is PlatformException.Unavailable -> reason
    is PlatformException.Canceled -> "Canceled."
    is PlatformException.NotSupported -> "Not supported."
}

private fun formatDuration(seconds: Double): String {
    if (seconds.isNaN() || seconds < 0) return "–"
    val total = seconds.toLong()
    return if (total >= 3600) {
        "%d:%02d:%02d".format(total / 3600, total % 3600 / 60, total % 60)
    } else {
        "%d:%02d".format(total / 60, total % 60)
    }
}

@Composable
private fun PipelineCheckScreen(state: PipelineState, onProbe: () -> Unit) {
    Scaffold { padding ->
        Column(
            modifier = Modifier.fillMaxSize().padding(padding).padding(24.dp),
            verticalArrangement = Arrangement.Center,
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            Text(stringResource(R.string.app_name), style = MaterialTheme.typography.displaySmall)
            Text(
                stringResource(R.string.engine_version, state.coreVersion ?: "…"),
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            state.runtime?.let {
                Text(
                    it,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                )
            }
            Spacer(Modifier.height(32.dp))
            Button(onClick = onProbe, enabled = state.probe != ProbeState.Running) {
                Text(stringResource(R.string.probe_test_url))
            }
            Spacer(Modifier.height(24.dp))
            when (val probe = state.probe) {
                ProbeState.Idle -> Unit
                ProbeState.Running -> CircularProgressIndicator()
                is ProbeState.Done -> ProbeResult(probe)
                is ProbeState.Failed -> Text(
                    probe.message,
                    color = MaterialTheme.colorScheme.error,
                    textAlign = TextAlign.Center,
                )
            }
        }
    }
}

@Composable
private fun ProbeResult(result: ProbeState.Done) {
    Card(modifier = Modifier.widthIn(max = 480.dp).fillMaxWidth()) {
        Column(Modifier.padding(16.dp)) {
            Text(result.title, style = MaterialTheme.typography.titleMedium)
            Text(
                stringResource(R.string.probe_took, listOfNotNull(result.duration, result.uploader).joinToString(" · "), result.seconds),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            result.download?.let {
                Text(it, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
    }
}
