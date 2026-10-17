package app.musicfetch.share

import android.Manifest
import android.app.Application
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.util.Patterns
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.Icon
import androidx.compose.material3.LinearWavyProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.SheetValue
import androidx.compose.material3.rememberBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.core.content.IntentCompat
import androidx.core.net.toUri
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewModelScope
import app.musicfetch.MainActivity
import app.musicfetch.R
import app.musicfetch.analysis.AnalysisRun
import app.musicfetch.analysis.AnalysisState
import app.musicfetch.appGraph
import app.musicfetch.core.MfException
import app.musicfetch.core.Phase
import app.musicfetch.core.SegmentKind
import app.musicfetch.core.Track
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.FileImports
import app.musicfetch.platform.ProviderPreference
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.common.userMessage
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.OpenInSplitButton
import app.musicfetch.ui.theme.MusicFetchTheme
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlin.math.ceil

/** What was shared: a link or a file. */
sealed interface SharedItem {
    data class Link(val url: String) : SharedItem

    data class File(val uri: Uri, val name: String) : SharedItem
}

/** Starts the shared item's analysis; the analysis outlives this screen (it runs in AnalysisService). */
class ShareViewModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph
    private val run = MutableStateFlow<AnalysisRun?>(null)
    private val _item = MutableStateFlow<SharedItem?>(null)
    val item: StateFlow<SharedItem?> = _item.asStateFlow()

    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()

    private val _nothing = MutableStateFlow(false)

    /** The shared text held no link. */
    val nothing: StateFlow<Boolean> = _nothing.asStateFlow()

    @OptIn(ExperimentalCoroutinesApi::class)
    val state: StateFlow<AnalysisState?> = run.flatMapLatest { it?.state ?: flowOf(null) }
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    /** Where results open; null until the settings are read. */
    val preference: StateFlow<ProviderPreference?> = graph.preference

    fun handle(intent: Intent) {
        if (_item.value != null) return
        val item = parse(intent)
        if (item == null) {
            _nothing.value = true
            return
        }
        _item.value = item
        start(item)
    }

    fun retry() {
        val item = _item.value ?: return
        _error.value = null
        run.value = null
        start(item)
    }

    fun stop() {
        state.value?.sessionId?.let { graph.analyses.cancel(it) }
    }

    private fun start(item: SharedItem) {
        viewModelScope.launch {
            try {
                run.value = when (item) {
                    is SharedItem.Link -> graph.analyses.analyze(item.url)
                    is SharedItem.File -> {
                        val file = FileImports.copy(getApplication(), item.uri)
                        if (file == null) {
                            _error.value = getApplication<Application>().getString(R.string.error_decode)
                            return@launch
                        }
                        graph.analyses.analyze(file.path)
                    }
                }
            } catch (e: MfException) {
                _error.value = e.userMessage(getApplication())
            }
        }
    }

    private fun parse(intent: Intent): SharedItem? {
        if (intent.action != Intent.ACTION_SEND) return null
        val type = intent.type.orEmpty()
        if (type.startsWith("audio/") || type.startsWith("video/")) {
            val uri = IntentCompat.getParcelableExtra(intent, Intent.EXTRA_STREAM, Uri::class.java) ?: return null
            val name = FileImports.displayName(getApplication(), uri) ?: uri.lastPathSegment ?: "audio"
            return SharedItem.File(uri, name)
        }
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: return null
        val matcher = Patterns.WEB_URL.matcher(text)
        while (matcher.find()) {
            val candidate = matcher.group()
            if (candidate.startsWith("http://", ignoreCase = true) || candidate.startsWith("https://", ignoreCase = true)) {
                return SharedItem.Link(candidate)
            }
        }
        return null
    }
}

/**
 * The share sheet (docs/design/DESIGN.md §6.11): identifies a shared link or audio/video file in
 * a bottom sheet over the app that shared it.
 */
class ShareIdentifyActivity : ComponentActivity() {
    private val viewModel: ShareViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        viewModel.handle(intent)
        setContent {
            MusicFetchTheme {
                val notifications = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { }
                LaunchedEffect(Unit) {
                    // The first background analysis asks for notifications so its result can arrive later.
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU && !Notifications.canPost(this@ShareIdentifyActivity)) {
                        notifications.launch(Manifest.permission.POST_NOTIFICATIONS)
                    }
                }
                ShareSheet(viewModel, onClose = { finish() }, onOpenApp = ::openApp)
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        viewModel.handle(intent)
    }

    private fun openApp(uri: String) {
        startActivity(Intent(Intent.ACTION_VIEW, uri.toUri(), this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
        finish()
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ShareSheet(viewModel: ShareViewModel, onClose: () -> Unit, onOpenApp: (String) -> Unit) {
    val item by viewModel.item.collectAsStateWithLifecycle()
    val state by viewModel.state.collectAsStateWithLifecycle()
    val error by viewModel.error.collectAsStateWithLifecycle()
    val nothing by viewModel.nothing.collectAsStateWithLifecycle()
    val preference by viewModel.preference.collectAsStateWithLifecycle()
    val sheet = rememberBottomSheetState(initialValue = SheetValue.Hidden, enabledValues = setOf(SheetValue.Hidden, SheetValue.Expanded))
    ModalBottomSheet(
        onDismissRequest = onClose,
        sheetState = sheet,
        containerColor = MaterialTheme.colorScheme.surfaceContainerLow,
        shape = RoundedCornerShape(topStart = 28.dp, topEnd = 28.dp),
    ) {
        Column(
            Modifier.fillMaxWidth().padding(horizontal = 24.dp).padding(bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            item?.let { SourceRow(it) }
            val current = state
            val sessionId = current?.sessionId
            // One recording that turned out to be one track is a result, even when it is long.
            val music = current?.segments?.filter { it.kind == SegmentKind.TRACK || it.kind == SegmentKind.UNIDENTIFIED }.orEmpty()
            val single = music.singleOrNull()?.track?.takeIf { (current?.source?.itemCount?.toInt() ?: 1) <= 1 }
            when {
                nothing -> Text(stringResource(R.string.share_nothing), style = MaterialTheme.typography.titleMedium)
                error != null -> Failure(error ?: "", onRetry = viewModel::retry)
                current == null -> Working(null, onStop = onClose)
                current.error != null -> Failure(current.error.message, onRetry = viewModel::retry)
                current.canceled -> Text(stringResource(R.string.library_canceled), style = MaterialTheme.typography.titleMedium)
                single != null && sessionId != null && current.isTerminal -> Result(
                    track = single,
                    preference = preference,
                    onOpenApp = { onOpenApp("musicfetch://session/$sessionId") },
                )
                current.isCollection && sessionId != null && current.identified > 0 ->
                    Collection(current, onOpen = { onOpenApp("musicfetch://session/$sessionId") }, onStop = viewModel::stop)
                single != null && sessionId != null && !current.isCollection -> Result(
                    track = single,
                    preference = preference,
                    onOpenApp = { onOpenApp("musicfetch://session/$sessionId") },
                )
                current.isTerminal -> NoMatch(
                    onHum = { onOpenApp("musicfetch://listen?mode=hum") },
                    onLyric = { onOpenApp("musicfetch://lyrics") },
                )
                else -> Working(current, onStop = viewModel::stop)
            }
        }
    }
}

@Composable
private fun SourceRow(item: SharedItem) {
    Row(verticalAlignment = Alignment.CenterVertically) {
        Icon(
            painterResource(if (item is SharedItem.Link) R.drawable.ic_link else R.drawable.ic_audio_file),
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(20.dp),
        )
        Spacer(Modifier.width(12.dp))
        Text(
            when (item) {
                is SharedItem.Link -> Formatters.shortUrl(item.url)
                is SharedItem.File -> item.name
            },
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
    }
}

@Composable
private fun Working(state: AnalysisState?, onStop: () -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        val fraction = when {
            state == null -> null
            state.totalMs > 0 && state.analyzedMs > 0 -> (state.analyzedMs.toFloat() / state.totalMs).coerceIn(0f, 1f)
            state.phase == Phase.DOWNLOADING -> state.fraction
            else -> null
        }
        if (fraction != null) {
            LinearWavyProgressIndicator(progress = { fraction }, modifier = Modifier.fillMaxWidth())
        } else {
            LinearWavyProgressIndicator(modifier = Modifier.fillMaxWidth())
        }
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text(
                workingText(state),
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(1f),
            )
            TextButton(onClick = onStop) { Text(stringResource(R.string.action_stop)) }
        }
    }
}

@Composable
private fun workingText(state: AnalysisState?): String = when (state?.phase) {
    null, Phase.RESOLVING, Phase.LISTENING -> stringResource(R.string.state_resolving)
    Phase.DOWNLOADING -> stringResource(R.string.state_downloading)
    Phase.DECODING, Phase.SEGMENTING -> stringResource(R.string.state_preparing)
    Phase.RECOGNIZING, Phase.MATCHING_MELODY, Phase.SEARCHING_LYRICS, Phase.FINISHING -> {
        val eta = state.etaMs
        if (state.isCollection && eta != null) {
            val left = if (eta < 60_000) {
                stringResource(R.string.mix_eta_under_minute)
            } else {
                val minutes = ceil(eta / 60_000.0).toInt()
                pluralStringResource(R.plurals.mix_eta, minutes, minutes)
            }
            stringResource(R.string.share_analyzing_eta, left.replaceFirstChar { it.lowercase() })
        } else {
            stringResource(R.string.state_matching)
        }
    }
}

@Composable
private fun Result(track: Track, preference: ProviderPreference?, onOpenApp: () -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(16.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Artwork(track.artworkUrl, 64.dp, corner = 12.dp)
            Column(Modifier.padding(start = 16.dp)) {
                Text(track.title, style = MaterialTheme.typography.titleLargeEmphasized, maxLines = 2, overflow = TextOverflow.Ellipsis)
                Text(track.artist, style = MaterialTheme.typography.bodyLarge, color = MaterialTheme.colorScheme.onSurfaceVariant, maxLines = 1)
                val meta = listOfNotNull(track.album?.takeIf { it.isNotBlank() }, track.releaseYear?.toString()).joinToString(" · ")
                if (meta.isNotEmpty()) {
                    Text(meta, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant, maxLines = 1)
                }
            }
        }
        Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
            preference?.let { OpenInSplitButton(track, it) }
            Spacer(Modifier.height(4.dp))
            TextButton(onClick = onOpenApp) { Text(stringResource(R.string.share_open_in_app)) }
        }
    }
}

@Composable
private fun Collection(state: AnalysisState, onOpen: () -> Unit, onStop: () -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        if (!state.isTerminal) Working(state, onStop)
        val found = state.identified
        Text(
            if (state.isTerminal) {
                pluralStringResource(R.plurals.tracks, found, found)
            } else {
                pluralStringResource(R.plurals.share_tracks_found, found, found)
            },
            style = MaterialTheme.typography.titleMedium,
        )
        Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
            FilledTonalButton(onClick = onOpen) { Text(stringResource(R.string.share_open_in_app)) }
        }
    }
}

@Composable
private fun NoMatch(onHum: () -> Unit, onLyric: () -> Unit) {
    Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Text(stringResource(R.string.state_no_match), style = MaterialTheme.typography.titleMedium, textAlign = TextAlign.Center)
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            FilledTonalButton(onClick = onHum) { Text(stringResource(R.string.action_hum_it)) }
            TextButton(onClick = onLyric) { Text(stringResource(R.string.action_type_lyric)) }
        }
    }
}

@Composable
private fun Failure(message: String, onRetry: () -> Unit) {
    Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Text(message, style = MaterialTheme.typography.titleMedium, textAlign = TextAlign.Center)
        FilledTonalButton(onClick = onRetry) { Text(stringResource(R.string.action_try_again)) }
    }
}
