package app.musicfetch.ui.lyrics

import android.app.Application
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LinearWavyProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.ui.common.tag
import app.musicfetch.analysis.AnalysisState
import app.musicfetch.appGraph
import app.musicfetch.core.Candidate
import app.musicfetch.core.MfException
import app.musicfetch.ui.common.userMessage
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.CandidateTierTag
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.SessionRoute
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Typed lyric search (docs/design/DESIGN.md §6.7). */
class LyricSearchViewModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph
    private val run = MutableStateFlow<app.musicfetch.analysis.AnalysisRun?>(null)
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()

    @OptIn(ExperimentalCoroutinesApi::class)
    val state: StateFlow<AnalysisState?> = run.flatMapLatest { it?.state ?: flowOf(null) }
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    fun search(text: String) {
        if (text.isBlank()) return
        _error.value = null
        viewModelScope.launch {
            try {
                run.value = graph.analyses.searchLyrics(text)
            } catch (e: MfException) {
                _error.value = e.userMessage(getApplication())
            }
        }
    }

    /** Pick a result: record it on the lyric session and open it. */
    fun choose(candidate: Candidate, open: (String) -> Unit) {
        val id = state.value?.sessionId ?: return
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            withContext(Dispatchers.IO) { runCatching { engine.confirmCandidate(id, candidate.track) } }
            graph.notifyLibraryChanged()
            open(id)
        }
    }

    fun clear() {
        run.value = null
        _error.value = null
    }
}

@Composable
fun LyricSearchScreen(viewModel: LyricSearchViewModel = viewModel()) {
    val navigator = LocalNavigator.current
    val keyboard = LocalSoftwareKeyboardController.current
    val state by viewModel.state.collectAsStateWithLifecycle()
    val error by viewModel.error.collectAsStateWithLifecycle()
    var text by rememberSaveable { mutableStateOf("") }
    val focus = remember { FocusRequester() }
    LaunchedEffect(Unit) { if (state == null) focus.requestFocus() }
    fun submit() {
        keyboard?.hide()
        viewModel.search(text)
    }
    Column(Modifier.fillMaxSize().windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Horizontal))) {
        Surface(
            shape = RoundedCornerShape(28.dp),
            color = MaterialTheme.colorScheme.surfaceContainerHigh,
            modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp).height(56.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(horizontal = 4.dp)) {
                IconButton(onClick = { navigator.back() }) {
                    Icon(painterResource(R.drawable.ic_arrow_back), contentDescription = stringResource(R.string.back))
                }
                Box(Modifier.weight(1f), contentAlignment = Alignment.CenterStart) {
                    if (text.isEmpty()) {
                        Text(
                            stringResource(R.string.lyrics_prompt),
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                    BasicTextField(
                        value = text,
                        onValueChange = { text = it },
                        singleLine = true,
                        textStyle = MaterialTheme.typography.bodyLarge.copy(color = MaterialTheme.colorScheme.onSurface),
                        cursorBrush = SolidColor(MaterialTheme.colorScheme.primary),
                        keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                        keyboardActions = KeyboardActions(onSearch = { submit() }),
                        modifier = Modifier.fillMaxWidth().focusRequester(focus).tag("lyrics.field"),
                    )
                }
                if (text.isNotEmpty()) {
                    IconButton(onClick = {
                        text = ""
                        viewModel.clear()
                        focus.requestFocus()
                    }) {
                        Icon(painterResource(R.drawable.ic_close), contentDescription = stringResource(R.string.settings_clear))
                    }
                }
                // Search without the keyboard's action key (mirrors the link field).
                if (text.isNotBlank()) {
                    IconButton(onClick = ::submit, modifier = Modifier.tag("lyrics.search")) {
                        Icon(painterResource(R.drawable.ic_search), contentDescription = stringResource(R.string.lyrics_search))
                    }
                }
            }
        }
        val current = state
        when {
            error != null -> Message(error ?: "")
            current == null -> Unit
            !current.isTerminal && current.candidates.isEmpty() -> Column(
                Modifier.fillMaxWidth().padding(24.dp),
                horizontalAlignment = Alignment.CenterHorizontally,
            ) {
                LinearWavyProgressIndicator(modifier = Modifier.width(120.dp))
                Spacer(Modifier.height(12.dp))
                Text(stringResource(R.string.lyrics_searching), color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
            current.candidates.isEmpty() -> Column(Modifier.fillMaxWidth().padding(24.dp), horizontalAlignment = Alignment.CenterHorizontally) {
                Text(stringResource(R.string.lyrics_empty), style = MaterialTheme.typography.titleMedium)
                TextButton(onClick = {
                    viewModel.clear()
                    focus.requestFocus()
                }) { Text(stringResource(R.string.lyrics_try_different)) }
            }
            else -> LazyColumn(
                contentPadding = PaddingValues(16.dp),
                verticalArrangement = Arrangement.spacedBy(8.dp),
                modifier = Modifier.fillMaxSize(),
            ) {
                items(current.candidates, key = { it.track.identityKey.ifEmpty { it.track.title + it.track.artist } }) { candidate ->
                    Surface(
                        onClick = { viewModel.choose(candidate) { id -> navigator.push(SessionRoute(id)) } },
                        shape = RoundedCornerShape(16.dp),
                        color = MaterialTheme.colorScheme.surfaceContainerLow,
                        modifier = Modifier.fillMaxWidth().animateItem(),
                    ) {
                        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(12.dp)) {
                            Artwork(candidate.track.artworkUrl, 56.dp, corner = 8.dp)
                            Column(Modifier.weight(1f).padding(horizontal = 12.dp)) {
                                Text(candidate.track.title, style = MaterialTheme.typography.titleMedium, maxLines = 1, overflow = TextOverflow.Ellipsis)
                                Text(
                                    candidate.track.artist,
                                    style = MaterialTheme.typography.bodyMedium,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                    maxLines = 1,
                                    overflow = TextOverflow.Ellipsis,
                                )
                            }
                            CandidateTierTag(candidate.tier)
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun Message(text: String) {
    Text(
        text,
        style = MaterialTheme.typography.bodyLarge,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
        modifier = Modifier.padding(24.dp),
    )
}
