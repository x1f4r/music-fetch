package app.musicfetch.ui.library

import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.FilterChip
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarDuration
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.SnackbarResult
import androidx.compose.material3.Surface
import androidx.compose.material3.SwipeToDismissBox
import androidx.compose.material3.SwipeToDismissBoxValue
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.rememberSwipeToDismissBoxState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionStatus
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.components.HatchedTile
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.SessionRoute
import app.musicfetch.ui.navigation.SettingsRoute
import app.musicfetch.ui.theme.StandardMotion
import app.musicfetch.ui.theme.TabularNumbers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.time.LocalDate

/** The library (docs/design/DESIGN.md §6.8, Android). */
@OptIn(ExperimentalFoundationApi::class)
@Composable
fun LibraryScreen(viewModel: LibraryViewModel = viewModel()) {
    val navigator = LocalNavigator.current
    val context = LocalContext.current
    val entries by viewModel.entries.collectAsStateWithLifecycle()
    val loaded by viewModel.loaded.collectAsStateWithLifecycle()
    val filter by viewModel.filter.collectAsStateWithLifecycle()
    val search by viewModel.search.collectAsStateWithLifecycle()
    val pending by viewModel.deletions.latest.collectAsStateWithLifecycle()
    val snackbar = remember { SnackbarHostState() }
    var confirm by remember { mutableStateOf<LibraryEntry?>(null) }
    val deletedLabel = stringResource(R.string.session_deleted)
    val undoLabel = stringResource(R.string.session_undo)
    LaunchedEffect(pending) {
        val deletion = pending ?: return@LaunchedEffect
        val result = withTimeoutOrNull(5_000) {
            snackbar.showSnackbar(deletedLabel, actionLabel = undoLabel, duration = SnackbarDuration.Indefinite)
        }
        if (result == SnackbarResult.ActionPerformed) viewModel.deletions.undo(deletion) else viewModel.deletions.acknowledge(deletion)
        snackbar.currentSnackbarData?.dismiss()
    }
    val listState = rememberLazyListState()
    val nearEnd by remember { derivedStateOf { listState.layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: 0 } }
    LaunchedEffect(nearEnd, entries.size) { if (nearEnd >= entries.size - 10) viewModel.loadMore() }

    StandardMotion {
        Scaffold(snackbarHost = { SnackbarHost(snackbar) }, contentWindowInsets = WindowInsets(0)) { padding ->
            Column(
                Modifier
                    .fillMaxSize()
                    .padding(padding)
                    .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Horizontal)),
            ) {
                SearchField(
                    text = search,
                    onText = viewModel::setSearch,
                    onSettings = { navigator.push(SettingsRoute) },
                    modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
                )
                LazyRow(
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    contentPadding = PaddingValues(horizontal = 16.dp),
                    modifier = Modifier.padding(bottom = 4.dp),
                ) {
                    items(LibraryFilter.entries) { option ->
                        FilterChip(
                            selected = filter == option,
                            onClick = { viewModel.setFilter(option) },
                            label = { Text(stringResource(filterLabel(option))) },
                            modifier = Modifier.height(32.dp),
                        )
                    }
                }
                if (loaded && entries.isEmpty()) {
                    if (search.isBlank() && filter == LibraryFilter.All) EmptyLibrary() else NoResults()
                } else {
                    val today = remember { LocalDate.now() }
                    val groups = remember(entries) { entries.groupBy { Formatters.day(it.createdAtMs) } }
                    LazyColumn(state = listState, contentPadding = PaddingValues(bottom = 24.dp), modifier = Modifier.fillMaxSize()) {
                        groups.forEach { (day, dayEntries) ->
                            stickyHeader(key = "day-$day") {
                                Text(
                                    when (day) {
                                        today -> stringResource(R.string.library_today)
                                        today.minusDays(1) -> stringResource(R.string.library_yesterday)
                                        else -> Formatters.longDate(context, dayEntries.first().createdAtMs, today)
                                    },
                                    style = MaterialTheme.typography.titleSmall,
                                    color = MaterialTheme.colorScheme.primary,
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .background(MaterialTheme.colorScheme.surface)
                                        .padding(start = 16.dp, end = 16.dp, top = 16.dp, bottom = 8.dp),
                                )
                            }
                            items(dayEntries, key = { it.id }) { entry ->
                                SwipeRow(
                                    entry = entry,
                                    onPin = { viewModel.setPinned(entry, !entry.pinned) },
                                    onDelete = {
                                        if (viewModel.needsConfirmation(entry)) confirm = entry else viewModel.delete(entry)
                                    },
                                    modifier = Modifier.animateItem(),
                                ) {
                                    LibraryRow(entry, onClick = { navigator.push(SessionRoute(entry.id)) })
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    confirm?.let { entry ->
        AlertDialog(
            onDismissRequest = { confirm = null },
            title = { Text(stringResource(R.string.session_delete_confirm_title, entry.title)) },
            text = { Text(stringResource(R.string.session_delete_confirm_message)) },
            confirmButton = {
                TextButton(onClick = {
                    confirm = null
                    viewModel.deleteNow(entry)
                }) { Text(stringResource(R.string.session_delete), color = MaterialTheme.colorScheme.error) }
            },
            dismissButton = { TextButton(onClick = { confirm = null }) { Text(stringResource(R.string.session_cancel)) } },
        )
    }
}

private fun filterLabel(filter: LibraryFilter): Int = when (filter) {
    LibraryFilter.All -> R.string.library_filter_all
    LibraryFilter.Heard -> R.string.library_filter_heard
    LibraryFilter.Hummed -> R.string.library_filter_hummed
    LibraryFilter.Links -> R.string.library_filter_links
    LibraryFilter.Mixes -> R.string.library_filter_mixes
}

@Composable
private fun SearchField(text: String, onText: (String) -> Unit, onSettings: () -> Unit, modifier: Modifier = Modifier) {
    val keyboard = LocalSoftwareKeyboardController.current
    var value by rememberSaveable { mutableStateOf(text) }
    Surface(
        shape = RoundedCornerShape(28.dp),
        color = MaterialTheme.colorScheme.surfaceContainerHigh,
        modifier = modifier.fillMaxWidth().height(56.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(start = 16.dp, end = 4.dp)) {
            Icon(painterResource(R.drawable.ic_search), contentDescription = null, tint = MaterialTheme.colorScheme.onSurfaceVariant)
            Spacer(Modifier.width(16.dp))
            Box(Modifier.weight(1f), contentAlignment = Alignment.CenterStart) {
                if (value.isEmpty()) {
                    Text(
                        stringResource(R.string.library_search),
                        style = MaterialTheme.typography.bodyLarge,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                BasicTextField(
                    value = value,
                    onValueChange = {
                        value = it
                        onText(it)
                    },
                    singleLine = true,
                    textStyle = MaterialTheme.typography.bodyLarge.copy(color = MaterialTheme.colorScheme.onSurface),
                    cursorBrush = SolidColor(MaterialTheme.colorScheme.primary),
                    keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                    keyboardActions = KeyboardActions(onSearch = { keyboard?.hide() }),
                    modifier = Modifier.fillMaxWidth(),
                )
            }
            if (value.isNotEmpty()) {
                IconButton(onClick = {
                    value = ""
                    onText("")
                }) {
                    Icon(painterResource(R.drawable.ic_close), contentDescription = stringResource(R.string.settings_clear))
                }
            }
            IconButton(onClick = onSettings) {
                Icon(painterResource(R.drawable.ic_settings), contentDescription = stringResource(R.string.settings_title))
            }
        }
    }
}

/** Swipe start→end pins, end→start deletes (docs/design/DESIGN.md §6.8). */
@Composable
private fun SwipeRow(
    entry: LibraryEntry,
    onPin: () -> Unit,
    onDelete: () -> Unit,
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit,
) {
    val state = rememberSwipeToDismissBoxState()
    val scope = rememberCoroutineScope()
    SwipeToDismissBox(
        state = state,
        modifier = modifier,
        onDismiss = { value ->
            when (value) {
                SwipeToDismissBoxValue.StartToEnd -> onPin()
                SwipeToDismissBoxValue.EndToStart -> onDelete()
                SwipeToDismissBoxValue.Settled -> Unit
            }
            // The row stays: pinning keeps it, and deletions hide it through the view model.
            scope.launch { state.reset() }
        },
        backgroundContent = {
            val toDelete = state.dismissDirection == SwipeToDismissBoxValue.EndToStart
            val colors = MaterialTheme.colorScheme
            Box(
                Modifier
                    .fillMaxSize()
                    .background(if (toDelete) colors.errorContainer else colors.secondaryContainer)
                    .padding(horizontal = 24.dp),
                contentAlignment = if (toDelete) Alignment.CenterEnd else Alignment.CenterStart,
            ) {
                Icon(
                    painterResource(
                        when {
                            toDelete -> R.drawable.ic_delete
                            entry.pinned -> R.drawable.ic_push_pin
                            else -> R.drawable.ic_push_pin_filled
                        },
                    ),
                    contentDescription = stringResource(
                        when {
                            toDelete -> R.string.session_delete
                            entry.pinned -> R.string.session_unpin
                            else -> R.string.session_pin
                        },
                    ),
                    tint = if (toDelete) colors.onErrorContainer else colors.onSecondaryContainer,
                )
            }
        },
    ) {
        Box(Modifier.background(MaterialTheme.colorScheme.surface)) { content() }
    }
}

@Composable
private fun LibraryRow(entry: LibraryEntry, onClick: () -> Unit) {
    val colors = MaterialTheme.colorScheme
    Surface(onClick = onClick, color = colors.surface) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.fillMaxWidth().heightIn(min = 72.dp).padding(horizontal = 16.dp, vertical = 8.dp),
        ) {
            val unresolved = entry.track == null && entry.artworkUrl == null &&
                (entry.status == SessionStatus.NO_MATCH || entry.status == SessionStatus.FAILED)
            if (unresolved) HatchedTile(56.dp) else Artwork(entry.track?.artworkUrl ?: entry.artworkUrl, 56.dp, corner = 8.dp)
            Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
                Text(
                    entry.track?.title ?: entry.title.ifBlank { stringResource(kindLabel(entry.kind)) },
                    style = MaterialTheme.typography.bodyLarge,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                Row(verticalAlignment = Alignment.CenterVertically) {
                    KindIcon(entry.kind, entry.trackCount >= 2u)
                    Spacer(Modifier.width(6.dp))
                    Text(
                        subtitle(entry),
                        style = MaterialTheme.typography.bodyMedium,
                        color = colors.onSurfaceVariant,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                }
            }
            if (entry.status == SessionStatus.RUNNING) {
                CircularProgressIndicator(modifier = Modifier.size(16.dp), strokeWidth = 2.dp)
                Spacer(Modifier.width(8.dp))
            }
            Text(
                Formatters.timeOfDay(entry.createdAtMs),
                style = MaterialTheme.typography.labelMedium.merge(TabularNumbers),
                color = colors.onSurfaceVariant,
            )
            if (entry.pinned) {
                Spacer(Modifier.width(8.dp))
                Icon(
                    painterResource(R.drawable.ic_push_pin_filled),
                    contentDescription = stringResource(R.string.session_pinned),
                    tint = colors.onSurfaceVariant,
                    modifier = Modifier.size(18.dp),
                )
            }
        }
    }
}

@Composable
private fun KindIcon(kind: SessionKind, mix: Boolean) {
    val tint = MaterialTheme.colorScheme.onSurfaceVariant
    val modifier = Modifier.size(16.dp)
    when {
        kind == SessionKind.HUM -> Box(modifier, contentAlignment = Alignment.Center) { ContourMark(tint, width = 16.dp, strokeWidth = 1.6.dp) }
        else -> Icon(
            painterResource(
                when {
                    mix -> R.drawable.ic_view_timeline
                    kind == SessionKind.LISTEN -> R.drawable.ic_mic
                    kind == SessionKind.SYSTEM_AUDIO -> R.drawable.ic_smartphone
                    kind == SessionKind.LINK -> R.drawable.ic_link
                    kind == SessionKind.FILE -> R.drawable.ic_attach_file
                    else -> R.drawable.ic_lyrics
                },
            ),
            contentDescription = stringResource(kindLabel(kind)),
            tint = tint,
            modifier = modifier,
        )
    }
}

private fun kindLabel(kind: SessionKind): Int = when (kind) {
    SessionKind.LISTEN -> R.string.kind_listen
    SessionKind.SYSTEM_AUDIO -> R.string.kind_system_audio
    SessionKind.HUM -> R.string.kind_hum
    SessionKind.LINK -> R.string.kind_link
    SessionKind.FILE -> R.string.kind_file
    SessionKind.LYRICS -> R.string.kind_lyrics
}

@Composable
private fun subtitle(entry: LibraryEntry): String {
    val artist = entry.track?.artist ?: entry.subtitle?.takeIf { it.isNotBlank() }
    val host = Formatters.host(entry.sourceUrl)
    val count = entry.trackCount.toInt()
    return when {
        entry.status == SessionStatus.RUNNING && count > 0 -> pluralStringResource(R.plurals.tracks_so_far, count, count)
        entry.status == SessionStatus.RUNNING -> stringResource(R.string.library_running)
        entry.status == SessionStatus.FAILED -> stringResource(R.string.library_failed)
        entry.status == SessionStatus.NO_MATCH -> stringResource(R.string.library_no_match)
        entry.status == SessionStatus.CANCELED && count == 0 -> stringResource(R.string.library_canceled)
        entry.status == SessionStatus.CANDIDATES && entry.track == null -> {
            listOfNotNull(artist).joinToString(" · ").ifEmpty { stringResource(R.string.kind_hum) }
        }
        count >= 2 -> listOfNotNull(artist ?: host, pluralStringResource(R.plurals.tracks, count, count)).joinToString(" · ")
        entry.kind == SessionKind.LINK -> listOfNotNull(artist, host).joinToString(" · ")
        else -> artist ?: ""
    }
}

@Composable
private fun EmptyLibrary() {
    Column(
        Modifier.fillMaxSize().padding(32.dp),
        verticalArrangement = Arrangement.Center,
        horizontalAlignment = Alignment.CenterHorizontally,
    ) {
        ContourMark(MaterialTheme.colorScheme.outline, width = 48.dp, strokeWidth = 3.dp)
        Spacer(Modifier.height(16.dp))
        Text(stringResource(R.string.library_empty_title), style = MaterialTheme.typography.titleMedium, textAlign = TextAlign.Center)
        Spacer(Modifier.height(4.dp))
        Text(
            stringResource(R.string.library_empty_message),
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center,
        )
    }
}

@Composable
private fun NoResults() {
    Box(Modifier.fillMaxWidth().padding(32.dp), contentAlignment = Alignment.Center) {
        Text(
            stringResource(R.string.library_no_results),
            style = MaterialTheme.typography.bodyLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}
