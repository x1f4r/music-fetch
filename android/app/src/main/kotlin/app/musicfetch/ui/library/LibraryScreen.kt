package app.musicfetch.ui.library

import android.app.Activity
import androidx.activity.compose.BackHandler
import androidx.compose.animation.Crossfade
import androidx.compose.animation.animateColorAsState
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
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
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.text.TextAutoSize
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.FilterChip
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.SwipeToDismissBox
import androidx.compose.material3.SwipeToDismissBoxValue
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.rememberSwipeToDismissBoxState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
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
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.platform.LocalResources
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.CustomAccessibilityAction
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.customActions
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.selected
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionStatus
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.common.LocalAppSnackbar
import app.musicfetch.ui.common.tag
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.components.HatchedTile
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.SessionRoute
import app.musicfetch.ui.navigation.SettingsRoute
import app.musicfetch.ui.navigation.Tab
import app.musicfetch.ui.session.share
import app.musicfetch.ui.theme.StandardMotion
import app.musicfetch.ui.theme.TabularNumbers
import java.time.LocalDate
import kotlinx.coroutines.launch

/** The library (docs/design/DESIGN.md §6.8, Android). */
@OptIn(ExperimentalFoundationApi::class)
@Composable
fun LibraryScreen(viewModel: LibraryViewModel = viewModel()) {
    val navigator = LocalNavigator.current
    val context = LocalContext.current
    val snackbar = LocalAppSnackbar.current
    val haptics = LocalHapticFeedback.current
    val scope = rememberCoroutineScope()
    val entries by viewModel.entries.collectAsStateWithLifecycle()
    val pinned by viewModel.pinned.collectAsStateWithLifecycle()
    val loaded by viewModel.loaded.collectAsStateWithLifecycle()
    val filter by viewModel.filter.collectAsStateWithLifecycle()
    val search by viewModel.search.collectAsStateWithLifecycle()
    val selection by viewModel.selection.collectAsStateWithLifecycle()
    val selecting = selection.isNotEmpty()
    var confirm by remember { mutableStateOf<List<LibraryEntry>?>(null) }
    var pinnedExpanded by rememberSaveable { mutableStateOf(true) }
    val undoLabel = stringResource(R.string.session_undo)
    val resources = LocalResources.current

    val listState = rememberLazyListState()
    val nearEnd by remember { derivedStateOf { listState.layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: 0 } }
    LaunchedEffect(nearEnd, entries.size) { if (nearEnd >= entries.size - 10) viewModel.loadMore() }

    // Back leaves selection mode, then clears the search, before it leaves the Library.
    BackHandler(enabled = selecting) { viewModel.clearSelection() }
    BackHandler(enabled = !selecting && search.isNotEmpty()) { viewModel.setSearch("") }
    // Selecting the Library item again scrolls to the top and ends the selection.
    LaunchedEffect(navigator) {
        navigator.reselected.collect { tab ->
            if (tab == Tab.Library) {
                viewModel.clearSelection()
                listState.animateScrollToItem(0)
            }
        }
    }
    // Leaving the Library ends the selection (rotation keeps it).
    DisposableEffect(Unit) {
        onDispose { if ((context as? Activity)?.isChangingConfigurations != true) viewModel.clearSelection() }
    }

    fun pin(ids: Collection<String>, value: Boolean) {
        val previous = viewModel.setPinned(ids, value)
        if (previous.isEmpty()) return
        val n = previous.size
        val label = when {
            n == 1 -> resources.getString(if (value) R.string.session_pinned else R.string.session_unpinned)
            else -> resources.getQuantityString(if (value) R.plurals.library_pinned_n else R.plurals.library_unpinned_n, n, n)
        }
        snackbar.post(label, undoLabel) { viewModel.restorePins(previous) }
    }

    fun delete(rows: List<LibraryEntry>) {
        if (rows.isEmpty()) return
        // A confirmation keeps the selection until it is answered: Cancel returns to it.
        if (viewModel.needsConfirmation(rows)) {
            confirm = rows
        } else {
            viewModel.delete(rows.map { it.id })
            viewModel.clearSelection()
        }
    }

    val showPinned = !filter.pinnedOnly && search.isBlank() && pinned.isNotEmpty()
    val dayRows = remember(entries, pinned, showPinned) { LibraryLists.dayRows(entries, if (showPinned) pinned else emptyList()) }
    val empty = loaded && entries.isEmpty() && !(showPinned && pinned.isNotEmpty())

    StandardMotion {
        Column(
            Modifier
                .fillMaxSize()
                .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Horizontal)),
        ) {
            // The selection bar takes the search field's place, so the list never moves.
            Box(Modifier.padding(horizontal = 16.dp, vertical = 8.dp)) {
                SearchField(
                    text = search,
                    onText = viewModel::setSearch,
                    onSettings = { navigator.push(SettingsRoute) },
                    modifier = if (selecting) Modifier.alpha(0f).clearAndSetSemantics { } else Modifier,
                )
                // While it fades out the bar keeps showing the last selection, not "0 selected".
                var shown by remember { mutableStateOf(emptyList<LibraryEntry>()) }
                if (selecting) shown = remember(selection, entries, pinned) { viewModel.selectedEntries() }
                androidx.compose.animation.AnimatedVisibility(visible = selecting, enter = fadeIn(), exit = fadeOut(), modifier = Modifier.matchParentSize()) {
                    SelectionBar(
                        count = shown.size,
                        unpins = Selection.unpins(shown),
                        canShare = shown.any { it.track != null || it.trackCount >= 2u },
                        onClose = viewModel::clearSelection,
                        onPin = { value ->
                            pin(selection, value)
                            viewModel.clearSelection()
                        },
                        onShare = {
                            val rows = viewModel.selectedEntries()
                            scope.launch {
                                viewModel.shareTextFor(rows)?.let { text ->
                                    share(context, rows.singleOrNull()?.let { it.track?.title ?: it.title } ?: "", text)
                                }
                                viewModel.clearSelection()
                            }
                        },
                        onDelete = { delete(viewModel.selectedEntries()) },
                        onSelectAll = viewModel::selectAll,
                    )
                }
            }
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
                        // Filters wait while sessions are selected.
                        enabled = !selecting,
                        modifier = Modifier.height(32.dp).tag("library.filter." + option.name.lowercase()),
                    )
                }
            }
            if (empty) {
                when {
                    search.isNotBlank() -> NoResults()
                    filter == LibraryFilter.All -> EmptyLibrary()
                    else -> EmptyFilter(filter)
                }
            } else {
                val today = remember { LocalDate.now() }
                val groups = remember(dayRows) { dayRows.groupBy { Formatters.day(it.createdAtMs) } }
                val collapsible = pinned.size > PINNED_COLLAPSE_AT
                LazyColumn(state = listState, contentPadding = PaddingValues(bottom = 24.dp), modifier = Modifier.fillMaxSize()) {
                    if (showPinned) {
                        stickyHeader(key = "pinned") {
                            PinnedHeader(
                                count = pinned.size,
                                collapsible = collapsible,
                                expanded = pinnedExpanded || !collapsible,
                                onToggle = { pinnedExpanded = !pinnedExpanded },
                            )
                        }
                        if (pinnedExpanded || !collapsible) {
                            items(pinned, key = { "pinned-${it.id}" }) { entry ->
                                LibraryItem(
                                    entry = entry,
                                    selecting = selecting,
                                    selected = entry.id in selection,
                                    onOpen = { navigator.push(SessionRoute(entry.id)) },
                                    onSelect = {
                                        haptics.performHapticFeedback(HapticFeedbackType.LongPress)
                                        viewModel.select(entry.id)
                                    },
                                    onToggle = { viewModel.toggleSelection(entry.id) },
                                    onPin = { pin(listOf(entry.id), !entry.pinned) },
                                    onDelete = { delete(listOf(entry)) },
                                    modifier = Modifier.animateItem(),
                                )
                            }
                        }
                    }
                    groups.forEach { (day, dayEntries) ->
                        stickyHeader(key = "day-$day") {
                            SectionHeader(
                                when (day) {
                                    today -> stringResource(R.string.library_today)
                                    today.minusDays(1) -> stringResource(R.string.library_yesterday)
                                    else -> Formatters.longDate(context, dayEntries.first().createdAtMs, today)
                                },
                            )
                        }
                        items(dayEntries, key = { it.id }) { entry ->
                            LibraryItem(
                                entry = entry,
                                selecting = selecting,
                                selected = entry.id in selection,
                                onOpen = { navigator.push(SessionRoute(entry.id)) },
                                onSelect = {
                                    haptics.performHapticFeedback(HapticFeedbackType.LongPress)
                                    viewModel.select(entry.id)
                                },
                                onToggle = { viewModel.toggleSelection(entry.id) },
                                onPin = { pin(listOf(entry.id), !entry.pinned) },
                                onDelete = { delete(listOf(entry)) },
                                modifier = Modifier.animateItem(),
                            )
                        }
                    }
                }
            }
        }
    }
    confirm?.let { rows ->
        val single = rows.singleOrNull()
        AlertDialog(
            onDismissRequest = { confirm = null },
            title = {
                Text(
                    if (single != null) {
                        stringResource(R.string.session_delete_confirm_title, single.track?.title ?: single.title)
                    } else {
                        pluralStringResource(R.plurals.library_delete_n_title, rows.size, rows.size)
                    },
                )
            },
            text = {
                Text(stringResource(if (single != null) R.string.session_delete_confirm_message else R.string.library_delete_n_message))
            },
            confirmButton = {
                TextButton(onClick = {
                    confirm = null
                    viewModel.deleteNow(rows.map { it.id })
                    viewModel.clearSelection()
                }, modifier = Modifier.tag("dialog.confirm")) { Text(stringResource(R.string.session_delete), color = MaterialTheme.colorScheme.error) }
            },
            dismissButton = { TextButton(onClick = { confirm = null }) { Text(stringResource(R.string.session_cancel)) } },
        )
    }
}

/** More pinned sessions than this make the section collapsible. */
private const val PINNED_COLLAPSE_AT = 5

private fun filterLabel(filter: LibraryFilter): Int = when (filter) {
    LibraryFilter.All -> R.string.library_filter_all
    LibraryFilter.Heard -> R.string.library_filter_heard
    LibraryFilter.Hummed -> R.string.library_filter_hummed
    LibraryFilter.Links -> R.string.library_filter_links
    LibraryFilter.Mixes -> R.string.library_filter_mixes
    LibraryFilter.Pinned -> R.string.library_filter_pinned
}

@Composable
private fun SectionHeader(text: String, modifier: Modifier = Modifier, trailing: @Composable () -> Unit = {}) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
            .padding(start = 16.dp, end = 8.dp)
            .heightIn(min = 48.dp)
            .semantics { heading() },
    ) {
        Text(
            text,
            style = MaterialTheme.typography.titleSmall,
            color = MaterialTheme.colorScheme.primary,
            modifier = Modifier.weight(1f).padding(top = 8.dp),
        )
        trailing()
    }
}

/** "Pinned" above the day groups; with many pinned sessions it folds away. */
@Composable
private fun PinnedHeader(count: Int, collapsible: Boolean, expanded: Boolean, onToggle: () -> Unit) {
    val title = stringResource(R.string.session_pinned)
    if (!collapsible) {
        SectionHeader(title, Modifier.tag("library.pinned"))
        return
    }
    val toggleLabel = stringResource(if (expanded) R.string.library_collapse else R.string.library_expand)
    Box(
        Modifier
            .clickable(onClickLabel = toggleLabel, onClick = onToggle)
            .tag("library.pinned"),
    ) {
        SectionHeader(title) {
            Text(
                count.toString(),
                style = MaterialTheme.typography.labelLarge.merge(TabularNumbers),
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            val rotation by animateFloatAsState(if (expanded) 180f else 0f, label = "chevron")
            Icon(
                painterResource(R.drawable.ic_keyboard_arrow_down),
                contentDescription = null,
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 12.dp).graphicsLayer { rotationZ = rotation },
            )
        }
    }
}

/** "3 selected" with Pin or Unpin, Share, Delete and Select all, in the search field's place. */
@Composable
private fun SelectionBar(
    count: Int,
    unpins: Boolean,
    canShare: Boolean,
    onClose: () -> Unit,
    onPin: (Boolean) -> Unit,
    onShare: () -> Unit,
    onDelete: () -> Unit,
    onSelectAll: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    Surface(
        shape = RoundedCornerShape(28.dp),
        color = MaterialTheme.colorScheme.secondaryContainer,
        contentColor = MaterialTheme.colorScheme.onSecondaryContainer,
        modifier = Modifier.fillMaxSize(),
    ) {
        BoxWithConstraints(contentAlignment = Alignment.CenterStart) {
            // Narrow screens with large fonts: Share moves into More, so the count stays readable.
            val compact = maxWidth - 8.dp - 48.dp * 5 < 64.dp * LocalDensity.current.fontScale
            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(horizontal = 4.dp)) {
                IconButton(onClick = onClose, modifier = Modifier.tag("library.selection.close")) {
                    Icon(painterResource(R.drawable.ic_close), contentDescription = stringResource(R.string.library_close_selection))
                }
                Text(
                    pluralStringResource(R.plurals.library_selected, count, count),
                    style = MaterialTheme.typography.titleMedium.merge(TabularNumbers),
                    maxLines = 1,
                    autoSize = TextAutoSize.StepBased(minFontSize = 12.sp, maxFontSize = MaterialTheme.typography.titleMedium.fontSize),
                    modifier = Modifier.weight(1f).padding(start = 4.dp).semantics { liveRegion = LiveRegionMode.Polite },
                )
                IconButton(onClick = { onPin(!unpins) }, modifier = Modifier.tag(if (unpins) "library.selection.unpin" else "library.selection.pin")) {
                    Icon(
                        painterResource(if (unpins) R.drawable.ic_keep_off else R.drawable.ic_push_pin),
                        contentDescription = stringResource(if (unpins) R.string.session_unpin else R.string.session_pin),
                    )
                }
                if (!compact) {
                    IconButton(onClick = onShare, enabled = canShare, modifier = Modifier.tag("library.selection.share")) {
                        Icon(painterResource(R.drawable.ic_share), contentDescription = stringResource(R.string.result_share))
                    }
                }
                IconButton(onClick = onDelete, modifier = Modifier.tag("library.selection.delete")) {
                    Icon(painterResource(R.drawable.ic_delete), contentDescription = stringResource(R.string.session_delete))
                }
                Box {
                    IconButton(onClick = { menu = true }, modifier = Modifier.tag("library.selection.more")) {
                        Icon(painterResource(R.drawable.ic_more_vert), contentDescription = stringResource(R.string.more))
                    }
                    DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
                        if (compact) {
                            DropdownMenuItem(
                                text = { Text(stringResource(R.string.result_share)) },
                                enabled = canShare,
                                onClick = {
                                    menu = false
                                    onShare()
                                },
                            )
                        }
                        DropdownMenuItem(
                            text = { Text(stringResource(R.string.library_select_all)) },
                            onClick = {
                                menu = false
                                onSelectAll()
                            },
                            modifier = Modifier.tag("library.selection.selectAll"),
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun SearchField(text: String, onText: (String) -> Unit, onSettings: () -> Unit, modifier: Modifier = Modifier) {
    val keyboard = LocalSoftwareKeyboardController.current
    var value by rememberSaveable { mutableStateOf(text) }
    // Cleared from outside (Back).
    LaunchedEffect(text) { if (text.isEmpty() && value.isNotEmpty()) value = "" }
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
                    modifier = Modifier.fillMaxWidth().tag("library.search"),
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
            IconButton(onClick = onSettings, modifier = Modifier.tag("library.settings")) {
                Icon(painterResource(R.drawable.ic_settings), contentDescription = stringResource(R.string.settings_title))
            }
        }
    }
}

/**
 * A library row with its gestures: tap opens, long-press selects; while selecting, a tap toggles.
 * Swipe start→end pins, end→start deletes (docs/design/DESIGN.md §6.8); TalkBack gets both as
 * actions.
 */
@Composable
private fun LibraryItem(
    entry: LibraryEntry,
    selecting: Boolean,
    selected: Boolean,
    onOpen: () -> Unit,
    onSelect: () -> Unit,
    onToggle: () -> Unit,
    onPin: () -> Unit,
    onDelete: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val pinLabel = stringResource(if (entry.pinned) R.string.session_unpin else R.string.session_pin)
    val deleteLabel = stringResource(R.string.session_delete)
    SwipeRow(entry = entry, enabled = !selecting, onPin = onPin, onDelete = onDelete, modifier = modifier) {
        LibraryRow(
            entry = entry,
            selecting = selecting,
            selected = selected,
            onClick = if (selecting) onToggle else onOpen,
            onLongClick = if (selecting) onToggle else onSelect,
            modifier = Modifier.semantics {
                if (!selecting) {
                    customActions = listOf(
                        CustomAccessibilityAction(pinLabel) { onPin(); true },
                        CustomAccessibilityAction(deleteLabel) { onDelete(); true },
                    )
                }
            },
        )
    }
}

/** Swipe start→end pins (the row springs back), end→start deletes. */
@Composable
private fun SwipeRow(
    entry: LibraryEntry,
    enabled: Boolean,
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
        enableDismissFromStartToEnd = enabled,
        enableDismissFromEndToStart = enabled,
        onDismiss = { value ->
            when (value) {
                SwipeToDismissBoxValue.StartToEnd -> {
                    onPin()
                    // Pinning keeps the row: it slides back into place.
                    scope.launch { state.reset() }
                }
                SwipeToDismissBoxValue.EndToStart -> {
                    // Settle first: the swipe state is saved per row, and a row that comes back
                    // after Undo must not arrive dismissed (it would delete itself again).
                    scope.launch {
                        state.snapTo(SwipeToDismissBoxValue.Settled)
                        onDelete()
                    }
                }
                SwipeToDismissBoxValue.Settled -> Unit
            }
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
                            entry.pinned -> R.drawable.ic_keep_off
                            else -> R.drawable.ic_push_pin_filled
                        },
                    ),
                    contentDescription = null,
                    tint = if (toDelete) colors.onErrorContainer else colors.onSecondaryContainer,
                )
            }
        },
    ) {
        Box(Modifier.background(MaterialTheme.colorScheme.surface)) { content() }
    }
}

@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun LibraryRow(
    entry: LibraryEntry,
    selecting: Boolean,
    selected: Boolean,
    onClick: () -> Unit,
    onLongClick: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val colors = MaterialTheme.colorScheme
    val background by animateColorAsState(if (selected) colors.secondaryContainer else colors.surface, label = "rowBackground")
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = modifier
            .fillMaxWidth()
            .background(background)
            .combinedClickable(
                onClick = onClick,
                onLongClick = onLongClick,
                onLongClickLabel = stringResource(R.string.library_select),
            )
            .semantics { if (selecting) this.selected = selected }
            .heightIn(min = 72.dp)
            .padding(horizontal = 16.dp, vertical = 8.dp)
            .tag("library.row.${entry.id}"),
    ) {
        val unresolved = entry.track == null && entry.artworkUrl == null &&
            (entry.status == SessionStatus.NO_MATCH || entry.status == SessionStatus.FAILED)
        Crossfade(targetState = selected, label = "rowLeading") { checked ->
            if (checked) {
                Box(
                    Modifier.size(56.dp).background(colors.primary, CircleShape),
                    contentAlignment = Alignment.Center,
                ) {
                    Icon(painterResource(R.drawable.ic_check), contentDescription = null, tint = colors.onPrimary)
                }
            } else if (unresolved) {
                HatchedTile(56.dp)
            } else {
                Artwork(entry.track?.artworkUrl ?: entry.artworkUrl, 56.dp, corner = 8.dp)
            }
        }
        Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
            Text(
                entry.track?.title ?: entry.title.ifBlank { stringResource(kindLabel(entry.kind)) },
                style = MaterialTheme.typography.bodyLarge,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
            )
            Row(verticalAlignment = Alignment.CenterVertically) {
                KindIcon(entry.kind, mix = entry.trackCount >= 2u && (entry.durationMs ?: 0u) > 0u)
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
        } else if (entry.status == SessionStatus.WAITING) {
            Icon(
                painterResource(R.drawable.ic_cloud_off),
                contentDescription = null,
                tint = colors.onSurfaceVariant,
                modifier = Modifier.size(18.dp),
            )
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
        entry.status == SessionStatus.WAITING -> stringResource(R.string.library_waiting)
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

/** A filter with nothing in it yet. */
@Composable
private fun EmptyFilter(filter: LibraryFilter) {
    Column(Modifier.fillMaxWidth().padding(32.dp), horizontalAlignment = Alignment.CenterHorizontally) {
        Text(
            stringResource(
                when (filter) {
                    LibraryFilter.Heard -> R.string.library_empty_heard
                    LibraryFilter.Hummed -> R.string.library_empty_hummed
                    LibraryFilter.Links -> R.string.library_empty_links
                    LibraryFilter.Mixes -> R.string.library_empty_mixes
                    LibraryFilter.Pinned, LibraryFilter.All -> R.string.library_empty_pinned
                },
            ),
            style = MaterialTheme.typography.bodyLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center,
        )
        if (filter == LibraryFilter.Pinned) {
            Spacer(Modifier.height(4.dp))
            Text(
                stringResource(R.string.library_empty_pinned_message),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                textAlign = TextAlign.Center,
            )
        }
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
