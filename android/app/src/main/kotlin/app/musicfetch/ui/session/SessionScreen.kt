package app.musicfetch.ui.session

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.material3.IconToggleButton
import androidx.compose.material3.TopAppBarScrollBehavior
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.core.ExportFormat
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionStatus
import app.musicfetch.core.Track
import app.musicfetch.notify.Notifications
import app.musicfetch.ui.AppIntents
import app.musicfetch.ui.common.ListRow
import app.musicfetch.ui.common.LocalAppSnackbar
import app.musicfetch.ui.common.tag
import app.musicfetch.ui.components.MatchingIndicator
import app.musicfetch.ui.listen.sourceOf
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.SessionRoute
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * A session: result, candidates, mix or collection by what it holds (docs/design/DESIGN.md §6.3–6.6).
 * [onListenTab]: shown on the Listen destination, where results offer "Listen again".
 */
@Composable
fun SessionScreen(
    id: String,
    onListenTab: Boolean = false,
    viewModel: SessionViewModel = viewModel(key = "session-$id", factory = SessionViewModel.Factory(id)),
) {
    val view by viewModel.view.collectAsStateWithLifecycle()
    val navigator = LocalNavigator.current
    val context = LocalContext.current
    val snackbar = LocalAppSnackbar.current
    val message by viewModel.messages.collectAsStateWithLifecycle()
    LaunchedEffect(message) {
        message?.let {
            snackbar.post(it)
            viewModel.message(null)
        }
    }
    // Seen in the app: its result notification has done its job.
    LaunchedEffect(id) { Notifications.dismissResult(context, id) }
    val onBack: () -> Unit = { navigator.back() }
    when (val v = view) {
        SessionView.Loading -> Scaffold(topBar = { BackBar(onBack) }) { padding ->
            // Most sessions load in a few milliseconds: show progress only after 300 ms.
            var slow by remember { mutableStateOf(false) }
            LaunchedEffect(Unit) {
                delay(300)
                slow = true
            }
            Box(Modifier.fillMaxSize().padding(padding), contentAlignment = Alignment.Center) {
                if (slow) MatchingIndicator(MaterialTheme.colorScheme.primary, Modifier.size(48.dp))
            }
        }
        SessionView.Missing -> Scaffold(topBar = { BackBar(onBack) }) { padding ->
            Box(Modifier.fillMaxSize().padding(padding), contentAlignment = Alignment.Center) {
                Text(
                    stringResource(R.string.session_not_found),
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                    modifier = Modifier.padding(24.dp),
                )
            }
        }
        is SessionView.Result -> ResultContent(v, viewModel, onBack, listenAgain = onListenTab)
        is SessionView.Candidates -> CandidatesContent(v, viewModel, onBack, listenAgain = onListenTab)
        is SessionView.Mix -> MixContent(v, viewModel, onBack)
        is SessionView.Empty -> EmptyContent(v, viewModel, onBack, listenAgain = onListenTab && !v.running)
    }
}

/**
 * The session app bar. [transparent] lets the result's wash show through until content scrolls
 * beneath it ([scrollBehavior]); then the bar takes the container colour.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
internal fun BackBar(
    onBack: () -> Unit,
    modifier: Modifier = Modifier,
    actions: @Composable () -> Unit = {},
    transparent: Boolean = false,
    scrollBehavior: TopAppBarScrollBehavior? = null,
    title: @Composable () -> Unit = {},
) {
    TopAppBar(
        title = title,
        modifier = modifier,
        navigationIcon = {
            IconButton(onClick = onBack, modifier = Modifier.tag("session.back")) {
                Icon(painterResource(R.drawable.ic_arrow_back), contentDescription = stringResource(R.string.back))
            }
        },
        actions = { actions() },
        scrollBehavior = scrollBehavior,
        colors = if (transparent) {
            TopAppBarDefaults.topAppBarColors(
                containerColor = Color.Transparent,
                scrolledContainerColor = MaterialTheme.colorScheme.surfaceContainer,
            )
        } else {
            TopAppBarDefaults.topAppBarColors()
        },
    )
}

/**
 * Pin, Share and More (copy, export, delete) for the session's app bar (docs/design/DESIGN.md
 * §6.3). Pin is a visible toggle, so a song is one tap from being kept; unconfirmed guesses
 * aren't shared ([withShare] false).
 */
@Composable
internal fun SessionActions(
    entry: LibraryEntry?,
    viewModel: SessionViewModel,
    track: Track? = null,
    isMix: Boolean = false,
    withShare: Boolean = true,
    withPin: Boolean = true,
) {
    val context = LocalContext.current
    val navigator = LocalNavigator.current
    val haptics = LocalHapticFeedback.current
    val scope = rememberCoroutineScope()
    var menu by remember { mutableStateOf(false) }
    var exportSheet by remember { mutableStateOf(false) }
    var confirmDelete by remember { mutableStateOf(false) }
    val copied = stringResource(R.string.result_copied)
    val exportFailed = stringResource(R.string.export_failed)
    val exportSaved = stringResource(R.string.export_saved)
    var pendingFormat by remember { mutableStateOf(ExportFormat.TEXT) }
    val createDocument = rememberLauncherForActivityResult(ActivityResultContracts.CreateDocument("*/*")) { uri ->
        if (uri != null) {
            viewModel.exportTo(context, uri, pendingFormat) { ok -> viewModel.message(if (ok) exportSaved else exportFailed) }
        }
    }

    if (withPin && entry != null) {
        val pinnedLabel = stringResource(if (entry.pinned) R.string.session_pinned else R.string.session_not_pinned)
        IconToggleButton(
            checked = entry.pinned,
            onCheckedChange = { checked ->
                haptics.performHapticFeedback(if (checked) HapticFeedbackType.ToggleOn else HapticFeedbackType.ToggleOff)
                viewModel.setPinned(checked)
            },
            modifier = Modifier.semantics { stateDescription = pinnedLabel }.tag("session.pin"),
        ) {
            Icon(
                painterResource(if (entry.pinned) R.drawable.ic_push_pin_filled else R.drawable.ic_push_pin),
                contentDescription = stringResource(R.string.session_pin),
            )
        }
    }
    if (withShare) {
        IconButton(onClick = {
            scope.launch {
                val text = if (isMix || track == null) viewModel.export(ExportFormat.TEXT) else shareText(track)
                if (text != null) share(context, entry?.title ?: track?.title ?: "", text)
            }
        }, modifier = Modifier.tag("session.share")) {
            Icon(painterResource(R.drawable.ic_share), contentDescription = stringResource(R.string.result_share))
        }
    }
    Box {
        IconButton(onClick = { menu = true }, modifier = Modifier.tag("session.more")) {
            Icon(painterResource(R.drawable.ic_more_vert), contentDescription = stringResource(R.string.more))
        }
        DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
            if (track != null) {
                DropdownMenuItem(
                    text = { Text(stringResource(R.string.result_copy_title_artist)) },
                    onClick = {
                        menu = false
                        copy(context, "${track.artist} – ${track.title}")
                        confirmCopy(viewModel, copied)
                    },
                )
                val link = track.links.firstOrNull { !it.isSearch }
                if (link != null) {
                    DropdownMenuItem(
                        text = { Text(stringResource(R.string.result_copy_link)) },
                        onClick = {
                            menu = false
                            copy(context, link.url)
                            confirmCopy(viewModel, copied)
                        },
                    )
                }
            }
            if (isMix) {
                DropdownMenuItem(
                    text = { Text(stringResource(R.string.export_copy_tracklist)) },
                    onClick = {
                        menu = false
                        scope.launch {
                            viewModel.export(ExportFormat.TEXT)?.let {
                                copy(context, it)
                                confirmCopy(viewModel, copied)
                            }
                        }
                    },
                )
                DropdownMenuItem(
                    text = { Text(stringResource(R.string.export_menu)) },
                    onClick = {
                        menu = false
                        exportSheet = true
                    },
                )
            }
            if (entry != null) {
                DropdownMenuItem(
                    modifier = Modifier.tag("session.delete"),
                    text = { Text(stringResource(R.string.session_delete), color = MaterialTheme.colorScheme.error) },
                    onClick = {
                        menu = false
                        if (isMix || entry.pinned) {
                            confirmDelete = true
                        } else {
                            viewModel.delete()
                            navigator.closeIfShowing(SessionRoute(entry.id))
                        }
                    },
                )
            }
        }
    }
    if (exportSheet) {
        ExportSheet(
            onDismiss = { exportSheet = false },
            onPick = { format ->
                exportSheet = false
                pendingFormat = format
                createDocument.launch(fileName(entry?.title ?: "tracklist", format))
            },
        )
    }
    if (confirmDelete && entry != null) {
        AlertDialog(
            onDismissRequest = { confirmDelete = false },
            title = { Text(stringResource(R.string.session_delete_confirm_title, entry.title)) },
            text = { Text(stringResource(R.string.session_delete_confirm_message)) },
            confirmButton = {
                TextButton(onClick = {
                    confirmDelete = false
                    viewModel.deleteNow()
                    navigator.closeIfShowing(SessionRoute(entry.id))
                }) { Text(stringResource(R.string.session_delete), color = MaterialTheme.colorScheme.error) }
            },
            dismissButton = { TextButton(onClick = { confirmDelete = false }) { Text(stringResource(R.string.session_cancel)) } },
        )
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ExportSheet(onDismiss: () -> Unit, onPick: (ExportFormat) -> Unit) {
    ModalBottomSheet(onDismissRequest = onDismiss) {
        Column(Modifier.padding(bottom = 24.dp)) {
            Text(
                stringResource(R.string.export_menu),
                style = MaterialTheme.typography.titleMedium,
                modifier = Modifier.padding(horizontal = 24.dp, vertical = 8.dp),
            )
            listOf(
                ExportFormat.TEXT to R.string.export_text,
                ExportFormat.CSV to R.string.export_csv,
                ExportFormat.CUE to R.string.export_cue,
                ExportFormat.JSON to R.string.export_json,
            ).forEach { (format, label) ->
                ListRow(headline = stringResource(label), modifier = Modifier.padding(horizontal = 8.dp), onClick = { onPick(format) })
            }
        }
    }
}

/** Nothing found, failed, or still starting. */
@Composable
private fun EmptyContent(view: SessionView.Empty, viewModel: SessionViewModel, onBack: () -> Unit, listenAgain: Boolean) {
    val navigator = LocalNavigator.current
    val entry = view.entry
    val liveSource = entry?.kind?.let(::sourceOf)
    Scaffold(
        topBar = { BackBar(onBack, actions = { SessionActions(entry, viewModel, withShare = false, withPin = false) }) },
        bottomBar = { if (listenAgain) ListenAgainDock() },
    ) { padding ->
        Column(
            Modifier.fillMaxSize().padding(padding).padding(24.dp),
            verticalArrangement = Arrangement.Center,
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            if (entry != null && entry.title.isNotBlank()) {
                Text(
                    entry.title,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                    maxLines = 2,
                )
            }
            // Saved while offline: identified automatically once the phone is connected again.
            val waiting = entry?.status == SessionStatus.WAITING
            Text(
                when {
                    waiting -> stringResource(R.string.library_waiting)
                    view.running -> stringResource(R.string.library_running)
                    view.failed -> view.message ?: stringResource(R.string.error_generic)
                    else -> view.message ?: stringResource(R.string.state_no_match)
                },
                style = MaterialTheme.typography.titleMedium,
                textAlign = TextAlign.Center,
                modifier = Modifier.padding(top = 8.dp, bottom = if (waiting) 4.dp else 16.dp),
            )
            when {
                waiting -> Text(
                    stringResource(R.string.session_waiting_message),
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                )
                view.running -> Unit
                // The dock below already offers every way to listen again.
                listenAgain && liveSource != null -> Unit
                else -> FilledTonalButton(onClick = {
                    val url = entry?.sourceUrl
                    when {
                        liveSource != null -> AppIntents.listenNow(liveSource, navigator)
                        entry?.kind == SessionKind.LINK && url != null -> {
                            AppIntents.analyzeLater(url)
                            navigator.showListen()
                        }
                        else -> navigator.showListen()
                    }
                }, modifier = Modifier.tag("session.tryAgain")) { Text(stringResource(R.string.action_try_again)) }
            }
        }
    }
}

/** "Copied": Android 13+ confirms clipboard writes itself. */
private fun confirmCopy(viewModel: SessionViewModel, copied: String) {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) viewModel.message(copied)
}

internal fun shareText(track: Track): String {
    val link = track.links.firstOrNull { !it.isSearch } ?: track.links.firstOrNull()
    return listOfNotNull("${track.title} — ${track.artist}", link?.url).joinToString("\n")
}

internal fun share(context: Context, subject: String, text: String) {
    val send = Intent(Intent.ACTION_SEND).setType("text/plain").putExtra(Intent.EXTRA_TEXT, text).putExtra(Intent.EXTRA_SUBJECT, subject)
    context.startActivity(Intent.createChooser(send, null).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
}

internal fun copy(context: Context, text: String) {
    val clipboard = context.getSystemService(ClipboardManager::class.java) ?: return
    clipboard.setPrimaryClip(ClipData.newPlainText("Music Fetch", text))
}

private fun fileName(title: String, format: ExportFormat): String {
    val base = title.replace(Regex("[\\\\/:*?\"<>|]"), " ").replace(Regex("\\s+"), " ").trim().take(80).ifEmpty { "tracklist" }
    val ext = when (format) {
        ExportFormat.TEXT -> "txt"
        ExportFormat.CSV -> "csv"
        ExportFormat.JSON -> "json"
        ExportFormat.CUE -> "cue"
    }
    return "$base.$ext"
}
