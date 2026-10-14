package app.musicfetch.ui.session

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
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
import androidx.compose.material3.ListItem
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
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.core.ExportFormat
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.SessionKind
import app.musicfetch.core.Track
import app.musicfetch.platform.ServiceLinkOpener
import app.musicfetch.ui.AppIntents
import app.musicfetch.ui.common.ListRow
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.SessionRoute
import kotlinx.coroutines.launch

/** A session: result, candidates, mix or collection by what it holds (docs/design/DESIGN.md §6.3–6.6). */
@Composable
fun SessionScreen(id: String, viewModel: SessionViewModel = viewModel(key = "session-$id", factory = SessionViewModel.Factory(id))) {
    val view by viewModel.view.collectAsStateWithLifecycle()
    val navigator = LocalNavigator.current
    val context = LocalContext.current
    val message by viewModel.messages.collectAsStateWithLifecycle()
    LaunchedEffect(message) {
        message?.let {
            Toast.makeText(context, it, Toast.LENGTH_SHORT).show()
            viewModel.message(null)
        }
    }
    val onBack: () -> Unit = { navigator.back() }
    when (val v = view) {
        SessionView.Loading -> Scaffold(topBar = { BackBar(onBack) }) { padding -> Box(Modifier.padding(padding)) }
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
        is SessionView.Result -> ResultContent(v, viewModel, onBack)
        is SessionView.Candidates -> CandidatesContent(v, viewModel, onBack)
        is SessionView.Mix -> MixContent(v, viewModel, onBack)
        is SessionView.Empty -> EmptyContent(v, viewModel, onBack)
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
internal fun BackBar(onBack: () -> Unit, actions: @Composable () -> Unit = {}, transparent: Boolean = false, title: @Composable () -> Unit = {}) {
    TopAppBar(
        title = title,
        navigationIcon = {
            IconButton(onClick = onBack) {
                Icon(painterResource(R.drawable.ic_arrow_back), contentDescription = stringResource(R.string.back))
            }
        },
        actions = { actions() },
        colors = if (transparent) {
            TopAppBarDefaults.topAppBarColors(containerColor = Color.Transparent, scrolledContainerColor = Color.Transparent)
        } else {
            TopAppBarDefaults.topAppBarColors()
        },
    )
}

/** Share and More (copy, pin, delete, export) for the session's app bar. */
@Composable
internal fun SessionActions(
    entry: LibraryEntry?,
    viewModel: SessionViewModel,
    track: Track? = null,
    isMix: Boolean = false,
) {
    val context = LocalContext.current
    val navigator = LocalNavigator.current
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

    IconButton(onClick = {
        scope.launch {
            val text = if (isMix || track == null) viewModel.export(ExportFormat.TEXT) else shareText(track)
            if (text != null) share(context, entry?.title ?: track?.title ?: "", text)
        }
    }) {
        Icon(painterResource(R.drawable.ic_share), contentDescription = stringResource(R.string.result_share))
    }
    Box {
        IconButton(onClick = { menu = true }) {
            Icon(painterResource(R.drawable.ic_more_vert), contentDescription = stringResource(R.string.more))
        }
        DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
            if (track != null) {
                DropdownMenuItem(
                    text = { Text(stringResource(R.string.result_copy_title_artist)) },
                    onClick = {
                        menu = false
                        copy(context, "${track.artist} – ${track.title}")
                        viewModel.message(copied)
                    },
                )
                val link = track.links.firstOrNull { !it.isSearch }
                if (link != null) {
                    DropdownMenuItem(
                        text = { Text(stringResource(R.string.result_copy_link)) },
                        onClick = {
                            menu = false
                            copy(context, link.url)
                            viewModel.message(copied)
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
                                viewModel.message(copied)
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
                    text = { Text(stringResource(if (entry.pinned) R.string.session_unpin else R.string.session_pin)) },
                    onClick = {
                        menu = false
                        viewModel.setPinned(!entry.pinned)
                    },
                )
                DropdownMenuItem(
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
private fun EmptyContent(view: SessionView.Empty, viewModel: SessionViewModel, onBack: () -> Unit) {
    val navigator = LocalNavigator.current
    Scaffold(topBar = { BackBar(onBack, actions = { SessionActions(view.entry, viewModel) }) }) { padding ->
        Column(
            Modifier.fillMaxSize().padding(padding).padding(24.dp),
            verticalArrangement = Arrangement.Center,
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            val entry = view.entry
            if (entry != null && entry.title.isNotBlank()) {
                Text(
                    entry.title,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                    maxLines = 2,
                )
            }
            Text(
                when {
                    view.running -> stringResource(R.string.library_running)
                    view.failed -> view.message ?: stringResource(R.string.error_generic)
                    else -> view.message ?: stringResource(R.string.state_no_match)
                },
                style = MaterialTheme.typography.titleMedium,
                textAlign = TextAlign.Center,
                modifier = Modifier.padding(top = 8.dp, bottom = 16.dp),
            )
            if (!view.running) {
                FilledTonalButton(onClick = {
                    val url = entry?.sourceUrl
                    if (entry?.kind == SessionKind.LINK && url != null) AppIntents.analyzeLater(url)
                    navigator.showListen()
                }) { Text(stringResource(R.string.action_try_again)) }
            }
        }
    }
}

internal fun shareText(track: Track): String {
    val link = ServiceLinkOpener.pick(track.links, track.links.firstOrNull()?.service ?: app.musicfetch.core.Service.SPOTIFY)
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
    val base = title.replace(Regex("[\\\\/:*?\"<>|]"), " ").trim().take(80).ifEmpty { "tracklist" }
    val ext = when (format) {
        ExportFormat.TEXT -> "txt"
        ExportFormat.CSV -> "csv"
        ExportFormat.JSON -> "json"
        ExportFormat.CUE -> "cue"
    }
    return "$base.$ext"
}
