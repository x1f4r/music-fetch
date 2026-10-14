package app.musicfetch.ui.settings

import android.app.StatusBarManager
import android.appwidget.AppWidgetManager
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.graphics.drawable.Icon
import android.os.Build
import android.provider.Settings
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.ListItem
import androidx.compose.material3.ListItemDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.draw.rotate
import androidx.compose.ui.res.painterResource
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.remember
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.role
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.VisualTransformation
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.BuildConfig
import app.musicfetch.R
import app.musicfetch.capture.PhoneAudioTileService
import app.musicfetch.capture.SongTileService
import app.musicfetch.engine.MelodyDownload
import app.musicfetch.platform.CredentialField
import app.musicfetch.platform.ServiceLinkOpener
import app.musicfetch.platform.displayName
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.common.ListRow
import app.musicfetch.ui.navigation.LicensesRoute
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.session.BackBar
import app.musicfetch.ui.theme.StandardMotion
import app.musicfetch.widget.ListenWidgetReceiver

/** Settings (docs/design/DESIGN.md §6.9, Android). */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SettingsScreen(viewModel: SettingsViewModel = viewModel()) {
    val navigator = LocalNavigator.current
    val context = LocalContext.current
    val settings by viewModel.settings.collectAsStateWithLifecycle()
    val credentials by viewModel.credentialsSet.collectAsStateWithLifecycle()
    val melody by viewModel.melodyStatus.collectAsStateWithLifecycle()
    val download by viewModel.melodyDownload.collectAsStateWithLifecycle()
    val checkFailed by viewModel.melodyCheckFailed.collectAsStateWithLifecycle()
    val storage by viewModel.storage.collectAsStateWithLifecycle()
    val engineVersion by viewModel.engineVersion.collectAsStateWithLifecycle()
    var serviceDialog by rememberSaveable { mutableStateOf(false) }
    var clearDialog by rememberSaveable { mutableStateOf(false) }
    var expanded by rememberSaveable { mutableStateOf<String?>(null) }

    StandardMotion {
        Scaffold(topBar = { BackBar({ navigator.back() }, title = { Text(stringResource(R.string.settings_title)) }) }) { padding ->
            LazyColumn(contentPadding = PaddingValues(top = padding.calculateTopPadding(), bottom = 32.dp), modifier = Modifier.fillMaxSize()) {
                item { Header(R.string.settings_general) }
                item {
                    ListRow(
                        headline = stringResource(R.string.settings_open_songs_in),
                        supporting = settings?.defaultService?.displayName ?: "",
                        onClick = { serviceDialog = true },
                    )
                }
                item {
                    val on = settings?.notifyInBackground ?: true
                    ListRow(
                        headline = stringResource(R.string.settings_notify),
                        trailing = { Switch(checked = on, onCheckedChange = null) },
                        modifier = Modifier.semantics { role = Role.Switch },
                        onClick = { viewModel.setNotify(!on) },
                    )
                }
                item {
                    ListRow(
                        headline = stringResource(R.string.settings_notifications),
                        supporting = stringResource(R.string.settings_notifications_summary),
                        onClick = { openNotificationSettings(context) },
                    )
                }
                item { Header(R.string.settings_microphone) }
                item { MicrophoneSection() }
                item { Header(R.string.settings_qs_tile) }
                item {
                    TileRow(R.string.tile_song, R.drawable.ic_qs_tile, SongTileService::class.java)
                }
                item {
                    TileRow(R.string.tile_phone_audio, R.drawable.ic_qs_tile_phone, PhoneAudioTileService::class.java)
                }
                item {
                    val manager = AppWidgetManager.getInstance(context)
                    val supported = manager?.isRequestPinAppWidgetSupported == true
                    ListRow(
                        headline = stringResource(R.string.settings_widget),
                        supporting = if (supported) null else stringResource(R.string.settings_widget_manual),
                        trailing = if (supported) {
                            {
                                OutlinedButton(onClick = {
                                    manager.requestPinAppWidget(ComponentName(context, ListenWidgetReceiver::class.java), null, null)
                                }) { Text(stringResource(R.string.settings_add_widget)) }
                            }
                        } else {
                            null
                        },
                    )
                }

                item { Header(R.string.melody_title) }
                item {
                    MelodySection(
                        installed = melody?.installed == true,
                        songs = melody?.songCount?.toInt() ?: 0,
                        sizeBytes = melody?.sizeBytes?.toLong() ?: 0L,
                        version = melody?.version,
                        updateVersion = melody?.availableVersion,
                        downloadBytes = melody?.downloadBytes?.toLong(),
                        download = download,
                        checkFailed = checkFailed,
                        onDownload = viewModel::downloadMelody,
                        onCancel = viewModel::cancelMelody,
                        onRemove = viewModel::removeMelody,
                        onCheck = viewModel::checkMelody,
                    )
                }

                item { Header(R.string.settings_services) }
                item {
                    CredentialGroup(
                        key = "audd",
                        title = R.string.settings_audd,
                        footer = R.string.settings_audd_footer,
                        fields = listOf(CredentialField.AuddToken to R.string.settings_audd_token),
                        set = credentials,
                        expanded = expanded == "audd",
                        onExpand = { expanded = if (expanded == "audd") null else "audd" },
                        onSave = viewModel::setCredential,
                    )
                }
                item {
                    CredentialGroup(
                        key = "acr",
                        title = R.string.settings_acr,
                        footer = R.string.settings_acr_footer,
                        fields = listOf(
                            CredentialField.AcrHost to R.string.settings_host,
                            CredentialField.AcrAccessKey to R.string.settings_access_key,
                            CredentialField.AcrAccessSecret to R.string.settings_access_secret,
                        ),
                        set = credentials,
                        expanded = expanded == "acr",
                        onExpand = { expanded = if (expanded == "acr") null else "acr" },
                        onSave = viewModel::setCredential,
                    )
                }
                item {
                    CredentialGroup(
                        key = "acrHum",
                        title = R.string.settings_acr_hum,
                        footer = R.string.settings_acr_hum_footer,
                        fields = listOf(
                            CredentialField.AcrHumHost to R.string.settings_host,
                            CredentialField.AcrHumAccessKey to R.string.settings_access_key,
                            CredentialField.AcrHumAccessSecret to R.string.settings_access_secret,
                        ),
                        set = credentials,
                        expanded = expanded == "acrHum",
                        onExpand = { expanded = if (expanded == "acrHum") null else "acrHum" },
                        onSave = viewModel::setCredential,
                    )
                }

                item { Header(R.string.settings_storage) }
                item {
                    val usage = storage
                    val total = usage?.let { (it.databaseBytes + it.cacheBytes + it.melodyIndexBytes).toLong() }
                    ListRow(
                        headline = stringResource(R.string.settings_on_disk),
                        supporting = total?.let { Formatters.bytes(context, it) } ?: "…",
                        trailing = {
                            OutlinedButton(onClick = { clearDialog = true }, enabled = (usage?.cacheBytes ?: 0u) > 0u) {
                                Text(stringResource(R.string.settings_clear_cache))
                            }
                        },
                    )
                }

                item { Header(R.string.settings_about) }
                item {
                    ListRow(
                        headline = stringResource(R.string.settings_version),
                        supporting = stringResource(R.string.settings_version_value, BuildConfig.VERSION_NAME, BuildConfig.VERSION_CODE, engineVersion),
                    )
                }
                item {
                    ListRow(headline = stringResource(R.string.settings_licenses), onClick = { navigator.push(LicensesRoute) })
                }
            }
        }
    }

    if (serviceDialog) {
        AlertDialog(
            onDismissRequest = { serviceDialog = false },
            title = { Text(stringResource(R.string.settings_open_songs_in)) },
            text = {
                Column {
                    for (service in ServiceLinkOpener.defaultChoices) {
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            modifier = Modifier
                                .fillMaxWidth()
                                .selectable(selected = settings?.defaultService == service, role = Role.RadioButton) {
                                    viewModel.setDefaultService(service)
                                    serviceDialog = false
                                }
                                .padding(vertical = 4.dp),
                        ) {
                            RadioButton(selected = settings?.defaultService == service, onClick = null)
                            Text(service.displayName, modifier = Modifier.padding(start = 12.dp))
                        }
                    }
                }
            },
            confirmButton = { TextButton(onClick = { serviceDialog = false }) { Text(stringResource(R.string.session_cancel)) } },
        )
    }
    if (clearDialog) {
        val cache = Formatters.bytes(context, storage?.cacheBytes?.toLong() ?: 0L)
        AlertDialog(
            onDismissRequest = { clearDialog = false },
            text = { Text(stringResource(R.string.settings_clear_cache_confirm, cache)) },
            confirmButton = {
                TextButton(onClick = {
                    clearDialog = false
                    viewModel.clearCache()
                }) { Text(stringResource(R.string.settings_clear_cache)) }
            },
            dismissButton = { TextButton(onClick = { clearDialog = false }) { Text(stringResource(R.string.session_cancel)) } },
        )
    }
}

@Composable
private fun Header(text: Int) {
    Text(
        stringResource(text),
        style = MaterialTheme.typography.titleSmall,
        color = MaterialTheme.colorScheme.primary,
        modifier = Modifier.padding(start = 16.dp, end = 16.dp, top = 24.dp, bottom = 4.dp),
    )
}

@Composable
private fun TileRow(label: Int, icon: Int, service: Class<*>) {
    val context = LocalContext.current
    val canRequest = Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
    ListRow(
        headline = stringResource(label),
        supporting = if (canRequest) null else stringResource(R.string.settings_tile_manual),
        trailing = if (canRequest) {
            {
                OutlinedButton(onClick = { requestTile(context, service, label, icon) }) { Text(stringResource(R.string.settings_add_tile)) }
            }
        } else {
            null
        },
    )
}

private fun requestTile(context: Context, service: Class<*>, label: Int, icon: Int) {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
    val manager = context.getSystemService(StatusBarManager::class.java) ?: return
    manager.requestAddTileService(
        ComponentName(context, service),
        context.getString(label),
        Icon.createWithResource(context, icon),
        context.mainExecutor,
    ) { }
}

private fun openNotificationSettings(context: Context) {
    val intent = Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS)
        .putExtra(Settings.EXTRA_APP_PACKAGE, context.packageName)
        .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
    context.startActivity(intent)
}

@Composable
private fun MelodySection(
    installed: Boolean,
    songs: Int,
    sizeBytes: Long,
    version: String?,
    updateVersion: String?,
    downloadBytes: Long?,
    download: MelodyDownload,
    checkFailed: Boolean,
    onDownload: () -> Unit,
    onCancel: () -> Unit,
    onRemove: () -> Unit,
    onCheck: () -> Unit,
) {
    val context = LocalContext.current
    Column(Modifier.padding(horizontal = 16.dp, vertical = 8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        val status = when {
            download is MelodyDownload.Running -> {
                val total = downloadBytes
                val fraction = download.fraction
                if (total != null && fraction != null) {
                    stringResource(R.string.melody_status_downloading, Formatters.bytes(context, (total * fraction).toLong()), Formatters.bytes(context, total))
                } else {
                    stringResource(R.string.melody_status_preparing)
                }
            }
            download is MelodyDownload.Failed -> stringResource(R.string.melody_error)
            installed && updateVersion != null && downloadBytes != null ->
                stringResource(R.string.melody_status_update_available, Formatters.bytes(context, downloadBytes))
            installed -> stringResource(R.string.melody_status_installed, "%,d".format(songs), Formatters.bytes(context, sizeBytes))
            downloadBytes != null -> stringResource(R.string.melody_status_not_installed, Formatters.bytes(context, downloadBytes))
            else -> stringResource(R.string.melody_status_not_installed_unknown)
        }
        Text(status, style = MaterialTheme.typography.bodyLarge)
        if (installed && version != null && download !is MelodyDownload.Running) {
            Text(
                stringResource(R.string.melody_updated, version),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        AnimatedVisibility(visible = download is MelodyDownload.Running) {
            val fraction = (download as? MelodyDownload.Running)?.fraction
            if (fraction != null) {
                LinearProgressIndicator(progress = { fraction }, modifier = Modifier.fillMaxWidth())
            } else {
                LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
            }
        }
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            when {
                download is MelodyDownload.Running -> OutlinedButton(onClick = onCancel) { Text(stringResource(R.string.session_cancel)) }
                download is MelodyDownload.Failed -> FilledTonalButton(onClick = onDownload) { Text(stringResource(R.string.melody_try_again)) }
                installed && updateVersion != null -> {
                    FilledTonalButton(onClick = onDownload) { Text(stringResource(R.string.melody_update)) }
                    OutlinedButton(onClick = onRemove) { Text(stringResource(R.string.melody_remove)) }
                }
                installed -> {
                    OutlinedButton(onClick = onCheck) { Text(stringResource(R.string.melody_check_for_updates)) }
                    OutlinedButton(onClick = onRemove) { Text(stringResource(R.string.melody_remove)) }
                }
                checkFailed && downloadBytes == null -> FilledTonalButton(onClick = onCheck) { Text(stringResource(R.string.melody_try_again)) }
                else -> FilledTonalButton(onClick = onDownload) { Text(stringResource(R.string.melody_download)) }
            }
        }
        if (checkFailed && !installed && download !is MelodyDownload.Running) {
            Text(stringResource(R.string.melody_error), style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.error)
        }
        Text(
            stringResource(R.string.melody_footer),
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

/** An optional provider: shows whether it is set; expands to edit. Values are never displayed. */
@Composable
private fun CredentialGroup(
    key: String,
    title: Int,
    footer: Int,
    fields: List<Pair<CredentialField, Int>>,
    set: Set<CredentialField>,
    expanded: Boolean,
    onExpand: () -> Unit,
    onSave: (CredentialField, String) -> Unit,
) {
    val complete = fields.all { it.first in set }
    Column {
        ListRow(
            headline = stringResource(title),
            supporting = stringResource(if (complete) R.string.settings_set else R.string.settings_not_set),
            onClick = onExpand,
        )
        AnimatedVisibility(visible = expanded) {
            Column(Modifier.padding(horizontal = 16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                for ((field, label) in fields) {
                    var value by rememberSaveable(key + field.name) { mutableStateOf("") }
                    val secret = field != CredentialField.AcrHost && field != CredentialField.AcrHumHost
                    OutlinedTextField(
                        value = value,
                        onValueChange = { value = it },
                        label = { Text(stringResource(label)) },
                        placeholder = { if (field in set) Text("••••") },
                        singleLine = true,
                        visualTransformation = if (secret) PasswordVisualTransformation() else VisualTransformation.None,
                        keyboardOptions = KeyboardOptions(keyboardType = if (secret) KeyboardType.Password else KeyboardType.Uri),
                        modifier = Modifier.fillMaxWidth(),
                        trailingIcon = {
                            Row {
                                if (value.isNotBlank()) {
                                    TextButton(onClick = {
                                        onSave(field, value)
                                        value = ""
                                    }) { Text(stringResource(R.string.settings_save)) }
                                } else if (field in set) {
                                    TextButton(onClick = { onSave(field, "") }) { Text(stringResource(R.string.settings_clear)) }
                                }
                            }
                        },
                    )
                }
                Text(
                    stringResource(footer),
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(bottom = 8.dp),
                )
            }
        }
        HorizontalDivider(Modifier.padding(horizontal = 16.dp), color = MaterialTheme.colorScheme.outlineVariant.copy(alpha = 0.5f))
    }
}

/** Microphone order: the first connected one is used; reorder with the arrows. */
@Composable
private fun MicrophoneSection() {
    val context = LocalContext.current
    val builtIn = stringResource(R.string.settings_microphone_builtin)
    val priority = remember { app.musicfetch.capture.MicrophonePriority(context) }
    var revision by remember { mutableStateOf(0) }
    val entries = remember(revision) { priority.entries(builtIn) }
    val active = remember(revision) { priority.activeKey() }
    DisposableEffect(Unit) {
        val audio = context.getSystemService(android.media.AudioManager::class.java)
        val callback = object : android.media.AudioDeviceCallback() {
            override fun onAudioDevicesAdded(added: Array<out android.media.AudioDeviceInfo>?) { revision++ }
            override fun onAudioDevicesRemoved(removed: Array<out android.media.AudioDeviceInfo>?) { revision++ }
        }
        audio?.registerAudioDeviceCallback(callback, null)
        onDispose { audio?.unregisterAudioDeviceCallback(callback) }
    }
    val forgettable = { entry: app.musicfetch.capture.MicrophonePriority.Entry ->
        !entry.connected && entry.key != app.musicfetch.capture.MicrophonePriority.BUILT_IN
    }
    // Keep the arrows aligned when only some rows can be forgotten.
    val reserveForget = entries.any(forgettable)
    Column {
        entries.forEachIndexed { index, entry ->
            val status = when {
                entry.key == active -> stringResource(R.string.settings_microphone_in_use)
                !entry.connected -> stringResource(R.string.settings_microphone_not_connected)
                else -> null
            }
            ListRow(
                headline = if (entries.size > 1) "${index + 1}  ${entry.name}" else entry.name,
                supporting = status,
                trailing = if (entries.size < 2) null else {
                    {
                        Row {
                            androidx.compose.material3.IconButton(onClick = { priority.move(entry.key, -1); revision++ }, enabled = index > 0) {
                                androidx.compose.material3.Icon(painterResource(R.drawable.ic_keyboard_arrow_down), modifier = Modifier.rotate(180f), contentDescription = stringResource(R.string.settings_microphone_up, entry.name))
                            }
                            androidx.compose.material3.IconButton(onClick = { priority.move(entry.key, 1); revision++ }, enabled = index < entries.lastIndex) {
                                androidx.compose.material3.Icon(painterResource(R.drawable.ic_keyboard_arrow_down), contentDescription = stringResource(R.string.settings_microphone_down, entry.name))
                            }
                            if (forgettable(entry)) {
                                androidx.compose.material3.IconButton(onClick = { priority.forget(entry.key); revision++ }) {
                                    androidx.compose.material3.Icon(painterResource(R.drawable.ic_close), contentDescription = stringResource(R.string.settings_microphone_forget, entry.name))
                                }
                            } else if (reserveForget) {
                                Spacer(Modifier.size(40.dp))
                            }
                        }
                    }
                },
            )
        }
        Text(
            stringResource(R.string.settings_microphone_footer),
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
        )
    }
}
