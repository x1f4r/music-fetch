package app.musicfetch.ui.provider

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.core.Service
import app.musicfetch.platform.CustomProvider
import app.musicfetch.platform.ProviderChoice
import app.musicfetch.platform.Providers
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.session.BackBar
import app.musicfetch.ui.theme.StandardMotion

/**
 * Settings › Open songs in: every provider plus the custom one. A provider applies as soon as
 * it is tapped; the custom provider is defined in a dialog and applies when saved.
 */
@Composable
fun ProviderSettingsScreen(model: ProviderModel = viewModel()) {
    val navigator = LocalNavigator.current
    val settings by model.settings.collectAsStateWithLifecycle()
    val installed by model.installed.collectAsStateWithLifecycle()
    val custom = settings?.customProvider
    val selected = when (val stored = settings?.preference?.choice) {
        is ProviderChoice.Catalog -> stored.service.name
        ProviderChoice.Custom -> CUSTOM_KEY
        null -> null
    }
    var editing by rememberSaveable { mutableStateOf(false) }
    val found = installed.orEmpty()
    fun choose(key: String) {
        Service.entries.firstOrNull { it.name == key }?.let { model.choose(ProviderChoice.Catalog(it)) }
    }

    StandardMotion {
        Scaffold(topBar = { BackBar({ navigator.back() }, title = { Text(stringResource(R.string.settings_open_songs_in)) }) }) { padding ->
            Column(
                Modifier
                    .fillMaxSize()
                    .padding(top = padding.calculateTopPadding())
                    .verticalScroll(rememberScrollState())
                    .padding(horizontal = 16.dp, vertical = 8.dp),
            ) {
                ProviderGroup { ProviderRows(Providers.common, selected, found, ::choose) }
                Spacer(Modifier.height(16.dp))
                ProviderGroup { ProviderRows(Providers.more, selected, found, ::choose) }
                Spacer(Modifier.height(16.dp))
                ProviderGroup {
                    ProviderRow(
                        custom?.name ?: stringResource(R.string.provider_custom),
                        selected == CUSTOM_KEY,
                        onClick = { if (custom != null) model.choose(ProviderChoice.Custom) else editing = true },
                        trailing = custom?.let {
                            { TextButton(onClick = { editing = true }) { Text(stringResource(R.string.provider_custom_edit)) } }
                        },
                    )
                }
                Spacer(Modifier.height(16.dp))
            }
        }
    }
    if (editing) {
        CustomProviderDialog(custom, model, onDismiss = { editing = false })
    }
}

/** Name and link template of the custom provider; Save stores and selects it. */
@Composable
private fun CustomProviderDialog(current: CustomProvider?, model: ProviderModel, onDismiss: () -> Unit) {
    var name by rememberSaveable { mutableStateOf(current?.name.orEmpty()) }
    var template by rememberSaveable { mutableStateOf(current?.template.orEmpty()) }
    var showProblem by rememberSaveable { mutableStateOf(false) }
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(R.string.provider_custom)) },
        text = {
            CustomProviderFields(
                name = name,
                onName = { name = it },
                template = template,
                onTemplate = { template = it },
                problem = model.problem(name, template),
                showProblem = showProblem,
                contentPadding = 0.dp,
            )
        },
        confirmButton = {
            TextButton(onClick = { if (model.saveCustom(name, template)) onDismiss() else showProblem = true }) {
                Text(stringResource(R.string.settings_save))
            }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text(stringResource(R.string.session_cancel)) } },
    )
}
