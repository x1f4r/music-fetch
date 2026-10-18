package app.musicfetch.ui.provider

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import app.musicfetch.ui.common.devTestTagsRoot
import app.musicfetch.ui.common.tag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.core.Service
import app.musicfetch.platform.ProviderChoice
import app.musicfetch.platform.Providers
import app.musicfetch.ui.components.ContourMark

/**
 * "Where do you listen?" (docs/APPS.md, "Opening links"): shown on launch until the user
 * chooses a provider or skips. Nothing is stored before that, so leaving the app shows the
 * panel again next time. An installed app is pre-selected.
 */
@Composable
fun ProviderPanel(model: ProviderModel = viewModel()) {
    val suggested by model.suggested.collectAsStateWithLifecycle()
    val installed by model.installed.collectAsStateWithLifecycle()
    val stored by model.settings.collectAsStateWithLifecycle()
    var selected by rememberSaveable { mutableStateOf<String?>(null) }
    var touched by rememberSaveable { mutableStateOf(false) }
    var showMore by rememberSaveable { mutableStateOf(false) }
    var customName by rememberSaveable { mutableStateOf("") }
    var customTemplate by rememberSaveable { mutableStateOf("") }
    var showProblem by rememberSaveable { mutableStateOf(false) }

    LaunchedEffect(suggested) {
        if (!touched) suggested?.let { selected = it.name }
    }
    // A custom provider from an earlier attempt (the decision itself was never stored).
    LaunchedEffect(stored?.customProvider) {
        stored?.customProvider?.let { custom ->
            if (customName.isEmpty() && customTemplate.isEmpty()) {
                customName = custom.name
                customTemplate = custom.template
            }
        }
    }
    val problem = if (selected == CUSTOM_KEY) model.problem(customName, customTemplate) else null
    fun select(key: String) {
        touched = true
        selected = key
        showProblem = false
    }

    Surface(color = MaterialTheme.colorScheme.surface, modifier = Modifier.fillMaxSize().devTestTagsRoot()) {
        Column(
            Modifier
                .fillMaxSize()
                .windowInsetsPadding(WindowInsets.safeDrawing)
                .imePadding(),
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            Column(
                Modifier
                    .weight(1f)
                    .widthIn(max = 560.dp)
                    .verticalScroll(rememberScrollState())
                    .padding(horizontal = 16.dp),
                horizontalAlignment = Alignment.CenterHorizontally,
            ) {
                Spacer(Modifier.height(56.dp))
                Box(Modifier.size(64.dp, 28.dp), contentAlignment = Alignment.Center) {
                    ContourMark(MaterialTheme.colorScheme.primary, width = 64.dp, strokeWidth = 4.dp)
                }
                Spacer(Modifier.height(24.dp))
                Text(
                    stringResource(R.string.provider_panel_title),
                    style = MaterialTheme.typography.headlineMediumEmphasized,
                    textAlign = TextAlign.Center,
                    modifier = Modifier.semantics { heading() },
                )
                Spacer(Modifier.height(8.dp))
                Text(
                    stringResource(R.string.provider_panel_message),
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                )
                Spacer(Modifier.height(32.dp))
                val found = installed.orEmpty()
                ProviderGroup {
                    ProviderRows(Providers.common, selected, found, ::select)
                    if (!showMore) {
                        RowDivider()
                        MoreRow(onClick = { showMore = true })
                    }
                }
                AnimatedVisibility(visible = showMore) {
                    Column {
                        Spacer(Modifier.height(16.dp))
                        ProviderGroup {
                            ProviderRows(Providers.more, selected, found, ::select)
                        }
                        Spacer(Modifier.height(16.dp))
                        ProviderGroup {
                            ProviderRow(
                                stringResource(R.string.provider_custom),
                                selected == CUSTOM_KEY,
                                onClick = { select(CUSTOM_KEY) },
                            )
                            AnimatedVisibility(visible = selected == CUSTOM_KEY) {
                                CustomProviderFields(
                                    name = customName,
                                    onName = { customName = it },
                                    template = customTemplate,
                                    onTemplate = { customTemplate = it },
                                    problem = problem,
                                    showProblem = showProblem,
                                )
                            }
                        }
                    }
                }
                Spacer(Modifier.height(24.dp))
            }
            Row(
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier
                    .widthIn(max = 560.dp)
                    .fillMaxWidth()
                    .padding(horizontal = 24.dp, vertical = 16.dp),
            ) {
                TextButton(onClick = model::skip, modifier = Modifier.heightIn(min = 48.dp).tag("provider.skip")) {
                    Text(stringResource(R.string.provider_skip))
                }
                Button(
                    onClick = {
                        when (val key = selected) {
                            null -> Unit
                            CUSTOM_KEY -> if (!model.saveCustom(customName, customTemplate)) showProblem = true
                            else -> Service.entries.firstOrNull { it.name == key }?.let { model.choose(ProviderChoice.Catalog(it)) }
                        }
                    },
                    enabled = selected != null,
                    modifier = Modifier.heightIn(min = 48.dp).tag("provider.continue"),
                ) {
                    Text(stringResource(R.string.action_continue))
                }
            }
        }
    }
}
