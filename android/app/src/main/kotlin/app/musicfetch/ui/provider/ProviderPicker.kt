package app.musicfetch.ui.provider

import androidx.compose.animation.animateColorAsState
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.RadioButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.R
import app.musicfetch.core.Service
import app.musicfetch.platform.CustomProvider
import app.musicfetch.platform.displayName

/** Identifies the selected row: a service name or [CUSTOM_KEY]. */
internal const val CUSTOM_KEY = "custom"

/** A rounded group of rows on `surfaceContainer`, read as one radio group. */
@Composable
internal fun ProviderGroup(modifier: Modifier = Modifier, content: @Composable ColumnScope.() -> Unit) {
    Column(
        modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(24.dp))
            .background(MaterialTheme.colorScheme.surfaceContainer)
            .selectableGroup(),
        content = content,
    )
}

/** One provider: a radio row; the selected row fills with `secondaryContainer`. */
@Composable
internal fun ProviderRow(
    name: String,
    selected: Boolean,
    onClick: () -> Unit,
    installed: Boolean = false,
    trailing: (@Composable () -> Unit)? = null,
) {
    val colors = MaterialTheme.colorScheme
    val fill by animateColorAsState(if (selected) colors.secondaryContainer else Color.Transparent, label = "fill")
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .background(fill)
            .selectable(selected = selected, role = Role.RadioButton, onClick = onClick)
            .heightIn(min = 56.dp)
            .padding(start = 8.dp, end = 20.dp),
    ) {
        RadioButton(selected = selected, onClick = null, modifier = Modifier.padding(horizontal = 8.dp))
        Text(
            name,
            style = MaterialTheme.typography.bodyLarge,
            color = if (selected) colors.onSecondaryContainer else colors.onSurface,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.weight(1f).padding(start = 8.dp),
        )
        if (installed) {
            Text(
                stringResource(R.string.provider_installed),
                style = MaterialTheme.typography.labelMedium,
                color = if (selected) colors.onSecondaryContainer else colors.onSurfaceVariant,
            )
        }
        trailing?.invoke()
    }
}

/** Rows for [services], each selectable by its name. */
@Composable
internal fun ColumnScope.ProviderRows(services: List<Service>, selected: String?, installed: Set<Service>, onSelect: (String) -> Unit) {
    services.forEachIndexed { index, service ->
        if (index > 0) RowDivider()
        ProviderRow(service.displayName, selected == service.name, onClick = { onSelect(service.name) }, installed = service in installed)
    }
}

/** "More…": reveals the other providers and the custom one. */
@Composable
internal fun MoreRow(onClick: () -> Unit) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .clickable(role = Role.Button, onClick = onClick)
            .heightIn(min = 56.dp)
            .padding(horizontal = 24.dp),
    ) {
        Text(
            stringResource(R.string.provider_more),
            style = MaterialTheme.typography.bodyLarge,
            color = MaterialTheme.colorScheme.primary,
            modifier = Modifier.weight(1f),
        )
        Icon(
            painterResource(R.drawable.ic_keyboard_arrow_down),
            contentDescription = null,
            tint = MaterialTheme.colorScheme.primary,
        )
    }
}

@Composable
internal fun RowDivider() {
    HorizontalDivider(Modifier.padding(start = 56.dp), color = MaterialTheme.colorScheme.surface, thickness = 1.dp)
}

/**
 * Name and link template of the custom provider. [problem] is shown once [showProblem] is set
 * (after the user tried to save), so typing isn't interrupted by errors.
 */
@Composable
internal fun CustomProviderFields(
    name: String,
    onName: (String) -> Unit,
    template: String,
    onTemplate: (String) -> Unit,
    problem: CustomProvider.Problem?,
    showProblem: Boolean,
    modifier: Modifier = Modifier,
    contentPadding: Dp = 16.dp,
) {
    val shown = problem.takeIf { showProblem }
    Column(modifier.fillMaxWidth().padding(contentPadding), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        OutlinedTextField(
            value = name,
            onValueChange = onName,
            label = { Text(stringResource(R.string.provider_custom_name)) },
            singleLine = true,
            isError = shown == CustomProvider.Problem.MissingName,
            supportingText = if (shown == CustomProvider.Problem.MissingName) {
                { Text(stringResource(R.string.provider_custom_needs_name)) }
            } else {
                null
            },
            keyboardOptions = KeyboardOptions(imeAction = ImeAction.Next),
            modifier = Modifier.fillMaxWidth(),
        )
        OutlinedTextField(
            value = template,
            onValueChange = onTemplate,
            label = { Text(stringResource(R.string.provider_custom_link)) },
            placeholder = { Text(stringResource(R.string.provider_custom_placeholder), maxLines = 1, overflow = TextOverflow.Ellipsis) },
            singleLine = true,
            isError = shown == CustomProvider.Problem.MissingPlaceholder || shown == CustomProvider.Problem.NotALink,
            supportingText = {
                val linkProblem = shown == CustomProvider.Problem.MissingPlaceholder || shown == CustomProvider.Problem.NotALink
                Text(stringResource(if (linkProblem) R.string.provider_custom_invalid else R.string.provider_custom_hint))
            },
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri, imeAction = ImeAction.Done),
            modifier = Modifier.fillMaxWidth(),
        )
    }
}
