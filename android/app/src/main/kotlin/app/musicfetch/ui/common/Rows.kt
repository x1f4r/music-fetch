package app.musicfetch.ui.common

import androidx.compose.material3.ListItem
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier

/** A one- or two-line list row (settings, menus) on the M3 `ListItem`. */
@Composable
fun ListRow(
    headline: String,
    modifier: Modifier = Modifier,
    supporting: String? = null,
    trailing: (@Composable () -> Unit)? = null,
    onClick: (() -> Unit)? = null,
) {
    val supportingContent: (@Composable () -> Unit)? = supporting?.let { { Text(it) } }
    if (onClick != null) {
        ListItem(onClick = onClick, modifier = modifier, supportingContent = supportingContent, trailingContent = trailing) {
            Text(headline)
        }
    } else {
        ListItem(modifier = modifier, supportingContent = supportingContent, trailingContent = trailing) {
            Text(headline)
        }
    }
}
