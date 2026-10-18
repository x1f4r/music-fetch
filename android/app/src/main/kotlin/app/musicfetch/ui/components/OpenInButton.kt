package app.musicfetch.ui.components

import android.content.Context
import android.net.Uri
import android.widget.Toast
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.size
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.SplitButtonDefaults
import androidx.compose.material3.SplitButtonLayout
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.rotate
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.R
import app.musicfetch.core.Track
import app.musicfetch.platform.OpenTarget
import app.musicfetch.platform.ProviderPreference
import app.musicfetch.platform.ServiceLinkOpener
import app.musicfetch.platform.displayName

/** Opens track links in the user's provider, with "Search the web" as the last resort. */
object TrackLinks {
    fun open(context: Context, target: OpenTarget) {
        if (!ServiceLinkOpener.open(context, target)) noApp(context)
    }

    fun searchWeb(context: Context, track: Track) {
        val query = listOf(track.artist, track.title).filter { it.isNotBlank() }.joinToString(" ")
        if (!ServiceLinkOpener.openUrl(context, "https://www.google.com/search?q=" + Uri.encode(query))) noApp(context)
    }

    private fun noApp(context: Context) {
        Toast.makeText(context, R.string.error_no_browser, Toast.LENGTH_SHORT).show()
    }
}

/** The provider's name as the primary button says it ("Open in YouTube Music"). */
val OpenTarget.providerName: String
    get() = when (this) {
        is OpenTarget.Link -> link.service.displayName
        is OpenTarget.Custom -> name
    }

/** A menu entry: the provider, marked when it opens a search rather than the track. */
@Composable
fun OpenTarget.menuLabel(): String =
    if (this is OpenTarget.Link && link.isSearch) stringResource(R.string.result_search, providerName) else providerName

/**
 * "Open in YouTube Music | ▾" (docs/design/DESIGN.md §5.11, docs/APPS.md "Opening links"):
 * the leading part opens the preferred provider (directly in its app when installed); the
 * chevron opens the track once in any other provider, exact links first, then "Search the web".
 */
@OptIn(ExperimentalMaterial3ExpressiveApi::class)
@Composable
fun OpenInSplitButton(track: Track, preference: ProviderPreference, modifier: Modifier = Modifier, height: Dp = 56.dp) {
    val context = LocalContext.current
    val choices = remember(track, preference) { ServiceLinkOpener.choices(track, preference) }
    val primary = choices.primary
    var expanded by remember { mutableStateOf(false) }
    val rotation by animateFloatAsState(if (expanded) 180f else 0f, label = "chevron")
    val label = primary?.let { stringResource(R.string.result_open_in, it.providerName) } ?: stringResource(R.string.result_search_web)
    val textStyle = if (height >= 56.dp) MaterialTheme.typography.titleMedium else MaterialTheme.typography.labelLarge
    val moreDescription = stringResource(R.string.result_more_services)
    SplitButtonLayout(
        modifier = modifier,
        leadingButton = {
            SplitButtonDefaults.LeadingButton(
                onClick = { primary?.let { TrackLinks.open(context, it) } ?: TrackLinks.searchWeb(context, track) },
                modifier = Modifier.heightIn(min = height),
                shapes = SplitButtonDefaults.leadingButtonShapesFor(height),
                contentPadding = SplitButtonDefaults.leadingButtonContentPaddingFor(height),
            ) {
                Text(label, style = textStyle, maxLines = 1, overflow = TextOverflow.Ellipsis)
            }
        },
        trailingButton = {
            Box {
                SplitButtonDefaults.TrailingButton(
                    checked = expanded,
                    onCheckedChange = { expanded = it },
                    modifier = Modifier.heightIn(min = height).semantics { contentDescription = moreDescription },
                    shapes = SplitButtonDefaults.trailingButtonShapesFor(height),
                    contentPadding = SplitButtonDefaults.trailingButtonContentPaddingFor(height),
                ) {
                    Icon(
                        painterResource(R.drawable.ic_keyboard_arrow_down),
                        contentDescription = null,
                        modifier = Modifier.size(SplitButtonDefaults.trailingButtonIconSizeFor(height)).rotate(rotation),
                    )
                }
                DropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
                    for (target in choices.others) {
                        DropdownMenuItem(
                            text = { Text(target.menuLabel()) },
                            onClick = {
                                expanded = false
                                TrackLinks.open(context, target)
                            },
                        )
                    }
                    DropdownMenuItem(
                        text = { Text(stringResource(R.string.result_search_web)) },
                        onClick = {
                            expanded = false
                            TrackLinks.searchWeb(context, track)
                        },
                    )
                }
            }
        },
    )
}
