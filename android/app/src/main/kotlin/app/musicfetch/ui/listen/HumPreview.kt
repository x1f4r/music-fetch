package app.musicfetch.ui.listen

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.onClick
import androidx.compose.ui.semantics.role
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.text.style.TextDirection
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LifecycleEventEffect
import app.musicfetch.R
import app.musicfetch.core.Candidate
import app.musicfetch.core.CandidateTier
import app.musicfetch.platform.GoogleSongSearch
import app.musicfetch.ui.common.tag
import app.musicfetch.ui.components.CandidateTierTag
import app.musicfetch.ui.components.ContourMark

/**
 * "Sounds like …" under the listening UI while someone hums: the progressive search's top answer
 * with its tier (docs/design/DESIGN.md §6.2). A quiet preview, not a result: it is not a live
 * region and is replaced by every new list. Tapping it stops listening, like the button, so the
 * final answer comes at once.
 */
@Composable
internal fun HumPreviewChip(candidate: Candidate, onStop: () -> Unit, modifier: Modifier = Modifier) {
    val colors = MaterialTheme.colorScheme
    val track = candidate.track
    val tier = stringResource(
        when (candidate.tier) {
            CandidateTier.STRONG -> R.string.cand_strong
            CandidateTier.POSSIBLE -> R.string.cand_possible
            CandidateTier.LONG_SHOT -> R.string.cand_long_shot
        },
    )
    val description = stringResource(R.string.hum_preview_description, track.title, track.artist, tier)
    val stopLabel = stringResource(R.string.hum_preview_stop)
    Surface(
        onClick = onStop,
        shape = RoundedCornerShape(24.dp),
        color = colors.surfaceContainerHigh,
        modifier = modifier
            .widthIn(max = 400.dp)
            .heightIn(min = 48.dp)
            .clearAndSetSemantics {
                contentDescription = description
                role = Role.Button
                onClick(label = stopLabel) {
                    onStop()
                    true
                }
            }
            .tag("listen.preview"),
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(12.dp),
            modifier = Modifier.padding(start = 16.dp, end = 12.dp, top = 6.dp, bottom = 6.dp),
        ) {
            Box(Modifier.size(20.dp), contentAlignment = Alignment.Center) {
                ContourMark(colors.primary, width = 20.dp, strokeWidth = 2.dp)
            }
            Column(Modifier.weight(1f, fill = false)) {
                Text(
                    stringResource(R.string.hum_sounds_like),
                    style = MaterialTheme.typography.labelMedium,
                    color = colors.onSurfaceVariant,
                    maxLines = 1,
                )
                // The title keeps its room; the artist takes what is left.
                Row {
                    // Titles keep their own direction ("99 Luftballons" in a right-to-left UI).
                    Text(
                        track.title,
                        style = MaterialTheme.typography.titleSmall.copy(textDirection = TextDirection.Content),
                        color = colors.onSurface,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                    if (track.artist.isNotBlank()) {
                        // The separator follows the layout, the name its own direction.
                        Text(" · ", style = MaterialTheme.typography.bodyMedium, color = colors.onSurfaceVariant, maxLines = 1)
                        Text(
                            track.artist,
                            style = MaterialTheme.typography.bodyMedium.copy(textDirection = TextDirection.Content),
                            color = colors.onSurfaceVariant,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.weight(1f, fill = false),
                        )
                    }
                }
            }
            CandidateTierTag(candidate.tier)
        }
    }
}

/** Whether Google's song search can be opened; checked again whenever the screen resumes. */
@Composable
internal fun rememberGoogleSongSearch(): Boolean {
    val context = LocalContext.current
    var available by remember { mutableStateOf(GoogleSongSearch.isAvailable(context)) }
    LifecycleEventEffect(Lifecycle.Event.ON_RESUME) { available = GoogleSongSearch.isAvailable(context) }
    return available
}
