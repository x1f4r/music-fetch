package app.musicfetch.ui.components

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import app.musicfetch.R
import app.musicfetch.core.CandidateTier

/** Tag styles: never colour alone (docs/design/DESIGN.md §4.1). */
enum class TagStyle { Filled, Tonal, Outlined }

/** A 22 dp label tag ("Strong", "Possible", "Long shot"). */
@Composable
fun Tag(text: String, style: TagStyle, modifier: Modifier = Modifier) {
    val colors = MaterialTheme.colorScheme
    val (container, content, border) = when (style) {
        TagStyle.Filled -> Triple(colors.primaryContainer, colors.onPrimaryContainer, null)
        TagStyle.Tonal -> Triple(colors.secondaryContainer, colors.onSecondaryContainer, null)
        TagStyle.Outlined -> Triple(Color.Transparent, colors.onSurfaceVariant, BorderStroke(1.dp, colors.outline))
    }
    Surface(
        modifier = modifier.heightIn(min = 22.dp),
        shape = RoundedCornerShape(6.dp),
        color = container,
        contentColor = content,
        border = border,
    ) {
        Text(text, style = MaterialTheme.typography.labelMedium, modifier = Modifier.padding(horizontal = 8.dp, vertical = 3.dp))
    }
}

@Composable
fun CandidateTierTag(tier: CandidateTier, modifier: Modifier = Modifier) {
    when (tier) {
        CandidateTier.STRONG -> Tag(stringResource(R.string.cand_strong), TagStyle.Filled, modifier)
        CandidateTier.POSSIBLE -> Tag(stringResource(R.string.cand_possible), TagStyle.Tonal, modifier)
        CandidateTier.LONG_SHOT -> Tag(stringResource(R.string.cand_long_shot), TagStyle.Outlined, modifier)
    }
}
