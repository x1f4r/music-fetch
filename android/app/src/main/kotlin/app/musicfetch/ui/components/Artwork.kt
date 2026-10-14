package app.musicfetch.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.R
import coil3.compose.AsyncImage

/** Square artwork with a quiet placeholder; [corner] follows docs/design/DESIGN.md §4.3. */
@Composable
fun Artwork(
    url: String?,
    size: Dp,
    modifier: Modifier = Modifier,
    corner: Dp = 8.dp,
    placeholderIcon: Int = R.drawable.ic_music_note,
    contentDescription: String? = null,
) {
    var failed by remember(url) { mutableStateOf(false) }
    val shape = RoundedCornerShape(corner)
    Box(
        modifier.size(size).clip(shape).background(MaterialTheme.colorScheme.surfaceContainerHighest),
        contentAlignment = Alignment.Center,
    ) {
        if (url == null || failed) {
            Icon(
                painterResource(placeholderIcon),
                contentDescription = null,
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(size * 0.4f),
            )
        } else {
            AsyncImage(
                model = url,
                contentDescription = contentDescription,
                contentScale = ContentScale.Crop,
                onError = { failed = true },
                modifier = Modifier.fillMaxSize(),
            )
        }
    }
}

/** The hatched tile of an unidentified segment ("ID"). */
@Composable
fun HatchedTile(size: Dp, modifier: Modifier = Modifier, corner: Dp = 8.dp) {
    val color = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.22f)
    Canvas(modifier.size(size).clip(RoundedCornerShape(corner)).background(MaterialTheme.colorScheme.surfaceContainerHigh)) {
        val step = 6.dp.toPx()
        val line = 1.5.dp.toPx()
        var start = -this.size.height
        while (start < this.size.width) {
            drawLine(color, Offset(start, this.size.height), Offset(start + this.size.height, 0f), strokeWidth = line)
            start += step
        }
    }
}
