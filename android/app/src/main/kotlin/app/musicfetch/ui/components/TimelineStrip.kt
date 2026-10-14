package app.musicfetch.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.gestures.rememberTransformableState
import androidx.compose.foundation.gestures.transformable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.BiasAlignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.drawscope.clipRect
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.hideFromAccessibility
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import app.musicfetch.core.Segment
import app.musicfetch.core.SegmentKind
import app.musicfetch.core.Tier
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.theme.TabularNumbers

/**
 * The mix timeline (docs/design/DESIGN.md §5.12): a 16 dp ruler and a 28 dp strip with one
 * block per segment, styled by tier (solid, 60 %, outline, hatch), speech as a thin bar,
 * the not-yet-analyzed part veiled with a frontier line, and the selection outlined.
 * Pinch to zoom (1…40×); tap a block to select it. Hidden from TalkBack (the list is the
 * accessible representation).
 */
@Composable
fun TimelineStrip(
    segments: List<Segment>,
    totalMs: Long,
    analyzedMs: Long?,
    selectedId: String?,
    onSelect: (Segment) -> Unit,
    modifier: Modifier = Modifier,
) {
    val colors = MaterialTheme.colorScheme
    var zoom by remember { mutableFloatStateOf(1f) }
    val scroll = rememberScrollState()
    val transform = rememberTransformableState { _, change, _, _ -> zoom = (zoom * change).coerceIn(1f, 40f) }
    val density = LocalDensity.current
    BoxWithConstraints(modifier.fillMaxWidth().semantics { hideFromAccessibility() }) {
        val viewport = maxWidth
        val contentWidth = viewport * zoom
        // Keep the selected segment in view when the selection or zoom changes.
        LaunchedEffect(selectedId, zoom, totalMs) {
            val selected = segments.firstOrNull { it.id == selectedId } ?: return@LaunchedEffect
            if (totalMs <= 0) return@LaunchedEffect
            val widthPx = with(density) { contentWidth.toPx() }
            val viewPx = with(density) { viewport.toPx() }
            val startPx = selected.startMs.toLong() / totalMs.toFloat() * widthPx
            val endPx = selected.endMs.toLong() / totalMs.toFloat() * widthPx
            if (startPx < scroll.value || endPx > scroll.value + viewPx) {
                scroll.animateScrollTo((startPx - viewPx / 4).toInt().coerceAtLeast(0))
            }
        }
        Box(Modifier.transformable(transform).horizontalScroll(scroll)) {
            Column(Modifier.width(contentWidth)) {
                Ruler(totalMs, Modifier.fillMaxWidth().height(16.dp))
                Canvas(
                    Modifier
                        .fillMaxWidth()
                        .height(28.dp)
                        .clip(RoundedCornerShape(6.dp))
                        .pointerInput(segments, totalMs) {
                            detectTapGestures { offset ->
                                if (totalMs <= 0) return@detectTapGestures
                                val t = (offset.x / size.width * totalMs).toLong()
                                segments.firstOrNull { it.startMs.toLong() <= t && t <= it.endMs.toLong() && it.kind != SegmentKind.SPEECH }
                                    ?.let(onSelect)
                            }
                        },
                ) {
                    drawRect(colors.surfaceContainerHigh)
                    if (totalMs <= 0) return@Canvas
                    val ms = size.width / totalMs
                    val minWidth = 3.dp.toPx()
                    val gap = 1.dp.toPx()
                    for (segment in segments) {
                        val start = segment.startMs.toLong() * ms
                        val width = maxOf((segment.endMs.toLong() - segment.startMs.toLong()) * ms - gap, minWidth)
                        drawSegment(segment, start, width, colors.primary, colors.onSurface)
                    }
                    analyzedMs?.takeIf { it < totalMs }?.let { analyzed ->
                        val x = analyzed * ms
                        drawRect(colors.surface.copy(alpha = 0.75f), topLeft = Offset(x, 0f), size = Size(size.width - x, size.height))
                        drawRect(colors.onSurface, topLeft = Offset(x, 0f), size = Size(1.5.dp.toPx(), size.height))
                    }
                    segments.firstOrNull { it.id == selectedId }?.let { segment ->
                        val start = segment.startMs.toLong() * ms
                        val width = maxOf((segment.endMs.toLong() - segment.startMs.toLong()) * ms - gap, minWidth)
                        val line = 1.5.dp.toPx()
                        drawRoundRect(
                            colors.surface,
                            topLeft = Offset(start + line, line),
                            size = Size(width - line * 2, size.height - line * 2),
                            cornerRadius = CornerRadius(4.dp.toPx()),
                            style = Stroke(line),
                        )
                        drawRoundRect(
                            colors.onSurface,
                            topLeft = Offset(start, 0f),
                            size = Size(width, size.height),
                            cornerRadius = CornerRadius(5.dp.toPx()),
                            style = Stroke(line),
                        )
                    }
                }
            }
        }
    }
}

private fun DrawScope.drawSegment(segment: Segment, x: Float, width: Float, accent: Color, label: Color) {
    val radius = CornerRadius(4.dp.toPx())
    when {
        segment.kind == SegmentKind.SILENCE -> Unit
        segment.kind == SegmentKind.SPEECH -> {
            val h = 6.dp.toPx()
            drawRoundRect(label.copy(alpha = 0.38f), Offset(x, (size.height - h) / 2), Size(width, h), CornerRadius(h / 2))
        }
        segment.kind == SegmentKind.UNIDENTIFIED || segment.tier == Tier.UNKNOWN -> hatch(x, width, label.copy(alpha = 0.22f))
        segment.tier == Tier.CONFIRMED -> drawRoundRect(accent, Offset(x, 0f), Size(width, size.height), radius)
        segment.tier == Tier.LIKELY -> drawRoundRect(accent.copy(alpha = 0.6f), Offset(x, 0f), Size(width, size.height), radius)
        else -> {
            val line = 1.5.dp.toPx()
            drawRoundRect(
                accent,
                Offset(x + line / 2, line / 2),
                Size(width - line, size.height - line),
                radius,
                style = Stroke(line),
            )
        }
    }
}

/** Diagonal hatch: 1.5 dp lines every 6 dp. */
private fun DrawScope.hatch(x: Float, width: Float, color: Color) {
    val step = 6.dp.toPx()
    val line = 1.5.dp.toPx()
    clipRect(x, 0f, x + width, size.height) {
        var start = x - size.height
        while (start < x + width) {
            drawLine(color, Offset(start, size.height), Offset(start + size.height, 0f), strokeWidth = line)
            start += step
        }
    }
}

@Composable
private fun Ruler(totalMs: Long, modifier: Modifier) {
    Box(modifier) {
        if (totalMs <= 0) return@Box
        val style = MaterialTheme.typography.labelSmall.merge(TabularNumbers)
        val color = MaterialTheme.colorScheme.onSurfaceVariant
        val marks = listOf(0f, 0.25f, 0.5f, 0.75f, 1f)
        Box(Modifier.fillMaxWidth()) {
            for (fraction in marks) {
                val text = Formatters.clock((totalMs * fraction).toLong())
                val align = when (fraction) {
                    0f -> Alignment.CenterStart
                    1f -> Alignment.CenterEnd
                    else -> Alignment.Center
                }
                val bias = fraction * 2 - 1
                Text(
                    text,
                    style = style,
                    color = color,
                    textAlign = TextAlign.Center,
                    modifier = Modifier.align(if (align == Alignment.Center) BiasAlignment(bias, 0f) else align),
                )
            }
        }
    }
}
