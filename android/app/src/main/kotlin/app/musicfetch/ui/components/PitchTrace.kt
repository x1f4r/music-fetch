package app.musicfetch.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.withFrameNanos
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.StrokeJoin
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.capture.PitchRing

private const val SEMITONES = 12f

/**
 * The live pitch trace (docs/design/DESIGN.md §5.6): the last 8 s with the newest frame at the
 * right edge, ±12 semitones around the running median, opacity following voicing confidence,
 * gaps where unvoiced, and a dot marking "now". Scrolls continuously between pitch batches.
 */
@Composable
fun PitchTrace(ring: PitchRing, color: Color, modifier: Modifier = Modifier, active: Boolean = true, stroke: Dp = 4.dp) {
    val frameNanos = remember { mutableLongStateOf(0L) }
    val arrival = remember { ArrivalClock() }
    LaunchedEffect(active) {
        if (!active) return@LaunchedEffect
        while (true) withFrameNanos { frameNanos.longValue = it }
    }
    Canvas(modifier) {
        val now = frameNanos.longValue
        val snapshot = ring.snapshot()
        if (snapshot.isEmpty) return@Canvas
        val latest = snapshot.times.last()
        val nowMs = if (active) arrival.estimate(snapshot.version, latest, now) else latest
        val window = PitchRing.WINDOW_MS.toFloat()
        val median = median(snapshot.midis) ?: return@Canvas
        val strokePx = stroke.toPx()
        val perSemitone = (size.height - strokePx) / (2 * SEMITONES)
        fun x(t: Long) = size.width - strokePx / 2 - (nowMs - t) / window * (size.width - strokePx)
        fun y(midi: Float) = size.height / 2 - (midi - median).coerceIn(-SEMITONES, SEMITONES) * perSemitone
        var last: Offset? = null
        for (i in snapshot.times.indices) {
            val midi = snapshot.midis[i]
            if (midi.isNaN()) {
                last = null
                continue
            }
            val point = Offset(x(snapshot.times[i]), y(midi))
            val prev = last
            if (prev != null && point.x >= 0f) {
                val alpha = 0.6f + 0.4f * snapshot.confidences[i].coerceIn(0f, 1f)
                drawLine(color, prev, point, strokeWidth = strokePx, cap = StrokeCap.Round, alpha = alpha)
            }
            last = point
        }
        // "Now": a dot at the end of the live segment.
        val endMidi = snapshot.midis.last()
        if (!endMidi.isNaN()) drawCircle(color, radius = strokePx, center = Offset(x(latest), y(endMidi)))
    }
}

/** Interpolates "now" in session time between pitch batches (~10 Hz) for smooth scrolling. */
private class ArrivalClock {
    private var version = -1L
    private var frameTime = 0L
    private var arrivedNanos = 0L

    fun estimate(version: Long, latestMs: Long, nowNanos: Long): Long {
        if (version != this.version) {
            this.version = version
            frameTime = latestMs
            arrivedNanos = nowNanos
        }
        val elapsed = ((nowNanos - arrivedNanos) / 1_000_000).coerceIn(0, MAX_EXTRAPOLATION_MS)
        return frameTime + elapsed
    }

    private companion object {
        const val MAX_EXTRAPOLATION_MS = 250L
    }
}

/**
 * A finished contour (semitones relative to its median, NaN = unvoiced) filling the frame:
 * the hum strip above the candidates.
 */
@Composable
fun FrozenTrace(contour: List<Float>, color: Color, modifier: Modifier = Modifier, stroke: Dp = 2.5.dp) {
    Canvas(modifier) {
        drawContourLine(contour, median(contour.toFloatArray()) ?: return@Canvas, color, stroke.toPx())
    }
}

/**
 * "Your hum vs. the song" (docs/design/DESIGN.md §5.7): the reference in [referenceColor]
 * (4 dp, drawn first) and the hum on top (2.5 dp), on one ±12-semitone scale around the
 * reference's median.
 */
@Composable
fun ContourOverlay(query: List<Float>, reference: List<Float>, queryColor: Color, referenceColor: Color, modifier: Modifier = Modifier) {
    Canvas(modifier) {
        val center = median(reference.toFloatArray()) ?: median(query.toFloatArray()) ?: return@Canvas
        val frames = maxOf(query.size, reference.size)
        drawContourLine(reference, center, referenceColor, 4.dp.toPx(), frames)
        drawContourLine(query, center, queryColor, 2.5.dp.toPx(), frames)
    }
}

private fun DrawScope.drawContourLine(values: List<Float>, center: Float, color: Color, strokePx: Float, frames: Int = values.size) {
    if (values.size < 2 || frames < 2) return
    val perSemitone = (size.height - strokePx) / (2 * SEMITONES)
    val step = (size.width - strokePx) / (frames - 1)
    val path = Path()
    var open = false
    values.forEachIndexed { i, v ->
        if (v.isNaN()) {
            open = false
            return@forEachIndexed
        }
        val x = strokePx / 2 + i * step
        val y = size.height / 2 - (v - center).coerceIn(-SEMITONES, SEMITONES) * perSemitone
        if (open) path.lineTo(x, y) else path.moveTo(x, y)
        open = true
    }
    drawPath(path, color, style = Stroke(width = strokePx, cap = StrokeCap.Round, join = StrokeJoin.Round))
}

private fun median(values: FloatArray): Float? {
    val voiced = values.filter { !it.isNaN() }.sorted()
    if (voiced.isEmpty()) return null
    return voiced[voiced.size / 2]
}
