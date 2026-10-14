package app.musicfetch.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.layout.size
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.StrokeJoin
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import kotlin.math.PI
import kotlin.math.min
import kotlin.math.sin

/**
 * The contour mark (docs/design/DESIGN.md §2.1): one round-capped stroke in a 48 × 20 box,
 * three humps of growing amplitude that settle. Also the live line it turns into while
 * listening (§4.4): the same stroke as a sum of three sines scaled by the input level.
 */
object ContourMarkGeometry {
    const val BOX_WIDTH = 48f
    const val BOX_HEIGHT = 20f
    const val STROKE = 3f

    private val segments = listOf(
        floatArrayOf(3f, 11f, 7f, 11f, 8f, 5f, 12f, 5f),
        floatArrayOf(12f, 5f, 16f, 5f, 17f, 15f, 21f, 15f),
        floatArrayOf(21f, 15f, 26f, 15f, 26f, 3f, 31f, 3f),
        floatArrayOf(31f, 3f, 36f, 3f, 36f, 13f, 40f, 13f),
        floatArrayOf(40f, 13f, 43f, 13f, 44f, 9f, 45f, 9f),
    )

    /** Number of samples of the live line. */
    const val SAMPLES = 65

    /** The mark's y (box units) at [SAMPLES] evenly spaced x between its ends. */
    val restingSamples: FloatArray by lazy {
        val points = ArrayList<Pair<Float, Float>>()
        for (seg in segments) {
            for (i in 0..40) {
                val t = i / 40f
                val u = 1 - t
                val x = u * u * u * seg[0] + 3 * u * u * t * seg[2] + 3 * u * t * t * seg[4] + t * t * t * seg[6]
                val y = u * u * u * seg[1] + 3 * u * u * t * seg[3] + 3 * u * t * t * seg[5] + t * t * t * seg[7]
                points += x to y
            }
        }
        val start = segments.first()[0]
        val end = segments.last()[6]
        FloatArray(SAMPLES) { i ->
            val x = start + (end - start) * i / (SAMPLES - 1)
            val k = points.indexOfFirst { it.first >= x }.coerceAtLeast(1)
            val (x0, y0) = points[k - 1]
            val (x1, y1) = points[k]
            if (x1 == x0) y0 else y0 + (y1 - y0) * (x - x0) / (x1 - x0)
        }
    }

    /** The exact mark as a path in a [size] frame (aspect kept, centred). */
    fun markPath(size: Size): Path {
        val scale = min(size.width / BOX_WIDTH, size.height / BOX_HEIGHT)
        val dx = (size.width - BOX_WIDTH * scale) / 2
        val dy = (size.height - BOX_HEIGHT * scale) / 2
        return Path().apply {
            moveTo(dx + segments[0][0] * scale, dy + segments[0][1] * scale)
            for (s in segments) {
                cubicTo(dx + s[2] * scale, dy + s[3] * scale, dx + s[4] * scale, dy + s[5] * scale, dx + s[6] * scale, dy + s[7] * scale)
            }
        }
    }

    private val amplitudes = floatArrayOf(0.6f, 0.3f, 0.15f)
    private val frequencies = floatArrayOf(1.0f, 2.3f, 3.7f)

    /**
     * Points of the line between the resting mark (`liveness` 0) and the live line (1) whose
     * amplitude is [level]; [timeSeconds] advances the phases at 1.0 / 2.3 / 3.7 rad/s.
     */
    fun linePath(size: Size, liveness: Float, level: Float, timeSeconds: Float, out: Path = Path()): Path {
        val scale = min(size.width / BOX_WIDTH, size.height / BOX_HEIGHT)
        val dx = (size.width - BOX_WIDTH * scale) / 2
        val dy = (size.height - BOX_HEIGHT * scale) / 2
        val x0 = segments.first()[0]
        val x1 = segments.last()[6]
        val resting = restingSamples
        val mid = BOX_HEIGHT / 2
        val half = (15f - 3f) / 2 + 1f
        out.reset()
        for (i in 0 until SAMPLES) {
            val u = i / (SAMPLES - 1f)
            var sum = 0f
            for (k in 0 until 3) {
                sum += amplitudes[k] * sin(2 * PI.toFloat() * frequencies[k] * (2 * u) + frequencies[k] * timeSeconds)
            }
            val envelope = sin(PI.toFloat() * u)
            val live = mid + envelope * level.coerceIn(0f, 1f) * half * sum
            val y = resting[i] + (live - resting[i]) * liveness.coerceIn(0f, 1f)
            val x = x0 + (x1 - x0) * u
            val px = dx + x * scale
            val py = dy + y * scale
            if (i == 0) out.moveTo(px, py) else out.lineTo(px, py)
        }
        return out
    }
}

/** Draws [path] as the mark's stroke: round caps and joins. */
fun DrawScope.drawContour(path: Path, color: Color, strokeWidth: Float, alpha: Float = 1f) {
    drawPath(path, color, alpha = alpha, style = Stroke(width = strokeWidth, cap = StrokeCap.Round, join = StrokeJoin.Round))
}

/** The static contour mark, e.g. for icons and the empty library. */
@Composable
fun ContourMark(color: Color, modifier: Modifier = Modifier, width: Dp = 48.dp, strokeWidth: Dp = 3.dp) {
    Canvas(modifier.size(width, width * (ContourMarkGeometry.BOX_HEIGHT / ContourMarkGeometry.BOX_WIDTH))) {
        val inset = strokeWidth.toPx() / 2
        val path = ContourMarkGeometry.markPath(Size(size.width - inset * 2, size.height - inset * 2))
        path.translate(Offset(inset, inset))
        drawContour(path, color, strokeWidth.toPx())
    }
}
