package app.musicfetch.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.withFrameNanos
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.StrokeJoin
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.graphics.drawscope.clipRect
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.drawscope.translate
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.capture.PitchRing
import app.musicfetch.platform.LocalLowPower
import kotlinx.coroutines.delay
import kotlin.math.roundToInt

private const val SEMITONES = 12f

/**
 * The live pitch trace (docs/design/DESIGN.md §5.6): the last 8 s with the newest frame at the
 * right edge, ±12 semitones around the running median, opacity following voicing confidence,
 * gaps where unvoiced, and a dot marking "now". Scrolls continuously between pitch batches.
 */
@Composable
fun PitchTrace(ring: PitchRing, color: Color, modifier: Modifier = Modifier, active: Boolean = true, stroke: Dp = 4.dp) {
    val lowPower = LocalLowPower.current
    val frameNanos = remember { mutableLongStateOf(0L) }
    val arrival = remember { ArrivalClock() }
    val cache = remember { TraceCache() }
    LaunchedEffect(active, lowPower) {
        if (!active) return@LaunchedEffect
        while (true) {
            withFrameNanos { frameNanos.longValue = it }
            if (lowPower) delay(LOW_POWER_FRAME_MS)
        }
    }
    Canvas(modifier) {
        val now = frameNanos.longValue
        val strokePx = stroke.toPx()
        val trace = cache.update(ring, size, strokePx) ?: return@Canvas
        val nowMs = if (active) arrival.estimate(trace.version, trace.latestMs, now) else trace.latestMs
        // The paths put the newest frame at the right edge; scrolling only shifts them left.
        val dx = (nowMs - trace.latestMs) * trace.pxPerMs
        clipRect {
            translate(left = -dx) {
                trace.paths.forEachIndexed { bucket, path ->
                    if (!path.isEmpty) drawPath(path, color, alpha = BUCKET_ALPHA[bucket], style = trace.style)
                }
                // "Now": a dot at the end of the live segment.
                trace.end?.let { drawCircle(color, radius = strokePx, center = it) }
            }
        }
    }
}

/** Opacity per voicing-confidence bucket (0.6 for unsure frames … 1.0 for clear ones). */
private val BUCKET_ALPHA = floatArrayOf(0.6f, 0.7f, 0.8f, 0.9f, 1f)

/** The trace laid out for one window of frames, reused until new frames arrive. */
private class TraceShape(
    val version: Long,
    val latestMs: Long,
    val pxPerMs: Float,
    val paths: List<Path>,
    val end: Offset?,
    val style: Stroke,
)

/**
 * Lays the window out again only when the ring changed (about 10 times a second), not on
 * every frame: the median, the copy of the frames and the paths are the expensive part.
 */
private class TraceCache {
    private var version = -1L
    private var size = Size.Zero
    private var shape: TraceShape? = null
    private val paths = List(BUCKET_ALPHA.size) { Path() }

    fun update(ring: PitchRing, size: Size, strokePx: Float): TraceShape? {
        val current = ring.version()
        if (current == version && size == this.size) return shape
        version = current
        this.size = size
        val snapshot = ring.snapshot()
        shape = if (snapshot.isEmpty) null else layout(snapshot, size, strokePx)
        return shape
    }

    private fun layout(snapshot: PitchRing.Snapshot, size: Size, strokePx: Float): TraceShape? {
        val median = median(snapshot.midis) ?: return null
        val latest = snapshot.times.last()
        val pxPerMs = (size.width - strokePx) / PitchRing.WINDOW_MS
        val perSemitone = (size.height - strokePx) / (2 * SEMITONES)
        fun x(t: Long) = size.width - strokePx / 2 - (latest - t) * pxPerMs
        fun y(midi: Float) = size.height / 2 - (midi - median).coerceIn(-SEMITONES, SEMITONES) * perSemitone
        paths.forEach { it.rewind() }
        var last: Offset? = null
        for (i in snapshot.times.indices) {
            val midi = snapshot.midis[i]
            if (midi.isNaN()) {
                last = null
                continue
            }
            val point = Offset(x(snapshot.times[i]), y(midi))
            last?.let { prev ->
                val bucket = (snapshot.confidences[i].coerceIn(0f, 1f) * (BUCKET_ALPHA.size - 1)).roundToInt()
                paths[bucket].moveTo(prev.x, prev.y)
                paths[bucket].lineTo(point.x, point.y)
            }
            last = point
        }
        val endMidi = snapshot.midis.last()
        val end = if (endMidi.isNaN()) null else Offset(x(latest), y(endMidi))
        return TraceShape(snapshot.version, latest, pxPerMs, paths, end, Stroke(width = strokePx, cap = StrokeCap.Round, join = StrokeJoin.Round))
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
