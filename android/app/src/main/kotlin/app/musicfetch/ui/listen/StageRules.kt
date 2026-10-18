package app.musicfetch.ui.listen

import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.core.Candidate
import app.musicfetch.core.CandidateTier
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.SessionKind
import app.musicfetch.platform.ListenSource

/** What the action row offers after a listen found nothing; every suggestion starts at once. */
enum class Suggestion { TryAgain, HumAgain, HumIt, TypeLyric, UseMicrophone, TryGoogle }

/**
 * The no-match actions for [reason] after listening with [last] (docs/design/DESIGN.md §5.4),
 * most useful first. A hum is retried as a hum; the other sources offer humming instead. Never
 * two actions that do the same thing. [google]: Google's own song search can be opened
 * ([app.musicfetch.platform.GoogleSongSearch]); it is the second choice after a hum that found
 * nothing.
 */
fun suggestions(reason: NoMatchReason, last: ListenSource, google: Boolean = false): List<Suggestion> {
    if (last == ListenSource.Hum) {
        val actions = when (reason) {
            NoMatchReason.NOT_FOUND -> listOf(Suggestion.HumAgain, Suggestion.TypeLyric, Suggestion.UseMicrophone)
            NoMatchReason.TOO_QUIET, NoMatchReason.NOISY, NoMatchReason.MOSTLY_SPEECH ->
                listOf(Suggestion.HumAgain, Suggestion.UseMicrophone)
            NoMatchReason.TOO_SHORT -> listOf(Suggestion.HumAgain, Suggestion.TypeLyric)
            // The microphone is never silenced by another app.
            NoMatchReason.CAPTURE_SILENT -> return listOf(Suggestion.HumAgain)
        }
        return if (google) actions.take(1) + Suggestion.TryGoogle + actions.drop(1) else actions
    }
    return when (reason) {
        NoMatchReason.NOT_FOUND -> listOf(Suggestion.TryAgain, Suggestion.HumIt, Suggestion.TypeLyric)
        NoMatchReason.TOO_QUIET, NoMatchReason.NOISY, NoMatchReason.MOSTLY_SPEECH, NoMatchReason.TOO_SHORT ->
            listOf(Suggestion.TryAgain, Suggestion.HumIt)
        NoMatchReason.CAPTURE_SILENT -> listOf(Suggestion.UseMicrophone, Suggestion.HumIt)
    }
}

/**
 * The source a finished live session used. A microphone listen that turned into humming or
 * singing ([impression]) counts as a hum, so its suggestions are a hum's.
 */
fun sourceOf(source: CaptureSource, mode: ListenMode, impression: Boolean = false): ListenSource = when {
    source == CaptureSource.SYSTEM_AUDIO -> ListenSource.Phone
    mode == ListenMode.HUM || impression -> ListenSource.Hum
    else -> ListenSource.Microphone
}

/**
 * What the Listen stage shows of the interim candidates while someone hums (docs/CORE_API.md,
 * "Humming, whistling, singing"): the top one when it is a Strong or Possible answer. A long
 * shot is right about once in fifteen, so it is not named before the hum ends.
 */
fun humPreview(candidates: List<Candidate>): Candidate? =
    candidates.firstOrNull()?.takeIf { it.tier == CandidateTier.STRONG || it.tier == CandidateTier.POSSIBLE }

/** The progressive search answered while humming, but with nothing better than a long shot. */
fun onlyLongShots(candidates: List<Candidate>): Boolean = candidates.isNotEmpty() && humPreview(candidates) == null

/** Google's song search is offered as a second opinion when a hum's best answer is not Strong. */
fun offersGoogle(candidates: List<Candidate>): Boolean = candidates.firstOrNull()?.tier != CandidateTier.STRONG

/** The source that repeats a stored live session; null for links, files and lyric searches. */
fun sourceOf(kind: SessionKind): ListenSource? = when (kind) {
    SessionKind.LISTEN -> ListenSource.Microphone
    SessionKind.SYSTEM_AUDIO -> ListenSource.Phone
    SessionKind.HUM -> ListenSource.Hum
    SessionKind.LINK, SessionKind.FILE, SessionKind.LYRICS -> null
}

/**
 * Where the stage's slots go (docs/design/DESIGN.md §6.2), as offsets of their top edges from the
 * top of a stage [height] tall. Tall stages use the spec positions around the centre. Short ones
 * (budget phones with three-button navigation, 360 × 640 dp) anchor the action row near the
 * bottom and shrink the button and the trace band so the band never reaches the link field.
 * Short and wide ones (phones in landscape) put the button on the start side and the band, the
 * state line and the actions in a column beside it ([side]).
 */
data class StageSlots(
    val bandTop: Dp,
    val bandHeight: Dp,
    /** Centre of the listen button. */
    val buttonCenter: Dp,
    val buttonScale: Float,
    val stateTop: Dp,
    val actionTop: Dp,
    val side: SideSlots? = null,
)

/** Horizontal slots of a side-by-side stage, from its start edge. */
data class SideSlots(
    /** Centre of the listen button. */
    val buttonCenter: Dp,
    /** The column holding the band, the state line and the actions. */
    val columnStart: Dp,
    val columnWidth: Dp,
)

private val BUTTON = 160.dp
private val BAND = 96.dp
private val MIN_BAND = 48.dp
private val MARGIN = 8.dp

fun stageSlots(height: Dp, width: Dp = 0.dp): StageSlots {
    val c = height / 2
    // Spec: band bottom at c − 144, button centre c − 40, state line at c + 60, actions at c + 124.
    if (c - 144.dp - BAND >= MARGIN) {
        return StageSlots(c - 144.dp - BAND, BAND, c - 40.dp, 1f, c + 60.dp, c + 124.dp)
    }
    if (width >= 480.dp && width >= height * 1.6f) return sideBySide(height, width)
    val actionTop = height - MARGIN - 40.dp
    val stateTop = actionTop - 64.dp
    val buttonBottom = stateTop - 20.dp
    var scale = 1f
    while (true) {
        val buttonTop = buttonBottom - BUTTON * scale
        val bandBottom = buttonTop - 24.dp * scale
        val band = minOf(BAND, bandBottom - MARGIN)
        if (band >= 64.dp || scale <= MIN_SCALE) {
            val shown = band.coerceAtLeast(MIN_BAND)
            return StageSlots(bandBottom - shown, shown, buttonBottom - BUTTON * scale / 2, scale, stateTop, actionTop)
        }
        scale = (scale - 0.05f).coerceAtLeast(MIN_SCALE)
    }
}

private const val MIN_SCALE = 0.7f

/** Below the band in a side column: 12 dp gap, the state line's 64 dp slot, the 40 dp action row. */
private val SIDE_REST = 116.dp

/**
 * The button centred on the start side, as large as the height allows; beside it the band on
 * top, then the state line and the actions, centred as a group. The band keeps its place while
 * idle so nothing moves when humming starts.
 */
private fun sideBySide(height: Dp, width: Dp): StageSlots {
    val scale = ((height - MARGIN * 2) / BUTTON).coerceIn(MIN_SCALE, 1f)
    val band = (height - MARGIN * 2 - SIDE_REST).coerceIn(MIN_BAND, BAND)
    val top = ((height - band - SIDE_REST) / 2).coerceAtLeast(MARGIN)
    val stateTop = top + band + 12.dp
    val columnStart = width * 0.46f
    return StageSlots(
        bandTop = top,
        bandHeight = band,
        buttonCenter = height / 2,
        buttonScale = scale,
        stateTop = stateTop,
        actionTop = stateTop + 64.dp,
        side = SideSlots(buttonCenter = width * 0.28f, columnStart = columnStart, columnWidth = width - columnStart - 24.dp),
    )
}
