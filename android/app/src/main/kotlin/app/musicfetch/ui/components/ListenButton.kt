package app.musicfetch.ui.components

import android.provider.Settings
import androidx.compose.animation.AnimatedContent
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.spring
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.togetherWith
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsPressedAsState
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.size
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.CircularWavyProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ripple
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.withFrameNanos
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.drawBehind
import androidx.compose.ui.draw.drawWithCache
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.geometry.center
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import app.musicfetch.R
import app.musicfetch.platform.LocalLowPower
import kotlinx.coroutines.delay

/** What the listen button's centre shows. */
enum class ButtonGlyph { Mark, Live, Matching, Link }

/** The ring around the button: hum guidance or link download progress. */
sealed interface ButtonRing {
    data object None : ButtonRing
    data class Determinate(val progress: () -> Float) : ButtonRing
    data object Indeterminate : ButtonRing
}

/** Whether the user turned animations off ("Remove animations"). */
@Composable
fun rememberReduceMotion(): Boolean {
    val context = LocalContext.current
    return remember(context) {
        Settings.Global.getFloat(context.contentResolver, Settings.Global.ANIMATOR_DURATION_SCALE, 1f) == 0f
    }
}

/**
 * The listen button (docs/design/DESIGN.md §5.1, §4.4): a `primary` cookie that morphs and
 * rotates with the input level while listening, with a breathing halo, the live line inside,
 * and an optional wavy ring (hum progress or download).
 *
 * [level] is read only while drawing, so the 30 Hz level updates never recompose callers.
 */
@OptIn(ExperimentalMaterial3ExpressiveApi::class)
@Composable
fun ListenButton(
    glyph: ButtonGlyph,
    level: () -> Float,
    onClick: () -> Unit,
    contentDescription: String,
    stateDescription: String,
    modifier: Modifier = Modifier,
    size: Dp = 160.dp,
    ring: ButtonRing = ButtonRing.None,
    enabled: Boolean = true,
    /** Live-line amplitude: 1 while listening, 0.5 once humming is detected. */
    lineAmplitude: Float = 1f,
    /** Predictive back: scales the button toward 0.85 and the halo toward 0. */
    backProgress: Float = 0f,
) {
    val reduceMotion = rememberReduceMotion()
    // Low power: about 30 frames per second and no shape morph or rotation; the halo and the
    // live line still follow the level.
    val lowPower = LocalLowPower.current
    val colors = MaterialTheme.colorScheme
    val interaction = remember { MutableInteractionSource() }
    val pressed by interaction.collectIsPressedAsState()
    val live = glyph == ButtonGlyph.Live
    val pressScale by animateFloatAsState(if (pressed) 0.96f else 1f, spring(0.6f, 800f), label = "press")
    val pressMorph by animateFloatAsState(if (pressed && !reduceMotion) 1f else 0f, spring(0.6f, 800f), label = "pressMorph")
    val liveness by animateFloatAsState(if (live) 1f else 0f, spring(0.8f, 380f), label = "liveness")
    val amplitude by animateFloatAsState(lineAmplitude, spring(0.8f, 380f), label = "amplitude")
    val haloAlpha by animateFloatAsState(if (live) 1f else 0f, tween(150), label = "halo")

    // Frame clock for the live line, the halo and the rotation; only runs while listening.
    val clock = remember { mutableFloatStateOf(0f) }
    LaunchedEffect(live, lowPower) {
        if (!live) return@LaunchedEffect
        val start = withFrameNanos { it }
        while (true) {
            withFrameNanos { clock.floatValue = (it - start) / 1_000_000_000f }
            if (lowPower) delay(LOW_POWER_FRAME_MS)
        }
    }

    val ringSize = size + 28.dp
    val haloSize = size * 1.56f
    Box(
        modifier
            .size(haloSize)
            .semantics(mergeDescendants = true) {
                this.contentDescription = contentDescription
                this.stateDescription = stateDescription
            },
        contentAlignment = Alignment.Center,
    ) {
        // Halo: primary → clear, breathing with the level. The gradient is drawn once; the
        // level only changes the layer's scale and alpha, so it never redraws.
        Box(
            Modifier
                .size(haloSize)
                .graphicsLayer {
                    val l = level()
                    val haloScale = if (reduceMotion) 1f else 1f + 0.12f * l
                    scaleX = haloScale
                    scaleY = haloScale
                    alpha = (haloAlpha * (1f - backProgress) * (0.15f + 0.45f * l) / 0.6f).coerceIn(0f, 1f)
                }
                .drawWithCache {
                    val radius = this.size.minDimension / 2
                    val brush = Brush.radialGradient(
                        0f to colors.primary.copy(alpha = 0.55f),
                        0.62f to colors.primary.copy(alpha = 0.18f),
                        1f to Color.Transparent,
                        center = this.size.center,
                        radius = radius,
                    )
                    onDrawBehind { drawCircle(brush, radius) }
                },
        )
        val ringStroke = with(LocalDensity.current) { 4.dp.toPx() }
        when {
            ring == ButtonRing.None -> Unit
            // Low power: the plain ring; the wavy one animates its wave on every frame.
            lowPower && ring is ButtonRing.Determinate -> CircularProgressIndicator(
                progress = ring.progress,
                modifier = Modifier.size(ringSize),
                color = colors.primary,
                trackColor = colors.surfaceContainerHighest,
                strokeWidth = 4.dp,
                gapSize = 0.dp,
            )
            lowPower -> CircularProgressIndicator(
                modifier = Modifier.size(ringSize),
                color = colors.primary,
                trackColor = colors.surfaceContainerHighest,
                strokeWidth = 4.dp,
            )
            ring is ButtonRing.Determinate -> CircularWavyProgressIndicator(
                progress = ring.progress,
                modifier = Modifier.size(ringSize),
                color = colors.primary,
                trackColor = colors.surfaceContainerHighest,
                stroke = Stroke(width = ringStroke),
                wavelength = 24.dp,
            )
            else -> CircularWavyProgressIndicator(
                modifier = Modifier.size(ringSize),
                color = colors.primary,
                trackColor = colors.surfaceContainerHighest,
            )
        }
        val buttonScale = pressScale * (1f - 0.15f * backProgress)
        Box(
            Modifier
                .size(size)
                .graphicsLayer {
                    scaleX = buttonScale
                    scaleY = buttonScale
                    shape = if (reduceMotion || lowPower) {
                        ListenButtonShapes.resting
                    } else {
                        ListenButtonShapes.shape(
                            level = if (live) level() * liveness else 0f,
                            press = pressMorph,
                            rotationDegrees = if (live) clock.floatValue * 30f % 360f else 0f,
                        )
                    }
                    clip = true
                }
                .background(colors.primary)
                .clickable(
                    interactionSource = interaction,
                    indication = ripple(color = colors.onPrimary),
                    enabled = enabled,
                    role = Role.Button,
                    onClick = onClick,
                ),
            contentAlignment = Alignment.Center,
        ) {
            val glyphWidth = size * 0.45f
            AnimatedContent(
                targetState = glyph == ButtonGlyph.Matching || glyph == ButtonGlyph.Link,
                transitionSpec = { fadeIn(tween(150)) togetherWith fadeOut(tween(150)) },
                label = "glyph",
            ) { special ->
                if (special) {
                    if (glyph == ButtonGlyph.Matching && lowPower) {
                        // The morphing indicator redraws a new shape on every frame.
                        CircularProgressIndicator(color = colors.onPrimary, strokeWidth = 4.dp, modifier = Modifier.size(size * 0.3f))
                    } else if (glyph == ButtonGlyph.Matching) {
                        MatchingIndicator(color = colors.onPrimary, modifier = Modifier.size(size * 0.36f))
                    } else {
                        Icon(
                            painterResource(R.drawable.ic_link),
                            contentDescription = null,
                            tint = colors.onPrimary,
                            modifier = Modifier.size(size * 0.3f),
                        )
                    }
                } else {
                    val path = remember { Path() }
                    Box(
                        Modifier
                            .size(glyphWidth, glyphWidth * (32f / 72f))
                            .drawBehind {
                                val stroke = 4.dp.toPx() * (size.value / 160f)
                                val inset = stroke / 2
                                ContourMarkGeometry.linePath(
                                    Size(this.size.width - inset * 2, this.size.height - inset * 2),
                                    liveness = liveness,
                                    level = level() * amplitude,
                                    timeSeconds = clock.floatValue,
                                    out = path,
                                )
                                path.translate(androidx.compose.ui.geometry.Offset(inset, inset))
                                drawContour(path, colors.onPrimary, stroke)
                            },
                    )
                }
            }
        }
    }
}

/** Frame interval of the low-power path (about 30 fps, the rate of the level events). */
internal const val LOW_POWER_FRAME_MS = 28L
