@file:OptIn(ExperimentalMaterial3ExpressiveApi::class)

package app.musicfetch.ui.components

import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.MaterialShapes
import androidx.compose.material3.toPath
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Outline
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.Shape
import androidx.compose.ui.graphics.asAndroidPath
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.LayoutDirection
import androidx.graphics.shapes.Morph
import androidx.graphics.shapes.RoundedPolygon

/**
 * The listen button's container (docs/design/DESIGN.md §4.4): `Cookie9Sided` at rest, morphing
 * through `SoftBurst` to `Sunny` with the input level, 40 % toward a circle when pressed.
 * Every experimental shape API is kept in this file.
 */
object ListenButtonShapes {
    private val cookie: RoundedPolygon = MaterialShapes.Cookie9Sided
    private val softBurst: RoundedPolygon = MaterialShapes.SoftBurst
    private val sunny: RoundedPolygon = MaterialShapes.Sunny
    private val circle: RoundedPolygon = MaterialShapes.Circle
    private val cookieWidget: RoundedPolygon = MaterialShapes.Cookie4Sided

    private val lowMorph = Morph(cookie, softBurst)
    private val highMorph = Morph(softBurst, sunny)
    private val pressMorph = Morph(cookie, circle)

    /** Shape for [level] (0..1) and [press] (0..1), rotated by [rotationDegrees]. */
    fun shape(level: Float, press: Float, rotationDegrees: Float): Shape {
        val l = level.coerceIn(0f, 1f)
        return when {
            press > 0.001f && l < 0.001f -> MorphShape(pressMorph, 0.4f * press.coerceIn(0f, 1f), rotationDegrees)
            l <= 0.5f -> MorphShape(lowMorph, l * 2, rotationDegrees)
            else -> MorphShape(highMorph, (l - 0.5f) * 2, rotationDegrees)
        }
    }

    /** The resting cookie, e.g. for Reduce Motion and the widget preview. */
    val resting: Shape = MorphShape(lowMorph, 0f, 0f)

    /** Smaller four-sided cookie for compact uses. */
    val compact: Shape = MorphShape(Morph(cookieWidget, cookieWidget), 0f, 0f)
}

/** A [Morph] frame as a Compose [Shape], scaled to the layout size and rotated about its centre. */
private class MorphShape(
    private val morph: Morph,
    private val progress: Float,
    private val rotationDegrees: Float,
) : Shape {
    override fun createOutline(size: Size, layoutDirection: LayoutDirection, density: Density): Outline {
        val path: Path = morph.toPath(progress)
        val matrix = android.graphics.Matrix()
        // MaterialShapes are normalized to the unit square.
        matrix.setRotate(rotationDegrees, 0.5f, 0.5f)
        matrix.postScale(size.width, size.height)
        path.asAndroidPath().transform(matrix)
        return Outline.Generic(path)
    }

    override fun equals(other: Any?): Boolean =
        other is MorphShape && other.morph === morph && other.progress == progress && other.rotationDegrees == rotationDegrees

    override fun hashCode(): Int = (morph.hashCode() * 31 + progress.hashCode()) * 31 + rotationDegrees.hashCode()
}
