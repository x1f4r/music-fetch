package app.musicfetch.ui.theme

import android.os.Build
import androidx.compose.animation.animateColorAsState
import androidx.compose.animation.core.AnimationSpec
import androidx.compose.animation.core.spring
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.ColorScheme
import androidx.compose.material3.MaterialExpressiveTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.MotionScheme
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import com.materialkolor.PaletteStyle
import com.materialkolor.dynamicColorScheme
import com.materialkolor.dynamiccolor.ColorSpec

/** "Fetch teal", the seed when the wallpaper offers no colours (before Android 12). */
val FetchTeal = Color(0xFF1FA48A)

/**
 * Material 3 Expressive with the wallpaper's dynamic colours on Android 12+, else a scheme
 * seeded from [FetchTeal] (docs/design/DESIGN.md §3.2).
 */
@Composable
fun MusicFetchTheme(darkTheme: Boolean = isSystemInDarkTheme(), content: @Composable () -> Unit) {
    val context = LocalContext.current
    val colorScheme = remember(darkTheme, context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            if (darkTheme) dynamicDarkColorScheme(context) else dynamicLightColorScheme(context)
        } else {
            seededScheme(FetchTeal, darkTheme, PaletteStyle.TonalSpot)
        }
    }
    MaterialExpressiveTheme(
        colorScheme = colorScheme,
        motionScheme = MotionScheme.expressive(),
        typography = AppTypography,
        content = content,
    )
}

/**
 * The content-based scheme of the result screen: seeded from the artwork's dominant colour
 * (Expressive palette, 2025 spec), cross-faded so the screen never flashes.
 */
@Composable
fun ArtworkTheme(seed: Color?, content: @Composable () -> Unit) {
    val dark = isSystemInDarkTheme()
    val base = MaterialTheme.colorScheme
    val target = remember(seed, dark, base) { seed?.let { seededScheme(it, dark, PaletteStyle.Expressive) } ?: base }
    MaterialTheme(colorScheme = animateScheme(target), content = content)
}

/** Lists, navigation chrome and dialogs use the standard (calmer) motion scheme. */
@Composable
fun StandardMotion(content: @Composable () -> Unit) {
    MaterialTheme(motionScheme = MotionScheme.standard(), content = content)
}

fun seededScheme(seed: Color, dark: Boolean, style: PaletteStyle): ColorScheme =
    dynamicColorScheme(
        seedColor = seed,
        isDark = dark,
        isAmoled = false,
        style = style,
        specVersion = ColorSpec.SpecVersion.SPEC_2025,
    )

@Composable
private fun animateScheme(target: ColorScheme): ColorScheme {
    val spec: AnimationSpec<Color> = spring(dampingRatio = 1f, stiffness = 1600f)
    val primary by animateColorAsState(target.primary, spec, label = "primary")
    val onPrimary by animateColorAsState(target.onPrimary, spec, label = "onPrimary")
    val primaryContainer by animateColorAsState(target.primaryContainer, spec, label = "primaryContainer")
    val onPrimaryContainer by animateColorAsState(target.onPrimaryContainer, spec, label = "onPrimaryContainer")
    val secondaryContainer by animateColorAsState(target.secondaryContainer, spec, label = "secondaryContainer")
    val onSecondaryContainer by animateColorAsState(target.onSecondaryContainer, spec, label = "onSecondaryContainer")
    val surface by animateColorAsState(target.surface, spec, label = "surface")
    val onSurface by animateColorAsState(target.onSurface, spec, label = "onSurface")
    val onSurfaceVariant by animateColorAsState(target.onSurfaceVariant, spec, label = "onSurfaceVariant")
    val surfaceContainer by animateColorAsState(target.surfaceContainer, spec, label = "surfaceContainer")
    val surfaceContainerHigh by animateColorAsState(target.surfaceContainerHigh, spec, label = "surfaceContainerHigh")
    return target.copy(
        primary = primary,
        onPrimary = onPrimary,
        primaryContainer = primaryContainer,
        onPrimaryContainer = onPrimaryContainer,
        secondaryContainer = secondaryContainer,
        onSecondaryContainer = onSecondaryContainer,
        surface = surface,
        onSurface = onSurface,
        onSurfaceVariant = onSurfaceVariant,
        surfaceContainer = surfaceContainer,
        surfaceContainerHigh = surfaceContainerHigh,
    )
}
