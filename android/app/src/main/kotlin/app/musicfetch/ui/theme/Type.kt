package app.musicfetch.ui.theme

import androidx.compose.material3.Typography
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.Font
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontVariation
import androidx.compose.ui.text.font.FontWeight
import app.musicfetch.R

/** Variable fonts are instanced per weight so the axis matches the requested weight. */
private fun variableFamily(resId: Int): FontFamily = FontFamily(
    listOf(400, 500, 600, 700, 800).map { weight ->
        Font(
            resId = resId,
            weight = FontWeight(weight),
            variationSettings = FontVariation.Settings(FontVariation.weight(weight)),
        )
    },
)

/** Google Sans Flex (SIL OFL) for display, headline and title roles. */
val DisplayFamily: FontFamily = variableFamily(R.font.google_sans_flex)

/** Roboto Flex (SIL OFL) for body and label roles. */
val BodyFamily: FontFamily = variableFamily(R.font.roboto_flex)

private val base = Typography()

private fun TextStyle.display() = copy(fontFamily = DisplayFamily)
private fun TextStyle.body() = copy(fontFamily = BodyFamily)

/** M3 type roles with the app's fonts (docs/design/DESIGN.md §4.2). */
val AppTypography: Typography = Typography(
    displayLarge = base.displayLarge.display(),
    displayMedium = base.displayMedium.display(),
    displaySmall = base.displaySmall.display(),
    headlineLarge = base.headlineLarge.display(),
    headlineMedium = base.headlineMedium.display(),
    headlineSmall = base.headlineSmall.display(),
    titleLarge = base.titleLarge.display(),
    titleMedium = base.titleMedium.display(),
    titleSmall = base.titleSmall.display(),
    bodyLarge = base.bodyLarge.body(),
    bodyMedium = base.bodyMedium.body(),
    bodySmall = base.bodySmall.body(),
    labelLarge = base.labelLarge.body(),
    labelMedium = base.labelMedium.body(),
    labelSmall = base.labelSmall.body(),
    displayLargeEmphasized = base.displayLargeEmphasized.display(),
    displayMediumEmphasized = base.displayMediumEmphasized.display(),
    displaySmallEmphasized = base.displaySmallEmphasized.display(),
    headlineLargeEmphasized = base.headlineLargeEmphasized.display(),
    headlineMediumEmphasized = base.headlineMediumEmphasized.display(),
    headlineSmallEmphasized = base.headlineSmallEmphasized.display(),
    titleLargeEmphasized = base.titleLargeEmphasized.display(),
    titleMediumEmphasized = base.titleMediumEmphasized.display(),
    titleSmallEmphasized = base.titleSmallEmphasized.display(),
    bodyLargeEmphasized = base.bodyLargeEmphasized.body(),
    bodyMediumEmphasized = base.bodyMediumEmphasized.body(),
    bodySmallEmphasized = base.bodySmallEmphasized.body(),
    labelLargeEmphasized = base.labelLargeEmphasized.body(),
    labelMediumEmphasized = base.labelMediumEmphasized.body(),
    labelSmallEmphasized = base.labelSmallEmphasized.body(),
)

/** Tabular figures for cue times and clocks. */
val TabularNumbers: TextStyle = TextStyle(fontFeatureSettings = "tnum")
