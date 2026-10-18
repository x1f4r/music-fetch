package app.musicfetch.ui.session

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.navigationBars
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.ButtonGroupDefaults
import androidx.compose.material3.ButtonShapes
import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.layout.Layout
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.isTraversalGroup
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Constraints
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.AppIntents
import app.musicfetch.ui.common.LocalAppSnackbar
import app.musicfetch.ui.common.tag
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.navigation.LocalNavigator

/**
 * "Listen again" under a result on the Listen destination (docs/design/DESIGN.md §6.3): the
 * stage's three sources as actions. A tap goes back to the stage and starts that source at
 * once, through the stage's permission and consent steps, so going from a phone-audio match to
 * humming is one tap. Disabled while another listen runs.
 */
@OptIn(ExperimentalMaterial3ExpressiveApi::class)
@Composable
internal fun ListenAgainDock(modifier: Modifier = Modifier) {
    val context = LocalContext.current
    val navigator = LocalNavigator.current
    val snackbar = LocalAppSnackbar.current
    val density = LocalDensity.current
    val listen by context.appGraph.listen.state.collectAsStateWithLifecycle()
    val enabled = !listen.isActive
    DisposableEffect(Unit) { onDispose { snackbar.bottomInset = 0.dp } }
    val groupLabel = stringResource(R.string.listen_again)
    val options = listOf(
        Triple(ListenSource.Microphone, R.string.source_microphone, R.drawable.ic_mic),
        Triple(ListenSource.Phone, R.string.source_phone, R.drawable.ic_smartphone),
        Triple(ListenSource.Hum, R.string.source_hum, 0),
    )
    val colors = MaterialTheme.colorScheme
    // No bar of its own: content scrolling beneath fades out above the buttons. Beside a
    // navigation rail the buttons stay clear of the gesture handle (a bar consumes it otherwise).
    Box(
        modifier
            .fillMaxWidth()
            .background(Brush.verticalGradient(0f to colors.surface.copy(alpha = 0f), 0.3f to colors.surface))
            .windowInsetsPadding(WindowInsets.navigationBars.only(WindowInsetsSides.Bottom))
            .onSizeChanged { snackbar.bottomInset = with(density) { it.height.toDp() } },
    ) {
        BoxWithConstraints(Modifier.fillMaxWidth(), contentAlignment = Alignment.Center) {
            // Same rule as the stage's toggles: icons only where three labels and icons fit.
            val showIcons = maxWidth >= 400.dp && density.fontScale <= 1.15f
            FitRow(
                spacing = ButtonGroupDefaults.ConnectedSpaceBetween,
                modifier = Modifier
                    .padding(start = 16.dp, end = 16.dp, top = 20.dp, bottom = 12.dp)
                    .semantics {
                        contentDescription = groupLabel
                        isTraversalGroup = true
                    }
                    .tag("listenAgain"),
            ) {
                options.forEachIndexed { index, (source, label, icon) ->
                    val shapes = when (index) {
                        0 -> ButtonGroupDefaults.connectedLeadingButtonShapes()
                        options.lastIndex -> ButtonGroupDefaults.connectedTrailingButtonShapes()
                        else -> ButtonGroupDefaults.connectedMiddleButtonShapes()
                    }
                    // The stage's three sources, as actions: neutral containers, accent labels.
                    FilledTonalButton(
                        onClick = { AppIntents.listenNow(source, navigator) },
                        enabled = enabled,
                        shapes = ButtonShapes(shapes.shape, shapes.pressedShape),
                        colors = ButtonDefaults.filledTonalButtonColors(
                            containerColor = colors.surfaceContainerHigh,
                            contentColor = colors.primary,
                        ),
                        contentPadding = PaddingValues(horizontal = 16.dp),
                        modifier = Modifier
                            .heightIn(min = 48.dp)
                            .tag(
                                "listenAgain." + when (source) {
                                    ListenSource.Microphone -> "mic"
                                    ListenSource.Phone -> "phone"
                                    ListenSource.Hum -> "hum"
                                },
                            ),
                    ) {
                        if (showIcons) {
                            if (icon != 0) {
                                Icon(painterResource(icon), contentDescription = null, modifier = Modifier.size(20.dp))
                            } else {
                                Box(Modifier.size(20.dp), contentAlignment = Alignment.Center) {
                                    ContourMark(LocalContentColor.current, width = 20.dp, strokeWidth = 2.dp)
                                }
                            }
                            Spacer(Modifier.width(8.dp))
                        }
                        Text(stringResource(label), maxLines = 1, overflow = TextOverflow.Ellipsis)
                    }
                }
            }
        }
    }
}

/**
 * A centred row of children at their natural widths; when they don't fit, each shrinks in
 * proportion, so a long label gives way no more than the others.
 */
@Composable
private fun FitRow(spacing: Dp, modifier: Modifier = Modifier, content: @Composable () -> Unit) {
    Layout(content = content, modifier = modifier) { measurables, constraints ->
        val gap = spacing.roundToPx()
        val gaps = gap * (measurables.size - 1).coerceAtLeast(0)
        val natural = measurables.map { it.maxIntrinsicWidth(constraints.maxHeight) }
        val total = natural.sum() + gaps
        val available = constraints.maxWidth
        val widths = if (total <= available || natural.sum() == 0) {
            natural
        } else {
            val factor = (available - gaps).toFloat() / natural.sum()
            natural.map { (it * factor).toInt() }
        }
        val placeables = measurables.mapIndexed { i, m ->
            m.measure(Constraints(minWidth = widths[i], maxWidth = widths[i], minHeight = 0, maxHeight = constraints.maxHeight))
        }
        val width = placeables.sumOf { it.width } + gaps
        val height = placeables.maxOfOrNull { it.height } ?: 0
        val layoutWidth = if (constraints.hasBoundedWidth) constraints.maxWidth else width
        layout(layoutWidth, height) {
            var x = (layoutWidth - width) / 2
            placeables.forEach {
                it.placeRelative(x, 0)
                x += it.width + gap
            }
        }
    }
}
