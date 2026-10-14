package app.musicfetch.ui.session

import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.spring
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.drawWithContent
import androidx.compose.ui.draw.shadow
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.RoundRect
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.PathMeasure
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.R
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.common.PreviewPlayer
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.OpenInSplitButton
import app.musicfetch.ui.theme.ArtworkTheme

/** A matched track (docs/design/DESIGN.md §6.3, Android). */
@Composable
internal fun ResultContent(view: SessionView.Result, viewModel: SessionViewModel, onBack: () -> Unit) {
    val seed by viewModel.seed.collectAsStateWithLifecycle()
    val lyrics by viewModel.lyrics.collectAsStateWithLifecycle()
    val preferred by viewModel.preferred.collectAsStateWithLifecycle()
    val preview by PreviewPlayer.state.collectAsStateWithLifecycle()
    val track = view.track
    // A preview belongs to its screen.
    DisposableEffect(Unit) { onDispose { PreviewPlayer.stop() } }
    ArtworkTheme(seed) {
        val colors = MaterialTheme.colorScheme
        Scaffold(
            topBar = {
                BackBar(onBack, transparent = true, actions = { SessionActions(view.entry, viewModel, track = track) })
            },
            containerColor = colors.surface,
            contentWindowInsets = WindowInsets(0),
        ) { padding ->
            Box(Modifier.fillMaxSize()) {
                // Wash: primaryContainer → transparent, 440 dp, 55 %.
                Box(
                    Modifier
                        .fillMaxWidth()
                        .height(440.dp)
                        .background(Brush.verticalGradient(listOf(colors.primaryContainer.copy(alpha = 0.55f), Color.Transparent))),
                )
                val reveal = remember(track.identityKey) { Animatable(0f) }
                LaunchedEffect(track.identityKey) { reveal.animateTo(1f, spring(0.8f, 380f)) }
                Column(
                    Modifier
                        .fillMaxSize()
                        .verticalScroll(rememberScrollState())
                        .padding(top = padding.calculateTopPadding())
                        .padding(horizontal = 24.dp),
                    horizontalAlignment = Alignment.CenterHorizontally,
                ) {
                    Spacer(Modifier.height(16.dp))
                    val playing = preview.url != null && preview.url == track.previewUrl
                    val previewLabel = stringResource(if (playing) R.string.result_pause_preview else R.string.result_play_preview)
                    Box(
                        Modifier
                            .graphicsLayer {
                                val p = reveal.value
                                alpha = p
                                scaleX = 0.92f + 0.08f * p
                                scaleY = 0.92f + 0.08f * p
                            }
                            .shadow(16.dp, RoundedCornerShape(20.dp))
                            .then(
                                if (track.previewUrl != null) {
                                    Modifier
                                        .clickable(role = Role.Button) { track.previewUrl?.let(PreviewPlayer::toggle) }
                                        .semantics { contentDescription = previewLabel }
                                } else {
                                    Modifier
                                },
                            )
                            .drawWithContent {
                                drawContent()
                                if (playing) {
                                    val stroke = 3.dp.toPx()
                                    val path = Path().apply {
                                        addRoundRect(
                                            RoundRect(
                                                stroke / 2,
                                                stroke / 2,
                                                size.width - stroke / 2,
                                                size.height - stroke / 2,
                                                CornerRadius(20.dp.toPx()),
                                            ),
                                        )
                                    }
                                    val measure = PathMeasure().apply { setPath(path, false) }
                                    val segment = Path()
                                    measure.getSegment(0f, measure.length * preview.progress, segment, true)
                                    drawPath(segment, colors.primary, style = Stroke(stroke, cap = StrokeCap.Round))
                                }
                            },
                    ) {
                        Artwork(track.artworkUrl, 240.dp, corner = 20.dp, contentDescription = stringResource(R.string.result_artwork))
                    }
                    Spacer(Modifier.height(24.dp))
                    Staggered(reveal.value, 0) {
                        Text(
                            track.title,
                            style = MaterialTheme.typography.displaySmallEmphasized,
                            textAlign = TextAlign.Center,
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.widthIn(max = 520.dp),
                        )
                    }
                    Spacer(Modifier.height(2.dp))
                    Staggered(reveal.value, 1) {
                        Text(track.artist, style = MaterialTheme.typography.titleMedium, textAlign = TextAlign.Center)
                    }
                    val meta = listOfNotNull(track.album?.takeIf { it.isNotBlank() }, track.releaseYear?.toString()).joinToString(" · ")
                    if (meta.isNotEmpty()) {
                        Spacer(Modifier.height(2.dp))
                        Staggered(reveal.value, 2) {
                            Text(
                                meta,
                                style = MaterialTheme.typography.bodyMedium,
                                color = colors.onSurfaceVariant,
                                textAlign = TextAlign.Center,
                            )
                        }
                    }
                    view.context?.let { context ->
                        Spacer(Modifier.height(12.dp))
                        Text(
                            when (context) {
                                is SessionContext.File -> stringResource(R.string.result_context_file, context.name)
                                is SessionContext.Link -> context.atMs?.let {
                                    stringResource(R.string.result_context_link, Formatters.clock(it), context.title)
                                } ?: stringResource(R.string.result_context_link_no_time, context.title)
                            },
                            style = MaterialTheme.typography.bodyMedium,
                            color = colors.onSurfaceVariant,
                            textAlign = TextAlign.Center,
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis,
                        )
                    }
                    Spacer(Modifier.height(22.dp))
                    OpenInSplitButton(track, preferred)
                    lyrics?.let { text ->
                        Spacer(Modifier.height(40.dp))
                        Column(Modifier.fillMaxWidth()) {
                            Text(stringResource(R.string.result_lyrics), style = MaterialTheme.typography.titleSmall)
                            Spacer(Modifier.height(8.dp))
                            Text(text.plain.trim(), style = MaterialTheme.typography.bodyLarge, color = colors.onSurfaceVariant)
                        }
                    }
                    Spacer(Modifier.height(32.dp))
                }
            }
        }
    }
}

/** Title, artist and meta rise 8 dp into place, 40 ms apart (docs/design/DESIGN.md §4.4). */
@Composable
private fun Staggered(progress: Float, index: Int, content: @Composable () -> Unit) {
    val delayed = ((progress - index * 0.08f) / (1f - index * 0.08f)).coerceIn(0f, 1f)
    Box(
        Modifier.graphicsLayer {
            alpha = delayed
            translationY = (1f - delayed) * 8.dp.toPx()
        },
    ) { content() }
}
