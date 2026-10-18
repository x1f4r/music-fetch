package app.musicfetch.ui.session

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.spring
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.navigationBars
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
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
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.layout.boundsInRoot
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.positionInParent
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.R
import app.musicfetch.core.Track
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.common.PreviewPlayer
import app.musicfetch.ui.common.tag
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.OpenInSplitButton
import app.musicfetch.ui.theme.ArtworkTheme

/** A matched track (docs/design/DESIGN.md §6.3, Android). [listenAgain]: show the "Listen again" dock. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
internal fun ResultContent(view: SessionView.Result, viewModel: SessionViewModel, onBack: () -> Unit, listenAgain: Boolean) {
    val seed by viewModel.seed.collectAsStateWithLifecycle()
    val lyrics by viewModel.lyrics.collectAsStateWithLifecycle()
    val preference by viewModel.preference.collectAsStateWithLifecycle()
    val preview by PreviewPlayer.state.collectAsStateWithLifecycle()
    val track = view.track
    // A preview belongs to its screen.
    DisposableEffect(Unit) { onDispose { PreviewPlayer.stop() } }
    // The bar takes the container colour once content scrolls beneath it, and shows the title
    // once the hero title has scrolled away.
    val scrollBehavior = TopAppBarDefaults.pinnedScrollBehavior()
    var barBottom by remember { mutableFloatStateOf(0f) }
    var titleBottom by remember { mutableFloatStateOf(Float.MAX_VALUE) }
    val titleInBar by remember { derivedStateOf { titleBottom < barBottom } }
    // Height of the hero (top spacer to the bottom of "Open in") without the artwork.
    var measuredRest by remember(track.identityKey, view.context) { mutableStateOf<Dp?>(null) }
    val density = LocalDensity.current
    ArtworkTheme(seed) {
        val colors = MaterialTheme.colorScheme
        Scaffold(
            modifier = Modifier.nestedScroll(scrollBehavior.nestedScrollConnection),
            topBar = {
                BackBar(
                    onBack,
                    transparent = true,
                    scrollBehavior = scrollBehavior,
                    modifier = Modifier.onGloballyPositioned { barBottom = it.boundsInRoot().bottom },
                    actions = { SessionActions(view.entry, viewModel, track = track) },
                    title = {
                        AnimatedVisibility(titleInBar, enter = fadeIn(), exit = fadeOut()) {
                            Text(track.title, maxLines = 1, overflow = TextOverflow.Ellipsis, style = MaterialTheme.typography.titleLarge)
                        }
                    },
                )
            },
            bottomBar = { if (listenAgain) ListenAgainDock() },
            containerColor = colors.surface,
            contentWindowInsets = WindowInsets(0),
        ) { padding ->
            BoxWithConstraints(Modifier.fillMaxSize()) {
                val room = maxHeight - padding.calculateTopPadding() - padding.calculateBottomPadding()
                // Phones in landscape: artwork beside the title, so the hero fits between the bar
                // and the dock.
                val sideBySide = maxWidth >= 560.dp && maxHeight < 480.dp
                // Short screens and large fonts: the artwork gives way, so the "Open in" button
                // is clear of the bottom (and of the "Listen again" dock) at first sight. The
                // rest of the hero is measured once laid out; until then it is estimated.
                val heroRest = measuredRest ?: (if (view.context != null) 290.dp else 258.dp) * density.fontScale.coerceAtLeast(1f)
                val artworkSize = if (sideBySide) {
                    (room - 32.dp).coerceIn(120.dp, 240.dp)
                } else {
                    (room - heroRest - 8.dp).coerceIn(120.dp, 240.dp)
                }
                // Wash: primaryContainer → transparent, 440 dp, 55 %.
                Box(
                    Modifier
                        .fillMaxWidth()
                        .height(440.dp)
                        .background(Brush.verticalGradient(listOf(colors.primaryContainer.copy(alpha = 0.55f), Color.Transparent))),
                )
                val reveal = remember(track.identityKey) { Animatable(0f) }
                LaunchedEffect(track.identityKey) { reveal.animateTo(1f, spring(0.8f, 380f)) }
                val playing = preview.url != null && preview.url == track.previewUrl
                val onTitlePlaced = { bottom: Float -> titleBottom = bottom }
                Column(
                    Modifier
                        .fillMaxSize()
                        .verticalScroll(rememberScrollState())
                        .padding(top = padding.calculateTopPadding(), bottom = padding.calculateBottomPadding())
                        // Landscape with a cutout: keep the hero clear of it. Without the dock,
                        // beside a navigation rail: clear of the gesture handle.
                        .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Horizontal))
                        .then(if (listenAgain) Modifier else Modifier.windowInsetsPadding(WindowInsets.navigationBars.only(WindowInsetsSides.Bottom)))
                        .padding(horizontal = 24.dp),
                    horizontalAlignment = Alignment.CenterHorizontally,
                ) {
                    Spacer(Modifier.height(16.dp))
                    if (sideBySide) {
                        Row(
                            Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.spacedBy(32.dp, Alignment.CenterHorizontally),
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            HeroArtwork(track.artworkUrl, track.previewUrl, artworkSize, playing, preview.progress) { reveal.value }
                            Column(Modifier.weight(1f, fill = false).widthIn(max = 440.dp)) {
                                HeroText(track, view.context, reveal.value, TextAlign.Start, onTitlePlaced)
                                Spacer(Modifier.height(22.dp))
                                preference?.let { OpenInSplitButton(track, it) } ?: Spacer(Modifier.height(56.dp))
                            }
                        }
                    } else {
                        HeroArtwork(track.artworkUrl, track.previewUrl, artworkSize, playing, preview.progress) { reveal.value }
                        Spacer(Modifier.height(24.dp))
                        HeroText(track, view.context, reveal.value, TextAlign.Center, onTitlePlaced)
                        Spacer(Modifier.height(22.dp))
                        Box(
                            Modifier.onGloballyPositioned { coordinates ->
                                val bottom = with(density) { (coordinates.positionInParent().y + coordinates.size.height).toDp() }
                                measuredRest = bottom - artworkSize
                            },
                        ) {
                            preference?.let { OpenInSplitButton(track, it) } ?: Spacer(Modifier.height(56.dp))
                        }
                    }
                    lyrics?.let { text ->
                        Spacer(Modifier.height(40.dp))
                        Column(Modifier.widthIn(max = 640.dp).fillMaxWidth()) {
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

/** The artwork; it plays the 30 s preview, a ring drawing its progress. */
@Composable
private fun HeroArtwork(artworkUrl: String?, previewUrl: String?, artworkSize: Dp, playing: Boolean, progress: Float, reveal: () -> Float) {
    val colors = MaterialTheme.colorScheme
    val previewLabel = stringResource(if (playing) R.string.result_pause_preview else R.string.result_play_preview)
    Box(
        Modifier
            .graphicsLayer {
                val p = reveal()
                alpha = p
                scaleX = 0.92f + 0.08f * p
                scaleY = 0.92f + 0.08f * p
            }
            .shadow(16.dp, RoundedCornerShape(20.dp))
            .then(
                if (previewUrl != null) {
                    Modifier
                        .clickable(role = Role.Button) { PreviewPlayer.toggle(previewUrl) }
                        .semantics { contentDescription = previewLabel }
                        .tag("result.preview")
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
                    measure.getSegment(0f, measure.length * progress, segment, true)
                    drawPath(segment, colors.primary, style = Stroke(stroke, cap = StrokeCap.Round))
                }
            },
    ) {
        Artwork(artworkUrl, artworkSize, corner = 20.dp, contentDescription = stringResource(R.string.result_artwork))
        // The artwork plays the 30 s preview; the badge says so.
        if (previewUrl != null) PreviewBadge(playing, Modifier.align(Alignment.BottomEnd).padding(12.dp))
    }
}

/** Title, artist, album · year and where the song was found, in the caller's column. */
@Composable
private fun HeroText(track: Track, context: SessionContext?, reveal: Float, align: TextAlign, onTitlePlaced: (Float) -> Unit) {
    val colors = MaterialTheme.colorScheme
    Staggered(reveal, 0) {
        Text(
            track.title,
            style = MaterialTheme.typography.displaySmallEmphasized,
            textAlign = align,
            maxLines = 2,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier
                .widthIn(max = 520.dp)
                .onGloballyPositioned { onTitlePlaced(it.boundsInRoot().bottom) },
        )
    }
    Spacer(Modifier.height(2.dp))
    Staggered(reveal, 1) {
        Text(track.artist, style = MaterialTheme.typography.titleMedium, textAlign = align)
    }
    val meta = listOfNotNull(track.album?.takeIf { it.isNotBlank() }, track.releaseYear?.toString()).joinToString(" · ")
    if (meta.isNotEmpty()) {
        Spacer(Modifier.height(2.dp))
        Staggered(reveal, 2) {
            Text(meta, style = MaterialTheme.typography.bodyMedium, color = colors.onSurfaceVariant, textAlign = align)
        }
    }
    context?.let {
        Spacer(Modifier.height(12.dp))
        Text(
            when (it) {
                is SessionContext.File -> stringResource(R.string.result_context_file, it.name)
                is SessionContext.Link -> it.atMs?.let { at ->
                    stringResource(R.string.result_context_link, Formatters.clock(at), it.title)
                } ?: stringResource(R.string.result_context_link_no_time, it.title)
            },
            style = MaterialTheme.typography.bodyMedium,
            color = colors.onSurfaceVariant,
            textAlign = align,
            maxLines = 2,
            overflow = TextOverflow.Ellipsis,
        )
    }
}

/** Play/pause badge on the artwork (decorative: the artwork itself is the button). */
@Composable
private fun PreviewBadge(playing: Boolean, modifier: Modifier = Modifier) {
    Surface(
        shape = CircleShape,
        color = MaterialTheme.colorScheme.secondaryContainer.copy(alpha = 0.92f),
        contentColor = MaterialTheme.colorScheme.onSecondaryContainer,
        shadowElevation = 2.dp,
        modifier = modifier.size(40.dp).clearAndSetSemantics { },
    ) {
        Box(contentAlignment = Alignment.Center) {
            Icon(
                painterResource(if (playing) R.drawable.ic_pause_filled else R.drawable.ic_play_arrow_filled),
                contentDescription = null,
                modifier = Modifier.size(24.dp),
            )
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
