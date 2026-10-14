package app.musicfetch.ui.session

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import androidx.compose.animation.animateContentSize
import androidx.compose.animation.core.spring
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.FilledTonalIconButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.capture.ListenOrigin
import app.musicfetch.core.Candidate
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.core.SessionKind
import app.musicfetch.ui.common.PreviewPlayer
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.CandidateTierTag
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.components.ContourOverlay
import app.musicfetch.ui.components.FrozenTrace
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.LyricsRoute

/** Ranked hum or lyric candidates (docs/design/DESIGN.md §6.4, Android). */
@Composable
internal fun CandidatesContent(view: SessionView.Candidates, viewModel: SessionViewModel, onBack: () -> Unit) {
    val context = LocalContext.current
    val navigator = LocalNavigator.current
    val haptics = LocalHapticFeedback.current
    val preview by PreviewPlayer.state.collectAsStateWithLifecycle()
    var expanded by rememberSaveable(view.entry.id) { mutableStateOf(0) }
    val isHum = view.entry.kind == SessionKind.HUM || view.entry.kind == SessionKind.LISTEN
    Scaffold(
        topBar = {
            BackBar(onBack, actions = {
                if (isHum) {
                    TextButton(onClick = {
                        context.appGraph.listen.start(CaptureSource.MICROPHONE, ListenMode.HUM, ListenOrigin.App)
                        navigator.showListen()
                    }) {
                        Box(Modifier.size(18.dp), contentAlignment = Alignment.Center) {
                            ContourMark(MaterialTheme.colorScheme.primary, width = 18.dp, strokeWidth = 2.dp)
                        }
                        Spacer(Modifier.width(8.dp))
                        Text(stringResource(R.string.cand_hum_again))
                    }
                }
            })
        },
    ) { padding ->
        LazyColumn(
            Modifier.fillMaxSize(),
            contentPadding = PaddingValues(top = padding.calculateTopPadding(), bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            val contour = view.humContour
            if (contour != null && contour.any { !it.isNaN() }) {
                item(key = "hum") {
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier.fillMaxWidth().height(64.dp).padding(horizontal = 16.dp),
                    ) {
                        FrozenTrace(contour, MaterialTheme.colorScheme.primary, Modifier.weight(1f).height(48.dp))
                        Spacer(Modifier.width(12.dp))
                        Text(
                            stringResource(R.string.cand_your_hum, view.humSeconds),
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                }
            }
            itemsIndexed(view.candidates, key = { _, c -> c.track.identityKey.ifEmpty { c.track.title + c.track.artist } }) { index, candidate ->
                CandidateCard(
                    candidate = candidate,
                    expanded = index == expanded,
                    playing = preview.url != null && preview.url == candidate.track.previewUrl,
                    onExpand = { expanded = index },
                    onPreview = { candidate.track.previewUrl?.let(PreviewPlayer::toggle) },
                    onConfirm = {
                        haptics.performHapticFeedback(HapticFeedbackType.Confirm)
                        PreviewPlayer.stop()
                        viewModel.confirm(candidate)
                    },
                    modifier = Modifier.padding(horizontal = 16.dp).animateItem(),
                )
            }
            item(key = "footer") {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier.fillMaxWidth().padding(horizontal = 24.dp, vertical = 8.dp),
                ) {
                    Text(
                        stringResource(R.string.cand_not_here),
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    TextButton(onClick = { navigator.push(LyricsRoute) }) { Text(stringResource(R.string.action_type_lyric)) }
                    if (isHum && googleHumSearchAvailable(context)) {
                        TextButton(onClick = { askGoogle(context) }) { Text(stringResource(R.string.action_ask_google)) }
                    }
                }
            }
        }
    }
}

@Composable
private fun CandidateCard(
    candidate: Candidate,
    expanded: Boolean,
    playing: Boolean,
    onExpand: () -> Unit,
    onPreview: () -> Unit,
    onConfirm: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val colors = MaterialTheme.colorScheme
    val track = candidate.track
    val previewLabel = stringResource(if (playing) R.string.result_pause_preview else R.string.result_play_preview)
    Surface(
        onClick = onExpand,
        shape = RoundedCornerShape(16.dp),
        color = if (expanded) colors.surfaceContainer else colors.surfaceContainerLow,
        modifier = modifier.fillMaxWidth().animateContentSize(spring(0.8f, 380f)),
    ) {
        Column(Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Artwork(track.artworkUrl, 56.dp, corner = 8.dp)
                Column(Modifier.weight(1f).padding(horizontal = 12.dp)) {
                    Text(track.title, style = MaterialTheme.typography.titleMedium, maxLines = 1, overflow = TextOverflow.Ellipsis)
                    Text(
                        track.artist,
                        style = MaterialTheme.typography.bodyMedium,
                        color = colors.onSurfaceVariant,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                }
                CandidateTierTag(candidate.tier)
                if (!expanded && track.previewUrl != null) {
                    IconButton(onClick = onPreview, modifier = Modifier.semantics { contentDescription = previewLabel }) {
                        Icon(painterResource(if (playing) R.drawable.ic_pause_filled else R.drawable.ic_play_arrow_filled), contentDescription = null)
                    }
                }
            }
            if (expanded) {
                val alignment = candidate.alignment
                if (alignment != null && alignment.query.isNotEmpty()) {
                    Spacer(Modifier.height(12.dp))
                    val overlayLabel = stringResource(R.string.cand_overlay_description, track.title)
                    ContourOverlay(
                        query = alignment.query,
                        reference = alignment.reference,
                        queryColor = colors.primary,
                        referenceColor = colors.outlineVariant,
                        modifier = Modifier.fillMaxWidth().height(96.dp).semantics { contentDescription = overlayLabel },
                    )
                    Spacer(Modifier.height(8.dp))
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Swatch(colors.primary)
                        Text(stringResource(R.string.cand_legend_you), style = MaterialTheme.typography.labelMedium, modifier = Modifier.padding(start = 6.dp, end = 16.dp))
                        Swatch(colors.outlineVariant)
                        Text(
                            track.title,
                            style = MaterialTheme.typography.labelMedium,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.padding(start = 6.dp),
                        )
                    }
                }
                candidate.sources.firstNotNullOfOrNull { it.detail?.takeIf { d -> d.isNotBlank() } }?.let { detail ->
                    Spacer(Modifier.height(8.dp))
                    Text(detail, style = MaterialTheme.typography.bodySmall, color = colors.onSurfaceVariant, maxLines = 2, overflow = TextOverflow.Ellipsis)
                }
                Spacer(Modifier.height(12.dp))
                Row(verticalAlignment = Alignment.CenterVertically) {
                    if (track.previewUrl != null) {
                        FilledTonalIconButton(onClick = onPreview, modifier = Modifier.size(48.dp).semantics { contentDescription = previewLabel }) {
                            Icon(painterResource(if (playing) R.drawable.ic_pause_filled else R.drawable.ic_play_arrow_filled), contentDescription = null)
                        }
                    }
                    Spacer(Modifier.weight(1f))
                    Button(onClick = onConfirm, modifier = Modifier.heightIn(min = 48.dp)) { Text(stringResource(R.string.cand_this_is_it)) }
                }
            }
        }
    }
}

@Composable
private fun Swatch(color: androidx.compose.ui.graphics.Color) {
    Box(Modifier.size(width = 14.dp, height = 4.dp).background(color, RoundedCornerShape(2.dp)))
}

private const val GOOGLE_APP = "com.google.android.googlequicksearchbox"
private const val MUSIC_SEARCH = "com.google.android.googlequicksearchbox.MUSIC_SEARCH"

/** Google's own "hum to search", as a second opinion. */
private fun googleHumSearchAvailable(context: Context): Boolean =
    context.packageManager.resolveActivity(Intent(MUSIC_SEARCH).setPackage(GOOGLE_APP), PackageManager.MATCH_DEFAULT_ONLY) != null

private fun askGoogle(context: Context) {
    try {
        context.startActivity(Intent(MUSIC_SEARCH).setPackage(GOOGLE_APP).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
    } catch (_: ActivityNotFoundException) {
        // The Google app went away meanwhile.
    }
}
