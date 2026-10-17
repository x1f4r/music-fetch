package app.musicfetch.ui.session

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
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
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LinearWavyProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.capture.ListenOrigin
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.EvidenceSource
import app.musicfetch.core.ListenMode
import app.musicfetch.core.Segment
import app.musicfetch.core.SegmentKind
import app.musicfetch.core.Tier
import app.musicfetch.platform.ServiceLinkOpener
import app.musicfetch.platform.displayName
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.HatchedTile
import app.musicfetch.ui.components.OpenInSplitButton
import app.musicfetch.ui.components.Tag
import app.musicfetch.ui.components.TagStyle
import app.musicfetch.ui.components.TimelineStrip
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.theme.TabularNumbers
import kotlin.math.ceil

/** A mix with its timeline, or a playlist/album as a numbered list (docs/design/DESIGN.md §6.5, §6.6). */
@Composable
internal fun MixContent(view: SessionView.Mix, viewModel: SessionViewModel, onBack: () -> Unit) {
    val entry = view.entry
    var selectedId by rememberSaveable { mutableStateOf<String?>(null) }
    var sheetId by rememberSaveable { mutableStateOf<String?>(null) }
    val rows = view.segments.filter { it.kind == SegmentKind.TRACK || it.kind == SegmentKind.UNIDENTIFIED }
    val listState = rememberLazyListState()
    val humIt = humItAction()
    val title = view.source?.title ?: entry?.title ?: ""
    val site = view.source?.site ?: view.source?.service?.displayName ?: Formatters.host(view.source?.url ?: entry?.sourceUrl)
    val subtitle = listOfNotNull(
        site,
        view.totalMs.takeIf { it > 0 && view.timed }?.let(Formatters::clock),
    ).joinToString(" · ")
    LaunchedEffect(selectedId) {
        val index = rows.indexOfFirst { it.id == selectedId }
        if (index >= 0) listState.animateScrollToItem(index)
    }
    Scaffold(
        topBar = {
            BackBar(
                onBack,
                title = {
                    if (view.timed) {
                        Column {
                            Text(title, style = MaterialTheme.typography.titleLarge, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            if (subtitle.isNotEmpty()) {
                                Text(
                                    subtitle,
                                    style = MaterialTheme.typography.bodyMedium,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                    maxLines = 1,
                                )
                            }
                        }
                    }
                },
                actions = { SessionActions(entry, viewModel, isMix = true) },
            )
        },
    ) { padding ->
        Column(Modifier.fillMaxSize().padding(top = padding.calculateTopPadding())) {
            if (view.timed) {
                TimelineStrip(
                    segments = view.segments,
                    totalMs = view.totalMs,
                    analyzedMs = if (view.running) view.analyzedMs else null,
                    selectedId = selectedId,
                    onSelect = { selectedId = it.id },
                    modifier = Modifier.padding(horizontal = 16.dp),
                )
            } else {
                // Collections name the host, as in "Sabrina Carpenter · 12 tracks · open.spotify.com".
                CollectionHeader(view, title, Formatters.host(view.source?.url ?: entry?.sourceUrl) ?: site)
            }
            // A finished collection needs no status: the header already counts its tracks.
            if (view.timed || view.running || view.canceled) StatusRow(view, rows, onStop = { viewModel.stop() })
            LazyColumn(state = listState, contentPadding = PaddingValues(bottom = 24.dp), modifier = Modifier.fillMaxSize()) {
                items(rows, key = { it.id }) { segment ->
                    SegmentRow(
                        segment = segment,
                        timed = view.timed,
                        selected = segment.id == selectedId,
                        onClick = {
                            selectedId = segment.id
                            sheetId = segment.id
                        },
                        onConfirm = { segment.track?.let { viewModel.correct(segment, it) } },
                        onReject = {
                            selectedId = segment.id
                            viewModel.reject(segment)
                        },
                        onHumIt = { humIt(viewModel, segment) },
                        modifier = Modifier.animateItem(),
                    )
                }
            }
        }
    }
    val sheetSegment = rows.firstOrNull { it.id == sheetId }
    if (sheetSegment != null) {
        SegmentSheet(sheetSegment, view, viewModel, onHumIt = { humIt(viewModel, sheetSegment) }, onDismiss = { sheetId = null })
    }
}

/** Starts a hum whose confirmed answer fills in [segment]. */
@Composable
private fun humItAction(): (SessionViewModel, Segment) -> Unit {
    val context = LocalContext.current
    val navigator = LocalNavigator.current
    return { vm, segment ->
        vm.humForSegment(segment)
        context.appGraph.listen.start(CaptureSource.MICROPHONE, ListenMode.HUM, ListenOrigin.App)
        navigator.showListen()
    }
}

@Composable
private fun CollectionHeader(view: SessionView.Mix, title: String, site: String?) {
    val count = view.itemCount.takeIf { it > 1 } ?: view.segments.size
    // Spotify playlists name "Spotify" as their owner: don't say it twice.
    val artist = view.source?.uploader?.takeIf { !it.equals(site, ignoreCase = true) }
    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)) {
        Artwork(view.source?.thumbnailUrl ?: view.entry?.artworkUrl, 56.dp, corner = 8.dp)
        Column(Modifier.padding(start = 16.dp)) {
            Text(title, style = MaterialTheme.typography.titleLarge, maxLines = 2, overflow = TextOverflow.Ellipsis)
            Text(
                listOfNotNull(artist, pluralStringResource(R.plurals.tracks, count, count), site).joinToString(" · "),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
            )
        }
    }
}

@Composable
private fun StatusRow(view: SessionView.Mix, rows: List<Segment>, onStop: () -> Unit) {
    val identified = rows.count { it.kind == SegmentKind.TRACK && it.track != null }
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier.fillMaxWidth().heightIn(min = 40.dp).padding(start = 16.dp, end = 8.dp, top = 8.dp, bottom = 4.dp),
    ) {
        if (view.running) {
            val fraction = if (view.totalMs > 0) (view.analyzedMs.toFloat() / view.totalMs).coerceIn(0f, 1f) else null
            if (fraction != null && view.timed) {
                LinearWavyProgressIndicator(progress = { fraction }, modifier = Modifier.width(120.dp))
            } else {
                LinearWavyProgressIndicator(modifier = Modifier.width(120.dp))
            }
            Spacer(Modifier.width(12.dp))
            Text(
                eta(view.etaMs, view.timed),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(1f),
            )
            TextButton(onClick = onStop) { Text(stringResource(R.string.action_stop)) }
        } else {
            val text = when {
                view.canceled -> stringResource(R.string.mix_stopped, identified)
                rows.isNotEmpty() && view.timed -> stringResource(R.string.mix_identified, identified, rows.size)
                else -> stringResource(R.string.mix_identified_only, identified)
            }
            Text(text, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}

@Composable
private fun eta(etaMs: Long?, timed: Boolean): String = when {
    !timed || etaMs == null -> stringResource(R.string.mix_analyzing)
    etaMs < 60_000 -> stringResource(R.string.mix_eta_under_minute)
    else -> {
        val minutes = ceil(etaMs / 60_000.0).toInt()
        pluralStringResource(R.plurals.mix_eta, minutes, minutes)
    }
}

/** One tracklist row (docs/design/DESIGN.md §5.9): cue · artwork · title and artists · tier actions. */
@Composable
private fun SegmentRow(
    segment: Segment,
    timed: Boolean,
    selected: Boolean,
    onClick: () -> Unit,
    onConfirm: () -> Unit,
    onReject: () -> Unit,
    onHumIt: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val colors = MaterialTheme.colorScheme
    val track = segment.track
    val cue = if (timed) Formatters.clock(segment.startMs.toLong()) else (segment.itemIndex.toInt() + 1).toString()
    val tierLabel = tierText(segment)
    val description = if (track != null) {
        stringResource(R.string.mix_row_description, cue, track.title, track.artist, tierLabel)
    } else {
        "$cue, ${stringResource(R.string.mix_id)}"
    }
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = modifier
            .fillMaxWidth()
            .background(if (selected) colors.surfaceContainer else androidx.compose.ui.graphics.Color.Transparent)
            .clickable(onClick = onClick)
            .heightIn(min = 64.dp)
            .padding(horizontal = 16.dp, vertical = 8.dp)
            .semantics(mergeDescendants = true) { contentDescription = description },
    ) {
        Text(
            cue,
            style = MaterialTheme.typography.labelMedium.merge(TabularNumbers),
            color = colors.onSurfaceVariant,
            textAlign = if (timed) TextAlign.Start else TextAlign.End,
            modifier = Modifier.width(44.dp).clearAndSetSemantics { },
        )
        Spacer(Modifier.width(12.dp))
        if (track != null) {
            Artwork(track.artworkUrl, 40.dp, corner = 6.dp)
        } else {
            HatchedTile(40.dp, corner = 6.dp)
        }
        Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
            Text(
                track?.title ?: stringResource(R.string.mix_id),
                style = MaterialTheme.typography.bodyLarge,
                color = if (track != null) colors.onSurface else colors.onSurfaceVariant,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
            )
            if (track != null) {
                Text(
                    track.artists.takeIf { it.isNotEmpty() }?.joinToString(", ") ?: track.artist,
                    style = MaterialTheme.typography.bodyMedium,
                    color = colors.onSurfaceVariant,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
            }
        }
        when {
            track == null && timed -> TextButton(onClick = onHumIt) { Text(stringResource(R.string.mix_hum_it)) }
            track != null && segment.tier == Tier.POSSIBLE -> {
                Tag(stringResource(R.string.mix_possible), TagStyle.Outlined)
                IconButton(onClick = onConfirm) {
                    Icon(painterResource(R.drawable.ic_check), contentDescription = stringResource(R.string.mix_confirm))
                }
                IconButton(onClick = onReject) {
                    Icon(painterResource(R.drawable.ic_close), contentDescription = stringResource(R.string.mix_not_this))
                }
            }
        }
    }
}

@Composable
private fun tierText(segment: Segment): String = stringResource(
    when (segment.tier) {
        Tier.CONFIRMED -> R.string.mix_tier_confirmed
        Tier.LIKELY -> R.string.mix_tier_likely
        Tier.POSSIBLE -> R.string.mix_tier_possible
        Tier.UNKNOWN -> R.string.mix_tier_unknown
    },
)

/** Details of one row: match, evidence, open, the source at the cue, confirm or pick another. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun SegmentSheet(
    segment: Segment,
    view: SessionView.Mix,
    viewModel: SessionViewModel,
    onHumIt: () -> Unit,
    onDismiss: () -> Unit,
) {
    val context = LocalContext.current
    val preference by viewModel.preference.collectAsStateWithLifecycle()
    val colors = MaterialTheme.colorScheme
    val track = segment.track
    ModalBottomSheet(onDismissRequest = onDismiss) {
        Column(Modifier.padding(horizontal = 24.dp).padding(bottom = 32.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                if (track != null) Artwork(track.artworkUrl, 64.dp, corner = 8.dp) else HatchedTile(64.dp)
                Column(Modifier.padding(start = 16.dp)) {
                    Text(
                        track?.title ?: stringResource(R.string.mix_id),
                        style = MaterialTheme.typography.titleLarge,
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis,
                    )
                    track?.let {
                        Text(it.artists.takeIf { a -> a.isNotEmpty() }?.joinToString(", ") ?: it.artist, style = MaterialTheme.typography.bodyLarge)
                    }
                    if (view.timed) {
                        Text(
                            "${Formatters.clock(segment.startMs.toLong())} – ${Formatters.clock(segment.endMs.toLong())}",
                            style = MaterialTheme.typography.labelMedium.merge(TabularNumbers),
                            color = colors.onSurfaceVariant,
                        )
                    }
                }
            }
            if (track != null) {
                Detail(stringResource(R.string.mix_match), tierText(segment))
                evidenceText(segment)?.let { Detail(stringResource(R.string.mix_evidence), it) }
                track.album?.takeIf { it.isNotBlank() }?.let { Detail(stringResource(R.string.result_album), it) }
                preference?.let { OpenInSplitButton(track, it, height = 48.dp) }
            }
            val sourceUrl = segment.sourceUrl
            if (sourceUrl != null && view.timed) {
                val site = view.source?.site ?: view.source?.service?.displayName ?: Formatters.host(sourceUrl) ?: ""
                OutlinedButton(onClick = { ServiceLinkOpener.openUrl(context, sourceUrl) }) {
                    Text(stringResource(R.string.mix_open_source_at, site, Formatters.clock(segment.startMs.toLong())))
                }
            }
            if (track != null && segment.tier == Tier.POSSIBLE) {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    FilledTonalButton(onClick = {
                        viewModel.correct(segment, track)
                        onDismiss()
                    }) { Text(stringResource(R.string.mix_confirm)) }
                    FilledTonalButton(onClick = {
                        viewModel.reject(segment)
                        onDismiss()
                    }) { Text(stringResource(R.string.mix_not_this)) }
                }
            }
            if (segment.alternates.isNotEmpty()) {
                Text(stringResource(R.string.mix_alternates), style = MaterialTheme.typography.titleSmall, color = colors.primary)
                segment.alternates.forEach { alternate ->
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable {
                                viewModel.correct(segment, alternate)
                                onDismiss()
                            }
                            .padding(vertical = 4.dp),
                    ) {
                        Artwork(alternate.artworkUrl, 40.dp, corner = 6.dp)
                        Column(Modifier.padding(start = 12.dp)) {
                            Text(alternate.title, style = MaterialTheme.typography.bodyLarge, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            Text(alternate.artist, style = MaterialTheme.typography.bodyMedium, color = colors.onSurfaceVariant, maxLines = 1)
                        }
                    }
                }
            }
            if (track == null && view.timed) {
                FilledTonalButton(onClick = {
                    onDismiss()
                    onHumIt()
                }) { Text(stringResource(R.string.mix_hum_part)) }
            }
            Spacer(Modifier.size(1.dp))
        }
    }
}

@Composable
private fun Detail(label: String, value: String) {
    Row {
        Text(label, style = MaterialTheme.typography.bodyMedium, color = MaterialTheme.colorScheme.onSurfaceVariant, modifier = Modifier.width(96.dp))
        Text(value, style = MaterialTheme.typography.bodyMedium, modifier = Modifier.weight(1f))
    }
}

@Composable
private fun evidenceText(segment: Segment): String? {
    val shazam = segment.evidence.count { it.source == EvidenceSource.SHAZAM }
    val parts = buildList {
        if (shazam > 0) add(pluralStringResource(R.plurals.mix_evidence_shazam, shazam, shazam))
        segment.evidence.firstOrNull { it.source == EvidenceSource.ACR_CLOUD }?.let {
            add(stringResource(R.string.mix_evidence_acr, (it.score * 100).toInt()))
        }
        if (segment.evidence.any { it.source == EvidenceSource.AUD_D }) add(stringResource(R.string.mix_evidence_audd))
        if (segment.evidence.any { it.source == EvidenceSource.METADATA }) add(stringResource(R.string.mix_evidence_metadata))
        if (segment.evidence.any { it.source == EvidenceSource.PAGE_HINT } && shazam == 0) add(stringResource(R.string.mix_evidence_hint))
        if (segment.evidence.any { it.source == EvidenceSource.USER }) add(stringResource(R.string.mix_evidence_user))
    }
    return parts.takeIf { it.isNotEmpty() }?.joinToString(" · ")
}
