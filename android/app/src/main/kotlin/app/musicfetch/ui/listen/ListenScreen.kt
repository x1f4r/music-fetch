package app.musicfetch.ui.listen

import android.Manifest
import android.app.Activity
import android.app.StatusBarManager
import android.content.ClipDescription
import android.content.ClipboardManager
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.drawable.Icon
import android.media.projection.MediaProjectionManager
import android.os.Build
import android.provider.Settings
import android.view.textclassifier.TextClassifier
import androidx.activity.compose.PredictiveBackHandler
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.AnimatedContent
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.togetherWith
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.ButtonGroupDefaults
import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.ToggleButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.disabled
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LifecycleEventEffect
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import app.musicfetch.R
import app.musicfetch.analysis.AnalysisState
import app.musicfetch.capture.ListenPhase
import app.musicfetch.capture.ListenResult
import app.musicfetch.capture.ListenState
import app.musicfetch.capture.ProjectionGrant
import app.musicfetch.capture.SongTileService
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.Notice
import app.musicfetch.core.Phase
import app.musicfetch.engine.MelodyDownload
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.ListenSource
import app.musicfetch.platform.LocalLowPower
import app.musicfetch.ui.common.Formatters
import app.musicfetch.ui.components.Artwork
import app.musicfetch.ui.components.ButtonGlyph
import app.musicfetch.ui.components.ButtonRing
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.components.ListenButton
import app.musicfetch.ui.components.PitchTrace
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.LyricsRoute
import app.musicfetch.ui.navigation.SessionRoute
import app.musicfetch.ui.theme.TabularNumbers
import kotlinx.coroutines.delay
import kotlin.coroutines.cancellation.CancellationException

/** Seconds of voiced input the hum ring fills to (docs/design/DESIGN.md §4.4). */
private const val HUM_RING_SECONDS = 12f

/** The Listen stage (docs/design/DESIGN.md §6.2). */
@Composable
fun ListenScreen(viewModel: ListenViewModel = viewModel()) {
    val context = LocalContext.current
    val navigator = LocalNavigator.current
    val haptics = LocalHapticFeedback.current
    val state by viewModel.listen.collectAsStateWithLifecycle()
    val settings by viewModel.settings.collectAsStateWithLifecycle()
    val recent by viewModel.recent.collectAsStateWithLifecycle()
    val link by viewModel.link.collectAsStateWithLifecycle()
    val linkState by viewModel.linkState.collectAsStateWithLifecycle()
    val melodyStatus by viewModel.melodyStatus.collectAsStateWithLifecycle()
    val melodyDownload by viewModel.melodyDownload.collectAsStateWithLifecycle()
    val source = settings?.listenSource ?: ListenSource.Microphone
    var selectedSource by rememberSaveable { mutableStateOf<ListenSource?>(null) }
    val activeSource = selectedSource ?: source
    var micOff by rememberSaveable { mutableStateOf(false) }
    var phonePrePrompt by rememberSaveable { mutableStateOf(false) }
    var melodyPromptDismissed by rememberSaveable { mutableStateOf(false) }
    var backProgress by remember { mutableFloatStateOf(0f) }
    var fieldEmpty by remember { mutableStateOf(true) }

    // Results navigate to the session screen.
    LaunchedEffect(viewModel) {
        viewModel.openSession.collect { id ->
            haptics.performHapticFeedback(HapticFeedbackType.Confirm)
            navigator.push(SessionRoute(id))
        }
    }
    // musicfetch://analyze?url=…
    LaunchedEffect(Unit) {
        app.musicfetch.ui.AppIntents.linkToAnalyze.collect { url ->
            if (url != null) app.musicfetch.ui.AppIntents.takeLink()?.let { viewModel.analyze(it) }
        }
    }

    val consent = rememberLauncherForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
        val data = result.data
        if (result.resultCode == Activity.RESULT_OK && data != null) {
            viewModel.start(ListenSource.Phone, ProjectionGrant(result.resultCode, data))
        }
    }

    fun startPhone() {
        val manager = context.getSystemService(MediaProjectionManager::class.java) ?: return
        consent.launch(manager.createScreenCaptureIntent())
    }

    val micPermission = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        viewModel.micAsked()
        if (granted) {
            micOff = false
            if (activeSource == ListenSource.Phone) startPhone() else viewModel.start(activeSource)
        } else {
            micOff = true
        }
    }
    val notificationPermission = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { }

    fun hasMic() = context.checkSelfPermission(Manifest.permission.RECORD_AUDIO) == PackageManager.PERMISSION_GRANTED

    fun startListening(chosen: ListenSource) {
        selectedSource = chosen
        viewModel.selectSource(chosen)
        if (!hasMic()) {
            val activity = context as? Activity
            val denied = settings?.micExplained == true &&
                activity?.shouldShowRequestPermissionRationale(Manifest.permission.RECORD_AUDIO) == false
            if (denied) micOff = true else micPermission.launch(Manifest.permission.RECORD_AUDIO)
            return
        }
        micOff = false
        if (chosen == ListenSource.Phone) {
            if (settings?.phoneAudioExplained == true) startPhone() else phonePrePrompt = true
        } else {
            viewModel.start(chosen)
        }
    }

    fun askNotificationsOnce() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU && !Notifications.canPost(context)) {
            notificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
        }
    }

    // Offer the Quick Settings tile once, after the second successful listen.
    LaunchedEffect(settings?.successfulListens, settings?.tileOffered) {
        val s = settings ?: return@LaunchedEffect
        if (s.successfulListens >= 2 && !s.tileOffered && Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            viewModel.tileOffered()
            requestSongTile(context)
        }
    }

    // Predictive back while capturing scrubs the button; committing cancels the session.
    PredictiveBackHandler(enabled = state.isCapturing) { progress ->
        try {
            progress.collect { backProgress = it.progress }
            viewModel.cancel()
        } catch (e: CancellationException) {
            backProgress = 0f
            throw e
        } finally {
            backProgress = 0f
        }
    }

    val showMelodyPrompt = state.melodyIndexMissing && settings?.melodyPromptDeclined == false && !melodyPromptDismissed &&
        melodyStatus?.installed != true && melodyDownload !is MelodyDownload.Running
    if (showMelodyPrompt) {
        MelodyPrompt(
            sizeBytes = melodyStatus?.downloadBytes?.toLong(),
            onDownload = {
                melodyPromptDismissed = true
                viewModel.downloadMelody()
            },
            onDecline = {
                melodyPromptDismissed = true
                viewModel.declineMelody()
            },
        )
    }

    Column(Modifier.fillMaxSize().windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Top + WindowInsetsSides.Horizontal))) {
        LinkField(
            enabled = !state.isActive,
            onSubmit = { text ->
                askNotificationsOnce()
                viewModel.analyze(text)
            },
            onPickFile = { uri ->
                askNotificationsOnce()
                viewModel.analyzeFile(uri)
            },
            onEmptyChange = { fieldEmpty = it },
            onFocus = viewModel::prepareForLink,
            modifier = Modifier.padding(horizontal = 16.dp).padding(top = 8.dp),
        )
        Box(Modifier.weight(1f).fillMaxWidth()) {
            ClipboardSuggestion(
                visible = fieldEmpty && !state.isActive && link !is LinkStage.Running,
                onIdentify = { url ->
                    askNotificationsOnce()
                    viewModel.analyze(url)
                },
                modifier = Modifier.align(Alignment.TopStart).padding(start = 16.dp, top = 4.dp),
            )
            Stage(
                state = state,
                viewModel = viewModel,
                activeSource = activeSource,
                link = link,
                linkState = linkState,
                micOff = micOff,
                phonePrePrompt = phonePrePrompt,
                backProgress = backProgress,
                onButton = {
                    when {
                        state.isCapturing -> viewModel.stop()
                        state.isActive -> Unit
                        link is LinkStage.Running || link is LinkStage.Preparing -> Unit
                        else -> startListening(activeSource)
                    }
                },
                onSelectSource = {
                    micOff = false
                    phonePrePrompt = false
                    selectedSource = it
                    viewModel.selectSource(it)
                },
                onTryAgain = {
                    viewModel.dismissResult()
                    val result = (state.phase as? ListenPhase.Done)?.result
                    val again = when {
                        result == null -> activeSource
                        state.source == CaptureSource.SYSTEM_AUDIO -> ListenSource.Phone
                        state.mode == ListenMode.HUM -> ListenSource.Hum
                        else -> ListenSource.Microphone
                    }
                    startListening(again)
                },
                onHumIt = { startListening(ListenSource.Hum) },
                onTypeLyric = {
                    viewModel.dismissResult()
                    navigator.push(LyricsRoute)
                },
                onUseMicrophone = {
                    viewModel.dismissResult()
                    selectedSource = ListenSource.Microphone
                    viewModel.selectSource(ListenSource.Microphone)
                },
                onOpenSettings = {
                    context.startActivity(
                        Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, android.net.Uri.fromParts("package", context.packageName, null))
                            .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK),
                    )
                },
                onContinuePhone = {
                    phonePrePrompt = false
                    viewModel.phoneAudioExplained()
                    startPhone()
                },
                onStopLink = { viewModel.stopLink() },
                onDismissLinkError = {
                    viewModel.clearLink()
                },
                onHumRingFull = { haptics.performHapticFeedback(HapticFeedbackType.SegmentTick) },
                modifier = Modifier.fillMaxSize(),
            )
        }
        AnimatedVisibility(visible = recent != null, enter = fadeIn(), exit = fadeOut()) {
            recent?.let { entry ->
                RecentCard(
                    title = entry.track?.title ?: entry.title,
                    artist = entry.track?.artist ?: entry.subtitle,
                    time = Formatters.timeOfDay(entry.createdAtMs),
                    artworkUrl = entry.track?.artworkUrl ?: entry.artworkUrl,
                    onClick = { navigator.push(SessionRoute(entry.id)) },
                    modifier = Modifier.padding(start = 16.dp, end = 16.dp, bottom = 16.dp),
                )
            }
        }
    }
    LifecycleEventEffect(Lifecycle.Event.ON_RESUME) {
        if (micOff && hasMic()) micOff = false
    }
}

@Composable
private fun Stage(
    state: ListenState,
    viewModel: ListenViewModel,
    activeSource: ListenSource,
    link: LinkStage,
    linkState: AnalysisState?,
    micOff: Boolean,
    phonePrePrompt: Boolean,
    backProgress: Float,
    onButton: () -> Unit,
    onSelectSource: (ListenSource) -> Unit,
    onTryAgain: () -> Unit,
    onHumIt: () -> Unit,
    onTypeLyric: () -> Unit,
    onUseMicrophone: () -> Unit,
    onOpenSettings: () -> Unit,
    onContinuePhone: () -> Unit,
    onStopLink: () -> Unit,
    onDismissLinkError: () -> Unit,
    onHumRingFull: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val level by viewModel.signal.level.collectAsStateWithLifecycle()
    val phase = state.phase
    val done = (phase as? ListenPhase.Done)?.result
    // A streaming link usually resolves within 300 ms: only then show "Resolving link".
    var linkVisible by remember { mutableStateOf(false) }
    LaunchedEffect(link) {
        linkVisible = false
        if (link is LinkStage.Running || link is LinkStage.Preparing) {
            delay(300)
            linkVisible = true
        }
    }
    val linkActive = (link is LinkStage.Running || link is LinkStage.Preparing) && linkVisible
    val humming = state.isActive && (state.impression || state.mode == ListenMode.HUM) && phase != ListenPhase.Matching
    // Read in drawing code and derived states only, so the stage doesn't recompose 10 times a second.
    val voicedState = produceVoicedSeconds(viewModel, active = humming)
    val ringFull by remember { derivedStateOf { voicedState.value >= HUM_RING_SECONDS } }
    val voicedWhole by remember { derivedStateOf { voicedState.value.toInt() } }
    LaunchedEffect(ringFull) { if (ringFull) onHumRingFull() }

    val glyph = when {
        linkActive -> ButtonGlyph.Link
        phase == ListenPhase.Matching -> ButtonGlyph.Matching
        state.isCapturing -> ButtonGlyph.Live
        else -> ButtonGlyph.Mark
    }
    val ring = when {
        linkActive && linkState?.phase == Phase.DOWNLOADING && linkState.fraction != null -> {
            val fraction = linkState.fraction
            ButtonRing.Determinate { fraction }
        }
        linkActive -> ButtonRing.Indeterminate
        humming -> ButtonRing.Determinate { (voicedState.value / HUM_RING_SECONDS).coerceIn(0f, 1f) }
        else -> ButtonRing.None
    }
    val stateLine = stateLine(state, link, linkState, linkActive, micOff, phonePrePrompt)
    val stateActive = state.isActive || linkActive

    Box(modifier) {
        val pitchLabel = stringResource(R.string.your_pitch)
        val pitchValue = pluralStringResource(R.plurals.seconds_hummed, voicedWhole, voicedWhole)
        // Trace band: (width − 48) × 96, bottom edge 144 dp above the stage centre.
        AnimatedVisibility(
            visible = humming,
            enter = fadeIn(tween(150)),
            exit = fadeOut(tween(150)),
            modifier = Modifier.align(Alignment.Center).offset(y = (-144 - 48).dp),
        ) {
            PitchTrace(
                ring = viewModel.signal.pitch,
                color = MaterialTheme.colorScheme.primary,
                active = state.isCapturing,
                modifier = Modifier
                    .padding(horizontal = 24.dp)
                    .fillMaxWidth()
                    .height(96.dp)
                    .semantics {
                        contentDescription = pitchLabel
                        stateDescription = pitchValue
                    },
            )
        }
        val listenLabel = stringResource(R.string.listen_button)
        val valueLabel = stringResource(
            when {
                phase == ListenPhase.Matching -> R.string.listen_value_matching
                humming -> R.string.listen_value_humming
                state.isCapturing -> R.string.listen_value_listening
                else -> R.string.listen_value_idle
            },
        )
        ListenButton(
            glyph = glyph,
            level = { level },
            onClick = onButton,
            contentDescription = listenLabel,
            stateDescription = valueLabel,
            ring = ring,
            lineAmplitude = if (state.impression) 0.5f else 1f,
            backProgress = backProgress,
            modifier = Modifier.align(Alignment.Center).offset(y = (-40).dp),
        )
        // State line: 24 dp tall, top 60 dp below the centre.
        Box(Modifier.align(Alignment.Center).offset(y = (60 + 12).dp).fillMaxWidth().padding(horizontal = 24.dp)) {
            AnimatedContent(
                targetState = stateLine,
                transitionSpec = { fadeIn(tween(150)) togetherWith fadeOut(tween(150)) },
                label = "stateLine",
                modifier = Modifier.fillMaxWidth().heightIn(min = 24.dp),
            ) { text ->
                Text(
                    text,
                    style = MaterialTheme.typography.titleMedium,
                    color = if (stateActive) MaterialTheme.colorScheme.onSurface else MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center,
                    maxLines = 2,
                    modifier = Modifier.fillMaxWidth().semantics { liveRegion = LiveRegionMode.Polite },
                )
            }
        }
        // Action row: 40 dp tall, top 124 dp below the centre.
        Box(Modifier.align(Alignment.Center).offset(y = (124 + 20).dp).fillMaxWidth(), contentAlignment = Alignment.Center) {
            when {
                linkActive -> ActionRow { TextButton(onClick = onStopLink) { Text(stringResource(R.string.action_stop)) } }
                link is LinkStage.Failed -> ActionRow {
                    FilledTonalButton(onClick = onDismissLinkError) { Text(stringResource(R.string.action_ok)) }
                }
                micOff -> ActionRow { FilledTonalButton(onClick = onOpenSettings) { Text(stringResource(R.string.action_open_settings)) } }
                phonePrePrompt -> ActionRow {
                    FilledTonalButton(onClick = onContinuePhone) { Text(stringResource(R.string.action_continue)) }
                    TextButton(onClick = onUseMicrophone) { Text(stringResource(R.string.action_use_microphone)) }
                }
                state.isCapturing && state.notice is Notice.CaptureSilent -> ActionRow {
                    FilledTonalButton(onClick = {
                        viewModel.cancel()
                        onUseMicrophone()
                    }) { Text(stringResource(R.string.action_use_microphone)) }
                }
                state.isCapturing && state.notice is Notice.RateLimited -> ActionRow {
                    TextButton(onClick = { viewModel.stop() }) { Text(stringResource(R.string.action_stop)) }
                }
                done is ListenResult.NoMatch -> NoMatchActions(done.reason, onTryAgain, onHumIt, onTypeLyric, onUseMicrophone)
                done is ListenResult.Failed -> ActionRow {
                    FilledTonalButton(onClick = onTryAgain) { Text(stringResource(R.string.action_try_again)) }
                }
                done is ListenResult.Waiting -> ActionRow {
                    FilledTonalButton(onClick = { viewModel.dismissResult() }) { Text(stringResource(R.string.action_ok)) }
                }
                else -> SourceToggles(
                    selected = when {
                        state.isActive && state.source == CaptureSource.SYSTEM_AUDIO -> ListenSource.Phone
                        state.isActive && (state.mode == ListenMode.HUM || state.impression) -> ListenSource.Hum
                        state.isActive -> ListenSource.Microphone
                        else -> activeSource
                    },
                    enabled = !state.isActive && !linkActive,
                    onSelect = onSelectSource,
                )
            }
        }
    }
}

@Composable
private fun produceVoicedSeconds(viewModel: ListenViewModel, active: Boolean): androidx.compose.runtime.State<Float> {
    val value = remember { mutableFloatStateOf(0f) }
    val interval = if (LocalLowPower.current) 250L else 100L
    LaunchedEffect(active, interval) {
        if (!active) {
            value.floatValue = 0f
            return@LaunchedEffect
        }
        while (true) {
            value.floatValue = viewModel.signal.pitch.voicedSeconds()
            delay(interval)
        }
    }
    return value
}

@Composable
private fun stateLine(
    state: ListenState,
    link: LinkStage,
    linkState: AnalysisState?,
    linkActive: Boolean,
    micOff: Boolean,
    phonePrePrompt: Boolean,
): String {
    val done = (state.phase as? ListenPhase.Done)?.result
    return when {
        linkActive -> when (linkState?.phase) {
            Phase.DOWNLOADING -> stringResource(R.string.state_downloading)
            Phase.DECODING, Phase.SEGMENTING -> stringResource(R.string.state_preparing)
            Phase.RECOGNIZING, Phase.MATCHING_MELODY, Phase.SEARCHING_LYRICS, Phase.FINISHING -> stringResource(R.string.state_matching)
            Phase.RESOLVING, Phase.LISTENING -> stringResource(R.string.state_resolving)
            null -> if (link is LinkStage.Preparing) stringResource(R.string.state_preparing) else stringResource(R.string.state_resolving)
        }
        link is LinkStage.Failed -> link.message
        micOff -> stringResource(R.string.state_mic_off)
        phonePrePrompt -> stringResource(R.string.state_phone_audio_pre_prompt)
        state.phase == ListenPhase.Matching -> stringResource(R.string.state_matching)
        state.isCapturing -> when (val notice = state.notice) {
            is Notice.CaptureSilent -> stringResource(R.string.state_capture_silent)
            is Notice.MoveCloser -> stringResource(R.string.state_move_closer)
            is Notice.RateLimited -> rateLimitedLine(notice.retryInMs.toLong())
            is Notice.ImpressionDetected, is Notice.KeepGoing -> stringResource(R.string.state_keep_going)
            else -> if (state.mode == ListenMode.HUM) {
                stringResource(R.string.state_hum_start)
            } else if (state.impression) {
                stringResource(R.string.state_keep_going)
            } else {
                stringResource(R.string.state_listening)
            }
        }
        done is ListenResult.NoMatch -> stringResource(Notifications.noMatchText(done.reason))
        done is ListenResult.Failed -> done.message.ifBlank { stringResource(Notifications.errorText(done.code)) }
        done is ListenResult.Waiting -> stringResource(R.string.state_offline)
        else -> stringResource(R.string.state_idle)
    }
}

@Composable
private fun rateLimitedLine(retryMs: Long): String {
    var remaining by remember(retryMs) { mutableIntStateOf(((retryMs + 999) / 1000).toInt()) }
    LaunchedEffect(retryMs) {
        while (remaining > 0) {
            delay(1000)
            remaining--
        }
    }
    return stringResource(R.string.state_rate_limited, remaining.coerceAtLeast(0))
}

@Composable
private fun ActionRow(content: @Composable () -> Unit) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(8.dp, Alignment.CenterHorizontally),
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier.height(40.dp),
    ) { content() }
}

@Composable
private fun NoMatchActions(
    reason: NoMatchReason,
    onTryAgain: () -> Unit,
    onHumIt: () -> Unit,
    onTypeLyric: () -> Unit,
    onUseMicrophone: () -> Unit,
) {
    ActionRow {
        if (reason == NoMatchReason.CAPTURE_SILENT) {
            FilledTonalButton(onClick = onUseMicrophone) { Text(stringResource(R.string.action_use_microphone)) }
            return@ActionRow
        }
        FilledTonalButton(onClick = onTryAgain) { Text(stringResource(R.string.action_try_again)) }
        if (reason != NoMatchReason.TOO_SHORT) TextButton(onClick = onHumIt) { Text(stringResource(R.string.action_hum_it)) }
        if (reason == NoMatchReason.NOT_FOUND) TextButton(onClick = onTypeLyric) { Text(stringResource(R.string.action_type_lyric)) }
    }
}

/** Microphone · Phone · Hum (docs/design/DESIGN.md §5.2): a connected button group. */
@OptIn(ExperimentalMaterial3ExpressiveApi::class)
@Composable
private fun SourceToggles(selected: ListenSource, enabled: Boolean, onSelect: (ListenSource) -> Unit) {
    val options = listOf(
        Triple(ListenSource.Microphone, R.string.source_microphone, R.drawable.ic_mic),
        Triple(ListenSource.Phone, R.string.source_phone, R.drawable.ic_smartphone),
        Triple(ListenSource.Hum, R.string.source_hum, 0),
    )
    // Phones narrower than 400 dp (most budget phones are 360 dp) can't fit three icons and
    // labels; the labels stay.
    BoxWithConstraints(contentAlignment = Alignment.Center) {
        val showIcons = maxWidth >= 400.dp && LocalDensity.current.fontScale <= 1.15f
        Row(
            horizontalArrangement = Arrangement.spacedBy(ButtonGroupDefaults.ConnectedSpaceBetween),
            modifier = Modifier.padding(horizontal = 24.dp).alpha(if (enabled) 1f else 0.45f),
        ) {
            options.forEachIndexed { index, (option, label, icon) ->
                val shapes = when (index) {
                    0 -> ButtonGroupDefaults.connectedLeadingButtonShapes()
                    options.lastIndex -> ButtonGroupDefaults.connectedTrailingButtonShapes()
                    else -> ButtonGroupDefaults.connectedMiddleButtonShapes()
                }
                // While a session runs the group is dimmed to 45 % but keeps showing the current
                // source (a disabled ToggleButton would drop its checked colours).
                ToggleButton(
                    checked = selected == option,
                    onCheckedChange = { if (enabled) onSelect(option) },
                    shapes = shapes,
                    modifier = Modifier
                        .heightIn(min = 40.dp)
                        .semantics { if (!enabled) disabled() },
                ) {
                    if (showIcons) {
                        if (icon != 0) {
                            Icon(painterResource(icon), contentDescription = null, modifier = Modifier.size(20.dp))
                        } else {
                            Box(Modifier.size(20.dp), contentAlignment = Alignment.Center) {
                                ContourMark(androidx.compose.material3.LocalContentColor.current, width = 20.dp, strokeWidth = 2.dp)
                            }
                        }
                        Spacer(Modifier.width(8.dp))
                    }
                    Text(stringResource(label), maxLines = 1)
                }
            }
        }
    }
}

/** "Paste a link" with a file picker (docs/design/DESIGN.md §5.5). */
@Composable
private fun LinkField(
    enabled: Boolean,
    onSubmit: (String) -> Unit,
    onPickFile: (android.net.Uri) -> Unit,
    onEmptyChange: (Boolean) -> Unit,
    onFocus: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val keyboard = LocalSoftwareKeyboardController.current
    val focus = LocalFocusManager.current
    var text by rememberSaveable { mutableStateOf("") }
    val fieldLabel = stringResource(R.string.field_link)
    val picker = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri -> uri?.let(onPickFile) }
    LaunchedEffect(text.isEmpty()) { onEmptyChange(text.isEmpty()) }
    fun submit(value: String) {
        if (value.isBlank()) return
        keyboard?.hide()
        focus.clearFocus()
        onSubmit(value)
        text = ""
    }
    Column(modifier) {
        Surface(
            shape = RoundedCornerShape(28.dp),
            color = MaterialTheme.colorScheme.surfaceContainerHigh,
            modifier = Modifier.fillMaxWidth().height(56.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(start = 16.dp, end = 4.dp)) {
                Icon(painterResource(R.drawable.ic_link), contentDescription = null, tint = MaterialTheme.colorScheme.onSurfaceVariant)
                Spacer(Modifier.width(16.dp))
                Box(Modifier.weight(1f), contentAlignment = Alignment.CenterStart) {
                    if (text.isEmpty()) {
                        Text(
                            stringResource(R.string.field_link),
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                    BasicTextField(
                        value = text,
                        onValueChange = { text = it },
                        enabled = enabled,
                        singleLine = true,
                        textStyle = MaterialTheme.typography.bodyLarge.copy(color = MaterialTheme.colorScheme.onSurface),
                        cursorBrush = SolidColor(MaterialTheme.colorScheme.primary),
                        keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri, imeAction = ImeAction.Go),
                        keyboardActions = KeyboardActions(onGo = { submit(text) }),
                        modifier = Modifier
                            .fillMaxWidth()
                            .onFocusChanged { if (it.isFocused) onFocus() }
                            .semantics { contentDescription = fieldLabel },
                    )
                }
                if (text.isNotBlank()) {
                    IconButton(onClick = { submit(text) }, enabled = enabled) {
                        Icon(painterResource(R.drawable.ic_search), contentDescription = stringResource(R.string.field_identify))
                    }
                } else {
                    IconButton(onClick = {
                        focus.clearFocus()
                        picker.launch(arrayOf("audio/*", "video/*"))
                    }, enabled = enabled) {
                        Icon(painterResource(R.drawable.ic_attach_file), contentDescription = stringResource(R.string.field_attach))
                    }
                }
            }
        }
    }
}

/**
 * "Identify the link you copied" (docs/design/DESIGN.md §5.5), drawn over the stage so the
 * stage's slots never move. Checked on every resume.
 */
@Composable
private fun ClipboardSuggestion(visible: Boolean, onIdentify: (String) -> Unit, modifier: Modifier = Modifier) {
    val context = LocalContext.current
    var hasLink by remember { mutableStateOf(false) }
    var resumes by remember { mutableIntStateOf(0) }
    LifecycleEventEffect(Lifecycle.Event.ON_RESUME) { resumes++ }
    LaunchedEffect(resumes) {
        // The system classifies a fresh clip in the background; wait briefly for it.
        var state = clipboardLink(context)
        var checks = 1
        while (state == ClipState.Pending && checks < CLIP_CHECKS) {
            delay(CLIP_CHECK_INTERVAL_MS)
            state = clipboardLink(context)
            checks++
        }
        hasLink = state == ClipState.Link
    }
    AnimatedVisibility(visible = visible && hasLink, enter = fadeIn(), exit = fadeOut(), modifier = modifier) {
        TextButton(onClick = {
            hasLink = false
            readClipboardText(context)?.let(onIdentify)
        }) {
            Icon(painterResource(R.drawable.ic_content_paste), contentDescription = null, modifier = Modifier.size(18.dp))
            Spacer(Modifier.width(8.dp))
            Text(stringResource(R.string.field_clipboard_link), maxLines = 1, overflow = TextOverflow.Ellipsis)
        }
    }
}

private enum class ClipState { Link, NoLink, Pending }

private const val CLIP_CHECKS = 10
private const val CLIP_CHECK_INTERVAL_MS = 300L

/**
 * Whether the clipboard holds a link, without reading it (reading would show the system's
 * "pasted from your clipboard" notice): Android 12+ classifies clips on its own.
 */
private fun clipboardLink(context: Context): ClipState {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.S) return ClipState.NoLink
    val clipboard = context.getSystemService(ClipboardManager::class.java) ?: return ClipState.NoLink
    val description = clipboard.primaryClipDescription ?: return ClipState.NoLink
    if (!description.hasMimeType(ClipDescription.MIMETYPE_TEXT_PLAIN)) return ClipState.NoLink
    return when (description.classificationStatus) {
        ClipDescription.CLASSIFICATION_COMPLETE ->
            if (description.getConfidenceScore(TextClassifier.TYPE_URL) >= 0.5f) ClipState.Link else ClipState.NoLink
        ClipDescription.CLASSIFICATION_NOT_COMPLETE -> ClipState.Pending
        else -> ClipState.NoLink
    }
}

private fun readClipboardText(context: Context): String? {
    val clipboard = context.getSystemService(ClipboardManager::class.java) ?: return null
    val clip = clipboard.primaryClip ?: return null
    if (clip.itemCount == 0) return null
    return clip.getItemAt(0).coerceToText(context)?.toString()?.trim()?.takeIf { it.isNotEmpty() }
}

/** The last result (docs/design/DESIGN.md §6.2, "Recent card"). */
@Composable
private fun RecentCard(
    title: String,
    artist: String?,
    time: String,
    artworkUrl: String?,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
) {
    Surface(
        onClick = onClick,
        shape = RoundedCornerShape(16.dp),
        color = MaterialTheme.colorScheme.surfaceContainerLow,
        modifier = modifier.fillMaxWidth().heightIn(min = 72.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(8.dp)) {
            Artwork(artworkUrl, 56.dp, corner = 8.dp)
            Column(Modifier.weight(1f).padding(horizontal = 16.dp)) {
                Text(title, style = MaterialTheme.typography.titleMedium, maxLines = 1, overflow = TextOverflow.Ellipsis)
                // A long artist name gives way; the time is never cut.
                Row {
                    val style = MaterialTheme.typography.bodyMedium
                    val color = MaterialTheme.colorScheme.onSurfaceVariant
                    if (!artist.isNullOrBlank()) {
                        Text(artist, style = style, color = color, maxLines = 1, overflow = TextOverflow.Ellipsis, modifier = Modifier.weight(1f, fill = false))
                        Text(" · ", style = style, color = color, maxLines = 1)
                    }
                    Text(time, style = style.merge(TabularNumbers), color = color, maxLines = 1)
                }
            }
            Icon(
                painterResource(R.drawable.ic_chevron_right),
                contentDescription = null,
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(end = 8.dp),
            )
        }
    }
}

/** "Download the melody library?" (docs/design/DESIGN.md §6.1, `Notice::MelodyIndexMissing`). */
@Composable
private fun MelodyPrompt(sizeBytes: Long?, onDownload: () -> Unit, onDecline: () -> Unit) {
    val context = LocalContext.current
    AlertDialog(
        onDismissRequest = onDecline,
        title = { Text(stringResource(R.string.melody_prompt_title)) },
        text = {
            Text(
                if (sizeBytes != null && sizeBytes > 0) {
                    stringResource(R.string.melody_prompt_message, Formatters.bytes(context, sizeBytes))
                } else {
                    stringResource(R.string.melody_prompt_message_no_size)
                },
            )
        },
        confirmButton = { TextButton(onClick = onDownload) { Text(stringResource(R.string.melody_download)) } },
        dismissButton = { TextButton(onClick = onDecline) { Text(stringResource(R.string.melody_not_now)) } },
    )
}

/** Ask the system to add the "Identify song" tile (Android 13+). */
private fun requestSongTile(context: Context) {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
    val manager = context.getSystemService(StatusBarManager::class.java) ?: return
    manager.requestAddTileService(
        ComponentName(context, SongTileService::class.java),
        context.getString(R.string.tile_song),
        Icon.createWithResource(context, R.drawable.ic_qs_tile),
        context.mainExecutor,
    ) { }
}
