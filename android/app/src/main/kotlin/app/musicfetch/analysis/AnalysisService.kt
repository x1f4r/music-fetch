package app.musicfetch.analysis

import android.app.Notification
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.annotation.RequiresApi
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.app.ServiceCompat
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.core.Phase
import app.musicfetch.core.SegmentKind
import app.musicfetch.notify.Notifications
import app.musicfetch.ui.common.Formatters
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch

/**
 * Foreground service that keeps link and file analyses running when the share sheet closes or
 * the app goes to the background (`dataSync`; `mediaProcessing` on Android 15+), with a
 * progress notification and a Stop action. Stops itself when nothing is running.
 */
class AnalysisService : Service() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var watcher: Job? = null
    private var inForeground = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_CANCEL -> {
                val id = intent.getStringExtra(EXTRA_SESSION)
                if (id != null) appGraph.analyses.cancel(id) else appGraph.analyses.cancelAll()
            }
            else -> Unit
        }
        if (!inForeground && !enterForeground()) return START_NOT_STICKY
        if (watcher == null) watcher = scope.launch { watch() }
        return START_NOT_STICKY
    }

    private fun enterForeground(): Boolean {
        val type = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.VANILLA_ICE_CREAM) {
            ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROCESSING
        } else {
            ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
        }
        return try {
            // Not ServiceCompat: on API 34 it masks the API 35 mediaProcessing type to "none".
            startForeground(Notifications.ANALYSIS_ID, placeholder(), type)
            inForeground = true
            true
        } catch (e: RuntimeException) {
            Log.w(TAG, "could not enter the foreground", e)
            stopSelf()
            false
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun watch() {
        val controller = appGraph.analyses
        controller.runs
            .flatMapLatest { runs ->
                val active = runs.filter { it.showsProgress && !it.state.value.isTerminal }
                if (active.isEmpty()) flowOf(emptyList()) else combine(active.map { it.state }) { it.toList() }
            }
            .collectLatest { states ->
                val running = states.filterNot { it.isTerminal }
                if (running.isEmpty()) {
                    finish()
                } else {
                    update(running.last(), running.size)
                }
            }
    }

    private fun update(state: AnalysisState, count: Int) {
        if (!Notifications.canPost(this)) return
        val notification = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.BAKLAVA) {
            progressStyle(state, count)
        } else {
            compat(state, count)
        }
        try {
            NotificationManagerCompat.from(this).notify(Notifications.ANALYSIS_ID, notification)
        } catch (_: SecurityException) {
            // Notifications were turned off meanwhile.
        }
    }

    private fun finish() {
        if (!inForeground) return
        inForeground = false
        ServiceCompat.stopForeground(this, ServiceCompat.STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    override fun onTimeout(startId: Int, fgsType: Int) {
        // Android 15+ limits dataSync / mediaProcessing time; stop cleanly.
        Log.w(TAG, "foreground time limit reached")
        appGraph.analyses.cancelAll()
        finish()
    }

    override fun onDestroy() {
        scope.cancel()
        super.onDestroy()
    }

    private fun title(state: AnalysisState): String {
        val name = state.source?.title ?: state.item?.title
        return when {
            name != null -> getString(R.string.notif_analyzing, name)
            state.kind == app.musicfetch.core.SessionKind.FILE -> getString(R.string.notif_analyzing_file)
            else -> getString(R.string.notif_analyzing_link)
        }
    }

    private fun detail(state: AnalysisState, count: Int): String {
        val base = when {
            state.totalMs > 0 && state.analyzedMs > 0 -> getString(
                R.string.notif_analyzing_detail,
                Formatters.clock(state.analyzedMs),
                Formatters.clock(state.totalMs),
                resources.getQuantityString(R.plurals.tracks, state.identified, state.identified),
            )
            else -> getString(phaseText(state.phase))
        }
        return if (count > 1) getString(R.string.notif_more_running, base, count - 1) else base
    }

    private fun phaseText(phase: Phase?): Int = when (phase) {
        Phase.RESOLVING -> R.string.state_resolving
        Phase.DOWNLOADING -> R.string.state_downloading
        Phase.DECODING, Phase.SEGMENTING -> R.string.state_preparing
        Phase.RECOGNIZING, Phase.MATCHING_MELODY, Phase.SEARCHING_LYRICS, Phase.FINISHING -> R.string.state_matching
        Phase.LISTENING, null -> R.string.mix_analyzing
    }

    private fun stopAction(state: AnalysisState): PendingIntent = PendingIntent.getService(
        this,
        (state.sessionId ?: "").hashCode(),
        Intent(this, AnalysisService::class.java).setAction(ACTION_CANCEL).putExtra(EXTRA_SESSION, state.sessionId),
        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
    )

    private fun placeholder(): Notification = NotificationCompat.Builder(this, Notifications.CHANNEL_IDENTIFYING)
        .setSmallIcon(R.drawable.ic_notification)
        .setContentTitle(getString(R.string.notif_analyzing_link))
        .setProgress(0, 0, true)
        .setOngoing(true)
        .setSilent(true)
        .setForegroundServiceBehavior(NotificationCompat.FOREGROUND_SERVICE_IMMEDIATE)
        .build()

    private fun compat(state: AnalysisState, count: Int): Notification {
        val builder = NotificationCompat.Builder(this, Notifications.CHANNEL_IDENTIFYING)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(title(state))
            .setContentText(detail(state, count))
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setSilent(true)
            .setCategory(NotificationCompat.CATEGORY_PROGRESS)
            .setContentIntent(Notifications.openApp(this, state.sessionId))
            .addAction(R.drawable.ic_stop, getString(R.string.action_stop), stopAction(state))
        val fraction = progress(state)
        if (fraction != null) builder.setProgress(1000, (fraction * 1000).toInt(), false) else builder.setProgress(0, 0, true)
        return builder.build()
    }

    /** Android 16 progress-centric notification: one filled segment per identified track. */
    @RequiresApi(Build.VERSION_CODES.BAKLAVA)
    private fun progressStyle(state: AnalysisState, count: Int): Notification {
        val total = state.totalMs.coerceAtLeast(0)
        val style = Notification.ProgressStyle().setStyledByProgress(true)
        val fraction = progress(state)
        if (total > 0) {
            val accent = getColor(R.color.progress_track)
            val segments = mutableListOf<Notification.ProgressStyle.Segment>()
            var cursor = 0L
            val tracks = state.segments.filter { it.kind == SegmentKind.TRACK && it.endMs > it.startMs }
                .sortedBy { it.startMs.toLong() }
            for (segment in tracks) {
                val start = segment.startMs.toLong().coerceIn(cursor, total)
                val end = segment.endMs.toLong().coerceIn(start, total)
                if (start - cursor >= MIN_SEGMENT_MS) segments += Notification.ProgressStyle.Segment(units(start - cursor))
                if (end - start >= MIN_SEGMENT_MS) {
                    segments += Notification.ProgressStyle.Segment(units(end - start)).setColor(accent)
                }
                cursor = maxOf(cursor, end)
            }
            if (total - cursor >= MIN_SEGMENT_MS || segments.isEmpty()) {
                segments += Notification.ProgressStyle.Segment(units((total - cursor).coerceAtLeast(MIN_SEGMENT_MS)))
            }
            style.setProgressSegments(segments)
            val max = segments.sumOf { it.length }
            style.setProgress(((fraction ?: 0f) * max).toInt())
        } else {
            style.setProgressIndeterminate(fraction == null)
            fraction?.let { style.setProgress((it * 100).toInt()) }
        }
        return Notification.Builder(this, Notifications.CHANNEL_IDENTIFYING)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(title(state))
            .setContentText(detail(state, count))
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setCategory(Notification.CATEGORY_PROGRESS)
            .setContentIntent(Notifications.openApp(this, state.sessionId))
            .setStyle(style)
            .addAction(
                Notification.Action.Builder(null, getString(R.string.action_stop), stopAction(state)).build(),
            )
            .build()
    }

    private fun progress(state: AnalysisState): Float? = when {
        state.totalMs > 0 && state.analyzedMs > 0 -> (state.analyzedMs.toFloat() / state.totalMs).coerceIn(0f, 1f)
        state.phase == Phase.DOWNLOADING -> state.fraction
        else -> null
    }

    companion object {
        private const val TAG = "AnalysisService"
        private const val ACTION_CANCEL = "app.musicfetch.analysis.CANCEL"
        private const val EXTRA_SESSION = "session"
        private const val MIN_SEGMENT_MS = 1_000L

        private fun units(ms: Long): Int = (ms / 1000).toInt().coerceAtLeast(1)

        fun intent(context: Context): Intent = Intent(context, AnalysisService::class.java)
    }
}
