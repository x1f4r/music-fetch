package app.musicfetch.notify

import android.Manifest
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.net.Uri
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import app.musicfetch.MainActivity
import app.musicfetch.R
import app.musicfetch.capture.CaptureService
import app.musicfetch.capture.ListenOrigin
import app.musicfetch.capture.ListenPhase
import app.musicfetch.capture.ListenResult
import app.musicfetch.capture.ListenState
import app.musicfetch.capture.TileTrampolineActivity
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ErrorCode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.Notice
import app.musicfetch.core.Service
import app.musicfetch.core.Track
import app.musicfetch.platform.ServiceLinkOpener
import app.musicfetch.platform.displayName
import app.musicfetch.ui.common.ArtworkLoader

/** Notification channels and builders (docs/design/DESIGN.md §6.11). */
object Notifications {
    const val CHANNEL_LISTENING = "listening"
    const val CHANNEL_IDENTIFYING = "identifying"
    const val CHANNEL_RESULTS = "results"

    const val LISTENING_ID = 1
    const val LISTEN_RESULT_ID = 2
    const val ANALYSIS_ID = 3
    private const val ANALYSIS_RESULT_BASE = 1000

    fun createChannels(context: Context) {
        val manager = context.getSystemService(NotificationManager::class.java) ?: return
        val listening = NotificationChannel(
            CHANNEL_LISTENING,
            context.getString(R.string.channel_listening),
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            description = context.getString(R.string.channel_listening_description)
            setShowBadge(false)
            setSound(null, null)
        }
        val identifying = NotificationChannel(
            CHANNEL_IDENTIFYING,
            context.getString(R.string.channel_identifying),
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            description = context.getString(R.string.channel_identifying_description)
            setShowBadge(false)
            setSound(null, null)
        }
        val results = NotificationChannel(
            CHANNEL_RESULTS,
            context.getString(R.string.channel_results),
            NotificationManager.IMPORTANCE_DEFAULT,
        ).apply {
            description = context.getString(R.string.channel_results_description)
        }
        manager.createNotificationChannels(listOf(listening, identifying, results))
    }

    /** Whether notifications may be posted (always true before Android 13). */
    fun canPost(context: Context): Boolean =
        (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU ||
            ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED) &&
            NotificationManagerCompat.from(context).areNotificationsEnabled()

    /** The ongoing capture notification: what is captured, elapsed time, Stop. */
    fun listening(context: Context, state: ListenState): Notification {
        val phone = state.source == CaptureSource.SYSTEM_AUDIO
        val title = when (state.phase) {
            ListenPhase.Matching -> context.getString(R.string.notif_matching)
            else -> context.getString(if (phone) R.string.notif_listening_phone else R.string.notif_listening_mic)
        }
        val text = when {
            state.phase == ListenPhase.Matching -> null
            state.notice is Notice.CaptureSilent -> context.getString(R.string.state_capture_silent)
            state.notice is Notice.MoveCloser -> context.getString(R.string.state_move_closer)
            state.impression -> context.getString(R.string.state_keep_going)
            phone -> context.getString(R.string.notif_hint_phone)
            else -> context.getString(R.string.notif_hint_mic)
        }
        val builder = NotificationCompat.Builder(context, CHANNEL_LISTENING)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(title)
            .setContentText(text)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setSilent(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setForegroundServiceBehavior(NotificationCompat.FOREGROUND_SERVICE_IMMEDIATE)
            .setContentIntent(openApp(context, null))
        if (state.startedAtWallMs > 0 && state.phase != ListenPhase.Matching) {
            builder.setWhen(state.startedAtWallMs).setUsesChronometer(true).setShowWhen(true)
        } else {
            builder.setShowWhen(false)
        }
        if (state.isCapturing) {
            builder.addAction(
                R.drawable.ic_stop,
                context.getString(R.string.action_stop),
                PendingIntent.getService(
                    context,
                    1,
                    CaptureService.intent(context, CaptureService.ACTION_STOP),
                    PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
                ),
            )
        }
        return builder.build()
    }

    /** Refresh the ongoing capture notification while a session runs. */
    fun updateListening(context: Context, state: ListenState) {
        if (!state.isActive || state.phase == ListenPhase.Starting || !canPost(context)) return
        notify(context, LISTENING_ID, listening(context, state))
    }

    /**
     * The result of a live session, replacing the capture notification: the track with "Open in
     * ‹service›" and "Open", the best hum guess, or why nothing was found with "Try again".
     */
    suspend fun postListenResult(context: Context, state: ListenState, result: ListenResult, preferred: Service) {
        if (!canPost(context)) return
        val builder = NotificationCompat.Builder(context, CHANNEL_RESULTS)
            .setSmallIcon(R.drawable.ic_notification)
            .setAutoCancel(true)
            .setCategory(NotificationCompat.CATEGORY_STATUS)
        when (result) {
            is ListenResult.Matched -> {
                val track = result.track
                builder.setContentTitle(context.getString(R.string.notif_result_title, track.title, track.artist))
                    .setContentText(meta(track))
                    .setContentIntent(openApp(context, result.sessionId))
                artwork(context, track)?.let { builder.setLargeIcon(it) }
                addOpenIn(context, builder, track, preferred)
                builder.addAction(0, context.getString(R.string.notif_open), openApp(context, result.sessionId))
            }
            is ListenResult.Candidates -> {
                val best = result.candidates.firstOrNull()?.track
                if (best != null) {
                    builder.setContentTitle(context.getString(R.string.notif_best_guess, best.title, best.artist))
                    artwork(context, best)?.let { builder.setLargeIcon(it) }
                } else {
                    builder.setContentTitle(context.getString(R.string.state_no_match))
                }
                builder.setContentIntent(openApp(context, result.sessionId))
                    .addAction(0, context.getString(R.string.notif_see_all), openApp(context, result.sessionId))
            }
            is ListenResult.NoMatch -> {
                builder.setContentTitle(context.getString(noMatchText(result.reason)))
                    .setContentIntent(openApp(context, null))
                addTryAgain(context, builder, state)
            }
            is ListenResult.Failed -> {
                builder.setContentTitle(context.getString(R.string.state_no_match))
                    .setContentText(context.getString(errorText(result.code)))
                    .setContentIntent(openApp(context, null))
                addTryAgain(context, builder, state)
            }
        }
        notify(context, LISTEN_RESULT_ID, builder.build())
    }

    /** A link or file analysis finished while the app was in the background. */
    suspend fun postAnalysisResult(
        context: Context,
        sessionId: String,
        title: String,
        text: String?,
        track: Track?,
        preferred: Service,
    ) {
        if (!canPost(context)) return
        val builder = NotificationCompat.Builder(context, CHANNEL_RESULTS)
            .setSmallIcon(R.drawable.ic_notification)
            .setAutoCancel(true)
            .setContentTitle(title)
            .setContentText(text)
            .setContentIntent(openApp(context, sessionId))
        if (track != null) {
            artwork(context, track)?.let { builder.setLargeIcon(it) }
            addOpenIn(context, builder, track, preferred)
            builder.addAction(0, context.getString(R.string.notif_open), openApp(context, sessionId))
        }
        notify(context, analysisResultId(sessionId), builder.build())
    }

    /** The user opened [sessionId] (e.g. from an action button): its result notification is done. */
    fun dismissResult(context: Context, sessionId: String) {
        val manager = NotificationManagerCompat.from(context)
        manager.cancel(LISTEN_RESULT_ID)
        manager.cancel(analysisResultId(sessionId))
    }

    private fun analysisResultId(sessionId: String): Int = ANALYSIS_RESULT_BASE + (sessionId.hashCode() and 0xFFFF)

    fun noMatchText(reason: NoMatchReason): Int = when (reason) {
        NoMatchReason.TOO_QUIET -> R.string.state_too_quiet
        NoMatchReason.TOO_SHORT -> R.string.state_too_short
        NoMatchReason.NOISY -> R.string.state_noisy
        NoMatchReason.MOSTLY_SPEECH -> R.string.state_mostly_speech
        NoMatchReason.CAPTURE_SILENT -> R.string.state_capture_silent
        NoMatchReason.NOT_FOUND -> R.string.state_no_match
    }

    fun errorText(code: ErrorCode): Int = when (code) {
        ErrorCode.UNSUPPORTED_LINK -> R.string.error_unsupported_link
        ErrorCode.MEDIA_UNAVAILABLE -> R.string.error_media_unavailable
        ErrorCode.NETWORK -> R.string.error_network
        ErrorCode.RATE_LIMITED -> R.string.error_rate_limited
        ErrorCode.DECODE -> R.string.error_decode
        ErrorCode.INDEX_UNAVAILABLE -> R.string.error_index_unavailable
        ErrorCode.INVALID_INPUT, ErrorCode.PLATFORM, ErrorCode.STORAGE, ErrorCode.INTERNAL -> R.string.error_generic
    }

    /** Opens the app at a session (`musicfetch://session/<id>`), or at the Listen stage. */
    fun openApp(context: Context, sessionId: String?): PendingIntent {
        val intent = if (sessionId != null) {
            Intent(Intent.ACTION_VIEW, Uri.parse("musicfetch://session/" + Uri.encode(sessionId)))
                .setClass(context, MainActivity::class.java)
        } else {
            Intent(context, MainActivity::class.java)
        }.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        return PendingIntent.getActivity(
            context,
            sessionId?.hashCode() ?: 0,
            intent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
    }

    private fun addOpenIn(context: Context, builder: NotificationCompat.Builder, track: Track, preferred: Service) {
        val link = ServiceLinkOpener.pick(track.links, preferred) ?: return
        val intent = ServiceLinkOpener.intentFor(context, link)
        val pending = PendingIntent.getActivity(
            context,
            (track.identityKey + link.url).hashCode(),
            intent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        builder.addAction(0, context.getString(R.string.result_open_in, link.service.displayName), pending)
    }

    private fun addTryAgain(context: Context, builder: NotificationCompat.Builder, state: ListenState) {
        val intent = TileTrampolineActivity.intent(
            context,
            state.source,
            if (state.origin == ListenOrigin.App) ListenOrigin.Tile else state.origin,
        )
        val pending = PendingIntent.getActivity(
            context,
            10 + state.source.ordinal,
            intent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        builder.addAction(0, context.getString(R.string.action_try_again), pending)
    }

    private fun meta(track: Track): String? =
        listOfNotNull(track.album?.takeIf { it.isNotBlank() }, track.releaseYear?.toString()).joinToString(" · ").ifEmpty { null }

    private suspend fun artwork(context: Context, track: Track): Bitmap? =
        track.artworkUrl?.let { ArtworkLoader.bitmap(context, it, sizePx = 256) }

    private fun notify(context: Context, id: Int, notification: Notification) {
        if (!canPost(context)) return
        try {
            NotificationManagerCompat.from(context).notify(id, notification)
        } catch (_: SecurityException) {
            // The permission was revoked between the check and the call.
        }
    }
}
