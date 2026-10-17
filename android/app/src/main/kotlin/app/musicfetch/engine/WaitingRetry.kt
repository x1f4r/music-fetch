package app.musicfetch.engine

import android.app.Application
import android.content.Context
import android.util.Log
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.core.Candidate
import app.musicfetch.core.LibraryQuery
import app.musicfetch.core.MfException
import app.musicfetch.core.Outcome
import app.musicfetch.core.SessionEvent
import app.musicfetch.core.SessionObserver
import app.musicfetch.core.SessionStatus
import app.musicfetch.core.Track
import app.musicfetch.notify.Notifications
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.TimeUnit

/**
 * Identifies sessions captured while offline (status `Waiting`) once the phone is connected
 * again: a WorkManager job with a network constraint calls `Engine.retryWaiting`, so it also
 * runs when the app is no longer in memory. Results update the library and, while the app
 * is in the background, post the usual result notification.
 */
class WaitingRetry(
    private val app: Application,
    private val scope: CoroutineScope,
    private val engineHost: EngineHost,
) {
    private val mutex = Mutex()

    /** At process start: schedule a retry if an earlier run left sessions waiting. */
    fun start() {
        scope.launch(Dispatchers.IO) {
            if (hasWaiting()) schedule()
        }
    }

    /** A session was just saved for later. */
    fun onWaitingSaved() = schedule()

    /** Identify waiting sessions now. Returns true when none are left waiting. */
    suspend fun retryNow(): Boolean = mutex.withLock {
        val engine = try {
            engineHost.engine()
        } catch (e: EngineUnavailableException) {
            Log.w(TAG, "engine unavailable", e)
            return false
        }
        val observer = RetryObserver()
        val started = withContext(Dispatchers.IO) {
            try {
                engine.retryWaiting(observer).toInt()
            } catch (e: MfException) {
                Log.w(TAG, "retry failed", e)
                0
            }
        }
        if (started > 0) {
            Log.i(TAG, "identifying $started waiting session(s)")
            withTimeoutOrNull(RETRY_TIMEOUT_MS) { observer.finished.first { it >= started } }
        }
        app.appGraph.notifyLibraryChanged()
        !hasWaiting()
    }

    private fun schedule() {
        val request = OneTimeWorkRequestBuilder<RetryWaitingWorker>()
            .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, BACKOFF_SECONDS, TimeUnit.SECONDS)
            .build()
        // A job already waiting for the network covers this session too.
        WorkManager.getInstance(app).enqueueUniqueWork(WORK_NAME, ExistingWorkPolicy.KEEP, request)
    }

    private suspend fun hasWaiting(): Boolean {
        val engine = runCatching { engineHost.engine() }.getOrNull() ?: return false
        return withContext(Dispatchers.IO) {
            try {
                val query = LibraryQuery(kinds = emptyList(), search = null, pinnedOnly = false, limit = RECENT.toUInt(), offset = 0u)
                engine.library(query).any { it.status == SessionStatus.WAITING }
            } catch (e: MfException) {
                Log.w(TAG, "library unreadable", e)
                false
            }
        }
    }

    /**
     * Events of the retried sessions. Only `Started` names its session, so results are
     * attributed to the most recently started one (the engine retries one at a time).
     */
    private inner class RetryObserver : SessionObserver {
        val finished = MutableStateFlow(0)
        private var sessionId: String? = null
        private var candidates: List<Candidate> = emptyList()

        override fun onEvent(event: SessionEvent) {
            when (event) {
                is SessionEvent.Started -> synchronized(this) {
                    sessionId = event.sessionId
                    candidates = emptyList()
                }
                is SessionEvent.Candidates -> synchronized(this) { candidates = event.candidates }
                is SessionEvent.Finished -> {
                    val (id, best) = synchronized(this) { sessionId to candidates.firstOrNull()?.track }
                    when (val outcome = event.outcome) {
                        is Outcome.Matched -> notifyResult(id, outcome.track, best = false)
                        is Outcome.Candidates -> best?.let { notifyResult(id, it, best = true) }
                        else -> Unit
                    }
                    done()
                }
                is SessionEvent.Failed, is SessionEvent.Canceled -> done()
                else -> Unit
            }
        }

        private fun done() {
            finished.update { it + 1 }
            app.appGraph.notifyLibraryChanged()
        }
    }

    private fun notifyResult(sessionId: String?, track: Track, best: Boolean) {
        if (sessionId == null || app.appGraph.isAppVisible) return
        scope.launch {
            val settings = app.appGraph.settings
            if (!settings.current().notifyInBackground) return@launch
            val title = app.getString(if (best) R.string.notif_best_guess else R.string.notif_result_title, track.title, track.artist)
            Notifications.postAnalysisResult(app, sessionId, title, null, track.takeIf { !best }, settings.preference())
        }
    }

    companion object {
        private const val TAG = "WaitingRetry"
        private const val WORK_NAME = "retry-waiting-sessions"
        private const val BACKOFF_SECONDS = 30L
        private const val RETRY_TIMEOUT_MS = 5 * 60_000L
        private const val RECENT = 200

        /** Give up after this many attempts; the next waiting session or app start schedules again. */
        const val MAX_ATTEMPTS = 6
    }
}

/** Runs [WaitingRetry.retryNow] once the phone is connected. */
class RetryWaitingWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val done = applicationContext.appGraph.waitingRetry.retryNow()
        return when {
            done -> Result.success()
            runAttemptCount + 1 >= WaitingRetry.MAX_ATTEMPTS -> Result.success()
            else -> Result.retry()
        }
    }
}
