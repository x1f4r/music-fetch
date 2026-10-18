package app.musicfetch.notify

import android.app.Notification
import android.app.NotificationManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.core.MfException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * "Pin" / "Unpin" on a result notification: pins the session and swaps the action, leaving the
 * rest of the notification as it was. Not exported; only the app's own PendingIntents reach it.
 */
class PinReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != ACTION_PIN) return
        val sessionId = intent.getStringExtra(EXTRA_SESSION) ?: return
        val tag = intent.getStringExtra(EXTRA_TAG)
        val notificationId = intent.getIntExtra(EXTRA_NOTIFICATION, 0)
        val pin = intent.getBooleanExtra(EXTRA_PIN, true)
        val graph = context.appGraph
        val pending = goAsync()
        graph.scope.launch {
            try {
                val ok = try {
                    val engine = graph.engineHost.engine()
                    withContext(Dispatchers.IO) { engine.setPinned(sessionId, pin) }
                    true
                } catch (e: MfException) {
                    Log.w(TAG, "pin failed", e)
                    false
                } catch (e: Exception) {
                    Log.w(TAG, "engine unavailable", e)
                    false
                }
                if (ok) {
                    graph.notifyLibraryChanged()
                    swapAction(context, sessionId, tag, notificationId, pinned = pin)
                }
            } finally {
                pending.finish()
            }
        }
    }

    /** Re-post the notification with the opposite action, silently. */
    private fun swapAction(context: Context, sessionId: String, tag: String?, id: Int, pinned: Boolean) {
        val manager = context.getSystemService(NotificationManager::class.java) ?: return
        val shown = runCatching { manager.activeNotifications }.getOrNull()
            ?.firstOrNull { it.id == id && it.tag == tag }?.notification ?: return
        val labels = setOf(context.getString(R.string.session_pin), context.getString(R.string.session_unpin))
        val replacement = Notifications.pinAction(context, sessionId, tag, id, pinned)
        val actions = shown.actions.orEmpty().map { action ->
            if (action.title?.toString() in labels) {
                Notification.Action.Builder(null, replacement.title, replacement.actionIntent).build()
            } else {
                action
            }
        }
        val rebuilt = Notification.Builder.recoverBuilder(context, shown)
            .setActions(*actions.toTypedArray())
            .setOnlyAlertOnce(true)
            .build()
        Notifications.notify(context, tag, id, rebuilt)
    }

    companion object {
        private const val TAG = "PinReceiver"
        private const val ACTION_PIN = "app.musicfetch.action.PIN"
        private const val EXTRA_SESSION = "session"
        private const val EXTRA_TAG = "tag"
        private const val EXTRA_NOTIFICATION = "notification"
        private const val EXTRA_PIN = "pin"

        fun intent(context: Context, sessionId: String, tag: String?, notificationId: Int, pin: Boolean): Intent =
            Intent(context, PinReceiver::class.java)
                .setAction(ACTION_PIN)
                .putExtra(EXTRA_SESSION, sessionId)
                .putExtra(EXTRA_TAG, tag)
                .putExtra(EXTRA_NOTIFICATION, notificationId)
                .putExtra(EXTRA_PIN, pin)
    }
}
