package app.musicfetch.capture

import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.IBinder
import android.util.Log
import androidx.core.app.ServiceCompat
import app.musicfetch.appGraph
import app.musicfetch.core.CaptureSource
import app.musicfetch.notify.Notifications

/**
 * Foreground service that keeps microphone or phone-audio capture alive while the user is in
 * another app or the screen is off. It only hosts the ongoing notification;
 * [ListenController] owns the session and the recorder.
 */
class CaptureService : Service() {
    private val controller: ListenController get() = appGraph.listen
    private var released = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_START -> enterForeground()
            ACTION_STOP -> controller.stop()
            ACTION_CANCEL -> controller.cancel()
            else -> {
                // Restarted by the system without a session: nothing to capture.
                stopSelf()
            }
        }
        return START_NOT_STICKY
    }

    private fun enterForeground() {
        val state = controller.state.value
        val type = if (state.source == CaptureSource.SYSTEM_AUDIO) {
            ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROJECTION
        } else {
            ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE
        }
        try {
            ServiceCompat.startForeground(this, Notifications.LISTENING_ID, Notifications.listening(this, state), type)
        } catch (e: RuntimeException) {
            // Not allowed in the background, or the consent/permission is missing.
            Log.w(TAG, "could not enter the foreground", e)
            controller.cancel()
            stopSelf()
            return
        }
        released = false
        controller.onServiceForeground(this)
    }

    /** Leave the foreground and stop; the result notification (if any) is posted separately. */
    fun release() {
        if (released) return
        released = true
        ServiceCompat.stopForeground(this, ServiceCompat.STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    override fun onDestroy() {
        controller.onServiceStopped(this)
        super.onDestroy()
    }

    companion object {
        private const val TAG = "CaptureService"
        const val ACTION_START = "app.musicfetch.capture.START"
        const val ACTION_STOP = "app.musicfetch.capture.STOP"
        const val ACTION_CANCEL = "app.musicfetch.capture.CANCEL"

        fun intent(context: Context, action: String): Intent =
            Intent(context, CaptureService::class.java).setAction(action)
    }
}
