package app.musicfetch

import android.app.NotificationManager
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import androidx.test.platform.app.InstrumentationRegistry
import app.musicfetch.core.LibraryQuery
import app.musicfetch.notify.Notifications
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import org.junit.runner.RunWith

/** "Pin" on a result notification pins the session and turns into "Unpin". */
@LargeTest
@RunWith(AndroidJUnit4::class)
class NotificationPinTest {
    @Test
    fun pinFromTheResultNotification() = runBlocking {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val graph = context.appGraph
        val engine = graph.engineHost.engine()
        val entry = engine.library(LibraryQuery(emptyList(), null, false, 50u, 0u)).firstOrNull { it.track != null && !it.pinned }
        assumeTrue("needs an unpinned result in the library", entry != null)
        assumeTrue("needs the notification permission", Notifications.canPost(context))
        val track = entry!!.track!!
        val id = Notifications.analysisResultId(entry.id)
        val manager = context.getSystemService(NotificationManager::class.java)
        try {
            Notifications.postAnalysisResult(context, entry.id, track.title, null, track, graph.preference.filterNotNull().first())
            val posted = withTimeout(10_000) {
                var found = manager.activeNotifications.firstOrNull { it.id == id }
                while (found == null) {
                    delay(100)
                    found = manager.activeNotifications.firstOrNull { it.id == id }
                }
                found
            }
            val pin = posted.notification.actions.first { it.title.toString() == context.getString(R.string.session_pin) }
            pin.actionIntent.send()
            withTimeout(10_000) {
                while (engine.session(entry.id)?.entry?.pinned != true) delay(200)
            }
            withTimeout(10_000) {
                val unpin = context.getString(R.string.session_unpin)
                while (manager.activeNotifications.firstOrNull { it.id == id }?.notification?.actions?.any { it.title.toString() == unpin } != true) {
                    delay(200)
                }
            }
            assertTrue(engine.session(entry.id)?.entry?.pinned == true)
        } finally {
            engine.setPinned(entry.id, false)
            graph.notifyLibraryChanged()
            manager.cancel(id)
        }
    }
}
