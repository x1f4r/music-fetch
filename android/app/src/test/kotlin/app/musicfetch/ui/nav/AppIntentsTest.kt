package app.musicfetch.ui.nav

import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.AppIntents
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class AppIntentsTest {
    @Test
    fun `listen links name the source to start`() {
        assertEquals(ListenSource.Hum, AppIntents.listenSource(mode = "hum", source = null))
        assertEquals(ListenSource.Hum, AppIntents.listenSource(mode = null, source = "hum"))
        assertEquals(ListenSource.Microphone, AppIntents.listenSource(mode = null, source = "mic"))
        assertEquals(ListenSource.Phone, AppIntents.listenSource(mode = null, source = "phone"))
        assertNull(AppIntents.listenSource(mode = null, source = null))
        assertNull(AppIntents.listenSource(mode = "other", source = "radio"))
    }

    @Test
    fun `listen links round-trip`() {
        for (source in ListenSource.entries) {
            val query = AppIntents.listenUri(source).substringAfter("?source=")
            assertEquals(source, AppIntents.listenSource(mode = null, source = query))
        }
    }
}
