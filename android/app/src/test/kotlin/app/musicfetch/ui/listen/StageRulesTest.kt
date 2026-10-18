package app.musicfetch.ui.listen

import androidx.compose.ui.unit.dp
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.SessionKind
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.listen.Suggestion.HumAgain
import app.musicfetch.ui.listen.Suggestion.HumIt
import app.musicfetch.ui.listen.Suggestion.TryAgain
import app.musicfetch.ui.listen.Suggestion.TypeLyric
import app.musicfetch.ui.listen.Suggestion.UseMicrophone
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class StageRulesTest {
    @Test
    fun `listening offers humming next`() {
        for (source in listOf(ListenSource.Microphone, ListenSource.Phone)) {
            assertEquals(listOf(TryAgain, HumIt, TypeLyric), suggestions(NoMatchReason.NOT_FOUND, source))
            assertEquals(listOf(TryAgain, HumIt), suggestions(NoMatchReason.TOO_QUIET, source))
            assertEquals(listOf(TryAgain, HumIt), suggestions(NoMatchReason.NOISY, source))
            assertEquals(listOf(TryAgain, HumIt), suggestions(NoMatchReason.MOSTLY_SPEECH, source))
            // Too short still offers humming: it used to be hidden.
            assertEquals(listOf(TryAgain, HumIt), suggestions(NoMatchReason.TOO_SHORT, source))
        }
        assertEquals(listOf(UseMicrophone, HumIt), suggestions(NoMatchReason.CAPTURE_SILENT, ListenSource.Phone))
    }

    @Test
    fun `a hum is retried as a hum, with other ways after it`() {
        assertEquals(listOf(HumAgain, TypeLyric, UseMicrophone), suggestions(NoMatchReason.NOT_FOUND, ListenSource.Hum))
        assertEquals(listOf(HumAgain, UseMicrophone), suggestions(NoMatchReason.TOO_QUIET, ListenSource.Hum))
        assertEquals(listOf(HumAgain, TypeLyric), suggestions(NoMatchReason.TOO_SHORT, ListenSource.Hum))
        assertEquals(listOf(HumAgain), suggestions(NoMatchReason.CAPTURE_SILENT, ListenSource.Hum))
    }

    @Test
    fun `no two suggestions do the same thing`() {
        for (reason in NoMatchReason.entries) {
            for (source in ListenSource.entries) {
                val list = suggestions(reason, source)
                assertEquals(list.distinct(), list)
                assertTrue(list.size in 1..3)
                // "Try again" with a hum would be "Hum again", which it then says.
                if (source == ListenSource.Hum) assertTrue(TryAgain !in list && HumIt !in list)
            }
        }
    }

    @Test
    fun `sources of live sessions`() {
        assertEquals(ListenSource.Phone, sourceOf(CaptureSource.SYSTEM_AUDIO, ListenMode.RECORDING))
        assertEquals(ListenSource.Hum, sourceOf(CaptureSource.MICROPHONE, ListenMode.HUM))
        assertEquals(ListenSource.Microphone, sourceOf(CaptureSource.MICROPHONE, ListenMode.AUTO))
        assertEquals(ListenSource.Microphone, sourceOf(SessionKind.LISTEN))
        assertEquals(ListenSource.Phone, sourceOf(SessionKind.SYSTEM_AUDIO))
        assertEquals(ListenSource.Hum, sourceOf(SessionKind.HUM))
        assertEquals(null, sourceOf(SessionKind.LINK))
        assertEquals(null, sourceOf(SessionKind.FILE))
        assertEquals(null, sourceOf(SessionKind.LYRICS))
    }

    @Test
    fun `tall stages use the spec slots`() {
        val slots = stageSlots(600.dp)
        assertEquals(300.dp - 240.dp, slots.bandTop)
        assertEquals(96.dp, slots.bandHeight)
        assertEquals(260.dp, slots.buttonCenter)
        assertEquals(1f, slots.buttonScale)
        assertEquals(360.dp, slots.stateTop)
        assertEquals(424.dp, slots.actionTop)
    }

    @Test
    fun `short stages keep the trace band below the link field`() {
        // 360 × 640 dp with three-button navigation and the recent card: ~352 dp.
        for (height in listOf(352, 380, 432, 470, 495)) {
            val slots = stageSlots(height.dp)
            assertTrue("band top at $height", slots.bandTop >= 8.dp)
            assertTrue("band height at $height", slots.bandHeight >= 48.dp)
            // The band ends above the button, the button above the state line, the actions last.
            val buttonTop = slots.buttonCenter - 80.dp * slots.buttonScale
            assertTrue(slots.bandTop + slots.bandHeight <= buttonTop)
            assertTrue(slots.buttonCenter + 80.dp * slots.buttonScale <= slots.stateTop)
            assertTrue(slots.stateTop + 24.dp <= slots.actionTop)
            assertTrue(slots.actionTop + 40.dp <= height.dp)
            assertTrue(slots.buttonScale in 0.7f..1f)
        }
    }

    @Test
    fun `wide short stages put the button beside a column`() {
        // Phones in landscape beside the navigation rail, with the recent card shown.
        for ((width, height) in listOf(496 to 184, 600 to 240, 818 to 211)) {
            val slots = stageSlots(height.dp, width.dp)
            val side = slots.side
            assertNotNull("side by side at $width × $height", side)
            side!!
            val radius = 80.dp * slots.buttonScale
            assertTrue("button fits at $height", slots.buttonCenter - radius >= 8.dp && slots.buttonCenter + radius <= height.dp - 8.dp)
            assertTrue("button clear of the column at $width", side.buttonCenter + radius <= side.columnStart)
            assertTrue(side.columnStart + side.columnWidth <= width.dp)
            assertTrue(slots.bandTop >= 8.dp && slots.bandHeight >= 48.dp)
            assertTrue(slots.bandTop + slots.bandHeight <= slots.stateTop)
            assertTrue(slots.stateTop + 24.dp <= slots.actionTop)
            assertTrue(slots.actionTop + 40.dp <= height.dp)
        }
    }

    @Test
    fun `narrow and tall stages stay stacked`() {
        assertNull(stageSlots(352.dp, 360.dp).side)
        assertNull(stageSlots(600.dp, 1200.dp).side)
    }
}
