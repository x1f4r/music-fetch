package app.musicfetch.platform

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class SungLyricsTest {
    @Test
    fun `segments are joined best first, with the current partial at the end`() {
        val t = SungLyrics.joinSegments(
            segments = listOf(listOf("never gonna give you up"), listOf("never gonna let you down")),
            partial = "never gonna run",
            language = "en-US",
            confidence = 0.8f,
            isFinal = false,
        )
        assertEquals("never gonna give you up never gonna let you down never gonna run", t.text)
        assertFalse(t.isFinal)
        assertEquals("en-US", t.language)
        // A confidence belongs to one segment, not to the joined text.
        assertNull(t.confidence)
    }

    @Test
    fun `alternatives swap one segment's next hypothesis`() {
        val t = SungLyrics.joinSegments(
            segments = listOf(listOf("hold me closer tiny dancer", "hold me closer Tony Danza"), listOf("count the headlights")),
            partial = "",
            language = "en-US",
            confidence = null,
            isFinal = true,
        )
        assertEquals("hold me closer tiny dancer count the headlights", t.text)
        assertEquals(listOf("hold me closer Tony Danza count the headlights"), t.alternatives)
        assertTrue(t.isFinal)
    }

    @Test
    fun `at most four alternatives, never the text itself`() {
        val segment = listOf("a", "b", "c", "d", "e")
        val t = SungLyrics.joinSegments(listOf(segment, segment), "", "de-DE", null, true)
        assertEquals("a a", t.text)
        assertEquals(4, t.alternatives.size)
        assertFalse("a a" in t.alternatives)
    }

    @Test
    fun `one segment keeps its confidence`() {
        val t = SungLyrics.joinSegments(listOf(listOf("is this the real life")), "", "en-GB", 0.62f, true)
        assertEquals(0.62f, t.confidence!!, 0f)
        // Recognizers report 0 or -1 when they have no confidence.
        assertNull(SungLyrics.joinSegments(listOf(listOf("x")), "", "en-GB", 0f, true).confidence)
    }

    @Test
    fun `the phone's own speech pack first, then one of its language`() {
        val supported = listOf("en-US", "de-DE", "es-ES", "fr-FR")
        assertEquals(SpeechPack("en-US", SpeechPack.State.Installed), SungLyrics.speechPack("en-US", listOf("en-US"), emptyList(), supported))
        // Austrian German reads with the German pack.
        assertEquals(SpeechPack("de-DE", SpeechPack.State.Installed), SungLyrics.speechPack("de-AT", listOf("en-US", "de-DE"), emptyList(), supported))
        assertEquals(SpeechPack("en-US", SpeechPack.State.Downloading), SungLyrics.speechPack("en-US", emptyList(), listOf("en-US"), supported))
        assertEquals(SpeechPack("en-US", SpeechPack.State.Available), SungLyrics.speechPack("en-US", emptyList(), emptyList(), supported))
        assertEquals(SpeechPack.State.Unsupported, SungLyrics.speechPack("fi-FI", listOf("en-US"), emptyList(), supported).state)
        // Only an installed pack is handed to the recognizer.
        assertEquals("de-DE", SpeechPack("de-DE", SpeechPack.State.Installed).recognizerLanguage)
        assertNull(SpeechPack("de-DE", SpeechPack.State.Available).recognizerLanguage)
    }

    @Test
    fun `nothing heard is an empty transcript`() {
        val t = SungLyrics.joinSegments(emptyList(), "", "en-US", null, true)
        assertEquals("", t.text)
        assertTrue(t.alternatives.isEmpty())
    }
}
