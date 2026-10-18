package app.musicfetch.ui.listen

import app.musicfetch.core.Candidate
import app.musicfetch.core.CandidateSource
import app.musicfetch.core.CandidateSourceKind
import app.musicfetch.core.CandidateTier
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.Track
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.listen.Suggestion.HumAgain
import app.musicfetch.ui.listen.Suggestion.HumIt
import app.musicfetch.ui.listen.Suggestion.TryAgain
import app.musicfetch.ui.listen.Suggestion.TryGoogle
import app.musicfetch.ui.listen.Suggestion.TypeLyric
import app.musicfetch.ui.listen.Suggestion.UseMicrophone
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class HumRulesTest {
    @Test
    fun `while humming only Strong and Possible answers are named`() {
        val strong = candidate("Billie Jean", 0.82f, CandidateTier.STRONG)
        val possible = candidate("Africa", 0.5f, CandidateTier.POSSIBLE)
        val longShot = candidate("Believe", 0.04f, CandidateTier.LONG_SHOT)
        assertSame(strong, humPreview(listOf(strong, longShot)))
        assertSame(possible, humPreview(listOf(possible, longShot)))
        assertNull(humPreview(listOf(longShot)))
        // An empty list clears the preview.
        assertNull(humPreview(emptyList()))
        // Only the top answer is ever named: a runner-up never stands in for a long shot.
        assertNull(humPreview(listOf(longShot, possible)))
    }

    @Test
    fun `Google is offered unless the answer is Strong`() {
        assertFalse(offersGoogle(listOf(candidate("Billie Jean", 0.9f, CandidateTier.STRONG))))
        assertTrue(offersGoogle(listOf(candidate("Africa", 0.5f, CandidateTier.POSSIBLE))))
        assertTrue(offersGoogle(listOf(candidate("Believe", 0.04f, CandidateTier.LONG_SHOT))))
        assertTrue(offersGoogle(emptyList()))
    }

    @Test
    fun `a hum that found nothing offers Google second`() {
        assertEquals(listOf(HumAgain, TryGoogle, TypeLyric, UseMicrophone), suggestions(NoMatchReason.NOT_FOUND, ListenSource.Hum, google = true))
        assertEquals(listOf(HumAgain, TryGoogle, UseMicrophone), suggestions(NoMatchReason.NOISY, ListenSource.Hum, google = true))
        assertEquals(listOf(HumAgain, TryGoogle, TypeLyric), suggestions(NoMatchReason.TOO_SHORT, ListenSource.Hum, google = true))
        // Without the Google app nothing takes its place.
        assertEquals(listOf(HumAgain, TypeLyric, UseMicrophone), suggestions(NoMatchReason.NOT_FOUND, ListenSource.Hum, google = false))
        // A silent microphone is not something Google could fix.
        assertEquals(listOf(HumAgain), suggestions(NoMatchReason.CAPTURE_SILENT, ListenSource.Hum, google = true))
    }

    @Test
    fun `recorded music keeps its own suggestions`() {
        for (source in listOf(ListenSource.Microphone, ListenSource.Phone)) {
            for (reason in NoMatchReason.entries) {
                assertEquals(suggestions(reason, source), suggestions(reason, source, google = true))
            }
        }
        assertEquals(listOf(TryAgain, HumIt, TypeLyric), suggestions(NoMatchReason.NOT_FOUND, ListenSource.Microphone, google = true))
    }

    @Test
    fun `a microphone listen that turned into humming counts as a hum`() {
        assertEquals(ListenSource.Hum, sourceOf(CaptureSource.MICROPHONE, ListenMode.AUTO, impression = true))
        assertEquals(ListenSource.Microphone, sourceOf(CaptureSource.MICROPHONE, ListenMode.AUTO, impression = false))
        // Phone audio is always recorded music.
        assertEquals(ListenSource.Phone, sourceOf(CaptureSource.SYSTEM_AUDIO, ListenMode.RECORDING, impression = true))
    }

    @Test
    fun `no suggestion list repeats an action`() {
        for (reason in NoMatchReason.entries) {
            for (source in ListenSource.entries) {
                for (google in listOf(false, true)) {
                    val list = suggestions(reason, source, google)
                    assertEquals(list.distinct(), list)
                    assertTrue(list.isNotEmpty())
                }
            }
        }
    }

    private fun candidate(title: String, score: Float, tier: CandidateTier) = Candidate(
        track = Track(
            title = title,
            artist = "Artist",
            artists = listOf("Artist"),
            album = null,
            isrc = null,
            durationMs = null,
            releaseYear = null,
            genre = null,
            artworkUrl = null,
            previewUrl = null,
            links = emptyList(),
            ids = emptyMap(),
            identityKey = title.lowercase(),
        ),
        score = score,
        tier = tier,
        sources = listOf(CandidateSource(CandidateSourceKind.MELODY, score, null)),
        alignment = null,
    )
}
