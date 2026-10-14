package app.musicfetch.capture

import app.musicfetch.core.PitchFrame
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class PitchRingTest {
    private fun frames(fromMs: Long, toMs: Long, stepMs: Long = 50, midi: (Long) -> Float = { 60f }) =
        (fromMs until toMs step stepMs).map { PitchFrame(it.toULong(), midi(it), 0.9f) }

    @Test
    fun `voiced time counts only steps between voiced frames`() {
        val ring = PitchRing()
        ring.add(frames(0, 2_000))
        ring.add(frames(2_000, 3_000) { Float.NaN })
        ring.add(frames(3_000, 4_000))
        // 2 s voiced (minus the first step) + 1 s voiced (minus its first step).
        assertEquals(2.9f, ring.voicedSeconds(), 0.06f)
    }

    @Test
    fun `the window keeps the last eight seconds`() {
        val ring = PitchRing()
        ring.add(frames(0, 20_000))
        val snapshot = ring.snapshot()
        assertTrue(snapshot.times.first() >= 20_000 - 50 - PitchRing.WINDOW_MS)
        assertEquals(19_950L, snapshot.times.last())
    }

    @Test
    fun `clear starts over`() {
        val ring = PitchRing()
        ring.add(frames(0, 1_000))
        ring.clear()
        assertTrue(ring.snapshot().isEmpty)
        assertEquals(0f, ring.voicedSeconds(), 0f)
    }
}
