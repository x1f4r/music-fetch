package app.musicfetch.platform

import org.junit.Assert.assertEquals
import org.junit.Test
import kotlin.math.PI
import kotlin.math.abs
import kotlin.math.sin
import kotlin.math.sqrt

class StreamingResamplerTest {
    private fun sine(rate: Int, hz: Double, seconds: Double) =
        FloatArray((rate * seconds).toInt()) { i -> (0.5 * sin(2 * PI * hz * i / rate)).toFloat() }

    private fun rms(values: FloatArray, from: Int, to: Int): Double =
        sqrt((from until to).sumOf { values[it].toDouble() * values[it] } / (to - from))

    @Test
    fun `48 kHz to 16 kHz in chunks keeps length and level`() {
        val input = sine(48_000, 440.0, 2.0)
        val resampler = StreamingResampler(48_000, 16_000)
        val out = ArrayList<Float>()
        var offset = 0
        // Uneven chunk sizes, as MediaCodec delivers them.
        for (size in generateSequence(997) { (it * 7) % 4_001 + 113 }) {
            if (offset >= input.size) break
            val chunk = input.copyOfRange(offset, minOf(input.size, offset + size))
            out += resampler.process(chunk, chunk.size).toList()
            offset += size
        }
        out += resampler.flush().toList()
        assertEquals(32_000.0, out.size.toDouble(), 4.0)
        val result = out.toFloatArray()
        // A 0.5-amplitude sine has an RMS of ~0.354.
        assertEquals(0.354, rms(result, 1_000, 31_000), 0.01)
    }

    @Test
    fun `content above the new Nyquist frequency is removed`() {
        val input = sine(48_000, 12_000.0, 1.0)
        val resampler = StreamingResampler(48_000, 16_000)
        val out = resampler.process(input, input.size) + resampler.flush()
        val level = rms(out, 500, out.size - 500)
        assertEquals(0.0, level, 0.02)
    }

    @Test
    fun `equal rates pass through`() {
        val input = sine(16_000, 300.0, 0.1)
        val out = StreamingResampler(16_000, 16_000).process(input, input.size)
        assertEquals(input.size, out.size)
        assertEquals(0.0, input.indices.maxOf { abs(input[it] - out[it]).toDouble() }, 0.0)
    }
}
