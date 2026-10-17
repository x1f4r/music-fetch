package app.musicfetch.capture

import android.util.Log
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Debug and benchmark builds only: plays a WAV file into the live session in real time instead of the
 * microphone (then silence), so capture, notifications and tiles can be exercised on an
 * emulator without sound in the room. Enabled by placing [app.musicfetch.platform.DevFiles.CAPTURE_WAV].
 */
class WavReplay private constructor(
    private val file: File,
    override val sampleRate: Int,
    override val channels: Int,
    private val bits: Int,
    private val float: Boolean,
    private val dataOffset: Long,
    private val dataBytes: Long,
) : PcmSource {
    override val description: String = "replay of ${file.name}"
    private val running = AtomicBoolean(false)
    private var thread: Thread? = null

    override fun start(sink: (ByteBuffer, Int, Int) -> Unit, onError: (String) -> Unit) {
        if (!running.compareAndSet(false, true)) return
        thread = Thread({ loop(sink) }, "mf-replay").apply { start() }
    }

    override fun stop() {
        running.set(false)
        thread?.takeIf { it !== Thread.currentThread() }?.join(500)
    }

    private fun loop(sink: (ByteBuffer, Int, Int) -> Unit) {
        val frames = sampleRate / BLOCKS_PER_SECOND
        val inBytes = frames * channels * (bits / 8)
        val raw = ByteArray(inBytes)
        val out = ByteBuffer.allocateDirect(frames * channels * 4).order(ByteOrder.LITTLE_ENDIAN)
        val started = System.nanoTime()
        var block = 0L
        try {
            RandomAccessFile(file, "r").use { input ->
                input.seek(dataOffset)
                var remaining = dataBytes
                while (running.get()) {
                    out.clear()
                    val count = if (remaining > 0) input.read(raw, 0, minOf(inBytes.toLong(), remaining).toInt()) else -1
                    if (count > 0) {
                        remaining -= count
                        val samples = count / (bits / 8)
                        val src = ByteBuffer.wrap(raw, 0, count).order(ByteOrder.LITTLE_ENDIAN)
                        repeat(samples) {
                            out.putFloat(
                                when {
                                    float -> src.getFloat()
                                    bits == 16 -> src.getShort() / 32_768f
                                    else -> src.getInt() / 2_147_483_648f
                                },
                            )
                        }
                    } else {
                        // After the file: silence, as a quiet room would give.
                        repeat(frames * channels) { out.putFloat(0f) }
                    }
                    out.flip()
                    sink(out, sampleRate, channels)
                    block++
                    val due = started + block * 1_000_000_000L / BLOCKS_PER_SECOND
                    val wait = (due - System.nanoTime()) / 1_000_000
                    if (wait > 0) Thread.sleep(wait)
                }
            }
        } catch (e: IOException) {
            Log.w(TAG, "replay failed", e)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    companion object {
        private const val TAG = "WavReplay"
        private const val BLOCKS_PER_SECOND = 50

        /** A replay of [file] when it is a PCM (16/32-bit) or float WAV, else null. */
        fun open(file: File): WavReplay? = try {
            RandomAccessFile(file, "r").use { raf ->
                val header = ByteArray(12)
                raf.readFully(header)
                require(String(header, 0, 4, Charsets.US_ASCII) == "RIFF" && String(header, 8, 4, Charsets.US_ASCII) == "WAVE")
                var rate = 0
                var channels = 0
                var bits = 0
                var format = 0
                while (raf.filePointer + 8 <= raf.length()) {
                    val id = ByteArray(4).also { raf.readFully(it) }.toString(Charsets.US_ASCII)
                    val size = Integer.reverseBytes(raf.readInt()).toLong() and 0xFFFF_FFFFL
                    if (id == "fmt ") {
                        val fmt = ByteBuffer.wrap(ByteArray(size.toInt()).also { raf.readFully(it) }).order(ByteOrder.LITTLE_ENDIAN)
                        format = fmt.getShort(0).toInt()
                        channels = fmt.getShort(2).toInt()
                        rate = fmt.getInt(4)
                        bits = fmt.getShort(14).toInt()
                    } else if (id == "data") {
                        val float = format == 3 || (format == 0xFFFE.toShort().toInt() && bits == 32)
                        require(rate > 0 && channels > 0 && (bits == 16 || bits == 32))
                        return WavReplay(file, rate, channels, bits, float, raf.filePointer, minOf(size, raf.length() - raf.filePointer))
                    } else {
                        raf.seek(raf.filePointer + size + (size and 1))
                    }
                }
                null
            }
        } catch (e: IOException) {
            Log.w(TAG, "not a usable WAV", e)
            null
        } catch (e: IllegalArgumentException) {
            Log.w(TAG, "not a usable WAV", e)
            null
        }
    }
}
