package app.musicfetch.platform

import android.media.AudioFormat
import android.media.MediaCodec
import android.media.MediaExtractor
import android.media.MediaFormat
import android.util.Log
import app.musicfetch.core.PlatformDecoder
import app.musicfetch.core.PlatformException
import app.musicfetch.core.TaskHandle
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.cos
import kotlin.math.roundToInt
import kotlin.math.sin

/**
 * [PlatformDecoder] on MediaExtractor + MediaCodec: the fallback for files the engine's own
 * decoder cannot read (HE-AAC, AMR, 3GP and other containers phones produce).
 *
 * Decodes the first audio track, downmixes to mono, resamples to the requested rate and writes
 * a 16-bit PCM WAV. Runs on the calling engine thread and checks for cancellation per buffer.
 */
class MediaCodecDecoder : PlatformDecoder {

    override fun decodeToWav(inputPath: String, outputPath: String, sampleRate: UInt, task: TaskHandle) {
        val target = sampleRate.toInt()
        if (target !in 8_000..192_000) throw PlatformException.Failed("Unsupported sample rate.")
        val output = File(outputPath)
        try {
            decode(File(inputPath), output, target, task)
        } catch (e: PlatformException) {
            output.delete()
            throw e
        } catch (e: IOException) {
            output.delete()
            Log.w(TAG, "decode failed", e)
            throw PlatformException.Failed("Couldn't read this file.")
        } catch (e: IllegalStateException) {
            // MediaCodec reports codec errors as IllegalStateException / CodecException.
            output.delete()
            Log.w(TAG, "codec failed", e)
            throw PlatformException.Failed("Couldn't decode this file.")
        } catch (e: IllegalArgumentException) {
            output.delete()
            Log.w(TAG, "unsupported input", e)
            throw PlatformException.Unavailable("This file has no audio Android can decode.")
        }
    }

    private fun decode(input: File, output: File, targetRate: Int, task: TaskHandle) {
        val extractor = MediaExtractor()
        var codec: MediaCodec? = null
        try {
            extractor.setDataSource(input.path)
            val track = (0 until extractor.trackCount).firstOrNull { index ->
                extractor.getTrackFormat(index).getString(MediaFormat.KEY_MIME)?.startsWith("audio/") == true
            } ?: throw PlatformException.Unavailable("This file has no audio track.")
            extractor.selectTrack(track)
            val format = extractor.getTrackFormat(track)
            val mime = format.getString(MediaFormat.KEY_MIME)!!
            val durationUs = if (format.containsKey(MediaFormat.KEY_DURATION)) format.getLong(MediaFormat.KEY_DURATION) else 0L

            codec = MediaCodec.createDecoderByType(mime)
            codec.configure(format, null, null, 0)
            codec.start()

            WavWriter(output, targetRate).use { wav ->
                var resampler: StreamingResampler? = null
                var channels = format.getIntegerOrNull(MediaFormat.KEY_CHANNEL_COUNT) ?: 1
                var encoding = AudioFormat.ENCODING_PCM_16BIT
                var inputDone = false
                val info = MediaCodec.BufferInfo()
                var mono = FloatArray(0)
                var lastReport = -1f

                while (true) {
                    if (task.isCanceled()) throw PlatformException.Canceled()
                    if (!inputDone) {
                        val inIndex = codec.dequeueInputBuffer(TIMEOUT_US)
                        if (inIndex >= 0) {
                            val buffer = codec.getInputBuffer(inIndex)!!
                            val size = extractor.readSampleData(buffer, 0)
                            if (size < 0) {
                                codec.queueInputBuffer(inIndex, 0, 0, 0, MediaCodec.BUFFER_FLAG_END_OF_STREAM)
                                inputDone = true
                            } else {
                                val time = extractor.sampleTime
                                codec.queueInputBuffer(inIndex, 0, size, time, 0)
                                extractor.advance()
                                if (durationUs > 0) {
                                    val fraction = (time.toFloat() / durationUs).coerceIn(0f, 1f)
                                    if (fraction - lastReport >= 0.01f) {
                                        lastReport = fraction
                                        task.reportProgress(fraction)
                                    }
                                }
                            }
                        }
                    }
                    val outIndex = codec.dequeueOutputBuffer(info, TIMEOUT_US)
                    when {
                        outIndex == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
                            val out = codec.outputFormat
                            channels = out.getIntegerOrNull(MediaFormat.KEY_CHANNEL_COUNT) ?: channels
                            encoding = out.getIntegerOrNull(MediaFormat.KEY_PCM_ENCODING) ?: AudioFormat.ENCODING_PCM_16BIT
                            val rate = out.getIntegerOrNull(MediaFormat.KEY_SAMPLE_RATE)
                            if (rate != null && rate > 0) resampler = StreamingResampler(rate, targetRate)
                        }
                        outIndex >= 0 -> {
                            if (info.size > 0) {
                                val rs = resampler ?: StreamingResampler(
                                    format.getIntegerOrNull(MediaFormat.KEY_SAMPLE_RATE) ?: targetRate,
                                    targetRate,
                                ).also { resampler = it }
                                val buffer = codec.getOutputBuffer(outIndex)!!
                                buffer.position(info.offset)
                                buffer.limit(info.offset + info.size)
                                mono = downmix(buffer.slice().order(ByteOrder.nativeOrder()), channels, encoding, mono)
                                val frames = info.size / (channels * bytesPerSample(encoding))
                                wav.write(rs.process(mono, frames))
                            }
                            codec.releaseOutputBuffer(outIndex, false)
                            if (info.flags and MediaCodec.BUFFER_FLAG_END_OF_STREAM != 0) {
                                resampler?.let { wav.write(it.flush()) }
                                break
                            }
                        }
                    }
                }
                if (wav.samples == 0L) throw PlatformException.Failed("No audio could be decoded.")
            }
            task.reportProgress(1f)
        } finally {
            runCatching { codec?.stop() }
            runCatching { codec?.release() }
            extractor.release()
        }
    }

    private companion object {
        const val TAG = "MediaCodecDecoder"
        const val TIMEOUT_US = 10_000L
    }
}

private fun MediaFormat.getIntegerOrNull(key: String): Int? = if (containsKey(key)) getInteger(key) else null

private fun bytesPerSample(encoding: Int): Int = when (encoding) {
    AudioFormat.ENCODING_PCM_FLOAT -> 4
    AudioFormat.ENCODING_PCM_8BIT -> 1
    AudioFormat.ENCODING_PCM_24BIT_PACKED -> 3
    AudioFormat.ENCODING_PCM_32BIT -> 4
    else -> 2
}

/** Interleaved PCM in [buffer] → mono floats in -1..1, reusing [scratch] when it is large enough. */
internal fun downmix(buffer: ByteBuffer, channels: Int, encoding: Int, scratch: FloatArray): FloatArray {
    val width = bytesPerSample(encoding)
    val ch = channels.coerceAtLeast(1)
    val frames = buffer.remaining() / (width * ch)
    val out = if (scratch.size >= frames) scratch else FloatArray(frames)
    for (frame in 0 until frames) {
        var sum = 0f
        for (c in 0 until ch) {
            sum += when (encoding) {
                AudioFormat.ENCODING_PCM_FLOAT -> buffer.getFloat()
                AudioFormat.ENCODING_PCM_8BIT -> ((buffer.get().toInt() and 0xFF) - 128) / 128f
                AudioFormat.ENCODING_PCM_24BIT_PACKED -> {
                    val b0 = buffer.get().toInt() and 0xFF
                    val b1 = buffer.get().toInt() and 0xFF
                    val b2 = buffer.get().toInt()
                    ((b2 shl 16) or (b1 shl 8) or b0) / 8_388_608f
                }
                AudioFormat.ENCODING_PCM_32BIT -> buffer.getInt() / 2_147_483_648f
                else -> buffer.getShort() / 32_768f
            }
        }
        out[frame] = sum / ch
    }
    return out
}

/**
 * Band-limited resampler for one mono stream fed in chunks: polyphase windowed sinc (Blackman,
 * 12 zero crossings per side at the lower rate, 256 phases). Pass-through when the rates match.
 */
internal class StreamingResampler(private val inRate: Int, private val outRate: Int) {
    private val step = inRate.toDouble() / outRate
    private val cutoff = minOf(1.0, outRate.toDouble() / inRate) * 0.94
    private val halfWidth = (ZERO_CROSSINGS / cutoff).toInt() + 1
    private val taps = 2 * halfWidth
    // kernel[phase * taps + k] weights input sample (center - halfWidth + 1 + k) for an output
    // at center + phase / PHASES; each phase is normalized to unity gain.
    private val kernel = FloatArray((PHASES + 1) * taps).also { table ->
        for (phase in 0..PHASES) {
            val frac = phase.toDouble() / PHASES
            var sum = 0.0
            val weights = DoubleArray(taps) { k ->
                val distance = frac + (halfWidth - 1 - k)
                (sinc(distance * cutoff) * blackman(distance / halfWidth)).also { sum += it }
            }
            for (k in 0 until taps) table[phase * taps + k] = (weights[k] / sum).toFloat()
        }
    }
    private var history = FloatArray(0)
    // Position of the next output sample, relative to the start of `history`.
    private var position = 0.0

    /** Resample the first [count] samples of [input]; returns the produced output samples. */
    fun process(input: FloatArray, count: Int): FloatArray {
        if (inRate == outRate) return input.copyOf(count)
        val buffer = FloatArray(history.size + count)
        history.copyInto(buffer)
        input.copyInto(buffer, history.size, 0, count)
        val out = FloatArray(((buffer.size - position) / step).toInt().coerceAtLeast(0) + 2)
        var produced = 0
        // Produce samples whose window lies fully inside the buffer.
        while (position.toInt() + halfWidth + 1 < buffer.size && produced < out.size) {
            out[produced++] = sample(buffer, position)
            position += step
        }
        val keepFrom = (position.toInt() - halfWidth + 1).coerceIn(0, buffer.size)
        history = buffer.copyOfRange(keepFrom, buffer.size)
        position -= keepFrom
        return out.copyOf(produced)
    }

    /** Output for the remaining buffered input (zero-padded on the right). */
    fun flush(): FloatArray {
        if (inRate == outRate || history.isEmpty()) return FloatArray(0)
        val end = history.size
        val padded = history.copyOf(history.size + taps + 1)
        val out = FloatArray(((end - position) / step).toInt().coerceAtLeast(0) + 1)
        var produced = 0
        while (position < end && produced < out.size) {
            out[produced++] = sample(padded, position)
            position += step
        }
        history = FloatArray(0)
        position = 0.0
        return out.copyOf(produced)
    }

    private fun sample(buffer: FloatArray, at: Double): Float {
        var center = at.toInt()
        var phase = ((at - center) * PHASES).roundToInt()
        if (phase == PHASES) {
            center += 1
            phase = 0
        }
        val first = center - halfWidth + 1
        val base = phase * taps
        var acc = 0f
        for (k in 0 until taps) {
            val i = first + k
            if (i >= 0 && i < buffer.size) acc += buffer[i] * kernel[base + k]
        }
        return acc
    }

    private companion object {
        const val ZERO_CROSSINGS = 12.0
        const val PHASES = 256

        fun sinc(x: Double): Double = if (x == 0.0) 1.0 else sin(Math.PI * x) / (Math.PI * x)

        /** Blackman window over -1..1. */
        fun blackman(t: Double): Double {
            if (t <= -1.0 || t >= 1.0) return 0.0
            val u = (t + 1.0) / 2.0
            return 0.42 - 0.5 * cos(2 * Math.PI * u) + 0.08 * cos(4 * Math.PI * u)
        }
    }
}

/** Streams mono 16-bit PCM into a WAV file; the header is completed on [close]. */
internal class WavWriter(private val file: File, private val sampleRate: Int) : AutoCloseable {
    private val out = file.outputStream().buffered(64 * 1024)
    private val bytes = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN)

    /** Samples written so far. */
    var samples = 0L
        private set

    init {
        out.write(ByteArray(HEADER_SIZE))
    }

    fun write(mono: FloatArray) {
        for (value in mono) {
            if (bytes.remaining() < 2) flushBytes()
            val clamped = value.coerceIn(-1f, 1f)
            bytes.putShort((clamped * 32_767f).toInt().toShort())
        }
        samples += mono.size
    }

    override fun close() {
        flushBytes()
        out.close()
        writeHeader()
    }

    private fun flushBytes() {
        out.write(bytes.array(), 0, bytes.position())
        bytes.clear()
    }

    private fun writeHeader() {
        val dataBytes = samples * 2
        val header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN).apply {
            put("RIFF".toByteArray(Charsets.US_ASCII))
            putInt((36 + dataBytes).coerceAtMost(UInt.MAX_VALUE.toLong()).toInt())
            put("WAVE".toByteArray(Charsets.US_ASCII))
            put("fmt ".toByteArray(Charsets.US_ASCII))
            putInt(16)
            putShort(1) // PCM
            putShort(1) // mono
            putInt(sampleRate)
            putInt(sampleRate * 2)
            putShort(2)
            putShort(16)
            put("data".toByteArray(Charsets.US_ASCII))
            putInt(dataBytes.coerceAtMost(UInt.MAX_VALUE.toLong()).toInt())
        }
        java.io.RandomAccessFile(file, "rw").use { raf ->
            raf.seek(0)
            raf.write(header.array())
        }
    }

    private companion object {
        const val HEADER_SIZE = 44
    }
}
