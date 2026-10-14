package app.musicfetch.capture

import android.Manifest
import android.content.Context
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioPlaybackCaptureConfiguration
import android.media.AudioRecord
import android.media.MediaRecorder
import android.media.projection.MediaProjection
import android.os.Process
import android.util.Log
import androidx.annotation.RequiresPermission
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean

/** Live float PCM for a session: the microphone, phone audio or (debug) a replayed file. */
interface PcmSource {
    val sampleRate: Int
    val channels: Int

    /** Human-readable source, for logs. */
    val description: String

    /**
     * Start delivering blocks to [sink] (on the source's own thread) as direct buffers with
     * position 0 and limit = bytes, which is what `LiveSession.pushPcm` borrows without
     * copying. [onError] is called once if reading fails.
     */
    fun start(sink: (ByteBuffer, Int, Int) -> Unit, onError: (String) -> Unit)

    /** Stop and release. Safe to call more than once, from any thread. */
    fun stop()
}

/** Reads 48 kHz float PCM from an [AudioRecord] on its own thread. */
class AudioCapture private constructor(
    private val record: AudioRecord,
    override val sampleRate: Int,
    override val channels: Int,
    override val description: String,
) : PcmSource {
    private val running = AtomicBoolean(false)
    private var thread: Thread? = null

    override fun start(sink: (ByteBuffer, Int, Int) -> Unit, onError: (String) -> Unit) {
        if (!running.compareAndSet(false, true)) return
        thread = Thread({ loop(sink, onError) }, "mf-capture").apply { start() }
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) {
            releaseQuietly()
            return
        }
        val current = thread
        if (current != null && current !== Thread.currentThread()) {
            runCatching { record.stop() }
            current.join(JOIN_TIMEOUT_MS)
        }
        releaseQuietly()
    }

    private fun loop(sink: (ByteBuffer, Int, Int) -> Unit, onError: (String) -> Unit) {
        Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
        val blockBytes = sampleRate / BLOCKS_PER_SECOND * channels * BYTES_PER_SAMPLE
        val buffer = ByteBuffer.allocateDirect(blockBytes).order(ByteOrder.LITTLE_ENDIAN)
        try {
            record.startRecording()
            if (record.recordingState != AudioRecord.RECORDSTATE_RECORDING) {
                running.set(false)
                onError("The microphone is in use by another app.")
                return
            }
            while (running.get()) {
                buffer.clear()
                val read = record.read(buffer, blockBytes, AudioRecord.READ_BLOCKING)
                if (read < 0) {
                    if (running.get()) {
                        Log.w(TAG, "read failed ($read) on $description")
                        running.set(false)
                        onError("Audio capture stopped.")
                    }
                    break
                }
                if (read == 0) continue
                // pushPcm borrows [0, remaining): keep position at 0.
                buffer.position(0)
                buffer.limit(read - read % (channels * BYTES_PER_SAMPLE))
                sink(buffer, sampleRate, channels)
            }
        } catch (e: IllegalStateException) {
            Log.w(TAG, "capture failed on $description", e)
            if (running.getAndSet(false)) onError("Audio capture stopped.")
        }
    }

    private fun releaseQuietly() {
        runCatching { record.stop() }
        runCatching { record.release() }
    }

    companion object {
        private const val TAG = "AudioCapture"
        const val SAMPLE_RATE = 48_000
        private const val BYTES_PER_SAMPLE = 4
        private const val BLOCKS_PER_SECOND = 50
        private const val JOIN_TIMEOUT_MS = 500L

        /**
         * The microphone without processing where the device offers it: UNPROCESSED, then
         * VOICE_RECOGNITION (no AGC/noise suppression on most devices), then MIC.
         */
        @RequiresPermission(Manifest.permission.RECORD_AUDIO)
        fun microphone(context: Context): AudioCapture? {
            val audio = context.getSystemService(AudioManager::class.java)
            val unprocessed = audio?.getProperty(AudioManager.PROPERTY_SUPPORT_AUDIO_SOURCE_UNPROCESSED) == "true"
            val sources = buildList {
                if (unprocessed) add(MediaRecorder.AudioSource.UNPROCESSED to "unprocessed")
                add(MediaRecorder.AudioSource.VOICE_RECOGNITION to "voice recognition")
                add(MediaRecorder.AudioSource.MIC to "mic")
            }
            for ((source, name) in sources) {
                val record = build(name) {
                    AudioRecord.Builder()
                        .setAudioSource(source)
                        .setAudioFormat(format(AudioFormat.CHANNEL_IN_MONO))
                        .setBufferSizeInBytes(bufferSize(AudioFormat.CHANNEL_IN_MONO))
                        .build()
                }
                if (record != null) return AudioCapture(record, SAMPLE_RATE, 1, name)
            }
            return null
        }

        /** Audio other apps play (media, games, unknown usage), through [projection]. */
        @RequiresPermission(Manifest.permission.RECORD_AUDIO)
        fun playback(projection: MediaProjection): AudioCapture? {
            val config = AudioPlaybackCaptureConfiguration.Builder(projection)
                .addMatchingUsage(AudioAttributes.USAGE_MEDIA)
                .addMatchingUsage(AudioAttributes.USAGE_GAME)
                .addMatchingUsage(AudioAttributes.USAGE_UNKNOWN)
                .build()
            val record = build("playback") {
                AudioRecord.Builder()
                    .setAudioPlaybackCaptureConfig(config)
                    .setAudioFormat(format(AudioFormat.CHANNEL_IN_STEREO))
                    .setBufferSizeInBytes(bufferSize(AudioFormat.CHANNEL_IN_STEREO))
                    .build()
            } ?: return null
            return AudioCapture(record, SAMPLE_RATE, 2, "playback")
        }

        private fun format(channelMask: Int): AudioFormat = AudioFormat.Builder()
            .setEncoding(AudioFormat.ENCODING_PCM_FLOAT)
            .setSampleRate(SAMPLE_RATE)
            .setChannelMask(channelMask)
            .build()

        private fun bufferSize(channelMask: Int): Int {
            val min = AudioRecord.getMinBufferSize(SAMPLE_RATE, channelMask, AudioFormat.ENCODING_PCM_FLOAT)
            // Half a second of headroom so a busy device never drops input.
            val channels = if (channelMask == AudioFormat.CHANNEL_IN_STEREO) 2 else 1
            return maxOf(min, SAMPLE_RATE / 2 * channels * BYTES_PER_SAMPLE)
        }

        private inline fun build(name: String, create: () -> AudioRecord): AudioRecord? = try {
            val record = create()
            if (record.state == AudioRecord.STATE_INITIALIZED) {
                record
            } else {
                record.release()
                Log.w(TAG, "AudioRecord ($name) did not initialize")
                null
            }
        } catch (e: UnsupportedOperationException) {
            Log.w(TAG, "AudioRecord ($name) unsupported", e)
            null
        } catch (e: IllegalArgumentException) {
            Log.w(TAG, "AudioRecord ($name) rejected its configuration", e)
            null
        } catch (e: SecurityException) {
            Log.w(TAG, "AudioRecord ($name) not permitted", e)
            null
        }
    }
}
