package app.musicfetch.platform

import android.content.Context
import android.content.Intent
import android.media.AudioFormat
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.os.ParcelFileDescriptor
import android.speech.RecognitionListener
import android.speech.RecognizerIntent
import android.speech.SpeechRecognizer
import android.util.Log
import androidx.annotation.RequiresApi
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/** What the recognizer heard so far in one capture (mirrors the engine's `Transcript`). */
data class SungTranscript(
    val text: String,
    val alternatives: List<String>,
    val language: String,
    /** 0..1, or null when the recognizer reports none. */
    val confidence: Float?,
    /** The recognizer's last result for this capture. */
    val isFinal: Boolean,
)

/**
 * Sung lyrics of a hum: Android's on-device speech recognizer listens to the capture while the
 * user sings, so the engine can match the words (docs/CORE_API.md, "Sung words").
 *
 * Android 13+ only. `SpeechRecognizer.createOnDeviceSpeechRecognizer` reads the captured audio
 * through `EXTRA_AUDIO_SOURCE` (a pipe fed from the capture thread, 16 kHz mono 16-bit PCM) in a
 * segmented session that ends when the pipe closes, so it never opens the microphone itself and
 * needs no permission beyond the app's. Android 12 has the on-device recognizer but cannot be
 * given audio; there, on phones without an on-device recognizer, and whenever it fails, there
 * is simply no transcript. A server recognizer is never used.
 */
class SungLyrics(context: Context) {
    private val appContext = context.applicationContext

    /** Whether this phone can read sung words on the device. Cheap; may be called often. */
    fun isAvailable(): Boolean =
        Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU && SpeechRecognizer.isOnDeviceRecognitionAvailable(appContext)

    /**
     * A recognition for one capture in [language] (BCP-47) that keeps what is [Session.write]n
     * until [Session.begin] (a listen that turns into singing only later), or null when the phone
     * can't. [onTranscript] gets every result on the main thread: partial ones while the user
     * sings, and exactly one final one after [Session.end] or a failure (never after
     * [Session.cancel]).
     */
    fun prepare(language: String, onTranscript: (SungTranscript) -> Unit): Session? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU || !isAvailable()) return null
        return Session(appContext, language, onTranscript)
    }

    /** [prepare] and [Session.begin] at once. */
    fun start(language: String, onTranscript: (SungTranscript) -> Unit): Session? =
        prepare(language, onTranscript)?.also { it.begin() }

    /**
     * One capture's recognition (only [prepare] makes one, on Android 13+). [write] is called
     * from the capture thread and never blocks; everything else from any thread.
     */
    class Session internal constructor(
        private val context: Context,
        private val language: String,
        private val onTranscript: (SungTranscript) -> Unit,
    ) {
        private val main = Handler(Looper.getMainLooper())
        private val chunks = LinkedBlockingQueue<ByteArray>()
        private val delivered = AtomicBoolean(false)
        private val begun = AtomicBoolean(false)

        /** [begin] was called: the recognizer reads (or read) the audio. */
        val started: Boolean get() = begun.get()

        // Guarded by [lock]: the capture thread writes, another thread ends.
        private val lock = Any()
        private var ended = false
        private var resampler: StreamingResampler? = null
        private var inputRate = 0
        private var queuedBytes = 0L

        // Main thread only.
        private var recognizer: SpeechRecognizer? = null

        private val segments = mutableListOf<List<String>>()
        private var segmentConfidence: Float? = null
        private var partial: String = ""

        /** Start the recognizer on everything written so far, then on what follows. Once. */
        fun begin() {
            if (!begun.compareAndSet(false, true) || delivered.get()) return
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
                deliverFinal()
                return
            }
            val fds = try {
                ParcelFileDescriptor.createPipe()
            } catch (e: IOException) {
                Log.w(TAG, "no pipe for the recognizer", e)
                deliverFinal()
                return
            }
            thread(name = "mf-sung-lyrics", isDaemon = true) { pump(fds[1]) }
            main.post { startRecognizer(fds[0]) }
        }

        /**
         * Captured audio: interleaved little-endian float, as pushed to the engine. Reads
         * [buffer] from 0 to its limit without moving it. Ignored after [end].
         */
        fun write(buffer: ByteBuffer, sampleRate: Int, channels: Int) {
            if (channels <= 0 || sampleRate <= 0) return
            val floats = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN)
            val frames = floats.limit() / (4 * channels)
            if (frames == 0) return
            val mono = FloatArray(frames)
            for (i in 0 until frames) {
                var sum = 0f
                for (c in 0 until channels) sum += floats.getFloat((i * channels + c) * 4)
                mono[i] = sum / channels
            }
            synchronized(lock) {
                if (ended) return
                val current = resampler?.takeIf { inputRate == sampleRate }
                    ?: StreamingResampler(sampleRate, RATE).also {
                        resampler = it
                        inputRate = sampleRate
                    }
                enqueue(current.process(mono, frames))
            }
        }

        /** Recorded 16-bit mono PCM (a whole recording at once, e.g. the engine's WAV). */
        fun writePcm16(data: ByteArray, sampleRate: Int) {
            if (data.size < 2 || sampleRate <= 0) return
            val shorts = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
            val mono = FloatArray(data.size / 2) { shorts.getShort(it * 2) / 32_768f }
            synchronized(lock) {
                if (ended) return
                val current = resampler?.takeIf { inputRate == sampleRate }
                    ?: StreamingResampler(sampleRate, RATE).also {
                        resampler = it
                        inputRate = sampleRate
                    }
                enqueue(current.process(mono, mono.size))
            }
        }

        /**
         * The audio ended: let the recognizer finish. The final transcript follows within
         * [finalTimeoutMs] at the latest (with what was heard by then).
         */
        fun end(finalTimeoutMs: Long = FINAL_TIMEOUT_MS) {
            synchronized(lock) {
                if (ended) return
                ended = true
                resampler?.flush()?.let { enqueue(it) }
            }
            chunks.put(END)
            if (!begun.get()) {
                // Never started: nothing was heard.
                deliverFinal()
                return
            }
            main.postDelayed({ deliverFinal() }, finalTimeoutMs)
        }

        /** Abandon without a transcript (the session was canceled). */
        fun cancel() {
            synchronized(lock) { ended = true }
            delivered.set(true)
            chunks.clear()
            chunks.put(END)
            main.post { release() }
        }

        /** Under [lock]. */
        private fun enqueue(samples: FloatArray) {
            if (samples.isEmpty()) return
            // The engine captures at most 30 s; a stuck reader must not grow memory forever.
            if (queuedBytes > MAX_QUEUED_BYTES) return
            val bytes = ByteBuffer.allocate(samples.size * 2).order(ByteOrder.LITTLE_ENDIAN)
            for (s in samples) bytes.putShort((s.coerceIn(-1f, 1f) * 32_767f).toInt().toShort())
            queuedBytes += bytes.capacity()
            chunks.put(bytes.array())
        }

        /** Writer thread: queue → pipe; closing the pipe ends the recognizer's session. */
        private fun pump(out: ParcelFileDescriptor) {
            ParcelFileDescriptor.AutoCloseOutputStream(out).use { stream ->
                try {
                    while (true) {
                        val chunk = chunks.poll(POLL_MS, TimeUnit.MILLISECONDS) ?: continue
                        if (chunk === END) break
                        stream.write(chunk)
                    }
                } catch (_: IOException) {
                    // The recognizer closed its end (it finished or failed); nothing left to do.
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
        }

        @RequiresApi(Build.VERSION_CODES.TIRAMISU)
        private fun startRecognizer(source: ParcelFileDescriptor) {
            if (delivered.get()) {
                runCatching { source.close() }
                return
            }
            try {
                val r = SpeechRecognizer.createOnDeviceSpeechRecognizer(context)
                recognizer = r
                r.setRecognitionListener(Listener())
                val intent = Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH)
                    .putExtra(RecognizerIntent.EXTRA_LANGUAGE_MODEL, RecognizerIntent.LANGUAGE_MODEL_FREE_FORM)
                    .putExtra(RecognizerIntent.EXTRA_LANGUAGE, language)
                    .putExtra(RecognizerIntent.EXTRA_MAX_RESULTS, MAX_RESULTS)
                    .putExtra(RecognizerIntent.EXTRA_PARTIAL_RESULTS, true)
                    .putExtra(RecognizerIntent.EXTRA_PREFER_OFFLINE, true)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE, source)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE_CHANNEL_COUNT, 1)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE_ENCODING, AudioFormat.ENCODING_PCM_16BIT)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE_SAMPLING_RATE, RATE)
                    // One session for the whole capture, ended by closing the pipe: singing
                    // pauses between lines must not end it.
                    .putExtra(RecognizerIntent.EXTRA_SEGMENTED_SESSION, RecognizerIntent.EXTRA_AUDIO_SOURCE)
                r.startListening(intent)
            } catch (e: RuntimeException) {
                Log.w(TAG, "on-device recognizer failed to start", e)
                deliverFinal()
            } finally {
                // The recognizer holds its own duplicate of the read end.
                runCatching { source.close() }
            }
        }

        private inner class Listener : RecognitionListener {
            override fun onSegmentResults(segmentResults: Bundle) {
                val texts = segmentResults.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION).orEmpty()
                    .map { it.trim() }.filter { it.isNotEmpty() }.take(MAX_RESULTS)
                if (texts.isNotEmpty()) {
                    segments += texts
                    segmentConfidence = segmentResults.getFloatArray(SpeechRecognizer.CONFIDENCE_SCORES)?.firstOrNull()
                }
                partial = ""
                deliver(isFinal = false)
            }

            override fun onPartialResults(partialResults: Bundle) {
                val text = partialResults.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION)?.firstOrNull()?.trim().orEmpty()
                if (text.isEmpty() || text == partial) return
                partial = text
                deliver(isFinal = false)
            }

            override fun onEndOfSegmentedSession() = deliverFinal()

            override fun onResults(results: Bundle) {
                // Not a segmented session after all (a recognizer that ignores the extra).
                onSegmentResults(results)
                deliverFinal()
            }

            override fun onError(error: Int) {
                // No speech at all (humming, whistling) is the common case, not a failure.
                if (error != SpeechRecognizer.ERROR_NO_MATCH && error != SpeechRecognizer.ERROR_SPEECH_TIMEOUT) {
                    Log.i(TAG, "on-device recognizer error $error")
                }
                deliverFinal()
            }

            override fun onReadyForSpeech(params: Bundle?) = Unit
            override fun onBeginningOfSpeech() = Unit
            override fun onRmsChanged(rmsdB: Float) = Unit
            override fun onBufferReceived(buffer: ByteArray?) = Unit
            override fun onEndOfSpeech() = Unit
            override fun onEvent(eventType: Int, params: Bundle?) = Unit
        }

        /** Main thread. */
        private fun deliver(isFinal: Boolean) {
            if (delivered.get()) return
            val transcript = joinSegments(segments, partial, language, segmentConfidence, isFinal)
            if (!isFinal && transcript.text.isEmpty()) return
            if (isFinal && !delivered.compareAndSet(false, true)) return
            onTranscript(transcript)
        }

        private fun deliverFinal() {
            if (Looper.myLooper() != Looper.getMainLooper()) {
                main.post { deliverFinal() }
                return
            }
            deliver(isFinal = true)
            release()
        }

        /** Main thread. */
        private fun release() {
            synchronized(lock) { ended = true }
            chunks.put(END)
            recognizer?.let {
                runCatching { it.cancel() }
                runCatching { it.destroy() }
            }
            recognizer = null
        }
    }

    internal companion object {
        const val TAG = "SungLyrics"
        const val RATE = 16_000
        const val MAX_RESULTS = 5
        const val POLL_MS = 200L

        /** How long the recognizer may take after the capture ended (the engine waits 2 s). */
        const val FINAL_TIMEOUT_MS = 1_800L

        /** 65 s of 16 kHz 16-bit audio (the engine captures at most 30 s, recordings 60 s). */
        const val MAX_QUEUED_BYTES = 65L * RATE * 2
        private val END = ByteArray(0)

        /**
         * One transcript from the segments so far (each a list of hypotheses, best first) and
         * the current segment's partial text: the best of each segment joined, then up to four
         * alternatives that swap in one segment's next hypotheses.
         */
        fun joinSegments(
            segments: List<List<String>>,
            partial: String,
            language: String,
            confidence: Float?,
            isFinal: Boolean,
        ): SungTranscript {
            val best = segments.mapNotNull { it.firstOrNull() }
            val text = (best + listOfNotNull(partial.takeIf { it.isNotBlank() })).joinToString(" ").trim()
            val alternatives = buildList {
                for ((index, segment) in segments.withIndex()) {
                    for (other in segment.drop(1)) {
                        if (size >= MAX_RESULTS - 1) return@buildList
                        val swapped = best.toMutableList().also { it[index] = other }
                        add((swapped + listOfNotNull(partial.takeIf { it.isNotBlank() })).joinToString(" ").trim())
                    }
                }
            }.filter { it.isNotEmpty() && it != text }.distinct()
            return SungTranscript(
                text = text,
                alternatives = alternatives,
                language = language,
                confidence = confidence?.takeIf { it > 0f && segments.size == 1 },
                isFinal = isFinal,
            )
        }
    }
}
