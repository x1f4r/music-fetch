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
import app.musicfetch.core.PlatformException
import app.musicfetch.core.SpeechToText
import app.musicfetch.core.TaskHandle
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.concurrent.thread

/**
 * [SpeechToText] on Android's on-device recognizer, used to read sung lyrics.
 *
 * Android 13+ (`EXTRA_AUDIO_SOURCE`) streams the engine's 16 kHz WAV into
 * `createOnDeviceSpeechRecognizer` through a pipe. Older releases, or devices without an
 * on-device recognizer, return no hypotheses: lyrics are an optional signal and nothing is
 * sent to a server.
 */
class AndroidSpeechToText(context: Context) : SpeechToText {
    private val appContext = context.applicationContext
    private val main = Handler(Looper.getMainLooper())

    override fun transcribe(wavPath: String, language: String, task: TaskHandle): List<String> {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return emptyList()
        if (task.isCanceled()) throw PlatformException.Canceled()
        if (!SpeechRecognizer.isOnDeviceRecognitionAvailable(appContext)) return emptyList()
        val wav = File(wavPath)
        val pcm = try {
            readPcm16Mono(wav)
        } catch (e: IOException) {
            Log.w(TAG, "unreadable WAV", e)
            throw PlatformException.Failed("Couldn't read the recording.")
        }
        if (pcm.data.isEmpty()) return emptyList()
        return recognize(pcm, language, task)
    }

    @RequiresApi(Build.VERSION_CODES.TIRAMISU)
    private fun recognize(pcm: Pcm, language: String, task: TaskHandle): List<String> {
        val result = CompletableFuture<List<String>>()
        val pipe = ParcelFileDescriptor.createPipe()
        var recognizer: SpeechRecognizer? = null
        main.post {
            try {
                val r = SpeechRecognizer.createOnDeviceSpeechRecognizer(appContext)
                recognizer = r
                r.setRecognitionListener(Listener(result))
                val intent = Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH)
                    .putExtra(RecognizerIntent.EXTRA_LANGUAGE_MODEL, RecognizerIntent.LANGUAGE_MODEL_FREE_FORM)
                    .putExtra(RecognizerIntent.EXTRA_LANGUAGE, language)
                    .putExtra(RecognizerIntent.EXTRA_MAX_RESULTS, MAX_RESULTS)
                    .putExtra(RecognizerIntent.EXTRA_PREFER_OFFLINE, true)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE, pipe[0])
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE_CHANNEL_COUNT, 1)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE_ENCODING, AudioFormat.ENCODING_PCM_16BIT)
                    .putExtra(RecognizerIntent.EXTRA_AUDIO_SOURCE_SAMPLING_RATE, pcm.sampleRate)
                    .putExtra(RecognizerIntent.EXTRA_SEGMENTED_SESSION, false)
                r.startListening(intent)
            } catch (e: RuntimeException) {
                Log.w(TAG, "recognizer failed to start", e)
                result.complete(emptyList())
            }
        }
        // The recognizer reads at its own pace; a writer thread keeps the engine thread free
        // to watch for cancellation.
        val writer = thread(name = "mf-speech-pipe", isDaemon = true) {
            try {
                ParcelFileDescriptor.AutoCloseOutputStream(pipe[1]).use { it.write(pcm.data) }
            } catch (_: IOException) {
                // The recognizer closed its end early; nothing left to do.
            }
        }
        try {
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_S)
            while (true) {
                if (task.isCanceled()) throw PlatformException.Canceled()
                if (System.nanoTime() > deadline) return emptyList()
                try {
                    return result.get(POLL_MS, TimeUnit.MILLISECONDS)
                } catch (_: TimeoutException) {
                    // keep polling
                } catch (e: ExecutionException) {
                    Log.w(TAG, "recognition failed", e.cause)
                    return emptyList()
                }
            }
        } finally {
            main.post {
                recognizer?.let {
                    runCatching { it.cancel() }
                    it.destroy()
                }
            }
            runCatching { pipe[0].close() }
            writer.interrupt()
        }
    }

    private class Listener(private val result: CompletableFuture<List<String>>) : RecognitionListener {
        override fun onResults(results: Bundle?) {
            val texts = results?.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION).orEmpty()
            result.complete(texts.filter { it.isNotBlank() }.take(MAX_RESULTS))
        }

        override fun onError(error: Int) {
            // ERROR_NO_MATCH / ERROR_SPEECH_TIMEOUT simply mean "no words".
            if (error != SpeechRecognizer.ERROR_NO_MATCH && error != SpeechRecognizer.ERROR_SPEECH_TIMEOUT) {
                Log.i(TAG, "recognizer error $error")
            }
            result.complete(emptyList())
        }

        override fun onReadyForSpeech(params: Bundle?) = Unit
        override fun onBeginningOfSpeech() = Unit
        override fun onRmsChanged(rmsdB: Float) = Unit
        override fun onBufferReceived(buffer: ByteArray?) = Unit
        override fun onEndOfSpeech() = Unit
        override fun onPartialResults(partialResults: Bundle?) = Unit
        override fun onEvent(eventType: Int, params: Bundle?) = Unit
    }

    private class Pcm(val data: ByteArray, val sampleRate: Int)

    /** The data chunk of a 16-bit mono PCM WAV (the format the engine writes). */
    private fun readPcm16Mono(file: File): Pcm {
        RandomAccessFile(file, "r").use { raf ->
            val header = ByteArray(12)
            raf.readFully(header)
            if (String(header, 0, 4, Charsets.US_ASCII) != "RIFF" || String(header, 8, 4, Charsets.US_ASCII) != "WAVE") {
                throw IOException("not a WAV file")
            }
            var sampleRate = 16_000
            while (raf.filePointer + 8 <= raf.length()) {
                val id = ByteArray(4).also { raf.readFully(it) }.toString(Charsets.US_ASCII)
                val size = Integer.reverseBytes(raf.readInt()).toLong() and 0xFFFF_FFFFL
                when (id) {
                    "fmt " -> {
                        val fmt = ByteArray(size.toInt()).also { raf.readFully(it) }
                        val channels = (fmt[2].toInt() and 0xFF) or ((fmt[3].toInt() and 0xFF) shl 8)
                        sampleRate = (fmt[4].toInt() and 0xFF) or ((fmt[5].toInt() and 0xFF) shl 8) or
                            ((fmt[6].toInt() and 0xFF) shl 16) or ((fmt[7].toInt() and 0xFF) shl 24)
                        val bits = (fmt[14].toInt() and 0xFF) or ((fmt[15].toInt() and 0xFF) shl 8)
                        if (channels != 1 || bits != 16) throw IOException("expected 16-bit mono")
                    }
                    "data" -> {
                        val available = (raf.length() - raf.filePointer).coerceAtMost(size)
                        val data = ByteArray(available.coerceAtMost(MAX_BYTES).toInt())
                        raf.readFully(data)
                        return Pcm(data, sampleRate)
                    }
                    else -> raf.seek(raf.filePointer + size + (size and 1))
                }
            }
        }
        throw IOException("no data chunk")
    }

    private companion object {
        const val TAG = "SpeechToText"
        const val MAX_RESULTS = 5
        const val TIMEOUT_S = 20L
        const val POLL_MS = 100L
        // 60 s of 16 kHz 16-bit audio; the engine sends at most 30 s.
        const val MAX_BYTES = 60L * 16_000 * 2
    }
}
