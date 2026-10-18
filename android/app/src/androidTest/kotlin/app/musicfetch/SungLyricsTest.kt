package app.musicfetch

import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.Bundle
import android.speech.RecognitionSupport
import android.speech.RecognitionSupportCallback
import android.speech.RecognizerIntent
import android.speech.SpeechRecognizer
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import androidx.test.platform.app.InstrumentationRegistry
import app.musicfetch.platform.SungLyrics
import app.musicfetch.platform.SungTranscript
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.io.RandomAccessFile
import java.util.Locale
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * Sung words on the device: a spoken line (the system's text-to-speech, so no recording is
 * needed) goes through [SungLyrics] the way a capture does and comes back as words. Skipped
 * where the phone has no on-device recognizer or no text-to-speech voice.
 */
@LargeTest
@RunWith(AndroidJUnit4::class)
class SungLyricsTest {
    private val context: Context = InstrumentationRegistry.getInstrumentation().targetContext

    @Test
    fun spokenWordsComeBackAsAFinalTranscript() {
        assumeTrue("needs Android 13", Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU)
        val sungLyrics = SungLyrics(context)
        assumeTrue("needs an on-device recognizer", sungLyrics.isAvailable())
        logSupport()
        val wav = synthesize("never gonna give you up never gonna let you down")
        assumeTrue("needs a text-to-speech voice", wav != null)
        val (pcm, rate) = readPcm16(wav!!)
        val partials = mutableListOf<SungTranscript>()
        val final = CompletableFuture<SungTranscript>()
        val session = sungLyrics.start("en-US") { t -> if (t.isFinal) final.complete(t) else partials += t }
        assertTrue(session != null)
        session!!.writePcm16(pcm, rate)
        session.end(15_000)
        val transcript = final.get(20, TimeUnit.SECONDS)
        Log.i(TAG, "partials ${partials.size}, final \"${transcript.text}\" alternatives ${transcript.alternatives}")
        assertTrue(transcript.isFinal)
        assertTrue("heard: \"${transcript.text}\"", transcript.text.lowercase().contains("give you up"))
    }

    /** What the on-device recognizer says about English, for the log. */
    private fun logSupport() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
        val done = CompletableFuture<String>()
        InstrumentationRegistry.getInstrumentation().runOnMainSync {
            val recognizer = SpeechRecognizer.createOnDeviceSpeechRecognizer(context)
            val intent = Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH).putExtra(RecognizerIntent.EXTRA_LANGUAGE, "en-US")
            recognizer.checkRecognitionSupport(
                intent,
                context.mainExecutor,
                object : RecognitionSupportCallback {
                    override fun onSupportResult(support: RecognitionSupport) {
                        done.complete("installed ${support.installedOnDeviceLanguages}, pending ${support.pendingOnDeviceLanguages}, supported ${support.supportedOnDeviceLanguages.take(8)}")
                        recognizer.destroy()
                    }

                    override fun onError(error: Int) {
                        done.complete("error $error")
                        recognizer.destroy()
                    }
                },
            )
        }
        Log.i(TAG, "on-device recognition support: ${runCatching { done.get(10, TimeUnit.SECONDS) }.getOrElse { "timeout" }}")
    }

    private fun synthesize(text: String): File? {
        val ready = CompletableFuture<Boolean>()
        lateinit var tts: TextToSpeech
        InstrumentationRegistry.getInstrumentation().runOnMainSync {
            tts = TextToSpeech(context) { status -> ready.complete(status == TextToSpeech.SUCCESS) }
        }
        if (!ready.get(20, TimeUnit.SECONDS)) return null
        val file = File(context.cacheDir, "sung-lyrics-test.wav")
        val done = CompletableFuture<Boolean>()
        tts.language = Locale.US
        tts.setOnUtteranceProgressListener(object : UtteranceProgressListener() {
            override fun onStart(utteranceId: String?) = Unit
            override fun onDone(utteranceId: String?) {
                done.complete(true)
            }

            @Deprecated("Deprecated in Java")
            override fun onError(utteranceId: String?) {
                done.complete(false)
            }
        })
        tts.synthesizeToFile(text, Bundle(), file, "line")
        val ok = runCatching { done.get(30, TimeUnit.SECONDS) }.getOrDefault(false)
        tts.shutdown()
        return file.takeIf { ok && it.length() > 44 }
    }

    /** 16-bit mono PCM of a WAV file and its sample rate. */
    private fun readPcm16(file: File): Pair<ByteArray, Int> {
        RandomAccessFile(file, "r").use { raf ->
            raf.seek(12)
            var rate = 16_000
            while (raf.filePointer + 8 <= raf.length()) {
                val id = ByteArray(4).also { raf.readFully(it) }.toString(Charsets.US_ASCII)
                val size = Integer.reverseBytes(raf.readInt())
                when (id) {
                    "fmt " -> {
                        val fmt = ByteArray(size).also { raf.readFully(it) }
                        rate = (fmt[4].toInt() and 0xFF) or ((fmt[5].toInt() and 0xFF) shl 8) or
                            ((fmt[6].toInt() and 0xFF) shl 16) or ((fmt[7].toInt() and 0xFF) shl 24)
                    }
                    "data" -> {
                        val n = minOf(size.toLong(), raf.length() - raf.filePointer).toInt()
                        return ByteArray(n).also { raf.readFully(it) } to rate
                    }
                    else -> raf.seek(raf.filePointer + size)
                }
            }
        }
        error("no data chunk")
    }

    private companion object {
        const val TAG = "SungLyricsTest"
    }
}
