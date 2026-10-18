package app.musicfetch.platform

import android.content.Context
import android.util.Log
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

/**
 * [SpeechToText] on Android's on-device recognizer ([SungLyrics]), for sessions whose words the
 * app did not already send while capturing (`ListenOptions.transcripts` off): the engine hands
 * over its 16 kHz WAV once capture ended.
 *
 * [isEnabled] is read on every call (Settings, "Also search lyrics you sing"). Where the phone
 * has no on-device recognizer (or runs Android 12 or older) there are no hypotheses: lyrics are an
 * optional signal and nothing is sent to a server.
 */
class AndroidSpeechToText(context: Context, private val isEnabled: () -> Boolean) : SpeechToText {
    private val sungLyrics = SungLyrics(context)

    override fun transcribe(wavPath: String, language: String, task: TaskHandle): List<String> {
        if (task.isCanceled()) throw PlatformException.Canceled()
        if (!isEnabled() || !sungLyrics.isAvailable()) return emptyList()
        val pcm = try {
            readPcm16Mono(File(wavPath))
        } catch (e: IOException) {
            Log.w(TAG, "unreadable WAV", e)
            throw PlatformException.Failed("Couldn't read the recording.")
        }
        if (pcm.data.isEmpty()) return emptyList()
        val result = CompletableFuture<SungTranscript>()
        val session = sungLyrics.start(language) { if (it.isFinal) result.complete(it) } ?: return emptyList()
        session.writePcm16(pcm.data, pcm.sampleRate)
        session.end(FINAL_TIMEOUT_MS)
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FINAL_TIMEOUT_MS + 500)
        while (true) {
            if (task.isCanceled()) {
                session.cancel()
                throw PlatformException.Canceled()
            }
            if (System.nanoTime() > deadline) {
                session.cancel()
                return emptyList()
            }
            try {
                val transcript = result.get(POLL_MS, TimeUnit.MILLISECONDS)
                return listOf(transcript.text).plus(transcript.alternatives).filter { it.isNotBlank() }.take(MAX_RESULTS)
            } catch (_: TimeoutException) {
                // keep polling
            } catch (e: ExecutionException) {
                Log.w(TAG, "recognition failed", e.cause)
                return emptyList()
            }
        }
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

        /** The engine gives speech recognition 15 s; the recognizer reads the file at its own pace. */
        const val FINAL_TIMEOUT_MS = 12_000L
        const val POLL_MS = 100L

        // 60 s of 16 kHz 16-bit audio; the engine sends at most 30 s.
        const val MAX_BYTES = 60L * 16_000 * 2
    }
}
