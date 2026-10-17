package app.musicfetch.platform

import android.content.Context
import app.musicfetch.BuildConfig
import java.io.File

/**
 * Debug and benchmark builds only: files a developer places to test without sound in the room
 * or a published melody index (a replay WAV for the microphone, a melody-index manifest URL).
 * Looked up in the app's external files directory, which `adb` can write to without
 * `run-as` (benchmark builds are not debuggable), then in its files directory. Release builds never
 * read them.
 */
object DevFiles {
    /** Replayed instead of the microphone (see `WavReplay`). */
    const val CAPTURE_WAV = "debug-capture.wav"

    /** One line: the melody-index manifest URL to use instead of the published one. */
    const val MELODY_MANIFEST_URL = "debug-melody-manifest-url.txt"

    fun find(context: Context, name: String): File? {
        if (!BuildConfig.DEV_TOOLS) return null
        return listOfNotNull(context.getExternalFilesDir(null), context.filesDir)
            .map { File(it, name) }
            .firstOrNull { it.isFile }
    }
}
