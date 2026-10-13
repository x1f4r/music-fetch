package app.musicfetch

import android.app.Application
import app.musicfetch.platform.ChaquopyMediaBackend

class MusicFetchApplication : Application() {
    /** yt-dlp through Chaquopy; handed to the engine as its `MediaBackend`. */
    val mediaBackend: ChaquopyMediaBackend by lazy { ChaquopyMediaBackend(this) }

    override fun onCreate() {
        super.onCreate()
        // Python.start(AndroidPlatform) and the yt-dlp import run off the main thread so
        // they don't delay the first frame.
        mediaBackend.warmUp()
    }
}
