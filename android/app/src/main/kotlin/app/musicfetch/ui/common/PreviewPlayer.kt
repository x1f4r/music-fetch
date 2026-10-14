package app.musicfetch.ui.common

import android.media.AudioAttributes
import android.media.MediaPlayer
import android.os.Handler
import android.os.Looper
import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/** Playback of 30 s track previews; one at a time, process-wide. Main thread only. */
object PreviewPlayer {
    /** The URL playing (or preparing) and its progress 0..1. */
    data class State(val url: String? = null, val progress: Float = 0f, val preparing: Boolean = false)

    private val _state = MutableStateFlow(State())
    val state: StateFlow<State> = _state.asStateFlow()

    private var player: MediaPlayer? = null
    private val main = Handler(Looper.getMainLooper())
    private val ticker = object : Runnable {
        override fun run() {
            val p = player ?: return
            val duration = runCatching { p.duration }.getOrDefault(0)
            if (duration > 0) {
                _state.value = _state.value.copy(progress = runCatching { p.currentPosition }.getOrDefault(0) / duration.toFloat())
            }
            main.postDelayed(this, 50)
        }
    }

    /** Start [url], or stop it when it is already playing. */
    fun toggle(url: String) {
        if (_state.value.url == url) {
            stop()
            return
        }
        stop()
        val p = MediaPlayer()
        player = p
        _state.value = State(url = url, preparing = true)
        try {
            p.setAudioAttributes(
                AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_MEDIA).setContentType(AudioAttributes.CONTENT_TYPE_MUSIC).build(),
            )
            p.setDataSource(url)
            p.setOnPreparedListener {
                if (player !== it) return@setOnPreparedListener
                it.start()
                _state.value = _state.value.copy(preparing = false)
                main.post(ticker)
            }
            p.setOnCompletionListener { if (player === it) stop() }
            p.setOnErrorListener { mp, what, extra ->
                Log.w(TAG, "preview failed ($what/$extra)")
                if (player === mp) stop()
                true
            }
            p.prepareAsync()
        } catch (e: java.io.IOException) {
            Log.w(TAG, "preview unavailable", e)
            stop()
        } catch (e: IllegalArgumentException) {
            Log.w(TAG, "preview unavailable", e)
            stop()
        } catch (e: IllegalStateException) {
            Log.w(TAG, "preview unavailable", e)
            stop()
        }
    }

    fun stop() {
        main.removeCallbacks(ticker)
        player?.let {
            runCatching { it.stop() }
            it.release()
        }
        player = null
        _state.value = State()
    }

    private const val TAG = "PreviewPlayer"
}
