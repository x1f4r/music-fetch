package app.musicfetch.platform

import android.content.Context
import android.util.Log
import app.musicfetch.core.MediaBackend
import app.musicfetch.core.PlatformException
import app.musicfetch.core.TaskHandle
import com.chaquo.python.PyException
import com.chaquo.python.PyObject
import com.chaquo.python.Python
import com.chaquo.python.android.AndroidPlatform
import org.json.JSONException
import org.json.JSONObject
import java.io.File
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

/**
 * [MediaBackend] running yt-dlp in Chaquopy's embedded Python (`src/main/python/mf_ytdlp.py`).
 *
 * The engine calls in from its worker threads. Python runs on a small pool of Java threads
 * owned by this class while the calling thread waits and polls the task's cancel flag, so a
 * canceled session returns promptly even while yt-dlp is still extracting. yt-dlp solves
 * YouTube's JS challenges with the bundled quickjs-ng (`libqjs.so` in nativeLibraryDir).
 */
class ChaquopyMediaBackend(context: Context) : MediaBackend {
    private val appContext = context.applicationContext
    private val threadCount = AtomicInteger()

    private val executor = ThreadPoolExecutor(
        MAX_CONCURRENT_CALLS, MAX_CONCURRENT_CALLS, 30, TimeUnit.SECONDS, LinkedBlockingQueue(),
    ) { runnable ->
        Thread(runnable, "mf-python-${threadCount.incrementAndGet()}").apply { isDaemon = true }
    }.apply { allowCoreThreadTimeOut(true) }

    private val module: PyObject by lazy { startPython(appContext).getModule("mf_ytdlp") }

    private val options: String by lazy {
        val cache = File(appContext.cacheDir, "yt-dlp")
        val qjs = File(appContext.applicationInfo.nativeLibraryDir, "libqjs.so")
        JSONObject()
            .put("qjs_path", if (qjs.canExecute()) qjs.path else JSONObject.NULL)
            .put("tmp_dir", File(cache, "tmp").path)
            .put("cache_dir", File(cache, "cache").path)
            .put("socket_timeout", SOCKET_TIMEOUT_S)
            .toString()
    }

    override fun probe(url: String, withComments: Boolean, task: TaskHandle): String =
        probe(url, withComments, isCanceled = task::isCanceled)

    override fun downloadAudio(url: String, destDir: String, task: TaskHandle): String =
        downloadAudio(url, destDir, isCanceled = task::isCanceled, onProgress = task::reportProgress)

    /**
     * yt-dlp `--dump-single-json --flat-playlist` output for [url]. With [withComments] it also
     * holds up to 40 top comments (YouTube: pinned first, sorted by top), where tracklists are
     * often posted. [extractorArgs] passes yt-dlp extractor arguments (diagnostics only), e.g.
     * `{"youtube": {"player_client": ["web_safari"]}}`.
     */
    @Throws(PlatformException::class)
    fun probe(
        url: String,
        withComments: Boolean = false,
        isCanceled: () -> Boolean = { false },
        extractorArgs: JSONObject? = null,
    ): String {
        val opts = extractorArgs?.let { JSONObject(options).put("extractor_args", it).toString() } ?: options
        return call(isCanceled) { it.callAttr("probe", url, opts, withComments).toString() }
    }

    /** Download the best audio-only stream of [url] into [destDir] and return the file path. */
    @Throws(PlatformException::class)
    fun downloadAudio(
        url: String,
        destDir: String,
        isCanceled: () -> Boolean = { false },
        onProgress: (Float) -> Unit = {},
    ): String {
        val progress = PythonCallbacks.Progress { onProgress(it.toFloat()) }
        val cancel = PythonCallbacks.CancelCheck { isCanceled() }
        return call(isCanceled) { it.callAttr("download_audio", url, destDir, options, progress, cancel).toString() }
    }

    /** Versions of Python, yt-dlp and the JS runtime, and whether the runtime executes here. */
    @Throws(PlatformException::class)
    fun runtimeInfo(): JSONObject {
        val json = call({ false }) { it.callAttr("runtime_info", options).toString() }
        return try {
            JSONObject(json)
        } catch (e: JSONException) {
            throw failure(e)
        }
    }

    /** Start Python and import yt-dlp in the background so the first request is fast. */
    fun warmUp() {
        executor.execute {
            runCatching { module }.onFailure { Log.w(TAG, "Python warm-up failed", it) }
        }
    }

    private fun call(isCanceled: () -> Boolean, block: (PyObject) -> String): String {
        if (isCanceled()) throw PlatformException.Canceled()
        val future = executor.submit<String> {
            if (isCanceled()) CANCELED_RESULT else block(module)
        }
        return unwrap(await(future, isCanceled))
    }

    private fun await(future: Future<String>, isCanceled: () -> Boolean): String {
        while (true) {
            try {
                return future.get(CANCEL_POLL_MS, TimeUnit.MILLISECONDS)
            } catch (_: TimeoutException) {
                // A download stops at its next progress hook; an extraction finishes on its own.
                if (isCanceled()) throw PlatformException.Canceled()
            } catch (e: ExecutionException) {
                throw failure(e.cause ?: e)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                throw PlatformException.Canceled()
            }
        }
    }

    /** `mf_ytdlp` returns `{"error": kind, "message": text}` instead of raising. */
    private fun unwrap(result: String): String {
        if (!result.startsWith(ERROR_PREFIX)) return result
        val error = try {
            JSONObject(result)
        } catch (e: JSONException) {
            throw failure(e)
        }
        val message = error.optString("message").ifBlank { "The media could not be read." }
        throw when (error.optString("error")) {
            "canceled" -> PlatformException.Canceled()
            "unavailable", "unsupported" -> PlatformException.Unavailable(message)
            else -> PlatformException.Failed(message)
        }
    }

    private fun failure(cause: Throwable): PlatformException {
        if (cause is PlatformException) return cause
        Log.e(TAG, "yt-dlp call failed", cause)
        val message = when (cause) {
            is PyException -> "The media downloader failed to run."
            else -> "The media downloader failed: ${cause.javaClass.simpleName}."
        }
        return PlatformException.Failed(message)
    }

    companion object {
        private const val TAG = "MediaBackend"
        private const val MAX_CONCURRENT_CALLS = 4
        private const val CANCEL_POLL_MS = 100L
        private const val SOCKET_TIMEOUT_S = 30
        private const val ERROR_PREFIX = "{\"error\": "
        private const val CANCELED_RESULT = "{\"error\": \"canceled\", \"message\": \"Canceled.\"}"

        private val startLock = Any()

        /** Start the embedded Python once per process; safe from any thread. */
        fun startPython(context: Context): Python {
            synchronized(startLock) {
                if (!Python.isStarted()) Python.start(AndroidPlatform(context.applicationContext))
            }
            return Python.getInstance()
        }
    }
}

/**
 * Callbacks handed to `mf_ytdlp.py`, which calls `report` and `isCanceled` by name;
 * proguard-rules.pro keeps them and their implementations.
 */
object PythonCallbacks {
    fun interface Progress {
        fun report(fraction: Double)
    }

    fun interface CancelCheck {
        fun isCanceled(): Boolean
    }
}
