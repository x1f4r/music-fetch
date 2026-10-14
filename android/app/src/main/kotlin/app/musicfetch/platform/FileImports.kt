package app.musicfetch.platform

import android.content.Context
import android.net.Uri
import android.provider.OpenableColumns
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.IOException
import java.util.UUID

/**
 * Copies shared or picked audio/video (content URIs) into the cache so the engine can read
 * them by path. Each import gets its own folder, keeping the original file name (the engine
 * shows it as the session title); [release] deletes it once the analysis is done.
 */
object FileImports {
    private const val TAG = "FileImports"
    private const val DIR = "imports"

    fun root(context: Context): File = File(context.cacheDir, DIR)

    /** The copied file, or null when the content can't be read. */
    suspend fun copy(context: Context, uri: Uri): File? = withContext(Dispatchers.IO) {
        val resolver = context.contentResolver
        val name = displayName(context, uri) ?: "audio"
        val dir = File(root(context), UUID.randomUUID().toString())
        val target = File(dir, sanitize(name))
        try {
            if (!dir.mkdirs()) throw IOException("could not create the import folder")
            val input = resolver.openInputStream(uri) ?: throw IOException("no content")
            input.use { source -> target.outputStream().use { source.copyTo(it, 256 * 1024) } }
            if (target.length() == 0L) throw IOException("empty file")
            target
        } catch (e: IOException) {
            Log.w(TAG, "import failed", e)
            dir.deleteRecursively()
            null
        } catch (e: SecurityException) {
            Log.w(TAG, "import not permitted", e)
            dir.deleteRecursively()
            null
        }
    }

    /** The shared item's display name (e.g. "Voice memo.m4a"). */
    fun displayName(context: Context, uri: Uri): String? = try {
        context.contentResolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)?.use { cursor ->
            if (cursor.moveToFirst()) cursor.getString(0) else null
        } ?: uri.lastPathSegment
    } catch (e: SecurityException) {
        Log.w(TAG, "name not readable", e)
        null
    } catch (e: IllegalArgumentException) {
        null
    }

    /** Delete an import once its analysis finished; ignores paths outside the import folder. */
    fun release(context: Context, path: String) {
        val file = File(path)
        val parent = file.parentFile ?: return
        if (parent.parentFile?.canonicalPath == root(context).canonicalPath) parent.deleteRecursively()
    }

    private fun sanitize(name: String): String {
        val cleaned = name.replace(Regex("[/\\\\:*?\"<>|\\u0000-\\u001f]"), "_").trim().trimStart('.')
        return cleaned.take(120).ifEmpty { "audio" }
    }
}
