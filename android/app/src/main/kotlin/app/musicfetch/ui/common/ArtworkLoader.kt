package app.musicfetch.ui.common

import android.content.Context
import android.graphics.Bitmap
import coil3.SingletonImageLoader
import coil3.request.ImageRequest
import coil3.request.SuccessResult
import coil3.request.allowHardware
import coil3.toBitmap

/** Software bitmaps of artwork for notifications, widgets and colour extraction. */
object ArtworkLoader {
    /** The image at [url] scaled to fit [sizePx], or null when it can't be loaded. */
    suspend fun bitmap(context: Context, url: String, sizePx: Int): Bitmap? {
        val request = ImageRequest.Builder(context)
            .data(url)
            .size(sizePx)
            .allowHardware(false)
            .build()
        val result = SingletonImageLoader.get(context).execute(request)
        return (result as? SuccessResult)?.image?.toBitmap()
    }
}
