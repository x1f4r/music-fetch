package app.musicfetch.ui.common

import android.content.Context
import app.musicfetch.R
import app.musicfetch.core.MfException

/** Short user-facing text for a synchronous engine error (docs/design/DESIGN.md §7, Failures). */
fun MfException.userMessage(context: Context): String = when (this) {
    is MfException.InvalidInput -> reason
    is MfException.UnsupportedLink -> context.getString(R.string.error_unsupported_link)
    is MfException.Network -> context.getString(R.string.error_network)
    is MfException.RateLimited -> context.getString(R.string.error_rate_limited)
    is MfException.MediaUnavailable -> context.getString(R.string.error_media_unavailable)
    is MfException.Decode -> context.getString(R.string.error_decode)
    is MfException.IndexUnavailable -> context.getString(R.string.error_index_unavailable)
    is MfException.Platform, is MfException.Storage, is MfException.Internal, is MfException.Canceled ->
        context.getString(R.string.error_generic)
}
