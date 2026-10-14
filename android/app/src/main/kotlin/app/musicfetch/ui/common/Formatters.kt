package app.musicfetch.ui.common

import android.content.Context
import android.net.Uri
import android.text.format.DateUtils
import android.text.format.Formatter
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle
import java.util.Locale

/** Display formatting shared by screens and notifications. */
object Formatters {
    /** "4:05", "1:00:33". */
    fun clock(ms: Long): String {
        val total = (ms.coerceAtLeast(0) / 1000)
        val h = total / 3600
        val m = total % 3600 / 60
        val s = total % 60
        return if (h > 0) String.format(Locale.ROOT, "%d:%02d:%02d", h, m, s) else String.format(Locale.ROOT, "%d:%02d", m, s)
    }

    /** Local time of day in the user's format, e.g. "14:32" or "2:32 PM". */
    fun timeOfDay(epochMs: Long, zone: ZoneId = ZoneId.systemDefault()): String =
        DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT).withZone(zone).format(Instant.ofEpochMilli(epochMs))

    /** The local calendar day of [epochMs]. */
    fun day(epochMs: Long, zone: ZoneId = ZoneId.systemDefault()): LocalDate =
        Instant.ofEpochMilli(epochMs).atZone(zone).toLocalDate()

    /** "Sunday, October 11" (with the year only outside the current year). */
    fun longDate(context: Context, epochMs: Long, today: LocalDate): String {
        var flags = DateUtils.FORMAT_SHOW_WEEKDAY or DateUtils.FORMAT_SHOW_DATE
        flags = if (day(epochMs).year == today.year) flags or DateUtils.FORMAT_NO_YEAR else flags or DateUtils.FORMAT_SHOW_YEAR
        return DateUtils.formatDateTime(context, epochMs, flags)
    }

    /** "48 MB". */
    fun bytes(context: Context, bytes: Long): String = Formatter.formatShortFileSize(context, bytes.coerceAtLeast(0))

    /** A URL without scheme and "www.", shortened: "youtube.com/shorts/x9Zk3pQ7lTw". */
    fun shortUrl(url: String, max: Int = 42): String {
        val uri = runCatching { Uri.parse(url) }.getOrNull()
        val host = uri?.host?.removePrefix("www.") ?: return ellipsize(url, max)
        val rest = (uri.encodedPath.orEmpty() + (uri.encodedQuery?.let { "?$it" } ?: "")).trimEnd('/')
        return ellipsize(host + rest, max)
    }

    /** The host of [url] without "www.", e.g. "tiktok.com". */
    fun host(url: String?): String? = url?.let { runCatching { Uri.parse(it).host }.getOrNull() }?.removePrefix("www.")

    private fun ellipsize(text: String, max: Int): String = if (text.length <= max) text else text.take(max - 1) + "…"
}
