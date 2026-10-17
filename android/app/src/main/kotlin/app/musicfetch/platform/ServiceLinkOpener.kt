package app.musicfetch.platform

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.util.Log
import androidx.core.net.toUri
import app.musicfetch.core.Service
import app.musicfetch.core.ServiceLink
import app.musicfetch.core.Track
import app.musicfetch.core.customProviderLink
import app.musicfetch.core.providerLinks
import java.net.URLDecoder

/**
 * Opens track links in the user's music app (docs/APPS.md, "Opening links").
 *
 * When the provider's app is installed the https link is handed to it directly with
 * `setPackage`; Spotify search links use the `spotify:search:` form. Otherwise the link opens
 * normally (browser or App Links).
 */
object ServiceLinkOpener {
    /** The split button's choices for [track]: `providerLinks` plus the custom provider. */
    fun choices(track: Track, preference: ProviderPreference): OpenChoices {
        val links = try {
            providerLinks(track)
        } catch (e: RuntimeException) {
            Log.w(TAG, "provider links unavailable (${e.javaClass.simpleName})")
            emptyList()
        }
        val custom = preference.custom?.let { customLink(track, it.template) }
        return OpenChoices.of(links, track.links, preference, custom)
    }

    /** [template] filled in for [track], or null when the result isn't an http(s) link. */
    fun customLink(track: Track, template: String): String? = try {
        customProviderLink(track, template)
    } catch (e: RuntimeException) {
        Log.w(TAG, "custom link unavailable (${e.javaClass.simpleName})")
        null
    }

    /** Intent that opens [link], in the provider's app when it is installed. */
    fun intentFor(context: Context, link: ServiceLink): Intent {
        val pkg = Providers.packages[link.service]?.takeIf { Providers.isInstalled(context, link.service) }
        val uri = if (pkg != null) appUri(link) else link.url.toUri()
        return Intent(Intent.ACTION_VIEW, uri).apply {
            addCategory(Intent.CATEGORY_BROWSABLE)
            if (pkg != null) setPackage(pkg)
            addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        }
    }

    /** Intent for any [target]; a custom provider opens like any web link (App Links apply). */
    fun intentFor(context: Context, target: OpenTarget): Intent = when (target) {
        is OpenTarget.Link -> intentFor(context, target.link)
        is OpenTarget.Custom -> webIntent(target.url)
    }

    /** Open [target]. Returns success. */
    fun open(context: Context, target: OpenTarget): Boolean = when (target) {
        is OpenTarget.Link -> open(context, target.link)
        is OpenTarget.Custom -> openUrl(context, target.url)
    }

    /** Open [link]; falls back to the plain https link if the app refuses it. Returns success. */
    fun open(context: Context, link: ServiceLink): Boolean {
        val intent = intentFor(context, link)
        return try {
            context.startActivity(intent)
            true
        } catch (_: ActivityNotFoundException) {
            openUrl(context, link.url)
        }
    }

    /** Open any web URL (segment deep links, sources). */
    fun openUrl(context: Context, url: String): Boolean = try {
        context.startActivity(webIntent(url))
        true
    } catch (e: ActivityNotFoundException) {
        Log.w(TAG, "no app opens links (${e.javaClass.simpleName})")
        false
    }

    /** The URI handed to the provider's app: `spotify:search:` for Spotify searches, else the link. */
    fun appUri(link: ServiceLink): Uri {
        if (link.service == Service.SPOTIFY && link.isSearch) {
            spotifySearchQuery(link.url)?.let { return ("spotify:search:" + Uri.encode(it)).toUri() }
        }
        return link.url.toUri()
    }

    /** The query of an `open.spotify.com/search/<query>` link (or `?q=`), decoded. */
    fun spotifySearchQuery(url: String): String? {
        val uri = runCatching { java.net.URI(url) }.getOrNull() ?: return null
        val path = uri.rawPath.orEmpty()
        val marker = "/search/"
        val decoded = if (path.contains(marker)) {
            // In a path "+" is literal; protect it from form decoding.
            decode(path.substringAfter(marker).trimEnd('/').replace("+", "%2B"))
        } else {
            uri.rawQuery?.split('&')?.firstOrNull { it.startsWith("q=") }?.let { decode(it.removePrefix("q=")) }
        }
        return decoded?.trim()?.takeIf { it.isNotEmpty() }
    }

    private fun webIntent(url: String): Intent =
        Intent(Intent.ACTION_VIEW, url.toUri())
            .addCategory(Intent.CATEGORY_BROWSABLE)
            .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)

    private fun decode(raw: String): String? = runCatching { URLDecoder.decode(raw, "UTF-8") }.getOrNull()

    private const val TAG = "ServiceLinkOpener"
}
