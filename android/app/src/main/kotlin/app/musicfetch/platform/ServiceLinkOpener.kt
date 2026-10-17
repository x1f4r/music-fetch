package app.musicfetch.platform

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.util.Log
import androidx.core.net.toUri
import app.musicfetch.core.Service
import app.musicfetch.core.ServiceLink
import java.net.URLDecoder

/**
 * Opens track links in the user's music app (docs/APPS.md, "Opening links").
 *
 * The link for the default service wins, then the first exact link, then the first search link.
 * When the service's app is installed the https link is handed to it directly with
 * `setPackage`; Spotify search links use the `spotify:search:` form. Otherwise the link opens
 * normally (browser or App Links).
 */
object ServiceLinkOpener {
    /** Display order of the "Open in" menu. */
    val menuOrder: List<Service> = listOf(
        Service.SPOTIFY, Service.APPLE_MUSIC, Service.YOU_TUBE_MUSIC, Service.YOU_TUBE, Service.DEEZER,
        Service.TIDAL, Service.AMAZON_MUSIC, Service.SOUND_CLOUD, Service.SHAZAM,
    )

    /** Services offered in Settings › Open songs in. */
    val defaultChoices: List<Service> = menuOrder - Service.SHAZAM

    /** Android packages of the services' apps (declared in the manifest's `<queries>`). */
    val packages: Map<Service, String> = mapOf(
        Service.SPOTIFY to "com.spotify.music",
        Service.YOU_TUBE_MUSIC to "com.google.android.apps.youtube.music",
        Service.YOU_TUBE to "com.google.android.youtube",
        Service.APPLE_MUSIC to "com.apple.android.music",
        Service.DEEZER to "deezer.android.app",
        Service.TIDAL to "com.aspiro.tidal",
        Service.AMAZON_MUSIC to "com.amazon.mp3",
        Service.SOUND_CLOUD to "com.soundcloud.android",
        Service.SHAZAM to "com.shazam.android",
    )

    /** The link to open for [preferred]: its own link, else the first exact link, else any. */
    fun pick(links: List<ServiceLink>, preferred: Service): ServiceLink? =
        links.firstOrNull { it.service == preferred && !it.isSearch }
            ?: links.firstOrNull { it.service == preferred }
            ?: links.firstOrNull { !it.isSearch }
            ?: links.firstOrNull()

    /** Menu entries in [menuOrder] (then anything else), one per service, exact links first. */
    fun menu(links: List<ServiceLink>): List<ServiceLink> {
        val byService = links.groupBy { it.service }.mapValues { (_, list) -> list.sortedBy { it.isSearch }.first() }
        val ordered = menuOrder.mapNotNull { byService[it] }
        return ordered + byService.values.filter { it.service !in menuOrder && it.service != Service.WEB } +
            listOfNotNull(byService[Service.WEB])
    }

    /** Intent that opens [link], in the service's app when it is installed. */
    fun intentFor(context: Context, link: ServiceLink): Intent {
        val pkg = packages[link.service]?.takeIf { isInstalled(context, it) }
        val uri = if (pkg != null) appUri(link) else link.url.toUri()
        return Intent(Intent.ACTION_VIEW, uri).apply {
            addCategory(Intent.CATEGORY_BROWSABLE)
            if (pkg != null) setPackage(pkg)
            addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        }
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
        context.startActivity(
            Intent(Intent.ACTION_VIEW, url.toUri())
                .addCategory(Intent.CATEGORY_BROWSABLE)
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK),
        )
        true
    } catch (e: ActivityNotFoundException) {
        Log.w(TAG, "no app opens links (${e.javaClass.simpleName})")
        false
    }

    /** Whether [pkg] is installed and launchable (visible through the manifest's `<queries>`). */
    fun isInstalled(context: Context, pkg: String): Boolean =
        context.packageManager.getLaunchIntentForPackage(pkg) != null

    /** The URI handed to the service's app: `spotify:search:` for Spotify searches, else the link. */
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

    private fun decode(raw: String): String? = runCatching { URLDecoder.decode(raw, "UTF-8") }.getOrNull()

    private const val TAG = "ServiceLinkOpener"
}

/** Human-readable service names (brand names are not translated). */
val Service.displayName: String
    get() = when (this) {
        Service.SPOTIFY -> "Spotify"
        Service.APPLE_MUSIC -> "Apple Music"
        Service.YOU_TUBE_MUSIC -> "YouTube Music"
        Service.YOU_TUBE -> "YouTube"
        Service.DEEZER -> "Deezer"
        Service.TIDAL -> "TIDAL"
        Service.AMAZON_MUSIC -> "Amazon Music"
        Service.SOUND_CLOUD -> "SoundCloud"
        Service.BANDCAMP -> "Bandcamp"
        Service.MIXCLOUD -> "Mixcloud"
        Service.VIMEO -> "Vimeo"
        Service.TIK_TOK -> "TikTok"
        Service.INSTAGRAM -> "Instagram"
        Service.FACEBOOK -> "Facebook"
        Service.X -> "X"
        Service.REDDIT -> "Reddit"
        Service.TWITCH -> "Twitch"
        Service.SHAZAM -> "Shazam"
        Service.GENIUS -> "Genius"
        Service.QOBUZ -> "Qobuz"
        Service.PANDORA -> "Pandora"
        Service.AUDIOMACK -> "Audiomack"
        Service.ANGHAMI -> "Anghami"
        Service.JIO_SAAVN -> "JioSaavn"
        Service.BOOMPLAY -> "Boomplay"
        Service.KKBOX -> "KKBOX"
        Service.YANDEX_MUSIC -> "Yandex Music"
        Service.NET_EASE_MUSIC -> "NetEase Cloud Music"
        Service.LAST_FM -> "Last.fm"
        Service.WEB -> "the web"
    }
