package app.musicfetch.platform

import android.content.Context
import android.content.pm.ApplicationInfo
import android.content.pm.PackageManager
import app.musicfetch.core.Service

/** The provider the user picked in the first-launch panel or in Settings › Open songs in. */
sealed interface ProviderChoice {
    data class Catalog(val service: Service) : ProviderChoice

    /** The user's own provider ([CustomProvider]). */
    data object Custom : ProviderChoice
}

/** A provider the app doesn't know: a name and a URL template with `{artist}`, `{title}`, `{query}`, `{isrc}`. */
data class CustomProvider(val name: String, val template: String) {
    /** Why a custom provider can't be saved yet. */
    enum class Problem { MissingName, MissingPlaceholder, NotALink }

    companion object {
        val placeholders = listOf("{artist}", "{title}", "{query}", "{isrc}")

        /**
         * What's wrong with [name] and [template], or null when they can be saved. [fill] is
         * `customProviderLink` for a sample track: null when the filled-in template isn't an
         * http(s) link.
         */
        fun problem(name: String, template: String, fill: (String) -> String?): Problem? = when {
            name.isBlank() -> Problem.MissingName
            placeholders.none { template.contains(it) } -> Problem.MissingPlaceholder
            fill(template.trim()) == null -> Problem.NotALink
            else -> null
        }
    }
}

/** Where results open: the user's choice, plus the custom provider when one is defined. */
data class ProviderPreference(val choice: ProviderChoice, val custom: CustomProvider?) {
    /** The name shown on the primary button, or null when it depends on the track's links. */
    val name: String?
        get() = when (choice) {
            is ProviderChoice.Catalog -> choice.service.displayName
            ProviderChoice.Custom -> custom?.name
        }
}

/**
 * The music providers songs can open in (docs/APPS.md, "Opening links"): the services
 * `provider_links` covers, their Android packages, and which of them are installed.
 */
object Providers {
    /** The decision Skip stores in the first-launch panel. */
    val skipChoice: Service = Service.SPOTIFY

    /** The most common services, offered first in the first-launch panel. */
    val common: List<Service> = listOf(
        Service.SPOTIFY, Service.YOU_TUBE_MUSIC, Service.APPLE_MUSIC, Service.TIDAL, Service.AMAZON_MUSIC,
        Service.DEEZER, Service.SOUND_CLOUD,
    )

    /** Every provider, in menu and Settings order. */
    val all: List<Service> = listOf(
        Service.SPOTIFY, Service.APPLE_MUSIC, Service.YOU_TUBE_MUSIC, Service.YOU_TUBE, Service.DEEZER,
        Service.TIDAL, Service.AMAZON_MUSIC, Service.SOUND_CLOUD, Service.QOBUZ, Service.PANDORA,
        Service.AUDIOMACK, Service.ANGHAMI, Service.JIO_SAAVN, Service.BOOMPLAY, Service.KKBOX,
        Service.YANDEX_MUSIC, Service.NET_EASE_MUSIC, Service.BANDCAMP, Service.SHAZAM, Service.LAST_FM,
        Service.GENIUS,
    )

    /** The providers that are not in [common], for "More…". */
    val more: List<Service> = all - common.toSet()

    /**
     * Android packages of the providers' apps, each declared in the manifest's `<queries>` so
     * [isInstalled] can see it. Last.fm and Genius always open in the browser.
     */
    val packages: Map<Service, String> = mapOf(
        Service.SPOTIFY to "com.spotify.music",
        Service.YOU_TUBE_MUSIC to "com.google.android.apps.youtube.music",
        Service.YOU_TUBE to "com.google.android.youtube",
        Service.APPLE_MUSIC to "com.apple.android.music",
        Service.DEEZER to "deezer.android.app",
        Service.TIDAL to "com.aspiro.tidal",
        Service.AMAZON_MUSIC to "com.amazon.mp3",
        Service.SOUND_CLOUD to "com.soundcloud.android",
        Service.QOBUZ to "com.qobuz.music",
        Service.PANDORA to "com.pandora.android",
        Service.AUDIOMACK to "com.audiomack",
        Service.ANGHAMI to "com.anghami",
        Service.JIO_SAAVN to "com.jio.media.jiobeats",
        Service.BOOMPLAY to "com.afmobi.boomplayer",
        Service.KKBOX to "com.skysoft.kkbox.android",
        Service.YANDEX_MUSIC to "ru.yandex.music",
        Service.NET_EASE_MUSIC to "com.netease.cloudmusic",
        Service.BANDCAMP to "com.bandcamp.android",
        Service.SHAZAM to "com.shazam.android",
    )

    /** Whether [service]'s app is installed and enabled. */
    fun isInstalled(context: Context, service: Service): Boolean = appInfo(context, service) != null

    /** The providers whose apps are installed. */
    fun installed(context: Context): Set<Service> = all.filterTo(mutableSetOf()) { isInstalled(context, it) }

    /**
     * The provider to pre-select in the first-launch panel: the first of [common] whose app the
     * user installed, else the first one that came with the phone (YouTube Music is preinstalled
     * on many phones, so it says little), else none.
     */
    fun suggested(context: Context): Service? {
        val found = common.mapNotNull { service -> appInfo(context, service)?.let { service to it } }
        return suggest(found.map { (service, info) -> service to (info.flags and ApplicationInfo.FLAG_SYSTEM != 0) })
    }

    /** [suggested] without the package manager: [found] pairs each installed service with "preinstalled". */
    fun suggest(found: List<Pair<Service, Boolean>>): Service? =
        found.firstOrNull { !it.second }?.first ?: found.firstOrNull()?.first

    /**
     * The provider to use before the user has decided (a result from the share sheet or a tile
     * before the app was first opened): what the panel would pre-select, else what Skip stores.
     */
    fun undecided(context: Context): ProviderPreference =
        ProviderPreference(ProviderChoice.Catalog(suggested(context) ?: skipChoice), null)

    private fun appInfo(context: Context, service: Service): ApplicationInfo? {
        val pkg = packages[service] ?: return null
        return try {
            context.packageManager.getApplicationInfo(pkg, 0).takeIf { it.enabled }
        } catch (_: PackageManager.NameNotFoundException) {
            null
        }
    }
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
