package app.musicfetch.ui

import android.content.Intent
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.navigation.LyricsRoute
import app.musicfetch.ui.navigation.Navigator
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/** What a deep link or notification asks the app to do. */
sealed interface AppAction {
    data class OpenSession(val id: String) : AppAction

    data class Analyze(val url: String) : AppAction

    data object ShowListen : AppAction

    /** Show the Listen stage and start listening with [source] (the share sheet's "Hum it"). */
    data class Listen(val source: ListenSource) : AppAction

    /** Typed lyric search (the share sheet's "Type a lyric"). */
    data object Lyrics : AppAction
}

/** Turns incoming intents into navigation, once they reach the UI. */
object AppIntents {
    private val _pending = MutableStateFlow<AppAction?>(null)
    val pending: StateFlow<AppAction?> = _pending.asStateFlow()

    private val _linkToAnalyze = MutableStateFlow<String?>(null)

    /** A link the Listen stage should identify (from `musicfetch://analyze`). */
    val linkToAnalyze: StateFlow<String?> = _linkToAnalyze.asStateFlow()

    private val _listenRequest = MutableStateFlow<ListenSource?>(null)

    /**
     * A listen the Listen stage should start as soon as it is shown ("Listen again", "Hum it").
     * The stage starts it through its own permission and consent handling.
     */
    val listenRequest: StateFlow<ListenSource?> = _listenRequest.asStateFlow()

    fun offer(intent: Intent?) {
        parse(intent)?.let { _pending.value = it }
    }

    /** Act on [action]. */
    fun consume(action: AppAction, navigator: Navigator) {
        _pending.compareAndSet(action, null)
        when (action) {
            is AppAction.OpenSession -> navigator.openSessionFromListen(action.id)
            is AppAction.Analyze -> {
                navigator.showListen()
                _linkToAnalyze.value = action.url
            }
            AppAction.ShowListen -> navigator.showListen()
            is AppAction.Listen -> listenNow(action.source, navigator)
            AppAction.Lyrics -> {
                navigator.showListen()
                navigator.push(LyricsRoute)
            }
        }
    }

    /** Show the Listen stage and start listening with [source] there. */
    fun listenNow(source: ListenSource, navigator: Navigator) {
        _listenRequest.value = source
        navigator.showListen()
    }

    /** The Listen stage took the request. */
    fun takeListen(): ListenSource? = _listenRequest.value.also { _listenRequest.value = null }

    /** Identify [url] on the Listen stage the next time it is shown. */
    fun analyzeLater(url: String) {
        _linkToAnalyze.value = url
    }

    /** The Listen stage took the link. */
    fun takeLink(): String? = _linkToAnalyze.value.also { _linkToAnalyze.value = null }

    /**
     * Parse `musicfetch://session/<id>`, `musicfetch://analyze?url=…`, `musicfetch://listen`
     * (`?mode=hum`, or `?source=mic|phone|hum` to start listening) and `musicfetch://lyrics`.
     */
    fun parse(intent: Intent?): AppAction? {
        if (intent?.action != Intent.ACTION_VIEW) return null
        val uri = intent.data ?: return null
        if (uri.scheme != "musicfetch") return null
        return when (uri.host) {
            "session" -> uri.pathSegments.firstOrNull()?.takeIf { it.isNotBlank() }?.let { AppAction.OpenSession(it) }
            "analyze" -> uri.getQueryParameter("url")?.takeIf { it.isNotBlank() }?.let { AppAction.Analyze(it) }
            "listen" -> listenSource(uri.getQueryParameter("mode"), uri.getQueryParameter("source"))
                ?.let { AppAction.Listen(it) } ?: AppAction.ShowListen
            "lyrics" -> AppAction.Lyrics
            else -> null
        }
    }

    /** The source a `musicfetch://listen` link asks for, if it asks to start at all. */
    internal fun listenSource(mode: String?, source: String?): ListenSource? = when {
        mode == "hum" || source == "hum" -> ListenSource.Hum
        source == "mic" -> ListenSource.Microphone
        source == "phone" -> ListenSource.Phone
        else -> null
    }

    /** `musicfetch://listen?source=…` for [source]. */
    fun listenUri(source: ListenSource): String = "musicfetch://listen?source=" + when (source) {
        ListenSource.Microphone -> "mic"
        ListenSource.Phone -> "phone"
        ListenSource.Hum -> "hum"
    }
}
