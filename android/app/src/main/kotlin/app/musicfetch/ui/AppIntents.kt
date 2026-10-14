package app.musicfetch.ui

import android.content.Intent
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

    /** Show the Listen stage and start humming (the share sheet's "Hum it"). */
    data object Hum : AppAction

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

    fun offer(intent: Intent?) {
        parse(intent)?.let { _pending.value = it }
    }

    /** Act on [action]; [startHum] starts a hum session when the microphone may be used. */
    fun consume(action: AppAction, navigator: Navigator, startHum: () -> Unit) {
        _pending.compareAndSet(action, null)
        when (action) {
            is AppAction.OpenSession -> navigator.openSessionFromListen(action.id)
            is AppAction.Analyze -> {
                navigator.showListen()
                _linkToAnalyze.value = action.url
            }
            AppAction.ShowListen -> navigator.showListen()
            AppAction.Hum -> {
                navigator.showListen()
                startHum()
            }
            AppAction.Lyrics -> {
                navigator.showListen()
                navigator.push(LyricsRoute)
            }
        }
    }

    /** Identify [url] on the Listen stage the next time it is shown. */
    fun analyzeLater(url: String) {
        _linkToAnalyze.value = url
    }

    /** The Listen stage took the link. */
    fun takeLink(): String? = _linkToAnalyze.value.also { _linkToAnalyze.value = null }

    /**
     * Parse `musicfetch://session/<id>`, `musicfetch://analyze?url=…`, `musicfetch://listen`
     * (`?mode=hum`) and `musicfetch://lyrics`.
     */
    fun parse(intent: Intent?): AppAction? {
        if (intent?.action != Intent.ACTION_VIEW) return null
        val uri = intent.data ?: return null
        if (uri.scheme != "musicfetch") return null
        return when (uri.host) {
            "session" -> uri.pathSegments.firstOrNull()?.takeIf { it.isNotBlank() }?.let { AppAction.OpenSession(it) }
            "analyze" -> uri.getQueryParameter("url")?.takeIf { it.isNotBlank() }?.let { AppAction.Analyze(it) }
            "listen" -> if (uri.getQueryParameter("mode") == "hum") AppAction.Hum else AppAction.ShowListen
            "lyrics" -> AppAction.Lyrics
            else -> null
        }
    }
}
