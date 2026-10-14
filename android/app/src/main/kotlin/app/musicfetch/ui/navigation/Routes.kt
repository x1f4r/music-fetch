package app.musicfetch.ui.navigation

import androidx.navigation3.runtime.NavKey
import kotlinx.serialization.Serializable

/** Screens (docs/design/DESIGN.md §3.2). Saved with the back stacks, so they survive process death. */
@Serializable
sealed interface Route : NavKey

@Serializable
data object ListenRoute : Route

@Serializable
data object LibraryRoute : Route

/** Result, candidates, mix or collection, by what the session holds. */
@Serializable
data class SessionRoute(val id: String) : Route

/** Typed lyric search. */
@Serializable
data object LyricsRoute : Route

@Serializable
data object SettingsRoute : Route

@Serializable
data object LicensesRoute : Route

/** The two navigation destinations, each with its own back stack. */
enum class Tab { Listen, Library }
