package app.musicfetch.ui.provider

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import app.musicfetch.appGraph
import app.musicfetch.core.Service
import app.musicfetch.core.Track
import app.musicfetch.platform.AppSettings
import app.musicfetch.platform.CustomProvider
import app.musicfetch.platform.ProviderChoice
import app.musicfetch.platform.Providers
import app.musicfetch.platform.ServiceLinkOpener
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

/** Installed apps, the stored choice and saving, for the first-launch panel and Settings. */
class ProviderModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph

    val settings: StateFlow<AppSettings?> = graph.settings.settings.stateIn(viewModelScope, SharingStarted.Eagerly, null)

    private val _installed = MutableStateFlow<Set<Service>?>(null)

    /** Providers whose apps are on this phone; null while the package manager is asked. */
    val installed: StateFlow<Set<Service>?> = _installed.asStateFlow()

    private val _suggested = MutableStateFlow<Service?>(null)

    /** What the first-launch panel pre-selects (an installed app), if anything. */
    val suggested: StateFlow<Service?> = _suggested.asStateFlow()

    init {
        viewModelScope.launch(Dispatchers.IO) {
            _suggested.value = Providers.suggested(app)
            _installed.value = Providers.installed(app)
        }
    }

    /** Store [choice] as the user's provider (this also ends the first-launch panel). */
    fun choose(choice: ProviderChoice) {
        viewModelScope.launch { graph.settings.setProvider(choice) }
    }

    /** Skip in the first-launch panel: Spotify, never asked again. */
    fun skip() = choose(ProviderChoice.Catalog(Providers.skipChoice))

    /** Save and select the custom provider; false when it isn't valid. */
    fun saveCustom(name: String, template: String): Boolean {
        if (problem(name, template) != null) return false
        val custom = CustomProvider(name.trim(), template.trim())
        viewModelScope.launch { graph.settings.setCustomProvider(custom) }
        return true
    }

    /** What keeps [name] and [template] from being saved, if anything. */
    fun problem(name: String, template: String): CustomProvider.Problem? =
        CustomProvider.problem(name, template) { ServiceLinkOpener.customLink(sampleTrack, it) }

    private companion object {
        val sampleTrack = Track(
            title = "Song",
            artist = "Artist",
            artists = emptyList(),
            album = null,
            isrc = "XX0000000000",
            durationMs = null,
            releaseYear = null,
            genre = null,
            artworkUrl = null,
            previewUrl = null,
            links = emptyList(),
            ids = emptyMap(),
            identityKey = "",
        )
    }
}
