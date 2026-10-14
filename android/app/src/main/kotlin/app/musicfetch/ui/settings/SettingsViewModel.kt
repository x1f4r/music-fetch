package app.musicfetch.ui.settings

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import app.musicfetch.appGraph
import app.musicfetch.core.Service
import app.musicfetch.core.StorageUsage
import app.musicfetch.core.coreVersion
import app.musicfetch.platform.AppSettings
import app.musicfetch.platform.CredentialField
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Settings (docs/APPS.md, `SettingsModel`). */
class SettingsViewModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph

    val settings: StateFlow<AppSettings?> = graph.settings.settings.stateIn(viewModelScope, SharingStarted.Eagerly, null)

    /** Which credential fields hold a value (the values themselves are never shown). */
    val credentialsSet: StateFlow<Set<CredentialField>> = graph.credentials.values.map { it.keys }
        .stateIn(viewModelScope, SharingStarted.Eagerly, emptySet())

    val melodyStatus = graph.melody.status
    val melodyDownload = graph.melody.download
    val melodyCheckFailed = graph.melody.checkFailed

    private val _storage = MutableStateFlow<StorageUsage?>(null)
    val storage: StateFlow<StorageUsage?> = _storage.asStateFlow()

    private val _engineVersion = MutableStateFlow("")
    val engineVersion: StateFlow<String> = _engineVersion.asStateFlow()

    init {
        graph.melody.refresh()
        graph.melody.check()
        refreshStorage()
        viewModelScope.launch(Dispatchers.IO) { _engineVersion.value = runCatching { coreVersion() }.getOrDefault("") }
    }

    fun setDefaultService(service: Service) {
        viewModelScope.launch { graph.settings.setDefaultService(service) }
    }

    fun setNotify(on: Boolean) {
        viewModelScope.launch { graph.settings.setNotifyInBackground(on) }
    }

    fun setCredential(field: CredentialField, value: String) {
        viewModelScope.launch { graph.credentials.set(field, value) }
    }

    fun downloadMelody() {
        viewModelScope.launch { graph.settings.setMelodyPromptDeclined(false) }
        graph.melody.install()
    }

    fun cancelMelody() = graph.melody.cancel()

    fun removeMelody() = graph.melody.remove()

    fun checkMelody() = graph.melody.check()

    fun refreshStorage() {
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            _storage.value = withContext(Dispatchers.IO) { engine.storageUsage() }
        }
    }

    fun clearCache() {
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            withContext(Dispatchers.IO) { runCatching { engine.clearCache() } }
            refreshStorage()
        }
    }
}
