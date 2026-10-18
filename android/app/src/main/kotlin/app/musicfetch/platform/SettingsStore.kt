package app.musicfetch.platform

import android.content.Context
import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.MutablePreferences
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.booleanPreferencesKey
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.intPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import app.musicfetch.core.Service
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map

private val Context.settingsData: DataStore<Preferences> by preferencesDataStore(name = "settings")

/** The source toggle on the Listen stage. */
enum class ListenSource { Microphone, Phone, Hum }

/** App preferences. Credentials live in [CredentialStore]. */
data class AppSettings(
    /**
     * "Open songs in": null until the user chose a provider or skipped the first-launch panel
     * (docs/APPS.md, "Opening links").
     */
    val provider: ProviderChoice? = null,
    /** The user's own provider, kept while another one is selected. */
    val customProvider: CustomProvider? = null,
    /** Post a notification when a result arrives while the app is in the background. */
    val notifyInBackground: Boolean = true,
    /** "Stop automatically after a pause" while humming (`ListenOptions.auto_stop`). */
    val autoStop: Boolean = true,
    /** "Also search lyrics you sing": the on-device recognizer reads sung words (SungLyrics). */
    val sungLyrics: Boolean = true,
    /** Last selected source toggle; phone audio is never restored (it needs consent each time). */
    val listenSource: ListenSource = ListenSource.Microphone,
    /** The microphone explanation was shown once. */
    val micExplained: Boolean = false,
    /** The phone-audio explanation was shown once. */
    val phoneAudioExplained: Boolean = false,
    /** The melody-library prompt was declined; ask again only from Settings. */
    val melodyPromptDeclined: Boolean = false,
    /** Successful listens, for offering the Quick Settings tile after the second one. */
    val successfulListens: Int = 0,
    /** The "add tile" offer was made. */
    val tileOffered: Boolean = false,
) {
    /** The user's provider, or null while the first-launch panel is still undecided. */
    val preference: ProviderPreference?
        get() = provider?.let { choice ->
            if (choice == ProviderChoice.Custom && customProvider == null) null else ProviderPreference(choice, customProvider)
        }
}

/** DataStore-backed [AppSettings]. */
class SettingsStore(context: Context) {
    private val appContext = context.applicationContext
    private val data = appContext.settingsData

    val settings: Flow<AppSettings> = data.data.map { prefs ->
        AppSettings(
            provider = readProvider(prefs),
            customProvider = readCustom(prefs),
            notifyInBackground = prefs[NOTIFY] ?: true,
            autoStop = prefs[AUTO_STOP] ?: true,
            sungLyrics = prefs[SUNG_LYRICS] ?: true,
            listenSource = prefs[SOURCE]?.let { name -> ListenSource.entries.firstOrNull { it.name == name } }
                ?.takeIf { it != ListenSource.Phone } ?: ListenSource.Microphone,
            micExplained = prefs[MIC_EXPLAINED] ?: false,
            phoneAudioExplained = prefs[PHONE_EXPLAINED] ?: false,
            melodyPromptDeclined = prefs[MELODY_DECLINED] ?: false,
            successfulListens = prefs[SUCCESSFUL_LISTENS] ?: 0,
            tileOffered = prefs[TILE_OFFERED] ?: false,
        )
    }

    suspend fun current(): AppSettings = settings.first()

    /**
     * Where results open right now: the user's provider, or before they decided, what the
     * first-launch panel would pre-select (else what Skip stores).
     */
    suspend fun preference(): ProviderPreference = current().preference ?: Providers.undecided(appContext)

    /** Store the user's decision (a provider chosen in the panel or Settings, or Skip). */
    suspend fun setProvider(choice: ProviderChoice) = data.edit { writeProvider(it, choice) }

    /** Save the custom provider and select it. */
    suspend fun setCustomProvider(custom: CustomProvider) = data.edit { prefs ->
        prefs[CUSTOM_NAME] = custom.name
        prefs[CUSTOM_TEMPLATE] = custom.template
        writeProvider(prefs, ProviderChoice.Custom)
    }

    suspend fun setNotifyInBackground(on: Boolean) = data.edit { it[NOTIFY] = on }
    suspend fun setAutoStop(on: Boolean) = data.edit { it[AUTO_STOP] = on }
    suspend fun setSungLyrics(on: Boolean) = data.edit { it[SUNG_LYRICS] = on }
    suspend fun setListenSource(source: ListenSource) = data.edit { it[SOURCE] = source.name }
    suspend fun setMicExplained() = data.edit { it[MIC_EXPLAINED] = true }
    suspend fun setPhoneAudioExplained() = data.edit { it[PHONE_EXPLAINED] = true }
    suspend fun setMelodyPromptDeclined(declined: Boolean) = data.edit { it[MELODY_DECLINED] = declined }
    suspend fun setTileOffered() = data.edit { it[TILE_OFFERED] = true }

    /** Count a successful listen; returns the new total. */
    suspend fun recordSuccessfulListen(): Int {
        var total = 0
        data.edit { prefs ->
            total = (prefs[SUCCESSFUL_LISTENS] ?: 0) + 1
            prefs[SUCCESSFUL_LISTENS] = total
        }
        return total
    }

    private fun writeProvider(prefs: MutablePreferences, choice: ProviderChoice) {
        prefs[PROVIDER] = when (choice) {
            is ProviderChoice.Catalog -> choice.service.name
            ProviderChoice.Custom -> CUSTOM
        }
        prefs.remove(LEGACY_DEFAULT_SERVICE)
    }

    private fun readProvider(prefs: Preferences): ProviderChoice? {
        // Builds before the first-launch panel stored "default_service" only when changed.
        val stored = prefs[PROVIDER] ?: prefs[LEGACY_DEFAULT_SERVICE] ?: return null
        if (stored == CUSTOM) return ProviderChoice.Custom
        return Service.entries.firstOrNull { it.name == stored }?.let { ProviderChoice.Catalog(it) }
    }

    private fun readCustom(prefs: Preferences): CustomProvider? {
        val name = prefs[CUSTOM_NAME]?.takeIf { it.isNotBlank() } ?: return null
        val template = prefs[CUSTOM_TEMPLATE]?.takeIf { it.isNotBlank() } ?: return null
        return CustomProvider(name, template)
    }

    private companion object {
        const val CUSTOM = "custom"
        val PROVIDER = stringPreferencesKey("provider")
        val LEGACY_DEFAULT_SERVICE = stringPreferencesKey("default_service")
        val CUSTOM_NAME = stringPreferencesKey("custom_provider_name")
        val CUSTOM_TEMPLATE = stringPreferencesKey("custom_provider_template")
        val NOTIFY = booleanPreferencesKey("notify_background")
        val AUTO_STOP = booleanPreferencesKey("auto_stop")
        val SUNG_LYRICS = booleanPreferencesKey("sung_lyrics")
        val SOURCE = stringPreferencesKey("listen_source")
        val MIC_EXPLAINED = booleanPreferencesKey("mic_explained")
        val PHONE_EXPLAINED = booleanPreferencesKey("phone_audio_explained")
        val MELODY_DECLINED = booleanPreferencesKey("melody_prompt_declined")
        val SUCCESSFUL_LISTENS = intPreferencesKey("successful_listens")
        val TILE_OFFERED = booleanPreferencesKey("tile_offered")
    }
}
