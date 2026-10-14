package app.musicfetch.platform

import android.content.Context
import androidx.datastore.core.DataStore
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
    /** "Open songs in". */
    val defaultService: Service = Service.SPOTIFY,
    /** Post a notification when a result arrives while the app is in the background. */
    val notifyInBackground: Boolean = true,
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
)

/** DataStore-backed [AppSettings]. */
class SettingsStore(context: Context) {
    private val data = context.applicationContext.settingsData

    val settings: Flow<AppSettings> = data.data.map { prefs ->
        AppSettings(
            defaultService = prefs[DEFAULT_SERVICE]?.let { name -> Service.entries.firstOrNull { it.name == name } }
                ?: Service.SPOTIFY,
            notifyInBackground = prefs[NOTIFY] ?: true,
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

    suspend fun setDefaultService(service: Service) = data.edit { it[DEFAULT_SERVICE] = service.name }
    suspend fun setNotifyInBackground(on: Boolean) = data.edit { it[NOTIFY] = on }
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

    private companion object {
        val DEFAULT_SERVICE = stringPreferencesKey("default_service")
        val NOTIFY = booleanPreferencesKey("notify_background")
        val SOURCE = stringPreferencesKey("listen_source")
        val MIC_EXPLAINED = booleanPreferencesKey("mic_explained")
        val PHONE_EXPLAINED = booleanPreferencesKey("phone_audio_explained")
        val MELODY_DECLINED = booleanPreferencesKey("melody_prompt_declined")
        val SUCCESSFUL_LISTENS = intPreferencesKey("successful_listens")
        val TILE_OFFERED = booleanPreferencesKey("tile_offered")
    }
}
