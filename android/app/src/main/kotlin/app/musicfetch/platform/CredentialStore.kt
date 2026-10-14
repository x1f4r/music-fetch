package app.musicfetch.platform

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import android.util.Log
import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import app.musicfetch.core.AcrCredentials
import app.musicfetch.core.Credentials
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import java.security.GeneralSecurityException
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

private val Context.credentialData: DataStore<Preferences> by preferencesDataStore(name = "credentials")

/** Which optional provider a credential belongs to. */
enum class CredentialField(internal val key: String) {
    AuddToken("audd_token"),
    AcrHost("acr_host"),
    AcrAccessKey("acr_key"),
    AcrAccessSecret("acr_secret"),
    AcrHumHost("acr_hum_host"),
    AcrHumAccessKey("acr_hum_key"),
    AcrHumAccessSecret("acr_hum_secret"),
}

/**
 * Credentials for the optional paid providers, stored in DataStore with every value encrypted
 * by an AES-256-GCM key that never leaves the Android Keystore. Values are never logged.
 *
 * Backups are off for the app, so a restored install never meets ciphertext it cannot decrypt;
 * if the key is lost anyway the affected values read as unset.
 */
class CredentialStore(context: Context) {
    private val data = context.applicationContext.credentialData

    /** Decrypted values by field; absent when unset. */
    val values: Flow<Map<CredentialField, String>> = data.data.map { prefs ->
        CredentialField.entries.mapNotNull { field ->
            prefs[stringPreferencesKey(field.key)]?.let { sealed -> open(sealed)?.let { field to it } }
        }.toMap()
    }

    /** The engine's view: providers whose required fields are all set. */
    val credentials: Flow<Credentials> = values.map(::toCredentials)

    suspend fun current(): Credentials = credentials.first()

    /** Set (or with a blank [value], clear) one field. */
    suspend fun set(field: CredentialField, value: String) {
        val trimmed = value.trim()
        data.edit { prefs ->
            val key = stringPreferencesKey(field.key)
            if (trimmed.isEmpty()) prefs.remove(key) else prefs[key] = seal(trimmed)
        }
    }

    private fun seal(plain: String): String {
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, key())
        val sealed = cipher.iv + cipher.doFinal(plain.toByteArray(Charsets.UTF_8))
        return Base64.encodeToString(sealed, Base64.NO_WRAP)
    }

    private fun open(sealed: String): String? = try {
        val bytes = Base64.decode(sealed, Base64.NO_WRAP)
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.DECRYPT_MODE, key(), GCMParameterSpec(TAG_BITS, bytes, 0, IV_BYTES))
        String(cipher.doFinal(bytes, IV_BYTES, bytes.size - IV_BYTES), Charsets.UTF_8)
    } catch (e: GeneralSecurityException) {
        Log.w(TAG, "a stored credential could not be decrypted (${e.javaClass.simpleName})")
        null
    } catch (e: IllegalArgumentException) {
        Log.w(TAG, "a stored credential is malformed")
        null
    }

    private fun key(): SecretKey {
        val keyStore = KeyStore.getInstance(KEYSTORE).apply { load(null) }
        (keyStore.getEntry(KEY_ALIAS, null) as? KeyStore.SecretKeyEntry)?.let { return it.secretKey }
        val generator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, KEYSTORE)
        generator.init(
            KeyGenParameterSpec.Builder(KEY_ALIAS, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build(),
        )
        return generator.generateKey()
    }

    companion object {
        private const val TAG = "CredentialStore"
        private const val KEYSTORE = "AndroidKeyStore"
        private const val KEY_ALIAS = "musicfetch.credentials"
        private const val TRANSFORMATION = "AES/GCM/NoPadding"
        private const val IV_BYTES = 12
        private const val TAG_BITS = 128

        /** Assemble [Credentials] from field values; incomplete providers are left out. */
        fun toCredentials(values: Map<CredentialField, String>): Credentials {
            fun acr(host: CredentialField, key: CredentialField, secret: CredentialField): AcrCredentials? {
                val h = values[host]?.takeIf { it.isNotBlank() } ?: return null
                val k = values[key]?.takeIf { it.isNotBlank() } ?: return null
                val s = values[secret]?.takeIf { it.isNotBlank() } ?: return null
                return AcrCredentials(host = h, accessKey = k, accessSecret = s)
            }
            return Credentials(
                auddToken = values[CredentialField.AuddToken]?.takeIf { it.isNotBlank() },
                acrFingerprint = acr(CredentialField.AcrHost, CredentialField.AcrAccessKey, CredentialField.AcrAccessSecret),
                acrHumming = acr(CredentialField.AcrHumHost, CredentialField.AcrHumAccessKey, CredentialField.AcrHumAccessSecret),
            )
        }
    }
}
