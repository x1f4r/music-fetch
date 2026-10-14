package app.musicfetch.platform

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class CredentialStoreTest {
    @Test
    fun `a provider is only passed on when all its fields are set`() {
        val partial = CredentialStore.toCredentials(
            mapOf(
                CredentialField.AcrHost to "identify-eu-west-1.acrcloud.com",
                CredentialField.AcrAccessKey to "key",
            ),
        )
        assertNull(partial.acrFingerprint)

        val complete = CredentialStore.toCredentials(
            mapOf(
                CredentialField.AcrHost to "identify-eu-west-1.acrcloud.com",
                CredentialField.AcrAccessKey to "key",
                CredentialField.AcrAccessSecret to "secret",
                CredentialField.AuddToken to "token",
            ),
        )
        assertNotNull(complete.acrFingerprint)
        assertEquals("secret", complete.acrFingerprint?.accessSecret)
        assertEquals("token", complete.auddToken)
        assertNull(complete.acrHumming)
    }

    @Test
    fun `blank values count as unset`() {
        val credentials = CredentialStore.toCredentials(mapOf(CredentialField.AuddToken to "  "))
        assertNull(credentials.auddToken)
    }
}
