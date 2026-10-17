package app.musicfetch.platform

import app.musicfetch.core.Service
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ProvidersTest {
    @Test
    fun `every provider has a name and the lists cover each one once`() {
        assertEquals(Providers.all.size, Providers.all.toSet().size)
        assertTrue(Providers.all.containsAll(Providers.common))
        assertEquals(Providers.all.toSet(), (Providers.common + Providers.more).toSet())
        assertTrue(Providers.all.all { it.displayName.isNotBlank() })
        assertTrue(Providers.packages.keys.all { it in Providers.all })
        assertEquals(Service.SPOTIFY, Providers.skipChoice)
    }

    @Test
    fun `an app the user installed beats a preinstalled one`() {
        val found = listOf(Service.YOU_TUBE_MUSIC to true, Service.DEEZER to false)
        assertEquals(Service.DEEZER, Providers.suggest(found))
        assertEquals(Service.YOU_TUBE_MUSIC, Providers.suggest(listOf(Service.YOU_TUBE_MUSIC to true)))
        assertNull(Providers.suggest(emptyList()))
    }

    @Test
    fun `custom providers need a name, a placeholder and an http link`() {
        val fill: (String) -> String? = { template ->
            template.replace("{query}", "Artist%20Song").takeIf { it.startsWith("https://") || it.startsWith("http://") }
        }
        assertNull(CustomProvider.problem("Radio", "https://radio.example/?q={query}", fill))
        assertEquals(CustomProvider.Problem.MissingName, CustomProvider.problem(" ", "https://radio.example/?q={query}", fill))
        assertEquals(CustomProvider.Problem.MissingPlaceholder, CustomProvider.problem("Radio", "https://radio.example/", fill))
        assertEquals(CustomProvider.Problem.NotALink, CustomProvider.problem("Radio", "radio://find/{query}", fill))
    }

    @Test
    fun `the preference waits for a decision`() {
        assertNull(AppSettings().preference)
        val spotify = AppSettings(provider = ProviderChoice.Catalog(Service.SPOTIFY))
        assertEquals(ProviderPreference(ProviderChoice.Catalog(Service.SPOTIFY), null), spotify.preference)
        // A custom choice without its template is no decision.
        assertNull(AppSettings(provider = ProviderChoice.Custom).preference)
        val custom = CustomProvider("Radio", "https://radio.example/?q={query}")
        assertEquals("Radio", AppSettings(provider = ProviderChoice.Custom, customProvider = custom).preference?.name)
    }
}
