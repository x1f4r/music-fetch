package app.musicfetch.platform

import app.musicfetch.core.Service
import app.musicfetch.core.ServiceLink
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class OpenChoicesTest {
    private fun search(service: Service) = ServiceLink(service, "https://search.example/${service.name}", true)
    private fun exact(service: Service) = ServiceLink(service, "https://track.example/${service.name}", false)

    /** What `providerLinks` returns for a track without exact links: a search per provider. */
    private val searches = Providers.all.map(::search)

    private fun catalog(service: Service, custom: CustomProvider? = null) = ProviderPreference(ProviderChoice.Catalog(service), custom)

    @Test
    fun `the primary button opens the preferred provider, exact link first`() {
        val choices = OpenChoices.of(searches, listOf(exact(Service.YOU_TUBE_MUSIC)), catalog(Service.YOU_TUBE_MUSIC), null)
        assertEquals(OpenTarget.Link(exact(Service.YOU_TUBE_MUSIC)), choices.primary)

        val searchOnly = OpenChoices.of(searches, emptyList(), catalog(Service.TIDAL), null)
        assertEquals(OpenTarget.Link(search(Service.TIDAL)), searchOnly.primary)
    }

    @Test
    fun `the menu holds every other provider once, exact links before searches`() {
        val trackLinks = listOf(exact(Service.DEEZER), exact(Service.SPOTIFY), search(Service.SPOTIFY))
        val choices = OpenChoices.of(searches, trackLinks, catalog(Service.SPOTIFY), null)
        val services = choices.others.map { (it as OpenTarget.Link).link.service }
        assertEquals(Providers.all.size - 1, services.size)
        assertEquals(services.toSet().size, services.size)
        assertEquals(Service.DEEZER, services.first())
        assertEquals(exact(Service.DEEZER), (choices.others.first() as OpenTarget.Link).link)
        // After the exact link, the searches follow the provider order.
        assertEquals(Providers.all - Service.SPOTIFY - Service.DEEZER, services.drop(1))
    }

    @Test
    fun `a missing preferred provider falls back to an exact link, then to any link`() {
        val choices = OpenChoices.of(listOf(search(Service.DEEZER)), listOf(exact(Service.TIDAL)), catalog(Service.QOBUZ), null)
        assertEquals(OpenTarget.Link(exact(Service.TIDAL)), choices.primary)
        val none = OpenChoices.of(emptyList(), emptyList(), catalog(Service.QOBUZ), null)
        assertNull(none.primary)
        assertEquals(emptyList<OpenTarget>(), none.others)
    }

    @Test
    fun `the custom provider is the primary when chosen and in the menu otherwise`() {
        val custom = CustomProvider("My Radio", "https://radio.example/find?q={query}")
        val url = "https://radio.example/find?q=Artist%20Song"
        val chosen = OpenChoices.of(searches, emptyList(), ProviderPreference(ProviderChoice.Custom, custom), url)
        assertEquals(OpenTarget.Custom("My Radio", url), chosen.primary)
        assertEquals(Providers.all.size, chosen.others.size)

        val other = OpenChoices.of(searches, emptyList(), catalog(Service.SPOTIFY, custom), url)
        assertEquals(OpenTarget.Custom("My Radio", url), other.others.last())

        // A template that doesn't fill in leaves the custom provider out.
        val broken = OpenChoices.of(searches, emptyList(), ProviderPreference(ProviderChoice.Custom, custom), null)
        assertEquals(OpenTarget.Link(search(Service.SPOTIFY)), broken.primary)
    }

    @Test
    fun `links of sites that are not providers are ignored`() {
        val merged = OpenChoices.merge(listOf(search(Service.SPOTIFY)), listOf(exact(Service.MIXCLOUD), exact(Service.WEB)))
        assertEquals(listOf(search(Service.SPOTIFY)), merged)
    }
}
