package app.musicfetch.platform

import app.musicfetch.core.Service
import app.musicfetch.core.ServiceLink
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class ServiceLinkOpenerTest {
    @Test
    fun `spotify search query is read from the path or the q parameter`() {
        val search = ServiceLink(Service.SPOTIFY, "https://open.spotify.com/search/Daft%20Punk%20Get%20Lucky", true)
        assertEquals("Daft Punk Get Lucky", ServiceLinkOpener.spotifySearchQuery(search.url))
        assertEquals("AC+DC", ServiceLinkOpener.spotifySearchQuery("https://open.spotify.com/search/AC+DC"))
        assertEquals("a b", ServiceLinkOpener.spotifySearchQuery("https://open.spotify.com/search?q=a+b"))
        assertNull(ServiceLinkOpener.spotifySearchQuery("https://open.spotify.com/track/abc"))
    }
}
