package app.musicfetch.platform

import app.musicfetch.core.Service
import app.musicfetch.core.ServiceLink
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class ServiceLinkOpenerTest {
    private val spotifySearch = ServiceLink(Service.SPOTIFY, "https://open.spotify.com/search/Daft%20Punk%20Get%20Lucky", true)
    private val spotifyTrack = ServiceLink(Service.SPOTIFY, "https://open.spotify.com/track/abc", false)
    private val deezer = ServiceLink(Service.DEEZER, "https://www.deezer.com/track/1", false)
    private val youTubeSearch = ServiceLink(Service.YOU_TUBE, "https://www.youtube.com/results?search_query=x", true)
    private val web = ServiceLink(Service.WEB, "https://www.google.com/search?q=x", true)

    @Test
    fun `the default service wins, exact before search`() {
        assertEquals(spotifyTrack, ServiceLinkOpener.pick(listOf(deezer, spotifySearch, spotifyTrack), Service.SPOTIFY))
        assertEquals(spotifySearch, ServiceLinkOpener.pick(listOf(deezer, spotifySearch), Service.SPOTIFY))
    }

    @Test
    fun `without the default service the first exact link is used, then any`() {
        assertEquals(deezer, ServiceLinkOpener.pick(listOf(youTubeSearch, deezer), Service.APPLE_MUSIC))
        assertEquals(youTubeSearch, ServiceLinkOpener.pick(listOf(youTubeSearch), Service.APPLE_MUSIC))
        assertNull(ServiceLinkOpener.pick(emptyList(), Service.SPOTIFY))
    }

    @Test
    fun `menu follows the service order, one entry per service, web last`() {
        val menu = ServiceLinkOpener.menu(listOf(web, deezer, youTubeSearch, spotifySearch, spotifyTrack))
        assertEquals(listOf(spotifyTrack, youTubeSearch, deezer, web), menu)
    }

    @Test
    fun `spotify search query is read from the path or the q parameter`() {
        assertEquals("Daft Punk Get Lucky", ServiceLinkOpener.spotifySearchQuery(spotifySearch.url))
        assertEquals("AC+DC", ServiceLinkOpener.spotifySearchQuery("https://open.spotify.com/search/AC+DC"))
        assertEquals("a b", ServiceLinkOpener.spotifySearchQuery("https://open.spotify.com/search?q=a+b"))
        assertNull(ServiceLinkOpener.spotifySearchQuery("https://open.spotify.com/track/abc"))
    }
}
