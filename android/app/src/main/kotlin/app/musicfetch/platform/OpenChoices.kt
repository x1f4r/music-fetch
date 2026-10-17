package app.musicfetch.platform

import app.musicfetch.core.Service
import app.musicfetch.core.ServiceLink

/** Something a result can be opened in. */
sealed interface OpenTarget {
    /** A provider's track page, or its search for the track when [ServiceLink.isSearch]. */
    data class Link(val link: ServiceLink) : OpenTarget

    /** The user's custom provider, filled in for this track. */
    data class Custom(val name: String, val url: String) : OpenTarget
}

/**
 * The "Open in …" split button (docs/APPS.md, "Opening links"): [primary] opens the preferred
 * provider; [others] are every other provider, exact links before searches.
 */
data class OpenChoices(val primary: OpenTarget?, val others: List<OpenTarget>) {
    companion object {
        /**
         * Choices for a track. [providerLinks] is `providerLinks(track)` (one link per provider);
         * [trackLinks] are the links the engine found for the track, which win when they are
         * exact. [customUrl] is the custom provider's link for this track, if one is defined and
         * its template fills in.
         */
        fun of(
            providerLinks: List<ServiceLink>,
            trackLinks: List<ServiceLink>,
            preference: ProviderPreference,
            customUrl: String?,
        ): OpenChoices {
            val links = merge(providerLinks, trackLinks)
            val custom = preference.custom?.let { c -> customUrl?.let { OpenTarget.Custom(c.name, it) } }
            val primary: OpenTarget? = when (val choice = preference.choice) {
                is ProviderChoice.Catalog -> links.firstOrNull { it.service == choice.service }?.let { OpenTarget.Link(it) }
                ProviderChoice.Custom -> custom
            } ?: links.firstOrNull { !it.isSearch }?.let { OpenTarget.Link(it) }
                ?: links.firstOrNull()?.let { OpenTarget.Link(it) }
                ?: custom
            val others = buildList<OpenTarget> {
                val rest = links.filter { (primary as? OpenTarget.Link)?.link?.service != it.service }
                rest.filter { !it.isSearch }.forEach { add(OpenTarget.Link(it)) }
                rest.filter { it.isSearch }.forEach { add(OpenTarget.Link(it)) }
                if (custom != null && primary != custom) add(custom)
            }
            return OpenChoices(primary, others)
        }

        /** One link per provider in [Providers.all] order; an exact link beats a search. */
        fun merge(providerLinks: List<ServiceLink>, trackLinks: List<ServiceLink>): List<ServiceLink> {
            val candidates = (trackLinks + providerLinks).filter { it.service in providerOrder }
            val best = candidates.groupBy { it.service }.mapValues { (_, list) -> list.firstOrNull { !it.isSearch } ?: list.first() }
            return best.values.sortedBy { providerOrder.getValue(it.service) }
        }

        private val providerOrder: Map<Service, Int> = Providers.all.withIndex().associate { (i, s) -> s to i }
    }
}
