package app.musicfetch.ui.library

import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.SessionKind
import app.musicfetch.core.SessionStatus
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class LibraryRulesTest {
    private fun entry(id: String, at: Long, pinned: Boolean = false) = LibraryEntry(
        id = id,
        kind = SessionKind.LISTEN,
        status = SessionStatus.MATCHED,
        createdAtMs = at,
        title = id,
        subtitle = null,
        artworkUrl = null,
        track = null,
        trackCount = 1u,
        durationMs = null,
        sourceUrl = null,
        pinned = pinned,
    )

    @Test
    fun `selection toggles and prunes`() {
        assertEquals(setOf("a"), Selection.toggle(emptySet(), "a"))
        assertEquals(emptySet<String>(), Selection.toggle(setOf("a"), "a"))
        assertEquals(setOf("a", "b"), Selection.toggle(setOf("a"), "b"))
        val kept = setOf("a", "b")
        assertTrue(Selection.prune(kept, setOf("a", "b", "c")) === kept)
        assertEquals(setOf("b"), Selection.prune(setOf("a", "b"), setOf("b", "c")))
    }

    @Test
    fun `the bar unpins only when everything selected is pinned`() {
        assertTrue(Selection.unpins(listOf(entry("a", 1, pinned = true), entry("b", 2, pinned = true))))
        assertFalse(Selection.unpins(listOf(entry("a", 1, pinned = true), entry("b", 2))))
        assertFalse(Selection.unpins(emptyList()))
    }

    @Test
    fun `pinning moves rows into the pinned section, newest first`() {
        val a = entry("a", 30)
        val b = entry("b", 20)
        val c = entry("c", 10, pinned = true)
        val list = listOf(a, b, c)
        val states = mapOf("a" to true, "b" to true)
        val updated = LibraryLists.applyPins(list, states)
        assertEquals(listOf(true, true, true), updated.map { it.pinned })
        val known = list.associateBy { it.id }
        assertEquals(listOf("a", "b", "c"), LibraryLists.pinnedAfter(listOf(c), known, states).map { it.id })
        // Undo restores the earlier state of each row.
        val restored = LibraryLists.applyPins(updated, mapOf("a" to false, "b" to false))
        assertEquals(list, restored)
        assertEquals(listOf("c"), LibraryLists.pinnedAfter(listOf(a, b, c), known, mapOf("a" to false, "b" to false)).map { it.id })
    }

    @Test
    fun `pinned rows are not repeated under their day`() {
        val a = entry("a", 30, pinned = true)
        val b = entry("b", 20)
        assertEquals(listOf(b), LibraryLists.dayRows(listOf(a, b), listOf(a)))
        // Without the section (search, the Pinned filter) every row stays.
        assertEquals(listOf(a, b), LibraryLists.dayRows(listOf(a, b), emptyList()))
    }
}
