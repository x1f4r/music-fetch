package app.musicfetch.ui.library

import android.app.Application
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import app.musicfetch.appGraph
import app.musicfetch.core.ExportFormat
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.LibraryQuery
import app.musicfetch.core.MfException
import app.musicfetch.core.SessionKind
import app.musicfetch.ui.session.shareText
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Library filters (docs/design/DESIGN.md §6.8). */
enum class LibraryFilter(val kinds: List<SessionKind>, val pinnedOnly: Boolean = false) {
    All(emptyList()),
    Heard(listOf(SessionKind.LISTEN, SessionKind.SYSTEM_AUDIO)),
    Hummed(listOf(SessionKind.HUM)),
    Links(listOf(SessionKind.LINK, SessionKind.FILE, SessionKind.LYRICS)),
    Mixes(listOf(SessionKind.LINK, SessionKind.FILE)),
    Pinned(emptyList(), pinnedOnly = true),
}

/**
 * The library list (docs/APPS.md, `LibraryModel`): paged, filtered, searchable, with the pinned
 * sessions above the day groups and a selection for acting on several sessions at once.
 */
class LibraryViewModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph
    private val _filter = MutableStateFlow(LibraryFilter.All)
    val filter: StateFlow<LibraryFilter> = _filter.asStateFlow()

    private val _search = MutableStateFlow("")
    val search: StateFlow<String> = _search.asStateFlow()

    private val _entries = MutableStateFlow<List<LibraryEntry>>(emptyList())
    private val _pinned = MutableStateFlow<List<LibraryEntry>>(emptyList())
    private val _loaded = MutableStateFlow(false)
    private val _selection = MutableStateFlow<Set<String>>(emptySet())

    /** Whether the first page arrived (the empty state waits for it). */
    val loaded: StateFlow<Boolean> = _loaded.asStateFlow()

    /** Entries minus the ones waiting to be deleted. */
    val entries: StateFlow<List<LibraryEntry>> = combine(_entries, graph.deletions.hidden) { list, hidden ->
        if (hidden.isEmpty()) list else list.filterNot { it.id in hidden }
    }.stateIn(viewModelScope, SharingStarted.Eagerly, emptyList())

    /** Pinned sessions of the current filter, for the section above the day groups. */
    val pinned: StateFlow<List<LibraryEntry>> = combine(_pinned, graph.deletions.hidden) { list, hidden ->
        if (hidden.isEmpty()) list else list.filterNot { it.id in hidden }
    }.stateIn(viewModelScope, SharingStarted.Eagerly, emptyList())

    /** Selected session ids; empty when not selecting. */
    val selection: StateFlow<Set<String>> = _selection.asStateFlow()

    val deletions = graph.deletions

    private var endReached = false
    private var loadJob: Job? = null

    init {
        reload()
        viewModelScope.launch { graph.libraryChanged.collect { reload(keepCount = true) } }
        @OptIn(FlowPreview::class)
        viewModelScope.launch { _search.drop(1).debounce(250).collect { reload() } }
        // Sessions that went away (deleted, filtered out) leave the selection.
        viewModelScope.launch {
            combine(entries, pinned) { a, b -> (a + b).mapTo(HashSet()) { it.id } }.collect { visible ->
                _selection.update { Selection.prune(it, visible) }
            }
        }
    }

    fun setFilter(filter: LibraryFilter) {
        if (_filter.value == filter) return
        _filter.value = filter
        clearSelection()
        reload()
    }

    fun setSearch(text: String) {
        _search.value = text
    }

    /** Load the next page when the list nears its end. */
    fun loadMore() {
        if (endReached || loadJob?.isActive == true) return
        loadJob = viewModelScope.launch { fetch(offset = _entries.value.size, limit = PAGE, replace = false) }
    }

    private fun reload(keepCount: Boolean = false) {
        loadJob?.cancel()
        val limit = if (keepCount) maxOf(PAGE, _entries.value.size) else PAGE
        loadJob = viewModelScope.launch {
            fetchPinned()
            fetch(offset = 0, limit = limit, replace = true)
        }
    }

    private suspend fun fetch(offset: Int, limit: Int, replace: Boolean) {
        val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return
        val filter = _filter.value
        val query = LibraryQuery(
            kinds = filter.kinds,
            search = _search.value.trim().ifEmpty { null },
            pinnedOnly = filter.pinnedOnly,
            limit = limit.toUInt(),
            offset = offset.toUInt(),
        )
        val page = withContext(Dispatchers.IO) {
            try {
                engine.library(query)
            } catch (_: MfException) {
                emptyList()
            }
        }
        endReached = page.size < limit
        val visible = if (filter == LibraryFilter.Mixes) page.filter { it.trackCount >= 2u } else page
        _entries.value = if (replace) visible else (_entries.value + visible).distinctBy { it.id }
        _loaded.value = true
        // Sparse filtered pages: keep going until the screen has something to show.
        if (filter == LibraryFilter.Mixes && !endReached && _entries.value.size < PAGE / 2) {
            fetch(offset + page.size, PAGE, replace = false)
        }
    }

    /** All pinned sessions of the filter (there are few); the Pinned filter lists them itself. */
    private suspend fun fetchPinned() {
        val filter = _filter.value
        if (filter.pinnedOnly) {
            _pinned.value = emptyList()
            return
        }
        val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return
        val query = LibraryQuery(kinds = filter.kinds, search = null, pinnedOnly = true, limit = 0u, offset = 0u)
        val list = withContext(Dispatchers.IO) {
            try {
                engine.library(query)
            } catch (_: MfException) {
                emptyList()
            }
        }
        _pinned.value = if (filter == LibraryFilter.Mixes) list.filter { it.trackCount >= 2u } else list
    }

    /**
     * Pin or unpin [ids] at once (optimistically; the engine catches up). Returns the previous
     * state of each session that changed, for Undo ([restorePins]).
     */
    fun setPinned(ids: Collection<String>, pinned: Boolean): Map<String, Boolean> {
        val known = (_entries.value + _pinned.value).associateBy { it.id }
        val previous = ids.mapNotNull { id -> known[id]?.let { id to it.pinned } }.filter { it.second != pinned }.toMap()
        if (previous.isEmpty()) return previous
        applyPins(previous.mapValues { pinned })
        return previous
    }

    /** Undo [setPinned]. */
    fun restorePins(previous: Map<String, Boolean>) = applyPins(previous)

    private fun applyPins(states: Map<String, Boolean>) {
        if (states.isEmpty()) return
        val filter = _filter.value
        val known = (_entries.value + _pinned.value).associateBy { it.id }
        _entries.update { list ->
            val updated = LibraryLists.applyPins(list, states)
            if (filter.pinnedOnly) updated.filter { it.pinned } else updated
        }
        if (!filter.pinnedOnly) _pinned.update { LibraryLists.pinnedAfter(it, known, states) }
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            withContext(Dispatchers.IO) {
                for ((id, value) in states) {
                    try {
                        engine.setPinned(id, value)
                    } catch (e: MfException) {
                        Log.w(TAG, "pin failed", e)
                    }
                }
            }
            graph.notifyLibraryChanged()
        }
    }

    // Selection

    fun select(id: String) = _selection.update { it + id }

    fun toggleSelection(id: String) = _selection.update { Selection.toggle(it, id) }

    fun selectAll() {
        _selection.value = (pinned.value + entries.value).mapTo(LinkedHashSet()) { it.id }
    }

    fun clearSelection() {
        _selection.value = emptySet()
    }

    /** The selected sessions in list order. */
    fun selectedEntries(): List<LibraryEntry> {
        val ids = _selection.value
        return (pinned.value + entries.value).distinctBy { it.id }.filter { it.id in ids }
    }

    /** Mixes and pinned sessions need a confirmation first. */
    fun needsConfirmation(entries: Collection<LibraryEntry>): Boolean = entries.any { it.pinned || it.trackCount >= 2u }

    fun delete(ids: List<String>) = graph.deletions.schedule(ids)

    fun deleteNow(ids: List<String>) = graph.deletions.deleteNow(ids)

    /**
     * One text for sharing [entries]: "Title — Artist" and the best link for single tracks, the
     * tracklist for mixes and collections, separated by blank lines. Null when none has an answer.
     */
    suspend fun shareTextFor(entries: List<LibraryEntry>): String? {
        val engine = runCatching { graph.engineHost.engine() }.getOrNull()
        val parts = entries.mapNotNull { entry ->
            val track = entry.track
            when {
                entry.trackCount >= 2u && engine != null -> withContext(Dispatchers.IO) {
                    try {
                        engine.exportSession(entry.id, ExportFormat.TEXT).trim().ifEmpty { null }
                    } catch (e: MfException) {
                        Log.w(TAG, "export failed", e)
                        null
                    }
                }
                track != null -> shareText(track)
                else -> null
            }
        }
        return parts.takeIf { it.isNotEmpty() }?.joinToString("\n\n")
    }

    private companion object {
        const val TAG = "LibraryViewModel"
        const val PAGE = 50
    }
}

/** Selection rules, kept pure for tests. */
object Selection {
    fun toggle(selection: Set<String>, id: String): Set<String> = if (id in selection) selection - id else selection + id

    /** Drop ids that are no longer shown. */
    fun prune(selection: Set<String>, visible: Set<String>): Set<String> =
        if (selection.all { it in visible }) selection else selection.filterTo(LinkedHashSet()) { it in visible }

    /** The bar offers Unpin when every selected session is pinned, else Pin. */
    fun unpins(selected: Collection<LibraryEntry>): Boolean = selected.isNotEmpty() && selected.all { it.pinned }
}

/** List updates for pinning, kept pure for tests. */
object LibraryLists {
    fun applyPins(list: List<LibraryEntry>, states: Map<String, Boolean>): List<LibraryEntry> =
        list.map { entry -> states[entry.id]?.let { if (it != entry.pinned) entry.copy(pinned = it) else entry } ?: entry }

    /** The pinned section after [states]: unpinned sessions leave, newly pinned ones join, newest first. */
    fun pinnedAfter(pinned: List<LibraryEntry>, known: Map<String, LibraryEntry>, states: Map<String, Boolean>): List<LibraryEntry> {
        val kept = pinned.filter { states[it.id] != false }
        val keptIds = kept.mapTo(HashSet()) { it.id }
        val added = states.filter { it.value && it.key !in keptIds }.keys.mapNotNull { known[it]?.copy(pinned = true) }
        return (kept + added).sortedByDescending { it.createdAtMs }
    }

    /**
     * The rows under the day headers: everything, minus the sessions already shown in the
     * pinned section (when it is shown).
     */
    fun dayRows(entries: List<LibraryEntry>, pinnedShown: Collection<LibraryEntry>): List<LibraryEntry> {
        if (pinnedShown.isEmpty()) return entries
        val ids = pinnedShown.mapTo(HashSet()) { it.id }
        return entries.filterNot { it.id in ids || it.pinned }
    }
}
