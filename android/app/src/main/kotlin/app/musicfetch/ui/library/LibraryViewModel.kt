package app.musicfetch.ui.library

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import app.musicfetch.appGraph
import app.musicfetch.core.LibraryEntry
import app.musicfetch.core.LibraryQuery
import app.musicfetch.core.MfException
import app.musicfetch.core.SessionKind
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Library filters (docs/design/DESIGN.md §6.8). */
enum class LibraryFilter(val kinds: List<SessionKind>) {
    All(emptyList()),
    Heard(listOf(SessionKind.LISTEN, SessionKind.SYSTEM_AUDIO)),
    Hummed(listOf(SessionKind.HUM)),
    Links(listOf(SessionKind.LINK, SessionKind.FILE, SessionKind.LYRICS)),
    Mixes(listOf(SessionKind.LINK, SessionKind.FILE)),
}

/** The library list (docs/APPS.md, `LibraryModel`): paged, filtered, searchable. */
class LibraryViewModel(app: Application) : AndroidViewModel(app) {
    private val graph = app.appGraph
    private val _filter = MutableStateFlow(LibraryFilter.All)
    val filter: StateFlow<LibraryFilter> = _filter.asStateFlow()

    private val _search = MutableStateFlow("")
    val search: StateFlow<String> = _search.asStateFlow()

    private val _entries = MutableStateFlow<List<LibraryEntry>>(emptyList())
    private val _loaded = MutableStateFlow(false)

    /** Whether the first page arrived (the empty state waits for it). */
    val loaded: StateFlow<Boolean> = _loaded.asStateFlow()

    /** Entries minus the ones waiting to be deleted. */
    val entries: StateFlow<List<LibraryEntry>> = combine(_entries, graph.deletions.hidden) { list, hidden ->
        if (hidden.isEmpty()) list else list.filterNot { it.id in hidden }
    }.stateIn(viewModelScope, SharingStarted.Eagerly, emptyList())

    val deletions = graph.deletions

    private var endReached = false
    private var loadJob: Job? = null

    init {
        reload()
        viewModelScope.launch { graph.libraryChanged.collect { reload(keepCount = true) } }
        @OptIn(FlowPreview::class)
        viewModelScope.launch { _search.drop(1).debounce(250).collect { reload() } }
    }

    fun setFilter(filter: LibraryFilter) {
        if (_filter.value == filter) return
        _filter.value = filter
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
        loadJob = viewModelScope.launch { fetch(offset = 0, limit = limit, replace = true) }
    }

    private suspend fun fetch(offset: Int, limit: Int, replace: Boolean) {
        val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return
        val filter = _filter.value
        val query = LibraryQuery(
            kinds = filter.kinds,
            search = _search.value.trim().ifEmpty { null },
            pinnedOnly = false,
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

    fun setPinned(entry: LibraryEntry, pinned: Boolean) {
        viewModelScope.launch {
            val engine = runCatching { graph.engineHost.engine() }.getOrNull() ?: return@launch
            withContext(Dispatchers.IO) { runCatching { engine.setPinned(entry.id, pinned) } }
            graph.notifyLibraryChanged()
        }
    }

    /** Mixes and pinned sessions need a confirmation first. */
    fun needsConfirmation(entry: LibraryEntry): Boolean = entry.pinned || entry.trackCount >= 2u

    fun delete(entry: LibraryEntry) = graph.deletions.schedule(listOf(entry.id))

    fun deleteNow(entry: LibraryEntry) = graph.deletions.deleteNow(listOf(entry.id))

    private companion object {
        const val PAGE = 50
    }
}
