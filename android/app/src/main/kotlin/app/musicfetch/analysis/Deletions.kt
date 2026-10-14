package app.musicfetch.analysis

import android.util.Log
import app.musicfetch.core.MfException
import app.musicfetch.engine.EngineHost
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

/** A deletion that can still be undone. */
class PendingDeletion internal constructor(val ids: List<String>) {
    internal var job: Job? = null
}

/**
 * Deletes sessions with a grace period (docs/design/DESIGN.md §6.8: "Deleted" + Undo for 5 s):
 * scheduled ids are hidden at once and removed from the engine when the period ends.
 */
class Deletions(
    private val scope: CoroutineScope,
    private val engineHost: EngineHost,
    private val onChanged: () -> Unit,
) {
    private val _hidden = MutableStateFlow<Set<String>>(emptySet())

    /** Sessions to leave out of lists. */
    val hidden: StateFlow<Set<String>> = _hidden.asStateFlow()

    private val _latest = MutableStateFlow<PendingDeletion?>(null)

    /** The newest deletion that can be undone (for the snackbar). */
    val latest: StateFlow<PendingDeletion?> = _latest.asStateFlow()

    /** Hide now, delete after [UNDO_MS] unless undone. */
    fun schedule(ids: List<String>) {
        if (ids.isEmpty()) return
        val pending = PendingDeletion(ids)
        _hidden.update { it + ids }
        _latest.value = pending
        pending.job = scope.launch {
            delay(UNDO_MS)
            commit(pending)
        }
        onChanged()
    }

    /** Delete without a grace period (confirmed deletions of mixes and pinned sessions). */
    fun deleteNow(ids: List<String>) {
        _hidden.update { it + ids }
        scope.launch { delete(ids) }
    }

    fun undo(pending: PendingDeletion) {
        pending.job?.cancel()
        _hidden.update { it - pending.ids.toSet() }
        if (_latest.value === pending) _latest.value = null
        onChanged()
    }

    /** The snackbar for [pending] was dismissed (the deletion still happens on schedule). */
    fun acknowledge(pending: PendingDeletion) {
        if (_latest.value === pending) _latest.value = null
    }

    private suspend fun commit(pending: PendingDeletion) {
        if (_latest.value === pending) _latest.value = null
        delete(pending.ids)
    }

    private suspend fun delete(ids: List<String>) {
        val engine = runCatching { engineHost.engine() }.getOrNull() ?: return
        try {
            kotlinx.coroutines.withContext(Dispatchers.IO) { engine.deleteSessions(ids) }
        } catch (e: MfException) {
            Log.w(TAG, "delete failed", e)
        }
        _hidden.update { it - ids.toSet() }
        onChanged()
    }

    companion object {
        private const val TAG = "Deletions"
        const val UNDO_MS = 5_000L
    }
}

/** A mix segment that a hum session should fill in once the user confirms a candidate. */
data class SegmentFix(val sessionId: String, val segmentId: String)
