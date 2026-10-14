package app.musicfetch.engine

import app.musicfetch.core.SessionEvent
import app.musicfetch.core.SessionObserver
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow

/**
 * A [SessionObserver] that hands engine events to coroutines.
 *
 * The engine calls [onEvent] on its worker threads and must never block, so events go into an
 * unbounded channel (a session emits a bounded number: ~30 levels and ~10 pitch batches per
 * second, a few hundred segments at most). [events] completes after the terminal event.
 *
 * Live and analysis sessions update their state holders directly from their observers
 * ([app.musicfetch.capture.ListenController], [app.musicfetch.analysis.AnalysisController]);
 * the bridge serves sessions that are simply collected, such as the melody-index download.
 */
class ObserverBridge : SessionObserver {
    private val channel = Channel<SessionEvent>(Channel.UNLIMITED)

    /** Events in emission order; collect once. */
    val events: Flow<SessionEvent> = channel.receiveAsFlow()

    override fun onEvent(event: SessionEvent) {
        channel.trySend(event)
        if (event.isTerminal) channel.close()
    }

    /** Ends [events] without a terminal event (the session could not be started). */
    fun close() {
        channel.close()
    }
}

/** `Finished`, `Failed` or `Canceled`: nothing follows. */
val SessionEvent.isTerminal: Boolean
    get() = this is SessionEvent.Finished || this is SessionEvent.Failed || this is SessionEvent.Canceled
