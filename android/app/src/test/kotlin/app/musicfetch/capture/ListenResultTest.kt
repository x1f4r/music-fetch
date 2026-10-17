package app.musicfetch.capture

import app.musicfetch.core.ErrorCode
import app.musicfetch.core.NoMatchReason
import app.musicfetch.core.Outcome
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ListenResultTest {
    @Test
    fun `a capture saved offline waits`() {
        assertEquals(ListenResult.Waiting("s"), listenResult(Outcome.Waiting, "s", emptyList()))
    }

    @Test
    fun `no match keeps its reason`() {
        assertEquals(
            ListenResult.NoMatch("s", NoMatchReason.TOO_QUIET),
            listenResult(Outcome.NoMatch(NoMatchReason.TOO_QUIET), "s", emptyList()),
        )
    }

    @Test
    fun `outcomes a live session cannot have are failures`() {
        val completed = listenResult(Outcome.Completed(1u, 1u), "s", emptyList())
        assertTrue(completed is ListenResult.Failed && completed.code == ErrorCode.INTERNAL)
        // Without a session id there is nothing to wait for or to open.
        assertTrue(listenResult(Outcome.Waiting, null, emptyList()) is ListenResult.Failed)
        assertTrue(listenResult(Outcome.Candidates(2u), null, emptyList()) is ListenResult.Failed)
    }
}
