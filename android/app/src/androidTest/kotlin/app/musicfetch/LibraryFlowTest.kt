package app.musicfetch

import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.state.ToggleableState
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.SemanticsNodeInteraction
import androidx.compose.ui.test.hasTestTag
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.longClick
import androidx.compose.ui.test.onAllNodesWithTag
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTouchInput
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.filters.LargeTest
import org.junit.Assert.assertNotEquals
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/**
 * The library's selection and the visible pin, against the library on the device (seed one
 * with tools/android-ux/push-seed.sh). Skipped when the library has fewer than two sessions.
 */
@OptIn(ExperimentalTestApi::class)
@LargeTest
@RunWith(AndroidJUnit4::class)
class LibraryFlowTest {
    @get:Rule
    val compose = createAndroidComposeRule<MainActivity>()

    private val row = SemanticsMatcher("library row") {
        it.config.getOrNull(SemanticsProperties.TestTag)?.startsWith("library.row.") == true
    }

    @Before
    fun openLibrary() {
        compose.waitUntil(30_000) {
            compose.onAllNodes(hasTestTag("nav.library") or hasTestTag("provider.skip")).fetchSemanticsNodes().isNotEmpty()
        }
        if (compose.onAllNodesWithTag("provider.skip").fetchSemanticsNodes().isNotEmpty()) {
            compose.onNodeWithTag("provider.skip").performClick()
        }
        compose.waitUntilAtLeastOneExists(hasTestTag("nav.library"), 30_000)
        compose.onNodeWithTag("nav.library").performClick()
        compose.waitUntil(30_000) { compose.onAllNodes(row).fetchSemanticsNodes().size >= 2 }
    }

    private fun rows(): List<SemanticsNodeInteraction> {
        val count = compose.onAllNodes(row).fetchSemanticsNodes().size
        return (0 until count).map { compose.onAllNodes(row)[it] }
    }

    @Test
    fun longPressSelectsAndBackEndsTheSelection() {
        val rows = rows()
        assumeTrue(rows.size >= 2)
        rows[0].performTouchInput { longClick() }
        compose.onNodeWithText("1 selected").assertExists()
        rows[1].performClick()
        compose.onNodeWithText("2 selected").assertExists()
        compose.onNodeWithTag("library.selection.delete").assertExists()
        compose.runOnUiThread { compose.activity.onBackPressedDispatcher.onBackPressed() }
        compose.waitUntilDoesNotExist(hasText("2 selected"), 5_000)
        // Still in the Library: Back ended the selection, not the destination.
        compose.onNodeWithTag("library.search").assertExists()
    }

    @Test
    fun theResultPinIsOneTapAndSticks() {
        val rows = rows()
        assumeTrue(rows.isNotEmpty())
        rows[0].performClick()
        compose.waitUntil(30_000) {
            compose.onAllNodes(hasTestTag("session.pin") or hasTestTag("session.back")).fetchSemanticsNodes().isNotEmpty()
        }
        assumeTrue(compose.onAllNodesWithTag("session.pin").fetchSemanticsNodes().isNotEmpty())
        val pin = compose.onNodeWithTag("session.pin")
        val before = pin.fetchSemanticsNode().config[SemanticsProperties.ToggleableState]
        pin.performClick()
        compose.waitForIdle()
        val after = compose.onNodeWithTag("session.pin").fetchSemanticsNode().config[SemanticsProperties.ToggleableState]
        assertNotEquals(before, after)
        // Put it back as it was.
        compose.onNodeWithTag("session.pin").performClick()
        compose.waitUntil(5_000) {
            compose.onNodeWithTag("session.pin").fetchSemanticsNode().config[SemanticsProperties.ToggleableState] == before
        }
        assumeTrue(before == ToggleableState.On || before == ToggleableState.Off)
    }
}
