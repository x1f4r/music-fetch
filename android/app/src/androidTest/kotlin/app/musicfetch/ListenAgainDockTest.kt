package app.musicfetch

import androidx.compose.material3.SnackbarHostState
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.test.ext.junit.runners.AndroidJUnit4
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.AppIntents
import app.musicfetch.ui.common.AppSnackbar
import app.musicfetch.ui.common.LocalAppSnackbar
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.Navigator
import app.musicfetch.ui.navigation.Tab
import app.musicfetch.ui.navigation.rememberNavigator
import app.musicfetch.ui.session.ListenAgainDock
import app.musicfetch.ui.theme.MusicFetchTheme
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/** "Listen again" under a result: one tap goes back to the stage with that source requested. */
@RunWith(AndroidJUnit4::class)
class ListenAgainDockTest {
    @get:Rule
    val compose = createComposeRule()

    private lateinit var navigator: Navigator

    private fun showDock() {
        compose.setContent {
            MusicFetchTheme {
                val (nav, _) = rememberNavigator()
                navigator = nav
                val scope = rememberCoroutineScope()
                val snackbar = remember { AppSnackbar(SnackbarHostState(), scope) }
                CompositionLocalProvider(LocalNavigator provides nav, LocalAppSnackbar provides snackbar) {
                    ListenAgainDock()
                }
            }
        }
    }

    @After
    fun clearRequest() {
        AppIntents.takeListen()
    }

    @Test
    fun humGoesToTheStageAndAsksForHumming() {
        showDock()
        compose.runOnIdle { navigator.openSessionFromListen("result") }
        compose.onNodeWithContentDescription("Listen again").assertExists()
        compose.onNodeWithTag("listenAgain.hum").assertIsEnabled().performClick()
        compose.runOnIdle {
            assertEquals(ListenSource.Hum, AppIntents.listenRequest.value)
            assertEquals(Tab.Listen, navigator.tab)
        }
    }

    @Test
    fun everySourceHasItsButton() {
        showDock()
        for ((tag, source) in listOf("mic" to ListenSource.Microphone, "phone" to ListenSource.Phone, "hum" to ListenSource.Hum)) {
            compose.onNodeWithTag("listenAgain.$tag").performClick()
            compose.runOnIdle { assertEquals(source, AppIntents.takeListen()) }
        }
    }
}
