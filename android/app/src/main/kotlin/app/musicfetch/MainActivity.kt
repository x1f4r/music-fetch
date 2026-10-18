package app.musicfetch

import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.animation.AnimatedContent
import androidx.compose.animation.EnterTransition
import androidx.compose.animation.ExitTransition
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.togetherWith
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Modifier
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.notify.Notifications
import app.musicfetch.platform.LocalLowPower
import app.musicfetch.ui.AppAction
import app.musicfetch.ui.AppIntents
import app.musicfetch.ui.navigation.AppNavigation
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.rememberNavigator
import app.musicfetch.ui.provider.ProviderPanel
import app.musicfetch.ui.theme.MusicFetchTheme

/** What the activity shows: nothing while the settings load, the provider panel, or the app. */
private enum class Stage { Loading, ChooseProvider, App }

/**
 * The app's one activity. Deep links: `musicfetch://session/<id>` opens a session,
 * `musicfetch://analyze?url=<link>` identifies a link, `musicfetch://listen` shows the
 * Listen stage (`?mode=hum` or `?source=mic|phone|hum` starts listening), `musicfetch://lyrics` opens the lyric search.
 *
 * Until the user chose where songs open (or skipped), the first-launch provider panel
 * replaces the app (docs/APPS.md, "Opening links"); links are opened behind it and shown after.
 */
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        if (savedInstanceState == null) AppIntents.offer(intent)
        val graph = appGraph
        setContent {
            val lowPower by graph.lowPower.enabled.collectAsStateWithLifecycle()
            val settingsFlow = remember { graph.settings.settings }
            val settings by settingsFlow.collectAsStateWithLifecycle(initialValue = null)
            val stage = when {
                settings == null -> Stage.Loading
                settings?.preference == null -> Stage.ChooseProvider
                else -> Stage.App
            }
            CompositionLocalProvider(LocalLowPower provides lowPower) {
                MusicFetchTheme {
                    val (navigator, stacks) = rememberNavigator()
                    val pending by AppIntents.pending.collectAsStateWithLifecycle()
                    // Navigate at once, even behind the provider panel: the app then opens on the
                    // linked screen instead of showing the Listen stage first.
                    LaunchedEffect(pending) {
                        val action = pending ?: return@LaunchedEffect
                        if (action is AppAction.OpenSession) Notifications.dismissResult(this@MainActivity, action.id)
                        AppIntents.consume(action, navigator)
                    }
                    CompositionLocalProvider(LocalNavigator provides navigator) {
                        AnimatedContent(
                            targetState = stage,
                            transitionSpec = {
                                // Returning users go straight to the app; only the panel fades.
                                if (initialState == Stage.Loading) {
                                    EnterTransition.None togetherWith ExitTransition.None
                                } else {
                                    fadeIn(tween(220)) togetherWith fadeOut(tween(150))
                                }
                            },
                            label = "stage",
                        ) { shown ->
                            when (shown) {
                                Stage.Loading -> Box(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface))
                                Stage.ChooseProvider -> ProviderPanel()
                                Stage.App -> AppNavigation(navigator, stacks.first, stacks.second)
                            }
                        }
                    }
                }
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        AppIntents.offer(intent)
    }
}
