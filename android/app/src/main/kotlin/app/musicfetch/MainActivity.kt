package app.musicfetch

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import app.musicfetch.capture.ListenOrigin
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.notify.Notifications
import app.musicfetch.ui.AppAction
import app.musicfetch.ui.AppIntents
import app.musicfetch.ui.navigation.AppNavigation
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.navigation.rememberNavigator
import app.musicfetch.ui.theme.MusicFetchTheme

/**
 * The app's one activity. Deep links: `musicfetch://session/<id>` opens a session,
 * `musicfetch://analyze?url=<link>` identifies a link, `musicfetch://listen` shows the
 * Listen stage (`?mode=hum` starts humming), `musicfetch://lyrics` opens the lyric search.
 */
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        if (savedInstanceState == null) AppIntents.offer(intent)
        setContent {
            MusicFetchTheme {
                val (navigator, stacks) = rememberNavigator()
                val pending by AppIntents.pending.collectAsStateWithLifecycle()
                LaunchedEffect(pending) {
                    val action = pending ?: return@LaunchedEffect
                    if (action is AppAction.OpenSession) Notifications.dismissResult(this@MainActivity, action.id)
                    AppIntents.consume(action, navigator, startHum = ::startHum)
                }
                CompositionLocalProvider(LocalNavigator provides navigator) {
                    AppNavigation(navigator, stacks.first, stacks.second)
                }
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        AppIntents.offer(intent)
    }

    private fun startHum() {
        if (checkSelfPermission(Manifest.permission.RECORD_AUDIO) == PackageManager.PERMISSION_GRANTED) {
            appGraph.listen.start(CaptureSource.MICROPHONE, ListenMode.HUM, ListenOrigin.App)
        }
    }
}
