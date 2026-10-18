package app.musicfetch.capture

import android.Manifest
import android.app.Activity
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.media.projection.MediaProjectionManager
import android.os.Build
import android.os.Bundle
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.result.contract.ActivityResultContracts
import androidx.core.content.ContextCompat
import androidx.core.content.IntentCompat
import androidx.core.net.toUri
import app.musicfetch.MainActivity
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.ListenMode
import app.musicfetch.platform.ListenSource
import app.musicfetch.ui.AppIntents

/**
 * Invisible activity behind the Quick Settings tiles and the widget: asks for the microphone
 * (and notification) permission when needed, for the per-session MediaProjection consent for
 * phone audio, starts [CaptureService] while it is in the foreground (tiles are not exempt
 * from the while-in-use restriction) and finishes.
 */
class TileTrampolineActivity : ComponentActivity() {
    private lateinit var source: CaptureSource
    private lateinit var origin: ListenOrigin

    private val permissions = registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { granted ->
        when {
            granted[Manifest.permission.RECORD_AUDIO] == true || hasMicrophone() -> proceed()
            // Denied for good: Android won't ask again. The app's stage explains it and links
            // to the settings ("Microphone access is off." · Open settings).
            !shouldShowRequestPermissionRationale(Manifest.permission.RECORD_AUDIO) -> {
                val listen = if (source == CaptureSource.SYSTEM_AUDIO) ListenSource.Phone else ListenSource.Microphone
                startActivity(
                    Intent(Intent.ACTION_VIEW, AppIntents.listenUri(listen).toUri(), this, MainActivity::class.java)
                        .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK),
                )
                finish()
            }
            else -> {
                Toast.makeText(this, R.string.state_mic_off, Toast.LENGTH_LONG).show()
                finish()
            }
        }
    }

    private val consent = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
        val data = result.data
        if (result.resultCode == Activity.RESULT_OK && data != null) {
            appGraph.listen.start(CaptureSource.SYSTEM_AUDIO, ListenMode.RECORDING, origin, ProjectionGrant(result.resultCode, data))
        }
        finish()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        source = IntentCompat.getSerializableExtra(intent, EXTRA_SOURCE, CaptureSource::class.java) ?: CaptureSource.MICROPHONE
        origin = IntentCompat.getSerializableExtra(intent, EXTRA_ORIGIN, ListenOrigin::class.java) ?: ListenOrigin.Tile
        // Recreated after a configuration change while a system dialog is up: the launchers
        // deliver their results to this instance.
        if (savedInstanceState != null) return
        if (appGraph.listen.state.value.isActive) {
            finish()
            return
        }
        val missing = buildList {
            if (!hasMicrophone()) add(Manifest.permission.RECORD_AUDIO)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                ContextCompat.checkSelfPermission(this@TileTrampolineActivity, Manifest.permission.POST_NOTIFICATIONS) !=
                PackageManager.PERMISSION_GRANTED
            ) {
                add(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
        if (missing.isEmpty()) proceed() else permissions.launch(missing.toTypedArray())
    }

    private fun proceed() {
        when (source) {
            CaptureSource.MICROPHONE -> {
                appGraph.listen.start(CaptureSource.MICROPHONE, ListenMode.AUTO, origin)
                finish()
            }
            CaptureSource.SYSTEM_AUDIO -> {
                val manager = getSystemService(MediaProjectionManager::class.java)
                if (manager == null) {
                    finish()
                    return
                }
                consent.launch(manager.createScreenCaptureIntent())
            }
        }
    }

    private fun hasMicrophone(): Boolean =
        ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO) == PackageManager.PERMISSION_GRANTED

    override fun finish() {
        super.finish()
        // No transition: the activity is never seen.
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            overrideActivityTransition(OVERRIDE_TRANSITION_CLOSE, 0, 0)
        } else {
            @Suppress("DEPRECATION") // Replaced by overrideActivityTransition on API 34+.
            overridePendingTransition(0, 0)
        }
    }

    companion object {
        private const val EXTRA_SOURCE = "source"
        private const val EXTRA_ORIGIN = "origin"

        fun intent(context: Context, source: CaptureSource, origin: ListenOrigin = ListenOrigin.Tile): Intent =
            Intent(context, TileTrampolineActivity::class.java)
                .putExtra(EXTRA_SOURCE, source)
                .putExtra(EXTRA_ORIGIN, origin)
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_NO_ANIMATION or Intent.FLAG_ACTIVITY_EXCLUDE_FROM_RECENTS)
    }
}
