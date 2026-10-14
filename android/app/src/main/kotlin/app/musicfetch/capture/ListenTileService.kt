package app.musicfetch.capture

import android.app.PendingIntent
import android.os.Build
import android.service.quicksettings.Tile
import android.service.quicksettings.TileService
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.core.CaptureSource

/**
 * Quick Settings tile that identifies a song (docs/APPS.md, "Android Quick Settings tiles").
 * A tap starts capture through [TileTrampolineActivity]; the tile is active while capturing
 * and a second tap stops. The ongoing notification shows what is being captured and the
 * result arrives as a notification.
 */
abstract class ListenTileService : TileService() {
    protected abstract val source: CaptureSource

    override fun onStartListening() {
        super.onStartListening()
        render()
    }

    override fun onClick() {
        super.onClick()
        val controller = appGraph.listen
        val state = controller.state.value
        if (state.isActive) {
            if (state.source == source && state.isCapturing) controller.stop()
            render()
            return
        }
        val intent = TileTrampolineActivity.intent(this, source)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            val pending = PendingIntent.getActivity(
                this,
                source.ordinal,
                intent,
                PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
            )
            startActivityAndCollapse(pending)
        } else {
            startTrampolineLegacy(intent)
        }
    }

    @Suppress("DEPRECATION") // The Intent overload is the only one before API 34.
    private fun startTrampolineLegacy(intent: android.content.Intent) {
        startActivityAndCollapse(intent)
    }

    private fun render() {
        val tile = qsTile ?: return
        val state = appGraph.listen.state.value
        val mine = state.isActive && state.source == source
        tile.state = when {
            mine -> Tile.STATE_ACTIVE
            state.isActive -> Tile.STATE_UNAVAILABLE
            else -> Tile.STATE_INACTIVE
        }
        tile.subtitle = when {
            mine && state.isCapturing -> getString(R.string.tile_listening)
            mine -> getString(R.string.tile_matching)
            else -> null
        }
        tile.updateTile()
    }
}

/** "Identify song": the microphone in auto mode (plays, hums, whistles and singing). */
class SongTileService : ListenTileService() {
    override val source = CaptureSource.MICROPHONE
}

/** "Identify phone audio": what other apps play on this phone. */
class PhoneAudioTileService : ListenTileService() {
    override val source = CaptureSource.SYSTEM_AUDIO
}
