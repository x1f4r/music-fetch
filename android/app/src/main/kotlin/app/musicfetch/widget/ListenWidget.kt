package app.musicfetch.widget

import android.content.Context
import android.graphics.Bitmap
import androidx.compose.runtime.Composable
import androidx.compose.ui.unit.DpSize
import androidx.compose.ui.unit.dp
import androidx.core.net.toUri
import androidx.glance.ColorFilter
import androidx.glance.GlanceId
import androidx.glance.GlanceModifier
import androidx.glance.GlanceTheme
import androidx.glance.Image
import androidx.glance.ImageProvider
import androidx.glance.LocalContext
import androidx.glance.LocalSize
import androidx.glance.action.clickable
import androidx.glance.appwidget.action.actionStartActivity
import androidx.glance.appwidget.GlanceAppWidget
import androidx.glance.appwidget.GlanceAppWidgetReceiver
import androidx.glance.appwidget.SizeMode
import androidx.glance.appwidget.cornerRadius
import androidx.glance.appwidget.provideContent
import androidx.glance.appwidget.updateAll
import androidx.glance.background
import androidx.glance.layout.Alignment
import androidx.glance.layout.Box
import androidx.glance.layout.Column
import androidx.glance.layout.Row
import androidx.glance.layout.Spacer
import androidx.glance.layout.fillMaxSize
import androidx.glance.layout.padding
import androidx.glance.layout.size
import androidx.glance.layout.width
import androidx.glance.semantics.contentDescription
import androidx.glance.semantics.semantics
import androidx.glance.text.FontWeight
import androidx.glance.text.Text
import androidx.glance.text.TextStyle
import app.musicfetch.MainActivity
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.capture.ListenOrigin
import app.musicfetch.capture.TileTrampolineActivity
import app.musicfetch.core.CaptureSource
import app.musicfetch.core.LibraryQuery
import app.musicfetch.core.SessionStatus
import app.musicfetch.ui.common.ArtworkLoader
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** The last answer shown on the 4×2 widget. */
private data class LastResult(val title: String, val artist: String, val artwork: Bitmap?, val sessionId: String)

/**
 * Home screen widget (docs/design/DESIGN.md §6.11): 2×2 is the listen button; 4×2 adds the last
 * result. A tap starts listening through [TileTrampolineActivity]; the result arrives as a
 * notification.
 */
class ListenWidget : GlanceAppWidget() {
    override val sizeMode = SizeMode.Responsive(setOf(SMALL, WIDE))

    override suspend fun provideGlance(context: Context, id: GlanceId) {
        val last = loadLast(context)
        provideContent {
            GlanceTheme {
                Content(last)
            }
        }
    }

    private suspend fun loadLast(context: Context): LastResult? = withContext(Dispatchers.IO) {
        val engine = runCatching { context.appGraph.engineHost.engine() }.getOrNull() ?: return@withContext null
        val entry = runCatching {
            engine.library(LibraryQuery(kinds = emptyList(), search = null, pinnedOnly = false, limit = 12u, offset = 0u))
        }.getOrDefault(emptyList()).firstOrNull { it.track != null && it.status != SessionStatus.RUNNING }
            ?: return@withContext null
        val track = entry.track
        // RemoteViews limit bitmap memory: artwork stays at or below 96 px.
        val art = (track?.artworkUrl ?: entry.artworkUrl)?.let { ArtworkLoader.bitmap(context, it, sizePx = 96) }
        LastResult(track?.title ?: entry.title, track?.artist ?: entry.subtitle.orEmpty(), art, entry.id)
    }

    @Composable
    private fun Content(last: LastResult?) {
        val context = LocalContext.current
        val size = LocalSize.current
        val wide = size.width >= WIDE.width && last != null
        val listen = actionStartActivity(TileTrampolineActivity.intent(context, CaptureSource.MICROPHONE, ListenOrigin.Widget))
        Box(
            GlanceModifier.fillMaxSize().background(GlanceTheme.colors.widgetBackground).cornerRadius(24.dp).padding(12.dp),
            contentAlignment = Alignment.Center,
        ) {
            if (!wide) {
                Column(horizontalAlignment = Alignment.CenterHorizontally, verticalAlignment = Alignment.CenterVertically) {
                    ListenMark(64, listen)
                    Spacer(GlanceModifier.size(6.dp))
                    Text(
                        context.getString(R.string.widget_listen),
                        style = TextStyle(color = GlanceTheme.colors.onSurface, fontWeight = FontWeight.Medium),
                    )
                }
            } else {
                Row(GlanceModifier.fillMaxSize(), verticalAlignment = Alignment.CenterVertically) {
                    ListenMark(64, listen)
                    Spacer(GlanceModifier.width(16.dp))
                    val open = actionStartActivity(
                        android.content.Intent(
                            android.content.Intent.ACTION_VIEW,
                            ("musicfetch://session/" + android.net.Uri.encode(last.sessionId)).toUri(),
                            context,
                            MainActivity::class.java,
                        ),
                    )
                    Row(GlanceModifier.defaultWeight().clickable(open), verticalAlignment = Alignment.CenterVertically) {
                        if (last.artwork != null) {
                            Image(
                                ImageProvider(last.artwork),
                                contentDescription = null,
                                modifier = GlanceModifier.size(48.dp).cornerRadius(8.dp),
                            )
                            Spacer(GlanceModifier.width(12.dp))
                        }
                        Column {
                            Text(
                                last.title,
                                maxLines = 1,
                                style = TextStyle(color = GlanceTheme.colors.onSurface, fontWeight = FontWeight.Medium),
                            )
                            Text(last.artist, maxLines = 1, style = TextStyle(color = GlanceTheme.colors.onSurfaceVariant))
                        }
                    }
                }
            }
        }
    }

    @Composable
    private fun ListenMark(sizeDp: Int, action: androidx.glance.action.Action) {
        val label = LocalContext.current.getString(R.string.widget_listen)
        Box(
            GlanceModifier.size(sizeDp.dp).clickable(action).semantics { contentDescription = label },
            contentAlignment = Alignment.Center,
        ) {
            Image(
                ImageProvider(R.drawable.widget_cookie),
                contentDescription = null,
                colorFilter = ColorFilter.tint(GlanceTheme.colors.primary),
                modifier = GlanceModifier.size(sizeDp.dp),
            )
            Image(
                ImageProvider(R.drawable.widget_mark),
                contentDescription = null,
                colorFilter = ColorFilter.tint(GlanceTheme.colors.onPrimary),
                modifier = GlanceModifier.size((sizeDp * 0.56f).dp, (sizeDp * 0.56f * 20f / 48f).dp),
            )
        }
    }

    companion object {
        private val SMALL = DpSize(110.dp, 110.dp)
        private val WIDE = DpSize(250.dp, 110.dp)

        /** Refresh every placed widget (after a new result). */
        suspend fun refresh(context: Context) {
            runCatching { ListenWidget().updateAll(context) }
        }
    }
}

class ListenWidgetReceiver : GlanceAppWidgetReceiver() {
    override val glanceAppWidget: GlanceAppWidget = ListenWidget()
}
