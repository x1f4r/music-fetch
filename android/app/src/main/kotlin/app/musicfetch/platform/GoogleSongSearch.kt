package app.musicfetch.platform

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.util.Log

/**
 * Google's own song search ("Search a song": hum, whistle, sing or play), opened in the Google
 * app as a second opinion when a hum's answer is not Strong. Nothing is sent from this app:
 * the Google app listens by itself.
 *
 * The Google app exports the `MUSIC_SEARCH` action of its sound-search activity (the one its
 * "Search a song" button and home-screen widget open). The action is offered only while it
 * resolves to the Google app itself, so a missing, disabled or older Google app shows nothing.
 * Package visibility: the manifest's `<queries>` names the Google app.
 */
object GoogleSongSearch {
    const val PACKAGE = "com.google.android.googlequicksearchbox"
    const val ACTION = "com.google.android.googlequicksearchbox.MUSIC_SEARCH"

    fun intent(): Intent = Intent(ACTION).setPackage(PACKAGE)

    /** Whether the Google app can open its song search on this phone. */
    fun isAvailable(context: Context): Boolean {
        val resolved = try {
            context.packageManager.resolveActivity(intent(), PackageManager.MATCH_DEFAULT_ONLY)
        } catch (e: RuntimeException) {
            // A dying package manager binder; offer nothing.
            Log.w(TAG, "could not resolve the Google song search", e)
            null
        }
        val activity = resolved?.activityInfo ?: return false
        return activity.packageName == PACKAGE && activity.exported && activity.enabled
    }

    /** Open the song search; false if the Google app went away meanwhile. */
    fun open(context: Context): Boolean = try {
        context.startActivity(intent().addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
        true
    } catch (e: ActivityNotFoundException) {
        Log.i(TAG, "Google song search unavailable", e)
        false
    } catch (e: SecurityException) {
        Log.w(TAG, "Google song search refused", e)
        false
    }

    private const val TAG = "GoogleSongSearch"
}
