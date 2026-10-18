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
 * The Google app exports a `MUSIC_SEARCH` action that opens its sound-search screen, the one its
 * own "Search a song" button opens (checked with Google app 16.9 on Android 16). Google apps from
 * before hum to search (2020) answer the same action with the old Sound Search, which may say
 * it is unavailable in the country, so the action is offered only for Google app 12 and newer,
 * and only while it resolves to the Google app itself: a missing, disabled or old Google app
 * shows nothing. Package visibility: the manifest's `<queries>` names the Google app.
 */
object GoogleSongSearch {
    const val PACKAGE = "com.google.android.googlequicksearchbox"
    const val ACTION = "com.google.android.googlequicksearchbox.MUSIC_SEARCH"

    /** Hum to search shipped in late 2020; Google app 12 and newer have it. */
    const val MIN_MAJOR_VERSION = 12

    fun intent(): Intent = Intent(ACTION).setPackage(PACKAGE)

    /** Whether the Google app can open its song search on this phone. */
    fun isAvailable(context: Context): Boolean {
        val pm = context.packageManager
        val (resolved, version) = try {
            pm.resolveActivity(intent(), PackageManager.MATCH_DEFAULT_ONLY) to pm.getPackageInfo(PACKAGE, 0).versionName
        } catch (_: PackageManager.NameNotFoundException) {
            return false
        } catch (e: RuntimeException) {
            // A dying package manager binder; offer nothing.
            Log.w(TAG, "could not resolve the Google song search", e)
            return false
        }
        val activity = resolved?.activityInfo ?: return false
        return activity.packageName == PACKAGE && activity.exported && activity.enabled && hasHumToSearch(version)
    }

    /** Google app version names look like "16.9.50.ve.x86_64"; the major version decides. */
    fun hasHumToSearch(versionName: String?): Boolean =
        (versionName?.substringBefore('.')?.trim()?.toIntOrNull() ?: 0) >= MIN_MAJOR_VERSION

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
