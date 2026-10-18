package app.musicfetch.ui.common

import androidx.compose.material3.SnackbarDuration
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.SnackbarResult
import androidx.compose.runtime.Stable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The app's one snackbar host (docs/design/DESIGN.md §6.8): messages and Undo from any screen,
 * shown above the navigation bar and above a screen's own bottom bar. Messages are posted in
 * the app's scope, so they outlive the screen that posted them (a session deleted from its own
 * screen is undone on the screen below).
 */
@Stable
class AppSnackbar(val host: SnackbarHostState, private val scope: CoroutineScope) {
    /** Height of the shown screen's own bottom bar, so snackbars sit above it. */
    var bottomInset: Dp by mutableStateOf(0.dp)

    /**
     * Show [message], replacing the one on screen, for [timeoutMs]. Returns whether [action]
     * was tapped.
     */
    suspend fun show(message: String, action: String? = null, timeoutMs: Long = if (action == null) MESSAGE_MS else ACTION_MS): Boolean {
        host.currentSnackbarData?.dismiss()
        val result = withTimeoutOrNull(timeoutMs) {
            host.showSnackbar(message, actionLabel = action, duration = SnackbarDuration.Indefinite)
        }
        return result == SnackbarResult.ActionPerformed
    }

    /** [show] in the app's scope; [onAction] runs when the action is tapped. */
    fun post(message: String, action: String? = null, onAction: () -> Unit = {}) {
        scope.launch { if (show(message, action)) onAction() }
    }

    companion object {
        const val MESSAGE_MS = 3_000L

        /** Messages with Undo stay as long as the undo window (Deletions.UNDO_MS). */
        const val ACTION_MS = 5_000L
    }
}

val LocalAppSnackbar = staticCompositionLocalOf<AppSnackbar> { error("No AppSnackbar provided") }
