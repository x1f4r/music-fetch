package app.musicfetch.ui.navigation

import androidx.activity.compose.BackHandler
import androidx.compose.animation.ContentTransform
import androidx.compose.animation.core.spring
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInHorizontally
import androidx.compose.animation.slideOutHorizontally
import androidx.compose.animation.togetherWith
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.WindowInsetsSides
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.only
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawing
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.windowInsetsPadding
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.adaptive.currentWindowAdaptiveInfoV2
import androidx.compose.material3.adaptive.navigationsuite.NavigationSuiteScaffold
import androidx.compose.material3.adaptive.navigationsuite.NavigationSuiteScaffoldDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.MutableState
import androidx.compose.runtime.Stable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.pluralStringResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.navigation3.rememberViewModelStoreNavEntryDecorator
import androidx.navigation3.runtime.NavBackStack
import androidx.navigation3.runtime.NavEntry
import androidx.navigation3.runtime.NavKey
import androidx.navigation3.runtime.entryProvider
import androidx.navigation3.runtime.rememberDecoratedNavEntries
import androidx.navigation3.runtime.rememberNavBackStack
import androidx.navigation3.runtime.rememberSaveableStateHolderNavEntryDecorator
import androidx.navigation3.ui.NavDisplay
import app.musicfetch.R
import app.musicfetch.appGraph
import app.musicfetch.ui.common.AppSnackbar
import app.musicfetch.ui.common.LocalAppSnackbar
import app.musicfetch.ui.common.devTestTagsRoot
import app.musicfetch.ui.common.tag
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.library.LibraryScreen
import app.musicfetch.ui.listen.ListenScreen
import app.musicfetch.ui.lyrics.LyricSearchScreen
import app.musicfetch.ui.provider.ProviderSettingsScreen
import app.musicfetch.ui.session.SessionScreen
import app.musicfetch.ui.settings.LicensesScreen
import app.musicfetch.ui.settings.SettingsScreen
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow

/** Moves between screens; one back stack per destination (docs/design/DESIGN.md §3.2). */
@Stable
class Navigator(
    private val listenStack: NavBackStack<NavKey>,
    private val libraryStack: NavBackStack<NavKey>,
    tabState: MutableState<Tab>,
) {
    var tab by tabState
        private set

    private val current: NavBackStack<NavKey> get() = if (tab == Tab.Listen) listenStack else libraryStack

    private val _reselected = MutableSharedFlow<Tab>(extraBufferCapacity = 1)

    /** A destination's item was selected again while its root was showing (scroll to top). */
    val reselected: SharedFlow<Tab> = _reselected.asSharedFlow()

    fun push(route: Route) {
        if (current.lastOrNull() != route) current.add(route)
    }

    /** Open a session on the Listen destination (results of listens, links and notifications). */
    fun openSessionFromListen(id: String) {
        tab = Tab.Listen
        while (listenStack.size > 1) listenStack.removeAt(listenStack.lastIndex)
        listenStack.add(SessionRoute(id))
    }

    /** Leave the current screen; false when there is nothing to go back to. */
    fun back(): Boolean {
        val stack = current
        if (stack.size > 1) {
            stack.removeAt(stack.lastIndex)
            return true
        }
        if (tab == Tab.Library) {
            tab = Tab.Listen
            return true
        }
        return false
    }

    /** Remove [route] wherever it is on top (e.g. a deleted session). */
    fun closeIfShowing(route: Route) {
        for (stack in listOf(listenStack, libraryStack)) {
            if (stack.size > 1 && stack.last() == route) stack.removeAt(stack.lastIndex)
        }
    }

    /** Select a destination; selecting the current one returns to its root, or scrolls it to the top. */
    fun select(target: Tab) {
        when {
            tab != target -> tab = target
            current.size > 1 -> while (current.size > 1) current.removeAt(current.lastIndex)
            else -> _reselected.tryEmit(target)
        }
    }

    /** Back to the Listen stage. */
    fun showListen() {
        tab = Tab.Listen
        while (listenStack.size > 1) listenStack.removeAt(listenStack.lastIndex)
    }
}

val LocalNavigator = staticCompositionLocalOf<Navigator> { error("No Navigator provided") }

@Composable
fun rememberNavigator(): Pair<Navigator, Pair<NavBackStack<NavKey>, NavBackStack<NavKey>>> {
    val listenStack = rememberNavBackStack(ListenRoute)
    val libraryStack = rememberNavBackStack(LibraryRoute)
    val tabState = rememberSaveable { mutableStateOf(Tab.Listen) }
    val navigator = remember(listenStack, libraryStack) { Navigator(listenStack, libraryStack, tabState) }
    return navigator to (listenStack to libraryStack)
}

/** The app: navigation bar (rail on wide screens) around per-destination back stacks. */
@Composable
fun AppNavigation(navigator: Navigator, listenStack: NavBackStack<NavKey>, libraryStack: NavBackStack<NavKey>) {
    // Sessions know which stack they are on: only the Listen stack offers "Listen again".
    fun provider(onListen: Boolean) = entryProvider<NavKey> {
        entry<ListenRoute> { ListenScreen() }
        entry<LibraryRoute> { LibraryScreen() }
        entry<SessionRoute> { route -> SessionScreen(route.id, onListenTab = onListen) }
        entry<LyricsRoute> { LyricSearchScreen() }
        entry<SettingsRoute> { SettingsScreen() }
        entry<LicensesRoute> { LicensesScreen() }
        entry<ProviderRoute> { ProviderSettingsScreen() }
    }
    val listenProvider = remember { provider(onListen = true) }
    val libraryProvider = remember { provider(onListen = false) }
    val listenEntries = rememberDecoratedNavEntries(
        listenStack,
        listOf(rememberSaveableStateHolderNavEntryDecorator(), rememberViewModelStoreNavEntryDecorator()),
        listenProvider,
    )
    val libraryEntries = rememberDecoratedNavEntries(
        libraryStack,
        listOf(rememberSaveableStateHolderNavEntryDecorator(), rememberViewModelStoreNavEntryDecorator()),
        libraryProvider,
    )
    val tab = navigator.tab
    BackHandler(enabled = tab == Tab.Library && libraryStack.size == 1) { navigator.back() }

    val scope = rememberCoroutineScope()
    val snackbar = remember { AppSnackbar(SnackbarHostState(), scope) }
    DeletionUndo(snackbar)

    // Short navigation bar on phones, wide rail from 600 dp (docs/design/DESIGN.md §3.2).
    val suiteType = NavigationSuiteScaffoldDefaults.navigationSuiteType(currentWindowAdaptiveInfoV2())
    CompositionLocalProvider(LocalAppSnackbar provides snackbar) {
        NavigationSuiteScaffold(
            layoutType = suiteType,
            modifier = Modifier.devTestTagsRoot(),
            navigationSuiteItems = {
                item(
                    selected = tab == Tab.Listen,
                    onClick = { navigator.select(Tab.Listen) },
                    icon = {
                        Box(Modifier.size(24.dp), contentAlignment = Alignment.Center) {
                            ContourMark(LocalContentColor.current, width = 24.dp, strokeWidth = 2.4.dp)
                        }
                    },
                    label = { Text(stringResource(R.string.nav_listen)) },
                    modifier = Modifier.tag("nav.listen"),
                )
                item(
                    selected = tab == Tab.Library,
                    onClick = { navigator.select(Tab.Library) },
                    icon = {
                        Icon(
                            painterResource(if (tab == Tab.Library) R.drawable.ic_video_library_filled else R.drawable.ic_video_library),
                            contentDescription = null,
                        )
                    },
                    label = { Text(stringResource(R.string.nav_library)) },
                    modifier = Modifier.tag("nav.library"),
                )
            },
        ) {
            Box(Modifier.fillMaxSize()) {
                val entries: List<NavEntry<NavKey>> = if (tab == Tab.Listen) listenEntries else libraryEntries
                NavDisplay(
                    entries = entries,
                    onBack = { navigator.back() },
                    transitionSpec = { push() },
                    popTransitionSpec = { pop() },
                    predictivePopTransitionSpec = { pop() },
                )
                SnackbarHost(
                    snackbar.host,
                    Modifier
                        .align(Alignment.BottomCenter)
                        .windowInsetsPadding(WindowInsets.safeDrawing.only(WindowInsetsSides.Horizontal + WindowInsetsSides.Bottom))
                        .padding(bottom = snackbar.bottomInset)
                        .tag("snackbar"),
                )
            }
        }
    }
}

/** "Deleted" + Undo for the newest deletion, wherever it was started (docs/design/DESIGN.md §6.8). */
@Composable
private fun DeletionUndo(snackbar: AppSnackbar) {
    val deletions = LocalContext.current.appGraph.deletions
    val pending by deletions.latest.collectAsStateWithLifecycle()
    val undo = stringResource(R.string.session_undo)
    val count = pending?.ids?.size ?: 0
    val label = if (count > 1) pluralStringResource(R.plurals.library_deleted_n, count, count) else stringResource(R.string.session_deleted)
    LaunchedEffect(pending) {
        val deletion = pending ?: return@LaunchedEffect
        if (snackbar.show(label, undo)) deletions.undo(deletion) else deletions.acknowledge(deletion)
    }
}

private val slideSpec = spring<IntOffset>(dampingRatio = 0.9f, stiffness = 700f)
private val fadeSpec = spring<Float>(dampingRatio = 1f, stiffness = 1600f)

private fun push(): ContentTransform =
    (slideInHorizontally(slideSpec) { it / 5 } + fadeIn(fadeSpec)) togetherWith
        (slideOutHorizontally(slideSpec) { -it / 10 } + fadeOut(fadeSpec))

private fun pop(): ContentTransform =
    (slideInHorizontally(slideSpec) { -it / 10 } + fadeIn(fadeSpec)) togetherWith
        (slideOutHorizontally(slideSpec) { it / 5 } + fadeOut(fadeSpec))
