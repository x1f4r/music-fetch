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
import androidx.compose.foundation.layout.size
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.Text
import androidx.compose.material3.adaptive.currentWindowAdaptiveInfoV2
import androidx.compose.material3.adaptive.navigationsuite.NavigationSuiteScaffold
import androidx.compose.material3.adaptive.navigationsuite.NavigationSuiteScaffoldDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.MutableState
import androidx.compose.runtime.Stable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
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
import app.musicfetch.ui.components.ContourMark
import app.musicfetch.ui.library.LibraryScreen
import app.musicfetch.ui.listen.ListenScreen
import app.musicfetch.ui.lyrics.LyricSearchScreen
import app.musicfetch.ui.provider.ProviderSettingsScreen
import app.musicfetch.ui.session.SessionScreen
import app.musicfetch.ui.settings.LicensesScreen
import app.musicfetch.ui.settings.SettingsScreen

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

    /** Select a destination; selecting the current one returns to its root. */
    fun select(target: Tab) {
        if (tab == target) {
            while (current.size > 1) current.removeAt(current.lastIndex)
        } else {
            tab = target
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
    val provider = entryProvider<NavKey> {
        entry<ListenRoute> { ListenScreen() }
        entry<LibraryRoute> { LibraryScreen() }
        entry<SessionRoute> { route -> SessionScreen(route.id) }
        entry<LyricsRoute> { LyricSearchScreen() }
        entry<SettingsRoute> { SettingsScreen() }
        entry<LicensesRoute> { LicensesScreen() }
        entry<ProviderRoute> { ProviderSettingsScreen() }
    }
    val listenEntries = rememberDecoratedNavEntries(
        listenStack,
        listOf(rememberSaveableStateHolderNavEntryDecorator(), rememberViewModelStoreNavEntryDecorator()),
        provider,
    )
    val libraryEntries = rememberDecoratedNavEntries(
        libraryStack,
        listOf(rememberSaveableStateHolderNavEntryDecorator(), rememberViewModelStoreNavEntryDecorator()),
        provider,
    )
    val tab = navigator.tab
    BackHandler(enabled = tab == Tab.Library && libraryStack.size == 1) { navigator.back() }
    // Short navigation bar on phones, wide rail from 600 dp (docs/design/DESIGN.md §3.2).
    val suiteType = NavigationSuiteScaffoldDefaults.navigationSuiteType(currentWindowAdaptiveInfoV2())
    NavigationSuiteScaffold(
        layoutType = suiteType,
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
            )
        },
    ) {
        val entries: List<NavEntry<NavKey>> = if (tab == Tab.Listen) listenEntries else libraryEntries
        NavDisplay(
            entries = entries,
            onBack = { navigator.back() },
            transitionSpec = { push() },
            popTransitionSpec = { pop() },
            predictivePopTransitionSpec = { pop() },
        )
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
