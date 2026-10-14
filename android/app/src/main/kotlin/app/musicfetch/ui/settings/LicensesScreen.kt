package app.musicfetch.ui.settings

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import app.musicfetch.R
import app.musicfetch.platform.ServiceLinkOpener
import app.musicfetch.ui.common.ListRow
import app.musicfetch.ui.navigation.LocalNavigator
import app.musicfetch.ui.session.BackBar
import app.musicfetch.ui.theme.StandardMotion

/** A bundled third-party component. */
private data class Component(val name: String, val license: String, val url: String)

private val components = listOf(
    Component("Music Fetch engine (mfcore)", "MIT", "https://github.com/x1f4r/music-fetch"),
    Component("SwiftF0 pitch model", "MIT", "https://github.com/lars76/swift-f0"),
    Component("Android Jetpack and Jetpack Compose", "Apache 2.0", "https://developer.android.com/jetpack"),
    Component("Material Components for Compose", "Apache 2.0", "https://developer.android.com/jetpack/androidx/releases/compose-material3"),
    Component("Kotlin, kotlinx.coroutines, kotlinx.serialization", "Apache 2.0", "https://kotlinlang.org"),
    Component("Coil", "Apache 2.0", "https://github.com/coil-kt/coil"),
    Component("MaterialKolor", "MIT", "https://github.com/jordond/MaterialKolor"),
    Component("JNA", "Apache 2.0 / LGPL 2.1", "https://github.com/java-native-access/jna"),
    Component("UniFFI", "MPL 2.0", "https://github.com/mozilla/uniffi-rs"),
    Component("Chaquopy", "MIT", "https://chaquo.com/chaquopy/"),
    Component("Python", "PSF License", "https://www.python.org"),
    Component("yt-dlp and yt-dlp-ejs", "Unlicense", "https://github.com/yt-dlp/yt-dlp"),
    Component("QuickJS-ng", "MIT", "https://github.com/quickjs-ng/quickjs"),
    Component("certifi", "MPL 2.0", "https://github.com/certifi/python-certifi"),
    Component("Brotli", "MIT", "https://github.com/google/brotli"),
    Component("PyCryptodome", "BSD 2-Clause / Public domain", "https://github.com/Legrandin/pycryptodome"),
    Component("Tokio", "MIT", "https://github.com/tokio-rs/tokio"),
    Component("reqwest, rustls, ring", "MIT / Apache 2.0 / ISC", "https://github.com/seanmonstar/reqwest"),
    Component("rusqlite and SQLite", "MIT / Public domain", "https://github.com/rusqlite/rusqlite"),
    Component("Symphonia", "MPL 2.0", "https://github.com/pdeljanov/Symphonia"),
    Component("Rubato and RealFFT", "MIT", "https://github.com/HEnquist/rubato"),
    Component("tract", "MIT / Apache 2.0", "https://github.com/sonos/tract"),
    Component("scraper, regex, url, serde", "MIT / Apache 2.0", "https://github.com/rust-lang/regex"),
    Component("Google Sans Flex", "SIL Open Font License 1.1", "https://fonts.google.com/specimen/Google+Sans+Flex"),
    Component("Roboto Flex", "SIL Open Font License 1.1", "https://fonts.google.com/specimen/Roboto+Flex"),
    Component("Material Symbols", "Apache 2.0", "https://github.com/google/material-design-icons"),
)

/** Third-party licenses (Settings › About › Licenses). */
@Composable
fun LicensesScreen() {
    val navigator = LocalNavigator.current
    val context = LocalContext.current
    StandardMotion {
        Scaffold(topBar = { BackBar({ navigator.back() }, title = { Text(stringResource(R.string.licenses_title)) }) }) { padding ->
            LazyColumn(contentPadding = PaddingValues(top = padding.calculateTopPadding(), bottom = 24.dp), modifier = Modifier.fillMaxSize()) {
                item {
                    Text(
                        stringResource(R.string.licenses_app),
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp),
                    )
                }
                items(components) { component ->
                    ListRow(
                        headline = component.name,
                        supporting = component.license,
                        onClick = { ServiceLinkOpener.openUrl(context, component.url) },
                    )
                }
            }
        }
    }
}
