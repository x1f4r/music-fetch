package app.musicfetch.ui.common

import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.testTagsAsResourceId
import app.musicfetch.BuildConfig

/**
 * Debug and benchmark builds expose test tags as resource ids, so UI Automator and `uiautomator
 * dump` can find views by tag (the emulator harness). Set once on each window's root.
 */
@OptIn(ExperimentalComposeUiApi::class)
fun Modifier.devTestTagsRoot(): Modifier = if (BuildConfig.DEV_TOOLS) semantics { testTagsAsResourceId = true } else this

/** A stable tag for tests and the harness; release builds leave it out. */
fun Modifier.tag(name: String): Modifier = if (BuildConfig.DEV_TOOLS) testTag(name) else this
