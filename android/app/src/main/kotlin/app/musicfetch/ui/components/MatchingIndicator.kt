@file:OptIn(ExperimentalMaterial3ExpressiveApi::class)

package app.musicfetch.ui.components

import androidx.compose.material3.ExperimentalMaterial3ExpressiveApi
import androidx.compose.material3.LoadingIndicator
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color

/** The expressive loading indicator shown in the listen button while matching (experimental API). */
@Composable
fun MatchingIndicator(color: Color, modifier: Modifier = Modifier) {
    LoadingIndicator(modifier = modifier, color = color)
}
