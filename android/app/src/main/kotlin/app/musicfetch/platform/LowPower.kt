package app.musicfetch.platform

import android.app.ActivityManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.PowerManager
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.core.content.ContextCompat
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/** What the device reports, reduced to the inputs of [LowPower.isWeakHardware]. */
data class DeviceProfile(val lowRamDevice: Boolean, val totalMemBytes: Long, val processors: Int)

/**
 * Whether the app should save CPU and memory: on weak hardware (budget phones) always, and on
 * any phone while Battery Saver is on. Drives `Engine.setLowPower` and the lighter animations of
 * the Listen stage.
 */
class LowPower(context: Context) {
    private val appContext = context.applicationContext
    private val power = appContext.getSystemService(PowerManager::class.java)

    /** The device as it was measured at start. */
    val device: DeviceProfile = measure(appContext)

    /** Weak hardware never changes while the process runs. */
    val weakHardware: Boolean = isWeakHardware(device)

    private val _enabled = MutableStateFlow(weakHardware || powerSave())

    /** True while the app runs its low-power path. */
    val enabled: StateFlow<Boolean> = _enabled.asStateFlow()

    private val receiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) {
            _enabled.value = weakHardware || powerSave()
        }
    }

    /** Follow Battery Saver; call once from `Application.onCreate`. */
    fun observe() {
        ContextCompat.registerReceiver(
            appContext,
            receiver,
            IntentFilter(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED),
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
        _enabled.value = weakHardware || powerSave()
    }

    private fun powerSave(): Boolean = power?.isPowerSaveMode == true

    companion object {
        /** Phones sold with 4 GB report a little less than 4 GiB, so they count as weak. */
        const val LOW_MEMORY_BYTES = 4L * 1024 * 1024 * 1024
        const val FEW_PROCESSORS = 4

        /** Low-RAM flag, less than 4 GB of memory, or four cores or fewer. */
        fun isWeakHardware(device: DeviceProfile): Boolean =
            device.lowRamDevice || (device.totalMemBytes in 1 until LOW_MEMORY_BYTES) || device.processors <= FEW_PROCESSORS

        fun measure(context: Context): DeviceProfile {
            val activity = context.getSystemService(ActivityManager::class.java)
            val info = ActivityManager.MemoryInfo().also { activity?.getMemoryInfo(it) }
            return DeviceProfile(
                lowRamDevice = activity?.isLowRamDevice == true,
                totalMemBytes = info.totalMem,
                processors = Runtime.getRuntime().availableProcessors(),
            )
        }
    }
}

/** Whether composables should take the low-power path (fewer frames, simpler animation). */
val LocalLowPower = staticCompositionLocalOf { false }
