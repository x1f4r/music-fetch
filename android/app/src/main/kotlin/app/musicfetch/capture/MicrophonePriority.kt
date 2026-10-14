package app.musicfetch.capture

import android.content.Context
import android.media.AudioDeviceInfo
import android.media.AudioManager
import android.os.Build
import androidx.core.content.edit

/**
 * The user's microphone order. Listening uses the first connected microphone in the list;
 * microphones never seen before are appended, so a newly connected one never jumps ahead
 * of the user's choice. All built-in microphones are one entry ("This phone"), routed by
 * the system.
 *
 * Bluetooth headset microphones are not offered: Android records them only through the
 * call (SCO) path, at a quality too low to recognise music.
 */
class MicrophonePriority(context: Context) {
    data class Entry(val key: String, val name: String, val connected: Boolean)

    private val app = context.applicationContext
    private val prefs = app.getSharedPreferences("microphones", Context.MODE_PRIVATE)
    private val audio = app.getSystemService(AudioManager::class.java)

    /** Connected, recognisable microphones keyed by a stable identity. */
    private fun connected(): Map<String, AudioDeviceInfo> {
        val devices = audio?.getDevices(AudioManager.GET_DEVICES_INPUTS).orEmpty()
        val result = LinkedHashMap<String, AudioDeviceInfo>()
        for (device in devices) {
            if (device.type !in SUPPORTED) continue
            val key = keyOf(device)
            if (key !in result) result[key] = device
        }
        return result
    }

    /** Entries in priority order, including remembered microphones that are not connected. */
    fun entries(builtInName: String): List<Entry> {
        val now = connected()
        val names = prefs.getStringSet(KEY_NAMES, emptySet()).orEmpty()
            .mapNotNull { it.split(SEP, limit = 2).takeIf { p -> p.size == 2 }?.let { p -> p[0] to p[1] } }
            .toMap().toMutableMap()
        for ((key, device) in now) names[key] = if (key == BUILT_IN) builtInName else device.productName.toString()
        val order = order().toMutableList()
        // The phone's own microphone leads a fresh list; new devices are appended.
        if (BUILT_IN !in order) order.add(0, BUILT_IN)
        now.keys.filter { it !in order }.forEach { order += it }
        save(order, names)
        return order.map { Entry(it, names[it] ?: if (it == BUILT_IN) builtInName else it, it in now || it == BUILT_IN) }
    }

    fun move(key: String, offset: Int) {
        val order = order().toMutableList()
        val from = order.indexOf(key)
        val to = from + offset
        if (from < 0 || to !in order.indices) return
        order.add(to, order.removeAt(from))
        prefs.edit { putString(KEY_ORDER, order.joinToString(LIST_SEP)) }
    }

    fun forget(key: String) {
        prefs.edit { putString(KEY_ORDER, order().filter { it != key }.joinToString(LIST_SEP)) }
    }

    /**
     * The device to record from: the first connected entry. Null means the built-in
     * microphone, which the system routes itself.
     */
    fun preferredDevice(): AudioDeviceInfo? {
        val now = connected()
        val first = order().firstOrNull { it == BUILT_IN || it in now } ?: return null
        return if (first == BUILT_IN) null else now[first]
    }

    /** Key of the microphone listening would use right now. */
    fun activeKey(): String = preferredDevice()?.let(::keyOf) ?: BUILT_IN

    private fun order(): List<String> =
        prefs.getString(KEY_ORDER, null)?.split(LIST_SEP)?.filter { it.isNotEmpty() }.orEmpty()

    private fun save(order: List<String>, names: Map<String, String>) {
        prefs.edit {
            putString(KEY_ORDER, order.joinToString(LIST_SEP))
            putStringSet(KEY_NAMES, names.map { "${it.key}$SEP${it.value}" }.toSet())
        }
    }

    private fun keyOf(device: AudioDeviceInfo): String =
        if (device.type == AudioDeviceInfo.TYPE_BUILTIN_MIC) BUILT_IN
        else "${device.type}:${device.productName}:${device.address}"

    companion object {
        const val BUILT_IN = "builtin"
        private const val KEY_ORDER = "order"
        private const val KEY_NAMES = "names"
        private const val SEP = "\u001F"
        private const val LIST_SEP = "\u001E"

        private val SUPPORTED: Set<Int> = buildSet {
            add(AudioDeviceInfo.TYPE_BUILTIN_MIC)
            add(AudioDeviceInfo.TYPE_WIRED_HEADSET)
            add(AudioDeviceInfo.TYPE_USB_DEVICE)
            add(AudioDeviceInfo.TYPE_USB_HEADSET)
            add(AudioDeviceInfo.TYPE_USB_ACCESSORY)
            add(AudioDeviceInfo.TYPE_LINE_ANALOG)
            add(AudioDeviceInfo.TYPE_LINE_DIGITAL)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) add(AudioDeviceInfo.TYPE_DOCK)
        }
    }
}
