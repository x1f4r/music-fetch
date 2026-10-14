import CoreAudio
import Foundation
import Observation

/// A Core Audio device as shown in Settings.
nonisolated struct AudioDevice: Identifiable, Hashable, Sendable {
    let uid: String
    let name: String
    let hasInput: Bool
    let hasOutput: Bool
    var id: String { uid }
}

/// Device lookup and the user's device preferences.
///
/// Microphone: an ordered priority list of device UIDs. Listening uses the first connected
/// device in the list, otherwise the system default input. Devices never seen in the list
/// are appended in system order, so a newly connected microphone never jumps ahead of the
/// user's choice. "This Mac": an optional output device to capture from (default output
/// when unset or disconnected).
nonisolated enum AudioDevices {
    static let inputPriorityKey = "inputDevicePriority"
    static let inputNamesKey = "inputDeviceNames"
    static let captureOutputKey = "captureOutputDevice"

    // MARK: Preferences

    static var inputPriority: [String] {
        get { UserDefaults.standard.stringArray(forKey: inputPriorityKey) ?? [] }
        set { UserDefaults.standard.set(newValue, forKey: inputPriorityKey) }
    }

    /// Last known names, so disconnected devices still show up in Settings.
    static var rememberedNames: [String: String] {
        get { UserDefaults.standard.dictionary(forKey: inputNamesKey) as? [String: String] ?? [:] }
        set { UserDefaults.standard.set(newValue, forKey: inputNamesKey) }
    }

    static var captureOutputUID: String? {
        get { UserDefaults.standard.string(forKey: captureOutputKey).flatMap { $0.isEmpty ? nil : $0 } }
        set { UserDefaults.standard.set(newValue ?? "", forKey: captureOutputKey) }
    }

    /// The microphone to listen with: the first connected device of the priority list.
    /// Nil means "use the system default".
    static func preferredInputDevice() -> AudioObjectID? {
        let connected = all().filter(\.hasInput)
        for uid in inputPriority where connected.contains(where: { $0.uid == uid }) {
            if let id = objectID(uid: uid) { return id }
        }
        return nil
    }

    /// The output device whose audio "This Mac" captures, when the user picked one and it
    /// is connected.
    static func preferredOutputUID() -> String? {
        guard let uid = captureOutputUID, all().contains(where: { $0.uid == uid && $0.hasOutput }) else { return nil }
        return uid
    }

    // MARK: Core Audio

    static func all() -> [AudioDevice] {
        var address = AudioObjectPropertyAddress(
            mSelector: kAudioHardwarePropertyDevices,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )
        var size: UInt32 = 0
        let system = AudioObjectID(kAudioObjectSystemObject)
        guard AudioObjectGetPropertyDataSize(system, &address, 0, nil, &size) == noErr, size > 0 else { return [] }
        var ids = [AudioObjectID](repeating: 0, count: Int(size) / MemoryLayout<AudioObjectID>.size)
        guard AudioObjectGetPropertyData(system, &address, 0, nil, &size, &ids) == noErr else { return [] }
        return ids.compactMap { id in
            guard let uid = string(id, kAudioDevicePropertyDeviceUID),
                  let name = string(id, kAudioObjectPropertyName),
                  !isPrivateAggregate(uid: uid)
            else { return nil }
            let input = hasStreams(id, scope: kAudioObjectPropertyScopeInput)
            let output = hasStreams(id, scope: kAudioObjectPropertyScopeOutput)
            guard input || output else { return nil }
            return AudioDevice(uid: uid, name: name, hasInput: input, hasOutput: output)
        }
    }

    static func objectID(uid: String) -> AudioObjectID? {
        var address = AudioObjectPropertyAddress(
            mSelector: kAudioHardwarePropertyTranslateUIDToDevice,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )
        var cfUID = uid as CFString
        var device = AudioObjectID(kAudioObjectUnknown)
        var size = UInt32(MemoryLayout<AudioObjectID>.size)
        let status = withUnsafePointer(to: &cfUID) { pointer in
            AudioObjectGetPropertyData(
                AudioObjectID(kAudioObjectSystemObject), &address,
                UInt32(MemoryLayout<CFString>.size), pointer, &size, &device
            )
        }
        return status == noErr && device != kAudioObjectUnknown ? device : nil
    }

    static func defaultInputUID() -> String? {
        var address = AudioObjectPropertyAddress(
            mSelector: kAudioHardwarePropertyDefaultInputDevice,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )
        var device = AudioObjectID(kAudioObjectUnknown)
        var size = UInt32(MemoryLayout<AudioObjectID>.size)
        guard AudioObjectGetPropertyData(AudioObjectID(kAudioObjectSystemObject), &address, 0, nil, &size, &device) == noErr
        else { return nil }
        return string(device, kAudioDevicePropertyDeviceUID)
    }

    private static func string(_ id: AudioObjectID, _ selector: AudioObjectPropertySelector) -> String? {
        var address = AudioObjectPropertyAddress(
            mSelector: selector, mScope: kAudioObjectPropertyScopeGlobal, mElement: kAudioObjectPropertyElementMain
        )
        var value: Unmanaged<CFString>?
        var size = UInt32(MemoryLayout<Unmanaged<CFString>?>.size)
        let status = withUnsafeMutablePointer(to: &value) { AudioObjectGetPropertyData(id, &address, 0, nil, &size, $0) }
        guard status == noErr, let value else { return nil }
        return value.takeRetainedValue() as String
    }

    private static func hasStreams(_ id: AudioObjectID, scope: AudioObjectPropertyScope) -> Bool {
        var address = AudioObjectPropertyAddress(
            mSelector: kAudioDevicePropertyStreams, mScope: scope, mElement: kAudioObjectPropertyElementMain
        )
        var size: UInt32 = 0
        return AudioObjectGetPropertyDataSize(id, &address, 0, nil, &size) == noErr && size > 0
    }

    /// Our own short-lived tap aggregates (and other apps' private devices) are not choices.
    private static func isPrivateAggregate(uid: String) -> Bool {
        uid.hasPrefix("CADefaultDeviceAggregate") || uid.contains("Music Fetch")
    }
}

/// Live device list for Settings; refreshes when devices are connected or removed.
@Observable
final class AudioDeviceList {
    private(set) var inputs: [AudioDevice] = []
    private(set) var outputs: [AudioDevice] = []
    /// Microphones in priority order, including remembered devices that are not connected.
    private(set) var orderedInputs: [AudioDevice] = []
    private(set) var connectedUIDs: Set<String> = []

    @ObservationIgnored nonisolated(unsafe) private var listener: AudioObjectPropertyListenerBlock?

    init() {
        refresh()
        let block: AudioObjectPropertyListenerBlock = { [weak self] _, _ in
            Task { @MainActor in self?.refresh() }
        }
        listener = block
        var address = Self.devicesAddress
        AudioObjectAddPropertyListenerBlock(AudioObjectID(kAudioObjectSystemObject), &address, .main, block)
    }

    deinit {
        if let listener {
            var address = Self.devicesAddress
            AudioObjectRemovePropertyListenerBlock(AudioObjectID(kAudioObjectSystemObject), &address, .main, listener)
        }
    }

    nonisolated private static let devicesAddress = AudioObjectPropertyAddress(
        mSelector: kAudioHardwarePropertyDevices,
        mScope: kAudioObjectPropertyScopeGlobal,
        mElement: kAudioObjectPropertyElementMain
    )

    func refresh() {
        let devices = AudioDevices.all()
        inputs = devices.filter(\.hasInput)
        outputs = devices.filter(\.hasOutput)
        connectedUIDs = Set(inputs.map(\.uid))

        var names = AudioDevices.rememberedNames
        for device in inputs { names[device.uid] = device.name }
        AudioDevices.rememberedNames = names

        // Stored order first; new devices after it, the system default input leading them.
        var order = AudioDevices.inputPriority
        let defaultUID = AudioDevices.defaultInputUID()
        let new = inputs.map(\.uid).filter { !order.contains($0) }
            .sorted { ($0 == defaultUID ? 0 : 1) < ($1 == defaultUID ? 0 : 1) }
        order += new
        if order != AudioDevices.inputPriority { AudioDevices.inputPriority = order }
        orderedInputs = order.map { uid in
            inputs.first { $0.uid == uid } ?? AudioDevice(uid: uid, name: names[uid] ?? uid, hasInput: true, hasOutput: false)
        }
    }

    func move(from source: IndexSet, to destination: Int) {
        var order = orderedInputs.map(\.uid)
        let moving = source.map { order[$0] }
        let insertAt = destination - source.filter { $0 < destination }.count
        for index in source.sorted(by: >) { order.remove(at: index) }
        order.insert(contentsOf: moving, at: max(0, min(insertAt, order.count)))
        AudioDevices.inputPriority = order
        refresh()
    }

    func moveUp(_ uid: String) {
        guard let index = orderedInputs.firstIndex(where: { $0.uid == uid }), index > 0 else { return }
        move(from: IndexSet(integer: index), to: index - 1)
    }

    func moveDown(_ uid: String) {
        guard let index = orderedInputs.firstIndex(where: { $0.uid == uid }), index < orderedInputs.count - 1 else { return }
        move(from: IndexSet(integer: index), to: index + 2)
    }

    /// Forget a disconnected device.
    func forget(_ uid: String) {
        AudioDevices.inputPriority = AudioDevices.inputPriority.filter { $0 != uid }
        var names = AudioDevices.rememberedNames
        names[uid] = nil
        AudioDevices.rememberedNames = names
        refresh()
    }

    /// The microphone listening would use right now.
    var activeInputUID: String? {
        orderedInputs.first { connectedUIDs.contains($0.uid) }?.uid ?? AudioDevices.defaultInputUID()
    }
}
