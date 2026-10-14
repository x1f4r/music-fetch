import AudioToolbox
import CoreAudio
import Foundation

/// Captures what this Mac plays with a Core Audio process tap: a stereo global tap that
/// excludes Music Fetch itself (so previews never feed back), wrapped in a private
/// aggregate device whose IO proc pushes the samples on.
///
/// macOS asks for "System Audio Recording" permission when the device starts. Denial is
/// silent (the buffers are zeros); the engine reports that as `Notice.captureSilent`.
/// When the default output device changes, the chain is rebuilt on the new device.
nonisolated final class SystemAudioCapture: AudioCapture, @unchecked Sendable {
    private let queue = DispatchQueue(label: "local.musicfetch.system-audio", qos: .userInitiated)
    // All state below is touched only on `queue`.
    private var handler: PCMHandler?
    private var tapID = AudioObjectID(kAudioObjectUnknown)
    private var aggregateID = AudioObjectID(kAudioObjectUnknown)
    private var procID: AudioDeviceIOProcID?
    private var format = AudioStreamBasicDescription()
    private var defaultOutputListener: AudioObjectPropertyListenerBlock?

    private static let defaultOutputAddress = AudioObjectPropertyAddress(
        mSelector: kAudioHardwarePropertyDefaultOutputDevice,
        mScope: kAudioObjectPropertyScopeGlobal,
        mElement: kAudioObjectPropertyElementMain
    )

    func start(handler: @escaping PCMHandler) throws {
        try queue.sync {
            self.handler = handler
            do {
                try buildChain()
            } catch {
                teardownChain()
                self.handler = nil
                throw error
            }
            let listener: AudioObjectPropertyListenerBlock = { [weak self] _, _ in
                self?.defaultOutputChanged()
            }
            defaultOutputListener = listener
            var address = Self.defaultOutputAddress
            AudioObjectAddPropertyListenerBlock(AudioObjectID(kAudioObjectSystemObject), &address, queue, listener)
        }
    }

    func stop() {
        queue.sync {
            if let listener = defaultOutputListener {
                var address = Self.defaultOutputAddress
                AudioObjectRemovePropertyListenerBlock(AudioObjectID(kAudioObjectSystemObject), &address, queue, listener)
            }
            defaultOutputListener = nil
            handler = nil
            teardownChain()
        }
    }

    // MARK: - Chain

    private func buildChain() throws {
        let description: CATapDescription
        if let ownProcess = Self.processObject(for: getpid()) {
            description = CATapDescription(stereoGlobalTapButExcludeProcesses: [ownProcess])
        } else {
            // Not registered with Core Audio yet (nothing played so far): exclude by bundle id.
            description = CATapDescription(stereoGlobalTapButExcludeProcesses: [])
        }
        if let bundleID = Bundle.main.bundleIdentifier {
            description.bundleIDs = [bundleID]
        }
        description.uuid = UUID()
        description.name = "Music Fetch"
        description.isPrivate = true
        description.muteBehavior = .unmuted

        var tap = AudioObjectID(kAudioObjectUnknown)
        try check(AudioHardwareCreateProcessTap(description, &tap), "create the audio tap")
        tapID = tap

        let outputUID = try Self.defaultOutputDeviceUID()
        let aggregate: [String: Any] = [
            kAudioAggregateDeviceNameKey: "Music Fetch Capture",
            kAudioAggregateDeviceUIDKey: "local.musicfetch.capture.\(UUID().uuidString)",
            kAudioAggregateDeviceMainSubDeviceKey: outputUID,
            kAudioAggregateDeviceIsPrivateKey: true,
            kAudioAggregateDeviceIsStackedKey: false,
            kAudioAggregateDeviceTapAutoStartKey: true,
            kAudioAggregateDeviceSubDeviceListKey: [[kAudioSubDeviceUIDKey: outputUID]],
            kAudioAggregateDeviceTapListKey: [
                [kAudioSubTapDriftCompensationKey: true, kAudioSubTapUIDKey: description.uuid.uuidString]
            ],
        ]
        var device = AudioObjectID(kAudioObjectUnknown)
        try check(AudioHardwareCreateAggregateDevice(aggregate as CFDictionary, &device), "create the capture device")
        aggregateID = device

        format = try Self.tapFormat(tap)

        var proc: AudioDeviceIOProcID?
        try check(
            AudioDeviceCreateIOProcIDWithBlock(&proc, device, queue) { [weak self] _, input, _, _, _ in
                self?.deliver(input)
            },
            "start system audio capture")
        procID = proc
        try check(AudioDeviceStart(device, proc), "start system audio capture")
    }

    private func teardownChain() {
        if aggregateID != kAudioObjectUnknown {
            if let procID {
                AudioDeviceStop(aggregateID, procID)
                AudioDeviceDestroyIOProcID(aggregateID, procID)
            }
            AudioHardwareDestroyAggregateDevice(aggregateID)
        }
        if tapID != kAudioObjectUnknown {
            AudioHardwareDestroyProcessTap(tapID)
        }
        procID = nil
        aggregateID = AudioObjectID(kAudioObjectUnknown)
        tapID = AudioObjectID(kAudioObjectUnknown)
    }

    private func deliver(_ input: UnsafePointer<AudioBufferList>) {
        guard let handler, format.mSampleRate > 0,
            let converted = PCMConversion.interleavedFloat32(input, format: format)
        else { return }
        handler(converted.data, UInt32(format.mSampleRate.rounded()), converted.channels)
    }

    private func defaultOutputChanged() {
        guard handler != nil else { return }
        teardownChain()
        do {
            try buildChain()
        } catch {
            Log.capture.error("Rebuilding system audio capture failed: \(error.localizedDescription, privacy: .public)")
            teardownChain()
        }
    }

    private func check(_ status: OSStatus, _ operation: String) throws {
        guard status == noErr else { throw CaptureError.coreAudio(operation: operation, status: status) }
    }

    // MARK: - Core Audio queries

    private static func processObject(for pid: pid_t) -> AudioObjectID? {
        var address = AudioObjectPropertyAddress(
            mSelector: kAudioHardwarePropertyTranslatePIDToProcessObject,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )
        var pid = pid
        var object = AudioObjectID(kAudioObjectUnknown)
        var size = UInt32(MemoryLayout<AudioObjectID>.size)
        let status = AudioObjectGetPropertyData(
            AudioObjectID(kAudioObjectSystemObject), &address, UInt32(MemoryLayout<pid_t>.size), &pid, &size, &object)
        return status == noErr && object != kAudioObjectUnknown ? object : nil
    }

    private static func defaultOutputDeviceUID() throws -> String {
        var deviceAddress = defaultOutputAddress
        var device = AudioObjectID(kAudioObjectUnknown)
        var size = UInt32(MemoryLayout<AudioObjectID>.size)
        var status = AudioObjectGetPropertyData(AudioObjectID(kAudioObjectSystemObject), &deviceAddress, 0, nil, &size, &device)
        guard status == noErr, device != kAudioObjectUnknown else {
            throw CaptureError.coreAudio(operation: "find the output device", status: status)
        }
        var uidAddress = AudioObjectPropertyAddress(
            mSelector: kAudioDevicePropertyDeviceUID,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )
        var uid: Unmanaged<CFString>?
        size = UInt32(MemoryLayout<Unmanaged<CFString>?>.size)
        status = withUnsafeMutablePointer(to: &uid) { AudioObjectGetPropertyData(device, &uidAddress, 0, nil, &size, $0) }
        guard status == noErr, let uid else {
            throw CaptureError.coreAudio(operation: "read the output device", status: status)
        }
        return uid.takeRetainedValue() as String
    }

    private static func tapFormat(_ tap: AudioObjectID) throws -> AudioStreamBasicDescription {
        var address = AudioObjectPropertyAddress(
            mSelector: kAudioTapPropertyFormat,
            mScope: kAudioObjectPropertyScopeGlobal,
            mElement: kAudioObjectPropertyElementMain
        )
        var description = AudioStreamBasicDescription()
        var size = UInt32(MemoryLayout<AudioStreamBasicDescription>.size)
        let status = AudioObjectGetPropertyData(tap, &address, 0, nil, &size, &description)
        guard status == noErr else { throw CaptureError.coreAudio(operation: "read the tap format", status: status) }
        return description
    }
}
