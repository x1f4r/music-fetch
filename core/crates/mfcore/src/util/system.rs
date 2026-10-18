//! Hardware facts used to pick the engine's low-power configuration.
//!
//! Weak machines (few cores or little memory: budget Android phones, the
//! dual-core Intel MacBook Air) run the engine in low-power mode by default.

/// Machines with at most this many logical CPUs are weak.
pub const LOW_POWER_MAX_CPUS: usize = 4;
/// Machines with less physical memory than this are weak.
pub const LOW_POWER_MIN_MEMORY: u64 = 4 << 30;

/// What the engine knows about the machine it runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemInfo {
    /// Logical CPUs available to this process.
    pub cpus: usize,
    /// Physical memory in bytes, when the platform reports it.
    pub memory_bytes: Option<u64>,
}

impl SystemInfo {
    /// The current machine.
    pub fn current() -> Self {
        Self { cpus: logical_cpus(), memory_bytes: physical_memory() }
    }

    /// True on machines with at most 4 logical CPUs or less than 4 GiB of memory.
    pub fn is_low_end(&self) -> bool {
        self.cpus <= LOW_POWER_MAX_CPUS || self.memory_bytes.is_some_and(|m| m < LOW_POWER_MIN_MEMORY)
    }
}

/// Logical CPUs available to this process (1 when unknown).
pub fn logical_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

/// Installed physical memory in bytes (`hw.memsize` on Apple platforms,
/// `_SC_PHYS_PAGES × _SC_PAGESIZE` on Linux and Android).
pub fn physical_memory() -> Option<u64> {
    imp::physical_memory().filter(|&bytes| bytes > 0)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
mod imp {
    pub fn physical_memory() -> Option<u64> {
        let mut value: u64 = 0;
        let mut size = std::mem::size_of::<u64>();
        // SAFETY: the name is NUL-terminated; `value` and `size` describe a writable
        // u64, which is the type of `hw.memsize`.
        let rc = unsafe {
            libc::sysctlbyname(
                c"hw.memsize".as_ptr(),
                (&raw mut value).cast::<libc::c_void>(),
                &raw mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        (rc == 0 && size == std::mem::size_of::<u64>()).then_some(value)
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod imp {
    pub fn physical_memory() -> Option<u64> {
        // SAFETY: sysconf only reads configuration values.
        let (pages, page_size) = unsafe { (libc::sysconf(libc::_SC_PHYS_PAGES), libc::sysconf(libc::_SC_PAGESIZE)) };
        let pages = u64::try_from(pages).ok()?;
        let page_size = u64::try_from(page_size).ok()?;
        Some(pages.saturating_mul(page_size))
    }
}

#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "linux", target_os = "android")))]
mod imp {
    pub fn physical_memory() -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_end_thresholds() {
        let gib = 1u64 << 30;
        let info = |cpus, memory: Option<u64>| SystemInfo { cpus, memory_bytes: memory };
        assert!(info(4, Some(16 * gib)).is_low_end(), "4 logical CPUs");
        assert!(info(2, None).is_low_end());
        assert!(info(8, Some(3 * gib + gib / 2)).is_low_end(), "a \"4 GB\" phone reports less than 4 GiB");
        assert!(!info(8, Some(4 * gib)).is_low_end());
        assert!(!info(6, None).is_low_end(), "unknown memory alone does not decide");
    }

    #[test]
    fn reads_this_machine() {
        let info = SystemInfo::current();
        assert!(info.cpus >= 1);
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let bytes = info.memory_bytes.expect("memory size");
            assert!(bytes >= 512 << 20, "{bytes}");
        }
    }
}
