//! CPU affinity for pinning one serving thread per core.

use crate::logging;

/// The CPUs this process may actually run on - identities, not the count
/// `available_parallelism` gives: under a cpuset of `{4,5,6,7}` pinning to
/// `0..4` would miss all four. Empty means the mask could not be read.
pub fn allowed_cpus() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| libc::CPU_ISSET(cpu, &set))
            .collect()
    }
}

/// Pins the calling thread to one CPU, keeping its connections' state on one
/// core instead of following the thread around. Best-effort: a cpuset that
/// refuses is a reason to serve unpinned, not to refuse to serve.
pub fn pin_to_cpu(cpu: usize) {
    if cpu >= libc::CPU_SETSIZE as usize {
        return; // CPU_SET would index past the mask
    }
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) != 0 {
            logging::debug!(
                r#type = "controller",
                cpu,
                error = %std::io::Error::last_os_error(),
                "could not pin a serving thread to its CPU"
            );
        }
    }
}

#[cfg(test)]
#[path = "cpu_tests.rs"]
mod tests;
