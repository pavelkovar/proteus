//! Rewrites the real argv buffer so `/proc/<pid>/cmdline` shows a custom
//! title. `prctl(PR_SET_NAME)` would only reach the shorter
//! `/proc/<pid>/comm`.
//!
//! The argv pointer is captured by an `.init_array` constructor. Linux and
//! glibc only; a no-op stub elsewhere.

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod imp {
    use std::os::raw::{c_char, c_int};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Written once before `main` and only ever read after; there is no
    // concurrent access to guard against.
    static ARGV0_ADDR: AtomicUsize = AtomicUsize::new(0);
    static ARGV_LEN: AtomicUsize = AtomicUsize::new(0);

    #[unsafe(link_section = ".init_array")]
    #[used]
    static CAPTURE: unsafe extern "C" fn(c_int, *mut *mut c_char) = capture;

    unsafe extern "C" fn capture(argc: c_int, argv: *mut *mut c_char) {
        if argv.is_null() || argc <= 0 {
            return;
        }
        let first = unsafe { *argv };
        let last = unsafe { *argv.offset((argc - 1) as isize) };
        if first.is_null() || last.is_null() {
            return;
        }
        // The kernel packs argv's strings contiguously at exec(), so this
        // span is exactly what /proc reports - and the only memory safe to
        // reuse without spilling into envp right behind it.
        let last_len = unsafe { libc::strlen(last) };
        let total_len = (last as usize + last_len + 1).saturating_sub(first as usize);
        ARGV0_ADDR.store(first as usize, Ordering::Relaxed);
        ARGV_LEN.store(total_len, Ordering::Relaxed);
    }

    pub fn set_title(title: &str) {
        let addr = ARGV0_ADDR.load(Ordering::Relaxed);
        let len = ARGV_LEN.load(Ordering::Relaxed);
        if addr == 0 || len == 0 {
            return;
        }
        let bytes = title.as_bytes();
        let write_len = bytes.len().min(len - 1);
        unsafe {
            let dst = addr as *mut u8;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, write_len);
            // /proc shows everything up to arg_end, so a shorter title must
            // blank what a longer one left behind.
            std::ptr::write_bytes(dst.add(write_len), 0, len - write_len);
        }
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod imp {
    pub fn set_title(_title: &str) {}
}

/// Best-effort: a no-op where the capture never ran, truncating if `title`
/// is too long. Safe after `fork()`, the child writing only its own
/// copy-on-write page.
pub fn set_title(title: &str) {
    imp::set_title(title);
}
