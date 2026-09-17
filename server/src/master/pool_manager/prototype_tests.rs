use super::*;
use std::os::fd::AsRawFd;

/// After relocation neither source can still sit in the target range, so the
/// `dup2`s that follow cannot clobber each other.
///
/// The collision itself cannot be provoked from a test, fd numbers being
/// whatever the OS has free, so this pins the invariant instead.
#[test]
fn relocate_above_moves_an_fd_clear_of_the_target_range() {
    let (read_end, write_end) = nix::unistd::pipe().unwrap();
    let original = read_end.as_raw_fd();

    let moved = unsafe { relocate_above(original, CONFIG_FD) }.expect("relocation must succeed");
    assert!(
        moved > CONFIG_FD,
        "relocated fd {moved} must be clear of CONTROL_FD/CONFIG_FD"
    );
    assert_ne!(moved, original);

    // The same underlying pipe, not merely some free number.
    nix::unistd::write(&write_end, b"x").unwrap();
    let mut buf = [0u8; 1];
    let n = unsafe { libc::read(moved, buf.as_mut_ptr() as *mut libc::c_void, 1) };
    assert_eq!(n, 1);
    assert_eq!(&buf, b"x");

    unsafe { libc::close(moved) };
}

/// Even an fd already above the floor gets a fresh number: the caller closes
/// what it gets back, and returning the original would close a descriptor it
/// does not own.
#[test]
fn relocate_above_returns_a_new_fd_even_when_the_original_is_already_high() {
    let (read_end, _write_end) = nix::unistd::pipe().unwrap();
    // Forced rather than assumed: which number the OS picks depends on what
    // else this process has open.
    let original = unsafe { libc::fcntl(read_end.as_raw_fd(), libc::F_DUPFD, 100) };
    assert!(
        original > CONFIG_FD,
        "fcntl(F_DUPFD, 100) must return a high fd"
    );

    let moved = unsafe { relocate_above(original, CONFIG_FD) }.unwrap();
    assert_ne!(
        moved, original,
        "must be a distinct descriptor the caller can close safely"
    );
    assert!(moved > CONFIG_FD);

    unsafe { libc::close(moved) };
    unsafe { libc::close(original) };
}
