//! Shared-memory SPSC ring buffers, master <-> worker: 4-byte LE length
//! prefix + postcard payload, with futex/eventfd standing in for a socket
//! buffer.
//!
//! A ring cannot distinguish a dead peer from a slow one - there is no fd to
//! see EOF on - so `PeerDeath` carries that signal in from the liveness
//! socket and wakes everyone parked on either ring.
//!
//! The two sides park differently and the calls are not interchangeable: the
//! worker has no tokio and parks its thread on a futex word, while master
//! parks the *task* via `AsyncFd`/eventfd, because blocking a shared tokio
//! thread would stall every other pooled connection.

use crate::logging;
use std::cell::UnsafeCell;
use std::os::fd::{BorrowedFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

/// Sized so that every request hyper is configured to accept fits one frame:
/// the head twice over - the URI and Host are each copied into a field of
/// their own - plus an inline body and the resolved paths.
pub const REQUEST_RING_CAPACITY: usize = 256 * 1024;

/// Enough for a streamed response to pipeline several frames ahead instead
/// of stalling the worker on master. Costs one mapping per pooled worker.
pub const RESPONSE_RING_CAPACITY: usize = 256 * 1024;

const EMPTY: u32 = 0;
const WAITING: u32 = 1;
/// Stamped over both state words before waking. Load-bearing: `futex_wait`
/// sleeps only while the word still reads `WAITING`, so a waiter that
/// checked `is_dead()` a moment too early gets `EAGAIN` instead of sleeping
/// through the only wake coming for it. `peer_death` remains the authority.
const DEAD: u32 = 2;

/// Little-endian frame length, ahead of every payload.
const LEN_PREFIX: usize = 4;

/// A `Ring` sits on the master<->worker trust boundary, where a
/// peer-triggered panic would take down the whole process. Errors, never
/// panics; both variants are fatal to the channel.
#[derive(Debug)]
pub enum RingError {
    FrameTooLarge,
    PeerGone,
    /// A length prefix was published without the payload behind it, which
    /// `raw_write_frame` makes impossible: the peer is scribbling on the
    /// cursors, so the ring cannot be read any further.
    Truncated,
}

/// Returns on expiry exactly as it does on a wake, so callers must recheck
/// their condition either way.
unsafe fn futex_wait(addr: &AtomicU32, expected: u32, timeout: Option<std::time::Duration>) {
    let spec = timeout.map(|d| libc::timespec {
        tv_sec: d.as_secs() as libc::time_t,
        tv_nsec: d.subsec_nanos() as _,
    });
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr as *const AtomicU32 as *const u32,
            libc::FUTEX_WAIT,
            expected,
            spec.as_ref()
                .map_or(std::ptr::null(), |s| s as *const libc::timespec),
        );
    }
}

unsafe fn futex_wake_all(addr: &AtomicU32) {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            addr as *const AtomicU32 as *const u32,
            libc::FUTEX_WAKE,
            i32::MAX,
        );
    }
}

/// Grouped so callers cannot swap the two by argument order.
pub struct NotifyEfds {
    pub req_space: OwnedFd,
    pub resp_data: OwnedFd,
}

impl NotifyEfds {
    /// Fresh fds onto the same eventfds, so a second owner can notify
    /// without racing whoever drops the original first.
    pub fn try_clone(&self) -> std::io::Result<NotifyEfds> {
        Ok(NotifyEfds {
            req_space: dup_cloexec(&self.req_space)?,
            resp_data: dup_cloexec(&self.resp_data)?,
        })
    }
}

fn dup_cloexec(fd: &OwnedFd) -> std::io::Result<OwnedFd> {
    use nix::fcntl::{FcntlArg, fcntl};
    use std::os::fd::FromRawFd;
    let raw = fcntl(fd, FcntlArg::F_DUPFD_CLOEXEC(0)).map_err(std::io::Error::from)?;
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

pub fn create_notify_eventfd() -> std::io::Result<OwnedFd> {
    use nix::sys::eventfd::{EfdFlags, EventFd};
    EventFd::from_flags(EfdFlags::EFD_NONBLOCK | EfdFlags::EFD_CLOEXEC)
        .map(OwnedFd::from)
        .map_err(std::io::Error::from)
}

/// Best-effort: a dropped wake is safe because waiters recheck before
/// parking, not because this cannot fail.
///
/// # Safety
/// `fd` must stay open for the whole call.
pub(crate) fn eventfd_notify(fd: RawFd) {
    let payload = 1u64.to_ne_bytes();
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let _ = nix::unistd::write(borrowed, &payload);
}

/// Parks the task, not a thread. Caller rechecks its condition after.
async fn wait_readable_and_drain(efd: &AsyncFd<OwnedFd>) -> std::io::Result<()> {
    efd.async_io(Interest::READABLE, |inner| {
        let mut buf = [0u8; 8];
        loop {
            match nix::unistd::read(inner, &mut buf) {
                Ok(_) => return Ok(()),
                Err(nix::errno::Errno::EINTR) => continue,
                Err(e) => return Err(std::io::Error::from(e)),
            }
        }
    })
    .await
}

/// Returns the range's physical pages to the kernel. Must be `fallocate`,
/// not `madvise(MADV_DONTNEED)`, which is a no-op on a MAP_SHARED memfd.
fn punch_hole(fd: &OwnedFd, offset: u64, len: u64) -> std::io::Result<()> {
    use nix::fcntl::{FallocateFlags, fallocate};
    fallocate(
        fd,
        FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE,
        offset as i64,
        len as i64,
    )
    .map_err(std::io::Error::from)
}

/// Set once, by master; a worker can only read it, never see its own death.
#[repr(C)]
pub struct PeerDeath {
    dead: AtomicU32,
}

impl PeerDeath {
    pub fn init_in_place(ptr: *mut PeerDeath) {
        unsafe { (*ptr).dead = AtomicU32::new(0) };
    }

    /// `SeqCst` so this shares one total order with the park protocol's own
    /// stores and rechecks - see `declare_waiting`.
    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst) != 0
    }

    fn mark_dead(&self) {
        self.dead.store(1, Ordering::SeqCst);
    }
}

/// The false-sharing granule, not the L1 line size: Intel prefetches 64B
/// lines in pairs and ARM's big cores run a 128B L2 line.
const CACHE_LINE: usize = 128;

/// `CAPACITY` must be a power of two. The positions never wrap, so free and
/// available fall out of plain subtraction with no empty-vs-full ambiguity.
///
/// The padding keeps the producer-only and consumer-only positions off one
/// another's cache lines; without it every producer write invalidates the
/// consumer's cached position and vice versa.
#[repr(C, align(128))]
pub struct Ring<const CAPACITY: usize> {
    write_pos: AtomicU64,
    _pad_write_pos: [u8; CACHE_LINE - size_of::<AtomicU64>()],
    read_pos: AtomicU64,
    _pad_read_pos: [u8; CACHE_LINE - size_of::<AtomicU64>()],
    /// `WAITING` only while someone is parked, so the common case reaches no
    /// syscall. Sharing a line with `data_state` is free: both are touched
    /// only on the parking path.
    space_state: AtomicU32,
    data_state: AtomicU32,
    /// How far `reclaim_if_due` has punched. Master owns it on both rings, so
    /// on the request ring a different process writes it than writes `read_pos`.
    reclaimed_pos: AtomicU64,
    _pad_state: [u8; CACHE_LINE - 2 * size_of::<AtomicU32>() - size_of::<AtomicU64>()],
    /// Mutated through `&Ring` by design, so it cannot be a bare array:
    /// writes through a shared reference would break Rust's aliasing rules
    /// even though they would appear to work.
    buf: UnsafeCell<[u8; CAPACITY]>,
}

// Guards the hand-computed padding: adding a field without redoing it would
// otherwise still compile, silently reintroducing false sharing.
const _: () = {
    assert!(std::mem::offset_of!(Ring<REQUEST_RING_CAPACITY>, read_pos) % CACHE_LINE == 0);
    assert!(std::mem::offset_of!(Ring<REQUEST_RING_CAPACITY>, space_state) % CACHE_LINE == 0);
    assert!(std::mem::offset_of!(Ring<REQUEST_RING_CAPACITY>, buf) % CACHE_LINE == 0);
};

// Safety: every shared access goes through the atomics and the wait/notify
// protocol below.
unsafe impl<const CAPACITY: usize> Sync for Ring<CAPACITY> {}

impl<const CAPACITY: usize> Ring<CAPACITY> {
    /// `MASK` substitutes for `% CAPACITY` only on a power of two; otherwise
    /// every wrapped access corrupts silently rather than failing visibly.
    const _CAPACITY_IS_POWER_OF_TWO: () = assert!(CAPACITY.is_power_of_two());

    /// Bounds every frame, so `raw_write_frame`'s `u32` length cannot truncate.
    const _CAPACITY_FITS_A_U32_LENGTH: () = assert!(CAPACITY <= u32::MAX as usize);

    const MASK: u64 = (CAPACITY as u64) - 1;

    pub fn init_in_place(ptr: *mut Ring<CAPACITY>) {
        // An associated const in a generic impl is evaluated only where it
        // is named; without this the assert never fires.
        () = Self::_CAPACITY_IS_POWER_OF_TWO;
        () = Self::_CAPACITY_FITS_A_U32_LENGTH;
        unsafe {
            (*ptr).write_pos = AtomicU64::new(0);
            (*ptr).read_pos = AtomicU64::new(0);
            (*ptr).space_state = AtomicU32::new(EMPTY);
            (*ptr).data_state = AtomicU32::new(EMPTY);
            (*ptr).reclaimed_pos = AtomicU64::new(0);
            // buf needs no init: every byte is written before it is read.
        }
    }

    /// Declare-then-recheck. `false` means the condition came true, or the
    /// peer died, during the window, with the state word already restored.
    ///
    /// The `WAITING` store and the recheck load must both be `SeqCst`.
    /// `Release`/`Acquire` permits a StoreLoad reorder (the SB litmus test),
    /// letting this read a stale position before its own store lands - so it
    /// wakes nobody and then sleeps through the only wake it was going to
    /// get. Reachable on x86_64, where those orderings are plain `mov`s;
    /// aarch64's `stlr`/`ldar` forbid the reorder, so no ARM test can
    /// exercise the bug.
    fn declare_waiting(
        state: &AtomicU32,
        peer: &PeerDeath,
        condition_met: impl Fn() -> bool,
    ) -> Result<bool, RingError> {
        state.store(WAITING, Ordering::SeqCst);
        if condition_met() {
            state.store(EMPTY, Ordering::SeqCst);
            return Ok(false);
        }
        // After the WAITING store, so a killer stamping DEAD between the two
        // either loses the race here or wins it and makes futex_wait EAGAIN.
        if peer.is_dead() {
            state.store(EMPTY, Ordering::SeqCst);
            return Err(RingError::PeerGone);
        }
        Ok(true)
    }

    /// Parks without spinning first: the wait is for the next unit of work
    /// and may be unbounded, unlike a mutex's short critical section.
    fn wait_for_space(&self, needed: usize, peer: &PeerDeath) -> Result<(), RingError> {
        let w = self.write_pos.load(Ordering::Relaxed); // producer-only
        loop {
            if self.has_space(w, needed) {
                return Ok(());
            }
            if peer.is_dead() {
                return Err(RingError::PeerGone);
            }
            if Self::declare_waiting(&self.space_state, peer, || self.has_space(w, needed))? {
                unsafe { futex_wait(&self.space_state, WAITING, None) };
            } else {
                return Ok(());
            }
        }
    }

    fn wait_for_data(&self, needed: usize, peer: &PeerDeath) -> Result<(), RingError> {
        self.wait_for_data_until(needed, peer, None).map(|_| ())
    }

    /// `Ok(false)` once `deadline` passes. The clock is rechecked around the
    /// park rather than inferred from the futex returning, which also happens
    /// on spurious wakes and on every real notify.
    fn wait_for_data_until(
        &self,
        needed: usize,
        peer: &PeerDeath,
        deadline: Option<std::time::Instant>,
    ) -> Result<bool, RingError> {
        let r = self.read_pos.load(Ordering::Relaxed); // consumer-only
        loop {
            if self.has_data(r, needed) {
                return Ok(true);
            }
            if peer.is_dead() {
                return Err(RingError::PeerGone);
            }
            let remaining = match deadline {
                Some(deadline) => {
                    match deadline.checked_duration_since(std::time::Instant::now()) {
                        Some(remaining) => Some(remaining),
                        None => return Ok(false),
                    }
                }
                None => None,
            };
            if Self::declare_waiting(&self.data_state, peer, || self.has_data(r, needed))? {
                unsafe { futex_wait(&self.data_state, WAITING, remaining) };
            } else {
                return Ok(true);
            }
        }
    }

    /// `SeqCst` so `declare_waiting`'s recheck joins the same total order as
    /// its `WAITING` store. Free on the fast path - a `SeqCst` load is the
    /// same instruction as an `Acquire` one on x86_64 and aarch64.
    fn has_space(&self, w: u64, needed: usize) -> bool {
        let r = self.read_pos.load(Ordering::SeqCst);
        CAPACITY - (w - r) as usize >= needed
    }

    fn has_data(&self, r: u64, needed: usize) -> bool {
        let w = self.write_pos.load(Ordering::SeqCst);
        (w - r) as usize >= needed
    }

    async fn wait_for_space_async(
        &self,
        needed: usize,
        peer: &PeerDeath,
        efd: &AsyncFd<OwnedFd>,
    ) -> Result<(), RingError> {
        let w = self.write_pos.load(Ordering::Relaxed);
        loop {
            if self.has_space(w, needed) {
                return Ok(());
            }
            if peer.is_dead() {
                return Err(RingError::PeerGone);
            }
            if !Self::declare_waiting(&self.space_state, peer, || self.has_space(w, needed))? {
                return Ok(());
            }
            // A broken eventfd is as unrecoverable as a dead peer.
            if wait_readable_and_drain(efd).await.is_err() {
                return Err(RingError::PeerGone);
            }
        }
    }

    async fn wait_for_data_async(
        &self,
        needed: usize,
        peer: &PeerDeath,
        efd: &AsyncFd<OwnedFd>,
    ) -> Result<(), RingError> {
        let r = self.read_pos.load(Ordering::Relaxed);
        loop {
            if self.has_data(r, needed) {
                return Ok(());
            }
            if peer.is_dead() {
                return Err(RingError::PeerGone);
            }
            if !Self::declare_waiting(&self.data_state, peer, || self.has_data(r, needed))? {
                return Ok(());
            }
            if wait_readable_and_drain(efd).await.is_err() {
                return Err(RingError::PeerGone);
            }
        }
    }

    /// Clears a waiter's claim and reports whether there was one. `SeqCst`
    /// for `declare_waiting`'s reason - this RMW is the other half of that
    /// total order. Clobbering `DEAD` back to `EMPTY` is harmless, since
    /// `peer_death` is the authority and waiters recheck it.
    fn take_waiter(state: &AtomicU32) -> bool {
        state.swap(EMPTY, Ordering::SeqCst) == WAITING
    }

    fn notify_data_written(&self) {
        if Self::take_waiter(&self.data_state) {
            unsafe { futex_wake_all(&self.data_state) };
        }
    }

    /// For a reader parked on `AsyncFd` rather than the futex word.
    fn notify_data_written_eventfd(&self, efd: RawFd) {
        if Self::take_waiter(&self.data_state) {
            eventfd_notify(efd);
        }
    }

    fn notify_space_freed(&self) {
        if Self::take_waiter(&self.space_state) {
            unsafe { futex_wake_all(&self.space_state) };
        }
    }

    fn notify_space_freed_eventfd(&self, efd: RawFd) {
        if Self::take_waiter(&self.space_state) {
            eventfd_notify(efd);
        }
    }

    /// Reaches a waiter whichever call it is blocked in. Futex waiters only;
    /// an `AsyncFd` waiter is woken through its eventfd instead.
    fn mark_dead_and_wake(&self) {
        self.space_state.store(DEAD, Ordering::SeqCst);
        self.data_state.store(DEAD, Ordering::SeqCst);
        unsafe {
            futex_wake_all(&self.space_state);
            futex_wake_all(&self.data_state);
        }
    }

    /// Keeps small responses from paying for a `fallocate` at all, while a
    /// large streamed one still returns its pages promptly.
    const RECLAIM_THRESHOLD: u64 = 64 * 1024;

    /// Two atomic loads, so a caller can skip the `spawn_blocking` hop to
    /// `reclaim_if_due` in the common case where it would do nothing.
    fn is_reclaim_due(&self) -> bool {
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        let reclaimed = self.reclaimed_pos.load(Ordering::Relaxed);
        read_pos - reclaimed >= Self::RECLAIM_THRESHOLD
    }

    /// Punches out the range consumed since the last reclaim. `file_offset`
    /// is `buf`'s offset within the memfd, not within the ring.
    ///
    /// # Caller's safety burden
    /// The writer must be provably done with this ring - between responses,
    /// never mid-write. The instant `read_pos` moves the writer may start
    /// filling the freed space, and punching it then zeroes live data. An
    /// earlier version reclaimed per frame and corrupted streamed responses.
    ///
    /// The empty-ring check below rejects the obvious violations, but it is a
    /// snapshot: only the protocol makes this safe.
    fn reclaim_if_due(&self, fd: &OwnedFd, file_offset: u64) {
        // Relaxed suffices because the position only grows: a stale read
        // punches less than it could, never more. On the request ring this is
        // the peer's write, not ours.
        let read_pos = self.read_pos.load(Ordering::Relaxed);
        let reclaimed = self.reclaimed_pos.load(Ordering::Relaxed);
        let consumed = read_pos - reclaimed;
        if consumed < Self::RECLAIM_THRESHOLD {
            return;
        }
        // Checked in release too, not merely asserted: punching a range the
        // writer may be filling zeroes live data, while skipping costs only
        // residency. A necessary condition, not a proof - see the safety note.
        if self.write_pos.load(Ordering::Relaxed) != read_pos {
            logging::warn!(
                r#type = "controller",
                "skipped a ring reclaim: the writer is not idle"
            );
            return;
        }

        let start = reclaimed & Self::MASK;
        let end = read_pos & Self::MASK;
        let result = if consumed >= CAPACITY as u64 {
            // A full lap: masked positions can no longer tell laps apart, and
            // under-punching would lose the skipped range for good.
            punch_hole(fd, file_offset, CAPACITY as u64)
        } else if end > start {
            punch_hole(fd, file_offset + start, end - start)
        } else {
            // fallocate EINVALs on zero length, hence the end == 0 skip.
            punch_hole(fd, file_offset + start, CAPACITY as u64 - start).and_then(|()| {
                if end == 0 {
                    Ok(())
                } else {
                    punch_hole(fd, file_offset, end)
                }
            })
        };
        if let Err(e) = result {
            logging::debug!(r#type = "controller", error = %e, "fallocate(FALLOC_FL_PUNCH_HOLE) failed, skipping reclaim");
        }
        // A failed punch costs residency, not correctness, and retrying it
        // would not help.
        self.reclaimed_pos.store(read_pos, Ordering::Relaxed);
    }

    /// Copies without publishing; the caller advances `write_pos` once the
    /// whole frame is in place.
    ///
    /// # Safety
    /// Single-producer only, and the range must be free space already waited for.
    unsafe fn copy_at(&self, pos: u64, data: &[u8]) {
        let start = (pos & Self::MASK) as usize;
        let len = data.len();
        let base = self.buf.get() as *mut u8;
        unsafe {
            if start + len <= CAPACITY {
                std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(start), len);
            } else {
                let first = CAPACITY - start;
                std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(start), first);
                std::ptr::copy_nonoverlapping(data.as_ptr().add(first), base, len - first);
            }
        }
    }

    /// Advances `write_pos` once per frame, so a reader never observes a
    /// length prefix without the payload behind it.
    ///
    /// # Safety
    /// Single-producer only - concurrent callers would race `write_pos` - and
    /// `LEN_PREFIX + payload.len()` bytes of space must already have been
    /// waited for.
    unsafe fn raw_write_frame(&self, payload: &[u8]) {
        debug_assert!(
            payload.len() <= CAPACITY - LEN_PREFIX,
            "caller must reject an oversized frame"
        );
        let w = self.write_pos.load(Ordering::Relaxed);
        unsafe {
            self.copy_at(w, &(payload.len() as u32).to_le_bytes());
            self.copy_at(w + LEN_PREFIX as u64, payload);
        }
        // Release: publishes both copies above to whoever reads this frame.
        self.write_pos
            .store(w + (LEN_PREFIX + payload.len()) as u64, Ordering::Release);
    }

    /// Copies out without touching `read_pos`, so the bytes stay available
    /// to a later consuming read.
    ///
    /// # Safety
    /// `pos..pos + len` must have been published by the writer, which for a
    /// reader means seen through an acquire load of `write_pos`. `dst` must
    /// be valid for `len` bytes of writes; every one of them is written, so
    /// it need not be initialized beforehand.
    unsafe fn peek_into(&self, pos: u64, dst: *mut u8, len: usize) {
        let start = (pos & Self::MASK) as usize;
        let base = self.buf.get() as *const u8;
        unsafe {
            if start + len <= CAPACITY {
                std::ptr::copy_nonoverlapping(base.add(start), dst, len);
            } else {
                let first = CAPACITY - start;
                std::ptr::copy_nonoverlapping(base.add(start), dst, first);
                std::ptr::copy_nonoverlapping(base, dst.add(first), len - first);
            }
        }
    }

    /// # Safety
    /// Single-consumer only - concurrent callers would race `read_pos`.
    /// Same requirement on `dst` as `peek_into`, at the current `read_pos`.
    unsafe fn raw_read_into(&self, dst: *mut u8, len: usize) {
        let r = self.read_pos.load(Ordering::Relaxed);
        unsafe { self.peek_into(r, dst, len) };
        self.read_pos.store(r + len as u64, Ordering::Release);
    }

    /// # Safety
    /// Single-consumer only, same as `raw_read_into`.
    unsafe fn raw_read(&self, out: &mut [u8]) {
        unsafe { self.raw_read_into(out.as_mut_ptr(), out.len()) }
    }

    /// Replaces `scratch` with the next `len` bytes, reusing its allocation.
    /// Not `resize(len, 0)`, whose zero-fill is overwritten in full on the
    /// very next line.
    ///
    /// # Safety
    /// Single-consumer only, same as `raw_read_into`.
    unsafe fn read_into_scratch(&self, scratch: &mut Vec<u8>, len: usize) {
        scratch.clear();
        scratch.reserve(len);
        unsafe {
            // `reserve` after `clear` guarantees `len` writable bytes, and
            // `raw_read_into` writes all of them before `set_len`.
            self.raw_read_into(scratch.as_mut_ptr(), len);
            scratch.set_len(len);
        }
    }

    /// Blocking, so worker-side only. Waits for the whole frame's space up
    /// front, so a reader never sees a partially-written frame.
    pub fn write_frame(
        &self,
        payload: &[u8],
        peer: &PeerDeath,
        notify_efd: RawFd,
    ) -> Result<(), RingError> {
        if payload.len() > CAPACITY - LEN_PREFIX {
            return Err(RingError::FrameTooLarge);
        }
        let total = LEN_PREFIX + payload.len();
        self.wait_for_space(total, peer)?;
        unsafe {
            self.raw_write_frame(payload);
        }
        self.notify_data_written_eventfd(notify_efd);
        Ok(())
    }

    #[cfg(test)]
    pub fn read_frame(
        &self,
        scratch: &mut Vec<u8>,
        peer: &PeerDeath,
        notify_efd: RawFd,
    ) -> Result<(), RingError> {
        self.read_frame_until(scratch, peer, notify_efd, None)
            .map(|_| ())
    }

    /// `deadline` bounds only the wait for a frame to *start*: abandoning a
    /// half-read frame would desync the ring, so the rest is waited for
    /// unconditionally. `Ok(false)` means it expired with nothing there.
    pub fn read_frame_until(
        &self,
        scratch: &mut Vec<u8>,
        peer: &PeerDeath,
        notify_efd: RawFd,
        deadline: Option<std::time::Instant>,
    ) -> Result<bool, RingError> {
        if !self.wait_for_data_until(LEN_PREFIX, peer, deadline)? {
            return Ok(false);
        }
        let mut len_buf = [0u8; 4];
        unsafe { self.raw_read(&mut len_buf) };
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > CAPACITY - LEN_PREFIX {
            return Err(RingError::FrameTooLarge);
        }
        self.wait_for_data(len, peer)?;
        unsafe { self.read_into_scratch(scratch, len) };
        // Once per frame, not per raw_read.
        self.notify_space_freed_eventfd(notify_efd);
        Ok(true)
    }

    pub async fn write_frame_async(
        &self,
        payload: &[u8],
        peer: &PeerDeath,
        space_efd: &AsyncFd<OwnedFd>,
    ) -> Result<(), RingError> {
        if payload.len() > CAPACITY - LEN_PREFIX {
            return Err(RingError::FrameTooLarge);
        }
        let total = LEN_PREFIX + payload.len();
        self.wait_for_space_async(total, peer, space_efd).await?;
        unsafe {
            self.raw_write_frame(payload);
        }
        // The reader here is always a worker, so always a futex waiter.
        self.notify_data_written();
        Ok(())
    }

    /// Reads one whole frame if the writer has already published one.
    /// `Ok(false)` means the ring is empty right now - never end of stream.
    ///
    /// Consumes either the whole frame or nothing, so an async caller can be
    /// cancelled around it without desyncing `read_pos`. Frees space through
    /// the futex, so the writer must be one that waits on it rather than on
    /// an eventfd.
    pub fn try_read_frame(
        &self,
        scratch: &mut Vec<u8>,
        peer: &PeerDeath,
    ) -> Result<bool, RingError> {
        let r = self.read_pos.load(Ordering::Relaxed); // consumer-only
        if !self.has_data(r, LEN_PREFIX) {
            if peer.is_dead() {
                return Err(RingError::PeerGone);
            }
            return Ok(false);
        }
        let mut len_buf = [0u8; LEN_PREFIX];
        unsafe { self.peek_into(r, len_buf.as_mut_ptr(), LEN_PREFIX) };
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > CAPACITY - LEN_PREFIX {
            return Err(RingError::FrameTooLarge);
        }
        if !self.has_data(r, LEN_PREFIX + len) {
            return Err(RingError::Truncated);
        }
        unsafe {
            self.raw_read(&mut len_buf);
            self.read_into_scratch(scratch, len);
        }
        // The writer here is always a worker, so always a futex waiter.
        self.notify_space_freed();
        Ok(true)
    }

    pub async fn read_frame_async(
        &self,
        scratch: &mut Vec<u8>,
        peer: &PeerDeath,
        data_efd: &AsyncFd<OwnedFd>,
    ) -> Result<(), RingError> {
        loop {
            if self.try_read_frame(scratch, peer)? {
                return Ok(());
            }
            self.wait_for_data_async(LEN_PREFIX, peer, data_efd).await?;
        }
    }
}

pub type RequestRing = Ring<REQUEST_RING_CAPACITY>;
pub type ResponseRing = Ring<RESPONSE_RING_CAPACITY>;

/// One worker's whole data channel, in a single memfd mapping shared with
/// master.
#[repr(C)]
pub struct Channel {
    pub request: RequestRing,
    pub response: ResponseRing,
    pub peer_death: PeerDeath,
}

impl Channel {
    pub fn init_in_place(ptr: *mut Channel) {
        unsafe {
            RequestRing::init_in_place(std::ptr::addr_of_mut!((*ptr).request));
            ResponseRing::init_in_place(std::ptr::addr_of_mut!((*ptr).response));
            PeerDeath::init_in_place(std::ptr::addr_of_mut!((*ptr).peer_death));
        }
    }

    /// `mark_dead()` strictly first: the `DEAD` stamp only bounces a waiter
    /// back into its retry loop, which must then find `peer_death` already
    /// set or it simply parks again.
    pub fn mark_peer_dead(&self) {
        self.peer_death.mark_dead();
        self.request.mark_dead_and_wake();
        self.response.mark_dead_and_wake();
    }

    /// `fallocate` needs an offset into the memfd, not into the ring.
    const RESPONSE_BUF_FILE_OFFSET: u64 =
        (std::mem::offset_of!(Channel, response) + std::mem::offset_of!(ResponseRing, buf)) as u64;

    const REQUEST_BUF_FILE_OFFSET: u64 =
        (std::mem::offset_of!(Channel, request) + std::mem::offset_of!(RequestRing, buf)) as u64;

    /// Without this, a worker that streamed one large response keeps every
    /// page resident for the rest of its life. Callable only between
    /// responses - see `reclaim_if_due`'s safety note.
    fn reclaim_response(&self, fd: &OwnedFd) {
        self.response
            .reclaim_if_due(fd, Self::RESPONSE_BUF_FILE_OFFSET);
    }

    /// The request ring reaches full residency on request *count* alone: the
    /// positions wrap, so even small requests eventually touch every page.
    fn reclaim_request(&self, fd: &OwnedFd) {
        self.request
            .reclaim_if_due(fd, Self::REQUEST_BUF_FILE_OFFSET);
    }
}

/// Owned mmap of one worker's [`Channel`]; `Drop` unmaps it.
pub struct MappedChannel {
    ptr: std::ptr::NonNull<Channel>,
    /// Held open only by master, which needs it to `fallocate`. A worker
    /// never reads the response ring and has nothing to reclaim.
    fd: Option<OwnedFd>,
}

unsafe impl Send for MappedChannel {}
unsafe impl Sync for MappedChannel {}

impl MappedChannel {
    pub fn channel(&self) -> &Channel {
        unsafe { self.ptr.as_ref() }
    }

    /// No-op on a worker's mapping. One moment serves both rings, but not for
    /// one reason: master is the response ring's reader and the request ring's
    /// writer, and at the done marker it is neither reading nor about to write.
    pub(crate) fn reclaim_if_due(&self) {
        if let Some(fd) = &self.fd {
            self.channel().reclaim_response(fd);
            self.channel().reclaim_request(fd);
        }
    }

    pub(crate) fn reclaim_is_due(&self) -> bool {
        self.fd.is_some()
            && (self.channel().response.is_reclaim_due() || self.channel().request.is_reclaim_due())
    }
}

impl Drop for MappedChannel {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, size_of::<Channel>());
        }
    }
}

fn mmap_channel(fd: RawFd) -> std::io::Result<std::ptr::NonNull<Channel>> {
    let size = size_of::<Channel>();
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { std::ptr::NonNull::new_unchecked(ptr as *mut Channel) })
}

/// Prototype side. The returned mapping is what an about-to-fork worker
/// inherits for free, since `MAP_SHARED` survives `fork()`.
pub fn create_channel() -> std::io::Result<(OwnedFd, MappedChannel)> {
    use std::os::fd::FromRawFd;

    let name = c"proteus-worker-channel";
    let raw_fd =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

    if unsafe { libc::ftruncate(raw_fd, size_of::<Channel>() as libc::off_t) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    seal_size(&fd)?;
    let ptr = mmap_channel(raw_fd)?;
    Channel::init_in_place(ptr.as_ptr());
    Ok((fd, MappedChannel { ptr, fd: None }))
}

/// A worker holds this same descriptor, and only the seal stops it shrinking
/// the file: master's mapping past the new end would raise SIGBUS on the next
/// access, killing the process the worker is isolated from.
///
/// Not `F_SEAL_WRITE`, which both sides need; `F_SEAL_SEAL` closes the
/// follow-up of adding one. Hole punching is not a resize, so reclaim still
/// works.
fn seal_size(fd: &OwnedFd) -> std::io::Result<()> {
    use nix::fcntl::{FcntlArg, SealFlag, fcntl};

    fcntl(
        fd,
        FcntlArg::F_ADD_SEALS(
            SealFlag::F_SEAL_SHRINK | SealFlag::F_SEAL_GROW | SealFlag::F_SEAL_SEAL,
        ),
    )
    .map_err(std::io::Error::from)?;
    Ok(())
}

/// Master side. The channel is already initialized by `create_channel`;
/// re-initializing it would stomp frames the worker is mid-write on.
pub fn map_existing_channel(fd: OwnedFd) -> std::io::Result<MappedChannel> {
    use std::os::fd::AsRawFd;
    let ptr = mmap_channel(fd.as_raw_fd())?;
    Ok(MappedChannel { ptr, fd: Some(fd) })
}

#[cfg(test)]
#[path = "shm_tests.rs"]
mod tests;
