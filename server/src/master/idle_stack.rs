//! Lock-free LIFO stack of idle workers, indexed rather than pointer-based:
//! the slot array outlives every operation, so `next` links need no deferred
//! reclamation. `head` packs a generation counter above the index to close ABA.
//!
//! A worker claims a slot at spawn and keeps it until death, so `push`/`pop`
//! run one CAS loop each. An earlier version that also moved slots between the
//! free list and the idle list ran two, and lost to the adaptively-spinning
//! `parking_lot` mutex it replaced.
//!
//! A slot is on `free`, on `idle`, or held by a busy worker - never two at
//! once, so both stacks share the `next` array.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Sentinel for "no slot"; 0 is a usable index.
const NONE: u32 = u32::MAX;

struct Slot<T> {
    /// Owned by the pusher until it publishes the index, then by whichever
    /// popper wins the CAS - never by two threads at once.
    value: UnsafeCell<Option<T>>,
    next: AtomicU32,
}

/// Generic over the payload so the concurrency can be tested without a real
/// worker channel.
pub(crate) struct IdleStack<T> {
    slots: Box<[Slot<T>]>,
    idle: AtomicU64,
    free: AtomicU64,
    /// Reporting only; never consulted for correctness, so a stale read is fine.
    len: AtomicU64,
}

// Safety: see `Slot::value` - one owner at a time, and `T: Send` covers
// moving it between them.
unsafe impl<T: Send> Sync for IdleStack<T> {}
unsafe impl<T: Send> Send for IdleStack<T> {}

fn pack(generation: u32, index: u32) -> u64 {
    ((generation as u64) << 32) | index as u64
}

fn unpack(head: u64) -> (u32, u32) {
    ((head >> 32) as u32, head as u32)
}

impl<T> IdleStack<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity < NONE as usize, "pool capacity must fit in a u32 slot index");
        let slots: Box<[Slot<T>]> = (0..capacity)
            .map(|i| Slot {
                value: UnsafeCell::new(None),
                next: AtomicU32::new(if i + 1 == capacity { NONE } else { i as u32 + 1 }),
            })
            .collect();
        IdleStack {
            slots,
            idle: AtomicU64::new(pack(0, NONE)),
            free: AtomicU64::new(pack(0, if capacity == 0 { NONE } else { 0 })),
            len: AtomicU64::new(0),
        }
    }

    /// Reserves a slot for a newly spawned worker, which keeps it until it
    /// dies. `None` means the pool is full.
    pub(crate) fn claim_slot(&self) -> Option<u32> {
        self.pop_index(&self.free)
    }

    /// Exactly once per `claim_slot`, and only once the worker is out of the
    /// idle stack - otherwise the slot is handed out while still linked here.
    pub(crate) fn release_slot(&self, index: u32) {
        self.push_index(&self.free, index);
    }

    /// Parks `worker` in its own slot, making it available to `pop`.
    pub(crate) fn push(&self, index: u32, worker: T) {
        // Sole owner: a busy worker's slot is on neither stack, and this is
        // not published until the CAS below.
        unsafe { *self.slots[index as usize].value.get() = Some(worker) };
        self.push_index(&self.idle, index);
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Most recently parked worker first: its heap and OPcache are warmest,
    /// and leaving the rest untouched lets them reach their own idle timeout.
    pub(crate) fn pop(&self) -> Option<(u32, T)> {
        let index = self.pop_index(&self.idle)?;
        // Sole owner, now that the CAS removed this index from the stack.
        let worker = unsafe { (*self.slots[index as usize].value.get()).take() };
        self.len.fetch_sub(1, Ordering::Relaxed);
        worker.map(|w| (index, w))
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed) as usize
    }

    fn push_index(&self, head: &AtomicU64, index: u32) {
        let mut current = head.load(Ordering::Relaxed);
        loop {
            let (generation, top) = unpack(current);
            self.slots[index as usize].next.store(top, Ordering::Relaxed);
            // Release: publishes both the `next` store above and the payload
            // `push` wrote before calling.
            match head.compare_exchange_weak(
                current,
                pack(generation.wrapping_add(1), index),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn pop_index(&self, head: &AtomicU64) -> Option<u32> {
        // Acquire: pairs with `push_index`'s Release.
        let mut current = head.load(Ordering::Acquire);
        loop {
            let (generation, top) = unpack(current);
            if top == NONE {
                return None;
            }
            let next = self.slots[top as usize].next.load(Ordering::Relaxed);
            match head.compare_exchange_weak(
                current,
                pack(generation.wrapping_add(1), next),
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(top),
                Err(actual) => current = actual,
            }
        }
    }
}

impl<T> Drop for IdleStack<T> {
    /// Dropping a parked worker is what signals it to exit.
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T> IdleStack<T> {
    /// Claim and park in one step, for callers with no long-lived worker to
    /// attach the slot to.
    #[cfg(test)]
    pub(crate) fn push_new(&self, worker: T) -> Option<T> {
        match self.claim_slot() {
            Some(index) => {
                self.push(index, worker);
                None
            }
            None => Some(worker),
        }
    }
}

#[cfg(test)]
#[path = "idle_stack_tests.rs"]
mod tests;
