//! A `DashMap` capped at a fixed size: insert unless already full, and even
//! then only skip - never evict a live entry.

use dashmap::DashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

pub(super) struct BoundedMap<K, V> {
    map: DashMap<K, V>,
    sweeping: AtomicBool,
}

impl<K, V> BoundedMap<K, V>
where
    K: Eq + Hash,
{
    pub(super) fn new() -> Self {
        BoundedMap {
            map: DashMap::new(),
            sweeping: AtomicBool::new(false),
        }
    }

    /// `true` if `key` is tracked or sweeping frees room for it; `false` if
    /// full, or if another thread is already sweeping (avoids a duplicate
    /// O(n) scan). Check-then-insert still races - an approximate cap.
    pub(super) fn has_room_for(
        &self,
        key: &K,
        max_entries: usize,
        is_stale: impl Fn(&V) -> bool,
    ) -> bool {
        if self.map.len() < max_entries || self.map.contains_key(key) {
            return true;
        }
        if self.sweeping.swap(true, Relaxed) {
            return false;
        }
        self.map.retain(|_, v| !is_stale(v));
        self.sweeping.store(false, Relaxed);
        self.map.len() < max_entries
    }
}

impl<K, V> Deref for BoundedMap<K, V> {
    type Target = DashMap<K, V>;

    fn deref(&self) -> &DashMap<K, V> {
        &self.map
    }
}

#[cfg(test)]
#[path = "bounded_map_tests.rs"]
mod tests;
