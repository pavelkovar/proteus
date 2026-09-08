//! Caches whether a path is a file, a directory, or missing - never its
//! content.
//!
//! TTL-bounded rather than inotify-based, which is unreliable across the
//! bind-mounted and overlay filesystems this commonly runs on. A change can
//! therefore take up to `ttl` to be seen.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FsKind {
    File,
    Dir,
    Missing,
}

struct Entry {
    kind: FsKind,
    cached_at: Instant,
}

pub(crate) struct FsCache {
    entries: RwLock<HashMap<PathBuf, Entry>>,
    max_entries: usize,
    ttl: Duration,
}

impl FsCache {
    pub(crate) fn new(max_entries: usize, ttl: Duration) -> Self {
        FsCache { entries: RwLock::new(HashMap::new()), max_entries, ttl }
    }

    /// `None` means uncached or expired: go and check for real. A zero `ttl`
    /// disables the cache entirely.
    pub(crate) fn get(&self, path: &Path) -> Option<FsKind> {
        if self.ttl.is_zero() {
            return None;
        }
        let guard = self.entries.read().unwrap();
        let entry = guard.get(path)?;
        (entry.cached_at.elapsed() < self.ttl).then_some(entry.kind)
    }

    /// Updating an existing key never counts against `max_entries`. Once
    /// genuinely full, sweeps expired entries once; if that does not free
    /// room, this path simply goes uncached rather than evicting a live entry
    /// or growing without bound.
    pub(crate) fn put(&self, path: PathBuf, kind: FsKind) {
        if self.ttl.is_zero() {
            return;
        }
        let mut guard = self.entries.write().unwrap();
        if guard.len() >= self.max_entries && !guard.contains_key(&path) {
            let ttl = self.ttl;
            guard.retain(|_, e| e.cached_at.elapsed() < ttl);
            if guard.len() >= self.max_entries {
                return;
            }
        }
        guard.insert(path, Entry { kind, cached_at: Instant::now() });
    }
}

#[cfg(test)]
#[path = "fs_cache_tests.rs"]
mod tests;
