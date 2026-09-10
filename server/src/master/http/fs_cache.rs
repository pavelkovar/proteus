//! Caches whether a path is a file, a directory, or missing - never its
//! content.
//!
//! TTL-bounded rather than inotify-based, which is unreliable across the
//! bind-mounted and overlay filesystems this commonly runs on. A change can
//! therefore take up to `ttl` to be seen.
//!
//! Every static-file request touches this cache unconditionally, so its
//! own concurrent access needs to stay cheap.

use super::bounded_map::BoundedMap;
use std::path::{Path, PathBuf};
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
    entries: BoundedMap<PathBuf, Entry>,
    max_entries: usize,
    ttl: Duration,
}

impl FsCache {
    pub(crate) fn new(max_entries: usize, ttl: Duration) -> Self {
        FsCache { entries: BoundedMap::new(), max_entries, ttl }
    }

    /// `None` means uncached or expired: go and check for real. A zero `ttl`
    /// disables the cache entirely.
    pub(crate) fn get(&self, path: &Path) -> Option<FsKind> {
        if self.ttl.is_zero() {
            return None;
        }
        let entry = self.entries.get(path)?;
        (entry.cached_at.elapsed() < self.ttl).then_some(entry.kind)
    }

    /// Updating an existing key never counts against `max_entries`; a
    /// genuinely full cache goes uncached rather than evicting a live entry.
    pub(crate) fn put(&self, path: PathBuf, kind: FsKind) {
        if self.ttl.is_zero() {
            return;
        }
        let ttl = self.ttl;
        if !self.entries.has_room_for(&path, self.max_entries, |e: &Entry| e.cached_at.elapsed() >= ttl) {
            return;
        }
        self.entries.insert(path, Entry { kind, cached_at: Instant::now() });
    }
}

#[cfg(test)]
#[path = "fs_cache_tests.rs"]
mod tests;
