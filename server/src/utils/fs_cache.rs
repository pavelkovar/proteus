//! Caches whether a path is a file, a directory, or missing - never its
//! content.
//!
//! TTL-bounded rather than inotify-based, which is unreliable across the
//! bind-mounted and overlay filesystems this commonly runs on. A change can
//! therefore take up to `ttl` to be seen.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FsKind {
    File,
    Dir,
    Missing,
}

#[derive(Clone, Copy)]
struct Entry {
    kind: FsKind,
    /// See `routing::on_local_fs`.
    local: bool,
    cached_at: Instant,
}

pub(crate) struct FsCache {
    entries: quick_cache::sync::Cache<PathBuf, Entry>,
    ttl: Duration,
}

impl FsCache {
    pub(crate) fn new(max_entries: usize, ttl: Duration) -> Self {
        FsCache {
            entries: quick_cache::sync::Cache::new(max_entries),
            ttl,
        }
    }

    /// `None` means uncached or expired: go and check for real. A zero `ttl`
    /// disables the cache entirely.
    pub(crate) fn get(&self, path: &Path) -> Option<FsKind> {
        self.get_with_locality(path).map(|(kind, _)| kind)
    }

    pub(crate) fn get_with_locality(&self, path: &Path) -> Option<(FsKind, bool)> {
        if self.ttl.is_zero() {
            return None;
        }
        let entry = self.entries.get(path)?;
        (entry.cached_at.elapsed() < self.ttl).then_some((entry.kind, entry.local))
    }

    /// An expired entry is left for the eviction policy rather than reclaimed
    /// here, which would put a scan on the request path; `get` already
    /// refuses to serve one.
    pub(crate) fn put(&self, path: PathBuf, kind: FsKind) {
        self.insert(path, kind, false);
    }

    pub(crate) fn put_file(&self, path: PathBuf, local: bool) {
        self.insert(path, FsKind::File, local);
    }

    fn insert(&self, path: PathBuf, kind: FsKind, local: bool) {
        if self.ttl.is_zero() {
            return;
        }
        self.entries.insert(
            path,
            Entry {
                kind,
                local,
                cached_at: Instant::now(),
            },
        );
    }
}

#[cfg(test)]
#[path = "fs_cache_tests.rs"]
mod tests;
