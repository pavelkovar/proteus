use super::*;
use std::time::Duration;

#[test]
fn miss_on_empty_cache() {
    let cache = FsCache::new(10, Duration::from_millis(100));
    assert_eq!(cache.get(Path::new("/tmp/nope")), None);
}

#[test]
fn put_then_get_within_ttl_returns_the_cached_kind() {
    let cache = FsCache::new(10, Duration::from_millis(100));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    cache.put(PathBuf::from("/tmp/b"), FsKind::Dir);
    cache.put(PathBuf::from("/tmp/c"), FsKind::Missing);
    assert_eq!(cache.get(Path::new("/tmp/a")), Some(FsKind::File));
    assert_eq!(cache.get(Path::new("/tmp/b")), Some(FsKind::Dir));
    assert_eq!(cache.get(Path::new("/tmp/c")), Some(FsKind::Missing));
}

#[test]
fn entry_expires_after_ttl() {
    let cache = FsCache::new(10, Duration::from_millis(10));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    assert_eq!(cache.get(Path::new("/tmp/a")), Some(FsKind::File));
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(cache.get(Path::new("/tmp/a")), None);
}

/// `put` restamps. A caller that writes an unchanged verdict back on every
/// request therefore keeps the entry alive indefinitely, which is why both
/// `stat_kind` and the static branch write only when the verdict changed.
#[test]
fn rewriting_an_entry_restamps_it() {
    const TTL: Duration = Duration::from_millis(100);
    const STEP: Duration = Duration::from_millis(60);
    let cache = FsCache::new(10, TTL);
    let path = || PathBuf::from("/tmp/restamp");

    cache.put(path(), FsKind::File);
    std::thread::sleep(STEP);
    cache.put(path(), FsKind::File);

    std::thread::sleep(STEP);
    assert_eq!(
        cache.get(&path()),
        Some(FsKind::File),
        "the rewrite must have restamped, or this would already have expired"
    );

    std::thread::sleep(STEP);
    assert_eq!(
        cache.get(&path()),
        None,
        "and with no further rewrite it must age out"
    );
}

#[test]
fn zero_ttl_disables_the_cache_entirely() {
    let cache = FsCache::new(10, Duration::from_millis(0));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    assert_eq!(cache.get(Path::new("/tmp/a")), None);
}

#[test]
fn updating_an_existing_key_never_counts_against_the_cap() {
    let cache = FsCache::new(1, Duration::from_millis(100));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    cache.put(PathBuf::from("/tmp/a"), FsKind::Missing);
    assert_eq!(cache.get(Path::new("/tmp/a")), Some(FsKind::Missing));
}

/// A working set larger than the cache must still get hits: a full cache
/// that stopped admitting would serve none at all.
#[test]
fn a_full_cache_evicts_to_admit_a_new_key() {
    let cache = FsCache::new(1, Duration::from_millis(100));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    cache.put(PathBuf::from("/tmp/b"), FsKind::Dir);
    assert_eq!(
        cache.get(Path::new("/tmp/b")),
        Some(FsKind::Dir),
        "the newest key must be admitted even though the cache was full"
    );
    assert_eq!(cache.get(Path::new("/tmp/a")), None);
}

/// Which key an eviction picks is the policy's business; that the cap holds
/// is not.
#[test]
fn the_cap_bounds_how_many_entries_are_kept() {
    const CAP: usize = 8;
    let cache = FsCache::new(CAP, Duration::from_secs(60));
    for i in 0..1000 {
        cache.put(PathBuf::from(format!("/tmp/{i}")), FsKind::File);
    }
    let resident = (0..1000)
        .filter(|i| cache.get(Path::new(&format!("/tmp/{i}"))).is_some())
        .count();
    assert!(
        resident <= CAP,
        "{resident} entries resident with a cap of {CAP}"
    );
}
