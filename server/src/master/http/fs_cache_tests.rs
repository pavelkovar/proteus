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

#[test]
fn full_cache_skips_caching_a_new_key_instead_of_evicting_a_live_one() {
    let cache = FsCache::new(1, Duration::from_millis(100));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    cache.put(PathBuf::from("/tmp/b"), FsKind::File);
    // "/tmp/a" is still fresh, so "/tmp/b" never got a slot.
    assert_eq!(cache.get(Path::new("/tmp/a")), Some(FsKind::File));
    assert_eq!(cache.get(Path::new("/tmp/b")), None);
}

#[test]
fn full_cache_makes_room_by_sweeping_expired_entries_first() {
    let cache = FsCache::new(1, Duration::from_millis(10));
    cache.put(PathBuf::from("/tmp/a"), FsKind::File);
    std::thread::sleep(Duration::from_millis(30));
    cache.put(PathBuf::from("/tmp/b"), FsKind::Dir);
    assert_eq!(cache.get(Path::new("/tmp/a")), None);
    assert_eq!(cache.get(Path::new("/tmp/b")), Some(FsKind::Dir));
}
