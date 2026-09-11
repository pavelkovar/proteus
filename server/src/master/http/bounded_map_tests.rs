use super::*;

#[test]
fn room_available_under_the_cap() {
    let bm: BoundedMap<u32, u32> = BoundedMap::new();
    bm.insert(1, 0);
    assert!(bm.has_room_for(&2, 2, |_| false));
}

#[test]
fn an_already_tracked_key_always_has_room_even_when_full() {
    let bm: BoundedMap<u32, u32> = BoundedMap::new();
    bm.insert(1, 0);
    assert!(
        bm.has_room_for(&1, 1, |_| false),
        "updating an existing key must never count against the cap"
    );
}

#[test]
fn full_map_sweeps_stale_entries_to_make_room() {
    let bm: BoundedMap<u32, bool> = BoundedMap::new();
    bm.insert(1, true); // stale
    assert!(bm.has_room_for(&2, 1, |stale| *stale));
    assert!(!bm.contains_key(&1), "the stale entry must have been swept");
}

#[test]
fn full_map_of_live_entries_reports_no_room_rather_than_evicting_one() {
    let bm: BoundedMap<u32, bool> = BoundedMap::new();
    bm.insert(1, false); // not stale
    assert!(!bm.has_room_for(&2, 1, |stale| *stale));
    assert!(
        bm.contains_key(&1),
        "a live entry must never be evicted to make room"
    );
}

#[test]
fn a_sweep_already_in_progress_is_not_duplicated() {
    let bm = BoundedMap {
        map: DashMap::new(),
        sweeping: AtomicBool::new(true),
    };
    bm.insert(1, true); // stale, would normally be swept
    assert!(
        !bm.has_room_for(&2, 1, |stale| *stale),
        "must not sweep while another thread already is"
    );
    assert!(
        bm.contains_key(&1),
        "the in-progress flag must have skipped this thread's own scan"
    );
}
