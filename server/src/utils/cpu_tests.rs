use super::*;

/// A restricted cpuset is what catches a count standing in for identities:
/// `available_parallelism` answers 2 for `{1,3}`, so pinning to `0..2` misses
/// both. Affinity is per-thread, so this restricts a thread of its own.
#[test]
fn allowed_cpus_reports_real_ids_under_a_restricted_cpuset() {
    let all = allowed_cpus();
    if all.len() < 2 {
        return; // nothing to restrict
    }
    // Deliberately not the lowest two: a bug that returns `0..n` passes on
    // a contiguous set starting at zero.
    let picked = vec![all[all.len() - 2], all[all.len() - 1]];
    let expected = picked.clone();

    std::thread::spawn(move || {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &cpu in &picked {
                libc::CPU_SET(cpu, &mut set);
            }
            assert_eq!(
                libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
                0,
                "could not restrict this thread's affinity"
            );
        }
        assert_eq!(allowed_cpus(), expected);

        pin_to_cpu(expected[1]);
        assert_eq!(allowed_cpus(), vec![expected[1]]);
    })
    .join()
    .expect("affinity thread panicked");
}

#[test]
fn a_quota_below_the_mask_caps_the_threads_and_leaves_them_unpinned() {
    assert_eq!(serving_cpus(&[4, 5, 6, 7], 2), vec![None, None]);
    // A quota under one CPU still serves.
    assert_eq!(serving_cpus(&[4, 5, 6, 7], 0), vec![None]);
}

#[test]
fn without_a_quota_below_the_mask_every_allowed_cpu_gets_a_pinned_thread() {
    assert_eq!(
        serving_cpus(&[4, 5, 6, 7], 4),
        vec![Some(4), Some(5), Some(6), Some(7)]
    );
}

#[test]
fn an_unreadable_mask_serves_the_budget_unpinned() {
    assert_eq!(serving_cpus(&[], 3), vec![None, None, None]);
}
