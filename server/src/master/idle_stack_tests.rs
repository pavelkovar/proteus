use super::*;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[test]
fn pops_in_lifo_order() {
    // LIFO keeps one worker hot and lets the rest reach their idle timeout.
    let stack: IdleStack<u32> = IdleStack::new(4);
    for i in 0..4 {
        assert!(stack.push_new(i).is_none());
    }
    assert_eq!(stack.len(), 4);
    assert_eq!((0..4).map(|_| stack.pop().unwrap().1).collect::<Vec<_>>(), vec![3, 2, 1, 0]);
    assert!(stack.pop().is_none());
    assert_eq!(stack.len(), 0);
}

#[test]
fn push_beyond_capacity_hands_the_value_back() {
    let stack: IdleStack<u32> = IdleStack::new(2);
    assert!(stack.push_new(1).is_none());
    assert!(stack.push_new(2).is_none());
    // Unreachable behind the semaphore, but returning beats dropping
    // something the caller still owns.
    assert_eq!(stack.push_new(3), Some(3));
    assert_eq!(stack.len(), 2);
}

#[test]
fn slots_are_reused_after_popping() {
    // Capacity bounds live entries, not lifetime pushes: a free-list leak
    // fails on the second round.
    let stack: IdleStack<u32> = IdleStack::new(2);
    // Round-trips slots through claim and release as the pool does.
    for round in 0..1000 {
        let a = stack.claim_slot().expect("slot available");
        let b = stack.claim_slot().expect("slot available");
        stack.push(a, round);
        stack.push(b, round + 10_000);
        assert_eq!(stack.pop().map(|(_, v)| v), Some(round + 10_000));
        assert_eq!(stack.pop().map(|(_, v)| v), Some(round));
        stack.release_slot(a);
        stack.release_slot(b);
    }
    assert_eq!(stack.len(), 0);
}

#[test]
fn an_empty_stack_pops_none() {
    let stack: IdleStack<u32> = IdleStack::new(0);
    assert!(stack.pop().is_none());
    assert_eq!(stack.push_new(1), Some(1), "a zero-capacity stack can hold nothing");
}

/// Every value must come out exactly once: a duplicate hands two requests
/// one worker, a loss leaks one. This is what ABA breaks, so the threads
/// deliberately churn one small set of slots.
#[test]
fn concurrent_push_pop_neither_duplicates_nor_loses_a_value() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 20_000;
    const CAPACITY: usize = 16;

    let stack: Arc<IdleStack<usize>> = Arc::new(IdleStack::new(CAPACITY));

    let seen: Arc<Vec<AtomicUsize>> = Arc::new((0..THREADS).map(|_| AtomicUsize::new(0)).collect());

    let handles: Vec<_> = (0..THREADS)
        .map(|id| {
            let (stack, seen) = (Arc::clone(&stack), Arc::clone(&seen));
            std::thread::spawn(move || {
                let mut held: Option<usize> = Some(id);
                for _ in 0..PER_THREAD {
                    match held.take() {
                        // Take whatever is on top, usually someone else's.
                        Some(v) => {
                            if let Some(rejected) = stack.push_new(v) {
                                held = Some(rejected);
                                continue;
                            }
                            held = stack.pop().map(|(slot, v)| {
                                stack.release_slot(slot);
                                v
                            });
                            if let Some(v) = held {
                                seen[v].fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        None => {
                            held = stack.pop().map(|(slot, v)| {
                                stack.release_slot(slot);
                                v
                            })
                        }
                    }
                }

                if let Some(v) = held {
                    let _ = stack.push_new(v);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    // The multiset of live values must be exactly what went in.
    let mut remaining = Vec::new();
    while let Some((_slot, v)) = stack.pop() {
        remaining.push(v);
    }
    remaining.sort_unstable();
    assert_eq!(remaining, (0..THREADS).collect::<Vec<_>>(), "values were duplicated or lost");
    assert_eq!(stack.len(), 0, "len drifted from the real contents");
}

/// `len` only reports, but must not drift permanently, or `/status` grows
/// progressively wrong.
#[test]
fn len_tracks_contents_under_concurrency() {
    const THREADS: usize = 8;
    let stack: Arc<IdleStack<u32>> = Arc::new(IdleStack::new(THREADS));
    let handles: Vec<_> = (0..THREADS as u32)
        .map(|id| {
            let stack = Arc::clone(&stack);
            std::thread::spawn(move || {
                for _ in 0..10_000 {
                    if stack.push_new(id).is_none()
                        && let Some((slot, _)) = stack.pop()
                    {
                        stack.release_slot(slot);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let drained = std::iter::from_fn(|| stack.pop()).count();
    assert_eq!(drained, 0, "threads should have drained everything they pushed");
    assert_eq!(stack.len(), 0);
}

/// Dropping the stack must drop what is still in it: for the real payload
/// that is what signals each parked worker to exit.
#[test]
fn dropping_the_stack_drops_the_values_it_still_holds() {
    struct CountsDrops(Arc<AtomicUsize>);
    impl Drop for CountsDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let stack: IdleStack<CountsDrops> = IdleStack::new(4);
        for _ in 0..3 {
            assert!(stack.push_new(CountsDrops(Arc::clone(&drops))).is_none());
        }
        assert_eq!(drops.load(Ordering::Relaxed), 0);
    }
    assert_eq!(drops.load(Ordering::Relaxed), 3, "values left in the stack were leaked, not dropped");
}
