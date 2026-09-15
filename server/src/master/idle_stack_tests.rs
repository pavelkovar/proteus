use super::*;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// The smallest payload that can be parked: something that remembers its own
/// slot, exactly as a real worker does through its `WorkerMeta`.
#[derive(Debug, PartialEq, Eq)]
struct Item {
    slot: u32,
    value: usize,
}

impl Slotted for Item {
    fn slot(&self) -> u32 {
        self.slot
    }
}

fn item(value: usize) -> impl FnOnce(u32) -> Item {
    move |slot| Item { slot, value }
}

#[test]
fn pops_in_lifo_order() {
    // LIFO keeps one worker hot and lets the rest reach their idle timeout.
    let stack: IdleStack<Item> = IdleStack::new(4);
    for i in 0..4 {
        assert!(stack.push_new(item(i)));
    }
    assert_eq!(stack.len(), 4);
    assert_eq!(
        (0..4)
            .map(|_| stack.pop().unwrap().value)
            .collect::<Vec<_>>(),
        vec![3, 2, 1, 0]
    );
    assert!(stack.pop().is_none());
    assert_eq!(stack.len(), 0);
}

#[test]
fn push_beyond_capacity_is_refused() {
    let stack: IdleStack<Item> = IdleStack::new(2);
    assert!(stack.push_new(item(1)));
    assert!(stack.push_new(item(2)));
    // Unreachable behind the semaphore, but refusing beats parking a third
    // entry in a slot another worker already owns.
    assert!(!stack.push_new(item(3)));
    assert_eq!(stack.len(), 2);
}

#[test]
fn slots_are_reused_after_popping() {
    // Capacity bounds live entries, not lifetime pushes: a free-list leak
    // fails on the second round.
    let stack: IdleStack<Item> = IdleStack::new(2);
    // Round-trips slots through claim and release as the pool does.
    for round in 0..1000 {
        let a = stack.claim_slot().expect("slot available");
        let b = stack.claim_slot().expect("slot available");
        stack.push(Item {
            slot: a,
            value: round,
        });
        stack.push(Item {
            slot: b,
            value: round + 10_000,
        });
        assert_eq!(stack.pop().map(|i| i.value), Some(round + 10_000));
        assert_eq!(stack.pop().map(|i| i.value), Some(round));
        stack.release_slot(a);
        stack.release_slot(b);
    }
    assert_eq!(stack.len(), 0);
}

#[test]
fn an_empty_stack_pops_none() {
    let stack: IdleStack<Item> = IdleStack::new(0);
    assert!(stack.pop().is_none());
    assert!(
        !stack.push_new(item(1)),
        "a zero-capacity stack can hold nothing"
    );
}

/// Every value must come out exactly once: a duplicate hands two requests
/// one worker, a loss leaks one. This is what ABA breaks, so the threads
/// deliberately churn one small set of slots.
#[test]
fn concurrent_push_pop_neither_duplicates_nor_loses_a_value() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 20_000;
    const CAPACITY: usize = 16;

    let stack: Arc<IdleStack<Item>> = Arc::new(IdleStack::new(CAPACITY));
    let seen: Arc<Vec<AtomicUsize>> = Arc::new((0..THREADS).map(|_| AtomicUsize::new(0)).collect());

    // Parks a value in a freshly claimed slot, then takes whatever is on top
    // and frees that one's slot - so the same few indices keep coming back.
    fn churn(stack: &IdleStack<Item>, value: usize) -> Option<usize> {
        if !stack.push_new(item(value)) {
            return Some(value);
        }
        let taken = stack.pop()?;
        stack.release_slot(taken.slot);
        Some(taken.value)
    }

    let handles: Vec<_> = (0..THREADS)
        .map(|id| {
            let (stack, seen) = (Arc::clone(&stack), Arc::clone(&seen));
            std::thread::spawn(move || {
                let mut held: Option<usize> = Some(id);
                for _ in 0..PER_THREAD {
                    held = match held.take() {
                        Some(v) => {
                            let next = churn(&stack, v);
                            if let Some(v) = next {
                                seen[v].fetch_add(1, Ordering::Relaxed);
                            }
                            next
                        }
                        None => stack.pop().map(|taken| {
                            stack.release_slot(taken.slot);
                            taken.value
                        }),
                    };
                }
                if let Some(v) = held {
                    stack.push_new(item(v));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    // The multiset of live values must be exactly what went in.
    let mut remaining: Vec<usize> = std::iter::from_fn(|| stack.pop())
        .map(|item| item.value)
        .collect();
    remaining.sort_unstable();
    assert_eq!(
        remaining,
        (0..THREADS).collect::<Vec<_>>(),
        "values were duplicated or lost"
    );
    assert_eq!(stack.len(), 0, "len drifted from the real contents");
}

/// `len` only reports, but must not drift permanently, or `/status` grows
/// progressively wrong.
#[test]
fn len_tracks_contents_under_concurrency() {
    const THREADS: usize = 8;
    let stack: Arc<IdleStack<Item>> = Arc::new(IdleStack::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|id| {
            let stack = Arc::clone(&stack);
            std::thread::spawn(move || {
                for _ in 0..10_000 {
                    if stack.push_new(item(id))
                        && let Some(taken) = stack.pop()
                    {
                        stack.release_slot(taken.slot);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let drained = std::iter::from_fn(|| stack.pop()).count();
    assert_eq!(
        drained, 0,
        "threads should have drained everything they pushed"
    );
    assert_eq!(stack.len(), 0);
}

/// Dropping the stack must drop what is still in it: for the real payload
/// that is what signals each parked worker to exit.
#[test]
fn dropping_the_stack_drops_the_values_it_still_holds() {
    struct CountsDrops {
        slot: u32,
        drops: Arc<AtomicUsize>,
    }
    impl Slotted for CountsDrops {
        fn slot(&self) -> u32 {
            self.slot
        }
    }
    impl Drop for CountsDrops {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let stack: IdleStack<CountsDrops> = IdleStack::new(4);
        for _ in 0..3 {
            assert!(stack.push_new(|slot| CountsDrops {
                slot,
                drops: Arc::clone(&drops),
            }));
        }
        assert_eq!(drops.load(Ordering::Relaxed), 0);
    }
    assert_eq!(
        drops.load(Ordering::Relaxed),
        3,
        "values left in the stack were leaked, not dropped"
    );
}

/// The whole point of the guard: releasing a slot whose worker is still
/// linked here hands that slot to a second worker, and the idle stack then
/// holds one entry under two owners.
#[test]
#[should_panic(expected = "still parked")]
#[cfg(debug_assertions)]
fn releasing_a_slot_that_is_still_parked_is_caught() {
    let stack: IdleStack<Item> = IdleStack::new(4);
    let slot = stack.claim_slot().expect("a free slot");
    stack.push(Item { slot, value: 1 });
    stack.release_slot(slot);
}

/// Parking two payloads under one index links that slot into the list twice,
/// and the list then loops back on itself.
#[test]
#[should_panic(expected = "parked twice")]
#[cfg(debug_assertions)]
fn parking_the_same_slot_twice_is_caught() {
    let stack: IdleStack<Item> = IdleStack::new(4);
    let slot = stack.claim_slot().expect("a free slot");
    stack.push(Item { slot, value: 1 });
    stack.push(Item { slot, value: 2 });
}
