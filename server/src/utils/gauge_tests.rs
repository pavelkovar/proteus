use super::*;
use std::sync::Arc;

/// A zero max takes the unbounded branch and must never reject, however many
/// units are held at once.
#[test]
fn a_zero_max_never_rejects() {
    let gauge = Gauge::default();
    let guards: Vec<_> = (0..10_000)
        .map(|_| gauge.enter_under(0).expect("max=0 must never reject"))
        .collect();
    assert_eq!(gauge.get(), 10_000);
    drop(guards);
    assert_eq!(gauge.get(), 0);
}

/// A real cap must reject once full and admit again once a unit frees.
#[test]
fn a_real_cap_rejects_once_reached_and_admits_again() {
    let gauge = Gauge::default();
    let first = gauge.enter_under(2).expect("unit 1 of 2");
    let second = gauge.enter_under(2).expect("unit 2 of 2");
    assert!(
        gauge.enter_under(2).is_none(),
        "a 3rd unit must be rejected at max=2"
    );

    drop(first);
    let _third = gauge
        .enter_under(2)
        .expect("a freed unit must be admitted again");
    drop(second);
}

/// A burst that all reads the same count must not all get past `max`: the
/// check and the increment are one step for exactly this case.
///
/// More threads than `MAX`, and each holds its unit while sampling, so a
/// separate load and add is observable rather than merely possible - every
/// thread holding at most one unit under a cap it cannot reach proves
/// nothing.
#[test]
fn a_concurrent_burst_cannot_exceed_the_cap() {
    const MAX: usize = 2;
    const THREADS: usize = 8;
    let gauge = Arc::new(Gauge::default());
    let peak = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let gauge = Arc::clone(&gauge);
            let peak = Arc::clone(&peak);
            std::thread::spawn(move || {
                for _ in 0..50_000 {
                    if let Some(_guard) = GaugeGuard::under(Arc::clone(&gauge), MAX) {
                        peak.fetch_max(gauge.get(), Relaxed);
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    assert!(
        peak.load(Relaxed) <= MAX as u64,
        "the cap was exceeded: peaked at {}",
        peak.load(Relaxed)
    );
    assert_eq!(gauge.get(), 0, "every guard must have released");
}

/// The unit must be released when the holding future is cancelled mid-wait,
/// not only on a normal return. Shaped like the real caller, with the guard
/// alive across an `.await` that never completes.
#[tokio::test]
async fn a_unit_is_released_when_the_holding_future_is_cancelled() {
    let gauge = Arc::new(Gauge::default());
    // Never has a free permit to hand out.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(0));

    let gauge_task = Arc::clone(&gauge);
    let semaphore_task = Arc::clone(&semaphore);
    let task = tokio::spawn(async move {
        let guard = GaugeGuard::new(gauge_task);
        let _permit = semaphore_task.acquire_owned().await;
        drop(guard); // unreachable: the semaphore never yields a permit
    });

    // Let it park on the await before cancelling.
    tokio::task::yield_now().await;
    assert_eq!(
        gauge.get(),
        1,
        "the guard should have raised the count before parking"
    );

    task.abort();
    let _ = task.await;

    assert_eq!(
        gauge.get(),
        0,
        "a guard must release even when cancelled mid-await, or the count leaks forever"
    );
}

/// An owning handle is what lets a guard outlive the call that took it, which
/// is how a response body keeps its request counted until the last frame.
#[test]
fn an_owned_guard_outlives_the_scope_that_took_it() {
    let gauge = Arc::new(Gauge::default());

    let guard = {
        let inner = Arc::clone(&gauge);
        GaugeGuard::new(inner)
    };
    assert_eq!(gauge.get(), 1, "the guard still holds its unit");

    drop(guard);
    assert_eq!(gauge.get(), 0);
}
