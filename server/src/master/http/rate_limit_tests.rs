use super::*;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

fn ip(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, last))
}

#[test]
fn burst_is_allowed_then_the_next_request_is_rejected() {
    let limiter = RateLimiter::new(3, 3600, Vec::new());
    let client = ip(1);
    assert!(limiter.check(client));
    assert!(limiter.check(client));
    assert!(limiter.check(client));
    assert!(!limiter.check(client), "a fourth request within the burst must be rejected");
}

#[test]
fn tokens_refill_over_time() {
    let limiter = RateLimiter::new(1, 1, Vec::new());
    let client = ip(2);
    assert!(limiter.check(client));
    assert!(!limiter.check(client), "burst of 1 must be exhausted after one request");

    std::thread::sleep(Duration::from_millis(1500));
    assert!(limiter.check(client), "a full period later, the token must have refilled");
}

/// A client checking faster than one refill interval must still eventually
/// recover, not stay stuck at zero forever.
#[test]
fn frequent_polling_still_recovers_instead_of_stalling_forever() {
    let limiter = RateLimiter::new(2, 2, Vec::new()); // 1 token per ~1s
    let client = ip(30);
    assert!(limiter.check(client));
    assert!(limiter.check(client));
    assert!(!limiter.check(client), "burst of 2 must be exhausted");

    // Poll much faster than the refill interval, for longer than the whole
    // period: a permanently-stalled bucket stays at 0 the entire time, a
    // correctly-refilling one must let at least one request through.
    let deadline = std::time::Instant::now() + Duration::from_millis(2500);
    let mut recovered = false;
    while std::time::Instant::now() < deadline {
        if limiter.check(client) {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(recovered, "a client polling faster than the refill interval must still eventually recover");
}

#[test]
fn per_ip_isolation() {
    let limiter = RateLimiter::new(1, 3600, Vec::new());
    let a = ip(3);
    let b = ip(4);
    assert!(limiter.check(a));
    assert!(!limiter.check(a), "a's own burst is exhausted");
    assert!(limiter.check(b), "b must have its own, untouched budget");
}

#[test]
fn should_limit_empty_patterns_matches_every_user_agent() {
    let limiter = RateLimiter::new(1, 60, Vec::new());
    assert!(limiter.should_limit("Mozilla/5.0"));
    assert!(limiter.should_limit(""));
}

#[test]
fn should_limit_only_matches_configured_patterns() {
    let patterns: Vec<MatchPattern> = vec!["*GPTBot*".to_string().try_into().unwrap(), "*ClaudeBot*".to_string().try_into().unwrap()];
    let limiter = RateLimiter::new(1, 60, patterns);
    assert!(limiter.should_limit("Mozilla/5.0 (compatible; GPTBot/1.0)"));
    assert!(limiter.should_limit("ClaudeBot/1.0"));
    assert!(!limiter.should_limit("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"));
}

/// A tracking table full of clients that are all still actively being
/// limited (none idle/full) has nothing safe to sweep, so a new IP must
/// fail open rather than evict one of them.
#[test]
fn a_full_tracking_table_of_active_clients_fails_open_rather_than_evicting_one() {
    // Room for 2 IPs, so a third forces the sweep-or-fail-open path.
    let limiter = RateLimiter::with_shape(1, 3600, Vec::new(), 2);

    let first = ip(10);
    let second = ip(11);
    assert!(limiter.check(first));
    assert!(limiter.check(second));
    // Both buckets now sit at 0/1 tokens - neither is "full", so neither is
    // sweep-eligible.

    let third = ip(12);
    assert!(limiter.check(third), "a full tracking table must fail open rather than block a client it cannot track");

    // Neither original client was evicted to make room for the fail-open one.
    assert!(!limiter.check(first), "an actively-limited client must never be evicted to make room");
    assert!(!limiter.check(second), "an actively-limited client must never be evicted to make room");
}

/// A tracking table with an idle entry must sweep it to make room for a new
/// IP, which must end up genuinely tracked, not just let through by the
/// fail-open path - checked by denying that IP's own very next request.
#[test]
fn eviction_sweeps_an_idle_entry_to_make_room_for_a_new_ip() {
    let limiter = RateLimiter::with_shape(1, 1, Vec::new(), 1); // tracking cap = 1

    let first = ip(13);
    assert!(limiter.check(first)); // fills the table's one slot, tokens now 0/1

    // Let it refill back to full capacity - now idle and sweep-eligible.
    std::thread::sleep(Duration::from_millis(1500));

    let second = ip(14);
    assert!(limiter.check(second), "the idle entry must be swept to make room for a new IP");
    assert!(
        !limiter.check(second),
        "second must be a real, tracked bucket (denies its own very next request) - a fail-open pass-through would keep allowing it"
    );
}

/// Many threads hammering the same IP concurrently must never let more
/// requests through than the burst capacity - the `compare_exchange` retry
/// loop must not lose an update under real contention.
#[test]
fn concurrent_hammering_never_exceeds_the_burst_capacity() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const CAPACITY: u32 = 50;
    const THREADS: usize = 16;
    const ATTEMPTS_PER_THREAD: usize = 200;

    let limiter = Arc::new(RateLimiter::new(CAPACITY, 3600, Vec::new()));
    let client = ip(20);
    let allowed = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let limiter = Arc::clone(&limiter);
            let allowed = Arc::clone(&allowed);
            std::thread::spawn(move || {
                for _ in 0..ATTEMPTS_PER_THREAD {
                    if limiter.check(client) {
                        allowed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(allowed.load(Ordering::Relaxed), CAPACITY as usize, "exactly the burst capacity must have been let through, never more");
}
