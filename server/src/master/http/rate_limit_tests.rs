use super::super::proxy::{Peer, resolve_client_ip};
use super::*;
use hyper::HeaderMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

fn ip(last: u8) -> ClientIdentity {
    ClientIdentity::for_test(IpAddr::V4(Ipv4Addr::new(127, 0, 0, last)))
}

/// Resolves the way `handle()` does. A hand-built identity would let a
/// spoofing test pass while the live call site stayed bypassable.
fn identity_for(peer: &str, xff: &str, trusted: &[&str]) -> ClientIdentity {
    let trusted: Vec<ipnetwork::IpNetwork> = trusted.iter().map(|c| c.parse().unwrap()).collect();
    let peer = Peer::resolve(peer.parse().unwrap(), &trusted);
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", xff.parse().unwrap());
    resolve_client_ip(peer, &headers, &trusted)
}

/// The reason identity resolution sits outside this module: a client
/// connected directly rewrites `X-Forwarded-For` freely and must still land
/// in the single bucket its peer address earns it.
#[test]
fn a_direct_client_rotating_x_forwarded_for_cannot_escape_its_bucket() {
    const BURST: u32 = 5;
    let limiter = RateLimiter::new(BURST, 3600, Vec::new());

    let allowed = (0..50)
        .filter(|n| {
            let spoofed = format!("1.1.1.{n}");
            limiter.check(identity_for("203.0.113.10", &spoofed, &["10.0.0.0/8"]))
        })
        .count();

    assert_eq!(
        allowed, BURST as usize,
        "a direct client must exhaust one shared bucket no matter what it forwards"
    );
}

/// The other half of the contract: a real proxy deployment must still limit
/// its clients individually rather than collapsing them onto the proxy.
#[test]
fn two_clients_behind_one_trusted_proxy_keep_separate_buckets() {
    let limiter = RateLimiter::new(1, 3600, Vec::new());
    let first = identity_for("10.0.0.20", "198.51.100.20", &["10.0.0.0/8"]);
    let second = identity_for("10.0.0.20", "198.51.100.21", &["10.0.0.0/8"]);

    assert!(limiter.check(first));
    assert!(!limiter.check(first), "first client's burst is exhausted");
    assert!(
        limiter.check(second),
        "a second client behind the same proxy must have its own budget"
    );
}

#[test]
fn burst_is_allowed_then_the_next_request_is_rejected() {
    let limiter = RateLimiter::new(3, 3600, Vec::new());
    let client = ip(1);
    assert!(limiter.check(client));
    assert!(limiter.check(client));
    assert!(limiter.check(client));
    assert!(
        !limiter.check(client),
        "a fourth request within the burst must be rejected"
    );
}

#[test]
fn tokens_refill_over_time() {
    let limiter = RateLimiter::new(1, 1, Vec::new());
    let client = ip(2);
    assert!(limiter.check(client));
    assert!(
        !limiter.check(client),
        "burst of 1 must be exhausted after one request"
    );

    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        limiter.check(client),
        "a full period later, the token must have refilled"
    );
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
    assert!(
        recovered,
        "a client polling faster than the refill interval must still eventually recover"
    );
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
    let patterns: Vec<MatchPattern> = vec![
        "*GPTBot*".to_string().try_into().unwrap(),
        "*ClaudeBot*".to_string().try_into().unwrap(),
    ];
    let limiter = RateLimiter::new(1, 60, patterns);
    assert!(limiter.should_limit("Mozilla/5.0 (compatible; GPTBot/1.0)"));
    assert!(limiter.should_limit("ClaudeBot/1.0"));
    assert!(!limiter.should_limit("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"));
}

/// A client arriving at a table that is already full must still be tracked.
/// Letting it through untracked would disable limiting for every newcomer
/// for as long as the table stayed full.
#[test]
fn a_client_arriving_at_a_full_table_is_still_tracked() {
    let limiter = RateLimiter::with_shape(1, 3600, Vec::new(), 2);
    assert!(limiter.check(ip(10)));
    assert!(limiter.check(ip(11)));

    let third = ip(12);
    assert!(limiter.check(third));
    assert!(
        !limiter.check(third),
        "the newcomer must own a real bucket, denying its own very next request"
    );
}

/// The property that makes eviction safe here: an offender that keeps coming
/// back must outlive the one-shot addresses of a spray, or rotating source
/// IPs would reset its bucket for it.
#[test]
fn a_spray_of_one_shot_ips_does_not_evict_a_repeat_offender() {
    const CAP: usize = 64;
    let limiter = RateLimiter::with_shape(1, 3600, Vec::new(), CAP);

    let offender = ClientIdentity::for_test(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 4)));
    assert!(limiter.check(offender), "offender spends its one token");

    for n in 0..(CAP as u32 * 100) {
        let spray = ClientIdentity::for_test(IpAddr::V4(Ipv4Addr::from(0x0A00_0000 + n)));
        limiter.check(spray);
        assert!(
            !limiter.check(offender),
            "the offender was evicted and handed a fresh burst after {n} spray addresses"
        );
    }
}

/// Many threads hammering the same IP concurrently must never let more
/// requests through than the burst capacity - the `compare_exchange` retry
/// loop must not lose an update under real contention.
#[test]
fn concurrent_hammering_never_exceeds_the_burst_capacity() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    assert_eq!(
        allowed.load(Ordering::Relaxed),
        CAPACITY as usize,
        "exactly the burst capacity must have been let through, never more"
    );
}
