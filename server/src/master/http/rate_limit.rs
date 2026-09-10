//! Per-client-IP token-bucket rate limiting, scoped by `User-Agent` so it
//! can target crawlers without touching ordinary visitors.
//!
//! A repeat check against an already-tracked IP never takes an exclusive
//! lock: `DashMap` gives that only its own per-shard lock, and the bucket
//! itself is one `AtomicU64` updated via `compare_exchange`.

use super::bounded_map::BoundedMap;
use crate::config::MatchPattern;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// An internal bound, not a config knob - same protection `fs_cache`'s
/// `max_entries` gives its own map, but nothing an operator needs to tune.
const MAX_TRACKED_IPS: usize = 64 * 1024;

/// Bits given to the token count within the packed `AtomicU64`; the rest
/// goes to the refill timestamp.
const TOKEN_BITS: u32 = 24;
const TOKEN_MASK: u64 = (1 << TOKEN_BITS) - 1;

/// A refill timestamp and a token count packed into one word, so a
/// refill-and-consume is a single `compare_exchange` loop, not a lock.
struct Bucket(AtomicU64);

fn pack(secs: u64, tokens: u32) -> u64 {
    (secs << TOKEN_BITS) | (tokens as u64 & TOKEN_MASK)
}

fn unpack(packed: u64) -> (u64, u32) {
    (packed >> TOKEN_BITS, (packed & TOKEN_MASK) as u32)
}

/// `tokens` after `elapsed` seconds of refill, capped at `capacity`.
fn refilled(tokens: u32, elapsed: u64, capacity: u32, period_secs: u64) -> u64 {
    // u128 so `elapsed * capacity` cannot overflow before the divide.
    let gained = (elapsed as u128 * capacity as u128 / period_secs as u128) as u64;
    (tokens as u64 + gained).min(capacity as u64)
}

impl Bucket {
    fn new(now_secs: u64, capacity: u32) -> Self {
        Bucket(AtomicU64::new(pack(now_secs, capacity)))
    }

    /// Lock-free: no mutex is touched, only this one atomic word.
    fn try_consume(&self, now_secs: u64, capacity: u32, period_secs: u64) -> bool {
        let mut current = self.0.load(Relaxed);
        loop {
            let (last_secs, tokens) = unpack(current);
            // A stale `last_secs` from a racing writer only means slightly
            // less refill got applied this round; the next check catches up.
            let elapsed = now_secs.saturating_sub(last_secs);
            let refilled = refilled(tokens, elapsed, capacity, period_secs);
            // Advance the baseline only by the time spent earning the tokens
            // just credited, or a client polling faster than one refill
            // interval would never accumulate enough elapsed time to earn one.
            let credited = refilled - tokens as u64;
            let spent = (credited as u128 * period_secs as u128 / capacity as u128) as u64;
            let new_last_secs = last_secs + spent;
            let (new_tokens, allowed) = if refilled >= 1 { (refilled as u32 - 1, true) } else { (0, false) };
            match self.0.compare_exchange_weak(current, pack(new_last_secs, new_tokens), Relaxed, Relaxed) {
                Ok(_) => return allowed,
                Err(actual) => current = actual,
            }
        }
    }

    /// Idle and holding nothing worth keeping - safe to sweep. Applies the
    /// same lazy refill as `try_consume`, or a bucket untouched since it
    /// last emptied would never look full, no matter how idle it's been.
    fn is_full(&self, now_secs: u64, capacity: u32, period_secs: u64) -> bool {
        let (last_secs, tokens) = unpack(self.0.load(Relaxed));
        let elapsed = now_secs.saturating_sub(last_secs);
        refilled(tokens, elapsed, capacity, period_secs) >= capacity as u64
    }
}

pub(crate) struct RateLimiter {
    map: BoundedMap<IpAddr, Bucket>,
    epoch: Instant,
    capacity: u32,
    period_secs: u64,
    user_agent: Vec<MatchPattern>,
    max_tracked: usize,
}

impl RateLimiter {
    pub(crate) fn new(requests: u32, period_seconds: u64, user_agent: Vec<MatchPattern>) -> Self {
        Self::with_shape(requests, period_seconds, user_agent, MAX_TRACKED_IPS)
    }

    /// Real construction path; `new` fixes the tracking cap to the
    /// production constant, tests exercise a small cap directly so the
    /// eviction-when-full path is reachable without filling 64K IPs.
    fn with_shape(requests: u32, period_seconds: u64, user_agent: Vec<MatchPattern>, max_tracked: usize) -> Self {
        // Above this the burst count no longer fits TOKEN_BITS - an encoding
        // limit, not a policy one, so clamped rather than rejected, but
        // logged since the configured value is then not what's enforced.
        let capacity = requests.min(TOKEN_MASK as u32);
        if capacity != requests {
            tracing::warn!(r#type = "controller", requests, capacity, "rate_limit.requests exceeds the encodable maximum, clamped");
        }
        RateLimiter { map: BoundedMap::new(), epoch: Instant::now(), capacity, period_secs: period_seconds, user_agent, max_tracked: max_tracked.max(1) }
    }

    /// Whether `user_agent` is subject to this limiter at all, checked
    /// before any shared state is touched - a non-matching request costs
    /// nothing beyond this.
    pub(crate) fn should_limit(&self, user_agent: &str) -> bool {
        crate::config::matches_any(&self.user_agent, user_agent)
    }

    /// For the `Retry-After` header on a 429.
    pub(crate) fn period_seconds(&self) -> u64 {
        self.period_secs
    }

    /// `true` if `ip` may proceed - and has just consumed one token - `false`
    /// if it is currently out of budget. Only meaningful once `should_limit`
    /// has already said yes.
    pub(crate) fn check(&self, ip: IpAddr) -> bool {
        let now_secs = self.epoch.elapsed().as_secs();

        // Fast path: an already-tracked IP only ever needs DashMap's own
        // fine-grained per-shard lock, never a lock shared with unrelated IPs.
        if let Some(bucket) = self.map.get(&ip) {
            return bucket.try_consume(now_secs, self.capacity, self.period_secs);
        }

        // Rare path: first sighting of this IP (or it was swept below).
        // Fails open rather than evicting a client actively being limited
        // if the table is still full even after sweeping idle entries.
        if !self.map.has_room_for(&ip, self.max_tracked, |bucket| bucket.is_full(now_secs, self.capacity, self.period_secs)) {
            return true;
        }
        self.map
            .entry(ip)
            .or_insert_with(|| Bucket::new(now_secs, self.capacity))
            .try_consume(now_secs, self.capacity, self.period_secs)
    }
}

#[cfg(test)]
#[path = "rate_limit_tests.rs"]
mod tests;
