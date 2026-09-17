//! Per-client token-bucket rate limiting, scoped by `User-Agent` so it can
//! target crawlers without touching ordinary visitors. Keyed on an opaque
//! `ClientIdentity`, so where that identity came from is not decided here.
//!
//! Tracking is a bounded cache, so a client can be evicted and return with a
//! fresh burst. What keeps that from being a bypass is the eviction policy
//! being scan-resistant: one-shot addresses cannot displace a repeat offender.

use super::proxy::ClientIdentity;
use crate::logging;
use crate::utils::match_pattern::MatchPattern;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

/// An internal bound, not a config knob: nothing an operator needs to tune.
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
            let (new_tokens, allowed) = if refilled >= 1 {
                (refilled as u32 - 1, true)
            } else {
                (0, false)
            };
            match self.0.compare_exchange_weak(
                current,
                pack(new_last_secs, new_tokens),
                Relaxed,
                Relaxed,
            ) {
                Ok(_) => return allowed,
                Err(actual) => current = actual,
            }
        }
    }
}

pub(crate) struct RateLimiter {
    /// `Arc` because the cache yields a clone of the value, and every holder
    /// has to reach the same word.
    map: quick_cache::sync::Cache<ClientIdentity, Arc<Bucket>>,
    epoch: Instant,
    capacity: u32,
    period_secs: u64,
    user_agent: Vec<MatchPattern>,
}

impl RateLimiter {
    pub(crate) fn new(requests: u32, period_seconds: u64, user_agent: Vec<MatchPattern>) -> Self {
        Self::with_shape(requests, period_seconds, user_agent, MAX_TRACKED_IPS)
    }

    /// Takes the tracking cap as a parameter so a caller can pick one small
    /// enough to reach the eviction path.
    fn with_shape(
        requests: u32,
        period_seconds: u64,
        user_agent: Vec<MatchPattern>,
        max_tracked: usize,
    ) -> Self {
        let max_tracked = max_tracked.max(1);
        // Above this the burst count no longer fits TOKEN_BITS - an encoding
        // limit, not a policy one, so clamped rather than rejected, but
        // logged since the configured value is then not what's enforced.
        let capacity = requests.min(TOKEN_MASK as u32);
        if capacity != requests {
            logging::warn!(
                r#type = "controller",
                requests,
                capacity,
                "rate_limit.requests exceeds the encodable maximum, clamped"
            );
        }
        RateLimiter {
            map: quick_cache::sync::Cache::new(max_tracked),
            epoch: Instant::now(),
            capacity,
            period_secs: period_seconds,
            user_agent,
        }
    }

    /// Whether `user_agent` is subject to this limiter at all. Touches no
    /// shared state, so it is safe to gate on before `check`.
    pub(crate) fn should_limit(&self, user_agent: &str) -> bool {
        crate::utils::match_pattern::matches_any(&self.user_agent, user_agent)
    }

    pub(crate) fn period_seconds(&self) -> u64 {
        self.period_secs
    }

    /// `true` if `client` may proceed - and has just consumed one token -
    /// `false` if it is currently out of budget. Only meaningful once
    /// `should_limit` has already said yes.
    pub(crate) fn check(&self, client: ClientIdentity) -> bool {
        let now_secs = self.epoch.elapsed().as_secs();
        let capacity = self.capacity;
        self.map
            .get_or_insert_with(&client, || {
                Ok::<_, std::convert::Infallible>(Arc::new(Bucket::new(now_secs, capacity)))
            })
            .expect("the initialiser cannot fail")
            .try_consume(now_secs, capacity, self.period_secs)
    }
}

#[cfg(test)]
#[path = "rate_limit_tests.rs"]
mod tests;
