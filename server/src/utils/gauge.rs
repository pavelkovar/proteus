//! A count of work in progress that releases itself.

use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// The only way to raise the count hands back a guard that lowers it again on
/// drop, so a holder that is cancelled mid-await or unwound past cannot strand
/// the count above zero for the life of the process.
#[derive(Default)]
pub(crate) struct Gauge(AtomicU64);

impl Gauge {
    pub(crate) fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }

    /// `None` once `max` units are outstanding; 0 means no limit. Borrows, so
    /// the guard cannot outlive the gauge; one that must takes
    /// `GaugeGuard::under` with an owning handle instead.
    pub(crate) fn enter_under(&self, max: usize) -> Option<GaugeGuard<&Gauge>> {
        GaugeGuard::under(self, max)
    }

    fn release(&self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

/// One unit of a [`Gauge`]. `H` is however the holder reaches it: `&Gauge`
/// where the guard is confined to one scope, `Arc<Gauge>` where it outlives
/// the call that took it.
pub(crate) struct GaugeGuard<H: Deref<Target = Gauge>>(H);

impl<H: Deref<Target = Gauge>> GaugeGuard<H> {
    pub(crate) fn new(handle: H) -> Self {
        handle.0.fetch_add(1, Relaxed);
        GaugeGuard(handle)
    }

    /// The check and the increment have to be one atomic step: as a separate
    /// load and add, every caller in a racing burst reads the same count and
    /// all of them get past `max`.
    pub(crate) fn under(handle: H, max: usize) -> Option<Self> {
        if max == 0 {
            return Some(Self::new(handle));
        }
        handle
            .0
            .try_update(Relaxed, Relaxed, |n| ((n as usize) < max).then_some(n + 1))
            .ok()?;
        Some(GaugeGuard(handle))
    }
}

impl<H: Deref<Target = Gauge>> Drop for GaugeGuard<H> {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[cfg(test)]
#[path = "gauge_tests.rs"]
mod tests;
