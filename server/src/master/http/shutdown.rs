//! Graceful-shutdown signal, latched so a late waiter still sees it.

use crate::logging;

/// SIGTERM (systemd/docker/k8s graceful stop) or SIGINT (Ctrl-C) - same
/// drain either way.
pub(super) async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => logging::info!(r#type = "controller", "received SIGTERM"),
        _ = sigint.recv() => logging::info!(r#type = "controller", "received SIGINT"),
    }
}

/// A latched signal: `notify_waiters` would wake only whoever is already
/// waiting, and an accept loop that missed it would never return - which master
/// joins on before it exits.
#[derive(Clone)]
pub struct Shutdown(tokio::sync::watch::Receiver<bool>);

impl Shutdown {
    pub fn channel() -> (tokio::sync::watch::Sender<bool>, Shutdown) {
        let (tx, rx) = tokio::sync::watch::channel(false);
        (tx, Shutdown(rx))
    }

    /// Returns at once if it has already fired.
    pub async fn wait(&mut self) {
        while !*self.0.borrow_and_update() {
            if self.0.changed().await.is_err() {
                return; // sender gone: nothing left to serve either
            }
        }
    }
}

#[cfg(test)]
#[path = "shutdown_tests.rs"]
mod tests;
