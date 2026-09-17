use super::*;

#[tokio::test]
async fn wait_returns_at_once_if_already_fired() {
    let (tx, mut shutdown) = Shutdown::channel();
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), shutdown.wait())
        .await
        .expect("wait() must not block once the signal already fired");
}

#[tokio::test]
async fn wait_unblocks_once_the_signal_fires() {
    let (tx, mut shutdown) = Shutdown::channel();
    let waited = tokio::spawn(async move {
        shutdown.wait().await;
    });
    // Give the spawned task a chance to start waiting before firing.
    tokio::task::yield_now().await;
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), waited)
        .await
        .expect("wait() must unblock once the signal fires")
        .expect("task panicked");
}

/// Without this, an accept loop blocked on `wait()` after master itself has
/// gone would never return, and the process could not exit.
#[tokio::test]
async fn wait_returns_when_the_sender_is_dropped() {
    let (tx, mut shutdown) = Shutdown::channel();
    drop(tx);
    tokio::time::timeout(std::time::Duration::from_millis(100), shutdown.wait())
        .await
        .expect("wait() must not hang once the sender is gone");
}

/// Every accept loop clones its own `Shutdown`; all of them must see one
/// signal, not just the original.
#[tokio::test]
async fn every_clone_observes_the_same_signal() {
    let (tx, shutdown) = Shutdown::channel();
    let mut a = shutdown.clone();
    let mut b = shutdown;
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), a.wait())
        .await
        .expect("clone a must see the signal");
    tokio::time::timeout(std::time::Duration::from_millis(100), b.wait())
        .await
        .expect("clone b must see the signal");
}
