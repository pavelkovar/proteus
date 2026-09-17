use super::*;

#[test]
fn a_fresh_connection_is_idle_from_the_moment_it_is_created() {
    let conn = ConnState::new();
    assert!(
        conn.idle_for().is_some(),
        "a client that connects and sends nothing must read as idle"
    );
}

#[test]
fn a_request_in_flight_is_never_idle() {
    let conn = ConnState::new();
    conn.request_started();
    assert!(conn.idle_for().is_none());
}

#[test]
fn idle_for_measures_since_the_last_request_finished() {
    let conn = ConnState::new();
    conn.request_started();
    conn.request_finished();
    let idle = conn.idle_for().expect("no request is in flight");
    assert!(idle < std::time::Duration::from_secs(1));
}

/// The order `request_finished` stores in matters: decrementing the count
/// before stamping the finish time would let this observe a zero count
/// still paired with the *previous* request's stamp.
#[test]
fn overlapping_requests_stay_busy_until_the_last_one_finishes() {
    let conn = ConnState::new();
    conn.request_started();
    conn.request_started();
    conn.request_finished();
    assert!(
        conn.idle_for().is_none(),
        "one request is still in flight"
    );
    conn.request_finished();
    assert!(conn.idle_for().is_some());
}

#[test]
fn conn_busy_guard_releases_on_drop() {
    let conn = std::sync::Arc::new(ConnState::new());
    {
        let _guard = ConnBusyGuard::new(std::sync::Arc::clone(&conn));
        assert!(conn.idle_for().is_none());
    }
    assert!(conn.idle_for().is_some());
}

#[tokio::test]
async fn wait_until_idle_returns_once_the_timeout_elapses_since_the_last_request() {
    let conn = ConnState::new();
    conn.request_started();
    conn.request_finished();
    let idle_timeout = std::time::Duration::from_millis(50);
    let start = tokio::time::Instant::now();
    wait_until_idle(&conn, idle_timeout).await;
    assert!(tokio::time::Instant::now() - start >= idle_timeout);
}

#[tokio::test]
async fn wait_until_idle_keeps_waiting_while_a_request_is_in_flight() {
    let conn = ConnState::new();
    conn.request_started();
    let idle_timeout = std::time::Duration::from_millis(30);
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        wait_until_idle(&conn, idle_timeout),
    )
    .await;
    assert!(
        result.is_err(),
        "must not return while the request is still in flight"
    );
}
