use super::*;

#[tokio::test]
async fn sleep_until_or_shutdown_returns_true_immediately_for_a_past_target() {
    let (_tx, mut rx) = watch::channel(false);
    let target = chrono::Local::now() - chrono::Duration::seconds(1);
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        sleep_until_or_shutdown(target, &mut rx),
    )
    .await
    .expect("must not need to actually sleep for a past target");
    assert!(result);
}

#[tokio::test]
async fn sleep_until_or_shutdown_returns_false_when_shutdown_fires_first() {
    let (tx, mut rx) = watch::channel(false);
    let target = chrono::Local::now() + chrono::Duration::seconds(120);
    let handle = tokio::spawn(async move { sleep_until_or_shutdown(target, &mut rx).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = tx.send(true);
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("shutdown must interrupt the sleep promptly")
        .expect("the sleeping task must not panic");
    assert!(!result);
}

#[tokio::test]
async fn job_loop_exits_immediately_when_shutdown_is_already_set() {
    let (tx, rx) = watch::channel(false);
    let _ = tx.send(true);
    let job = super::super::parse("* * * * * true\n").unwrap().remove(0);
    let identity = Arc::new(Identity::resolve(None, None));
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
    // Must return well before its schedule would ever be due (up to a
    // minute away) - proves the pre-sleep shutdown check, not a lucky race.
    tokio::time::timeout(
        Duration::from_secs(2),
        job_loop(job, identity, registry, rx),
    )
    .await
    .expect("a job_loop that sees shutdown=true up front must return immediately");
}

#[test]
fn lock_recovers_from_a_poisoned_mutex_instead_of_panicking() {
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
    let poisoned = Arc::clone(&registry);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned.lock().unwrap();
        panic!("deliberately poisoning the mutex");
    })
    .join();
    lock(&registry).insert(1, vec![Pid::from_raw(1)]);
    assert_eq!(lock(&registry).len(), 1);
}

/// A due run that starts while its predecessor's group is still draining
/// must not evict it from `registry` - or a shutdown landing in between
/// would miss it entirely and never send it SIGTERM.
#[test]
fn a_second_run_does_not_evict_the_first_runs_still_draining_pgid() {
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
    let first = Pid::from_raw(100);
    let second = Pid::from_raw(200);

    lock(&registry).entry(1).or_default().push(first);
    lock(&registry).entry(1).or_default().push(second);
    assert_eq!(lock(&registry).get(&1).unwrap(), &vec![first, second]);

    untrack_pgid(&registry, 1, first);
    assert_eq!(
        lock(&registry).get(&1).unwrap(),
        &vec![second],
        "untracking the first run must leave the second one signalable"
    );

    untrack_pgid(&registry, 1, second);
    assert!(
        lock(&registry).get(&1).is_none(),
        "the line's entry must go away once nothing of it is left"
    );
}
