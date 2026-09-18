//! One independent async loop per job - each computes its own next
//! occurrence and sleeps to it, rather than a shared per-minute tick.
//!
//! Overlap control falls out of this for free: a job's own loop cannot
//! reach its next occurrence before the current run's `.await` returns.

use super::CronJob;
use super::exec::{self, Identity};
use crate::logging;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// Line number of a job to the process group of its currently in-flight run,
/// if any. Shutdown reads this to know who to signal, and the orphan reaper
/// reads it to know which exited pids are already someone else's to reap.
type Registry = Arc<Mutex<HashMap<usize, Pid>>>;

fn lock(registry: &Registry) -> MutexGuard<'_, HashMap<usize, Pid>> {
    registry.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) async fn run(jobs: Vec<CronJob>, identity: Identity, shutdown_grace: Duration) {
    let identity = Arc::new(identity);
    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(reap_orphans(Arc::clone(&registry)));

    let mut tasks = JoinSet::new();
    for job in jobs {
        let identity = Arc::clone(&identity);
        let registry = Arc::clone(&registry);
        let shutdown_rx = shutdown_rx.clone();
        tasks.spawn(job_loop(job, identity, registry, shutdown_rx));
    }

    wait_for_shutdown_signal().await;
    logging::info!(
        r#type = "cron",
        "shutdown signal received, stopping the scheduler"
    );
    let _ = shutdown_tx.send(true);

    signal_running(&registry, Signal::SIGCONT);
    signal_running(&registry, Signal::SIGTERM);

    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(shutdown_grace, drain).await.is_err() {
        logging::warn!(
            r#type = "cron",
            "shutdown grace period elapsed, killing remaining jobs"
        );
        signal_running(&registry, Signal::SIGKILL);
        while tasks.join_next().await.is_some() {}
    }
}

/// SIGTERM is the normal container-stop signal; SIGINT covers interactive
/// use (Ctrl-C) - both must drain running jobs the same way, not just exit.
async fn wait_for_shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install a SIGTERM handler");
    let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("failed to install a SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

fn signal_running(registry: &Registry, sig: Signal) {
    for pgid in lock(registry).values() {
        exec::signal_group(*pgid, sig);
    }
}

/// `enable_child_subreaper` (main.rs) means any grandchild a job backgrounds
/// and outlives reparents to this process instead of init, so something has
/// to reap it - nothing else will.
///
/// Peeks the next exited pid without consuming it (`WNOWAIT`) so a pid
/// `registry` still owns is left for `Child::wait` to reap normally; only
/// pids `registry` doesn't recognize are actually reaped here.
async fn reap_orphans(registry: Registry) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        loop {
            let peeked = nix::sys::wait::waitid(
                nix::sys::wait::Id::All,
                nix::sys::wait::WaitPidFlag::WEXITED
                    | nix::sys::wait::WaitPidFlag::WNOHANG
                    | nix::sys::wait::WaitPidFlag::WNOWAIT,
            );
            let Ok(status) = peeked else { break };
            let Some(pid) = status.pid() else { break };
            if lock(&registry).values().any(|owned| *owned == pid) {
                break;
            }
            let _ = nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG));
        }
    }
}

async fn job_loop(
    job: CronJob,
    identity: Arc<Identity>,
    registry: Registry,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut after = chrono::Local::now();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let next = match job.schedule.find_next_occurrence(&after, false) {
            Ok(t) => t,
            Err(e) => {
                logging::error!(
                    r#type = "cron",
                    line = job.line,
                    error = %e,
                    "could not compute this job's next run; it will not run again"
                );
                return;
            }
        };
        if !sleep_until_or_shutdown(next, &mut shutdown).await {
            return;
        }
        run_job(&job, &identity, &registry).await;
        after = chrono::Local::now();
    }
}

/// Sleeps in bounded chunks and re-checks the wall clock on each wake,
/// since `tokio::time::sleep`'s monotonic clock does not advance while a
/// container is paused/suspended. Returns `false` if shutdown fired first.
async fn sleep_until_or_shutdown(
    target: chrono::DateTime<chrono::Local>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        let remaining = (target - chrono::Local::now()).to_std().unwrap_or_default();
        if remaining.is_zero() {
            return true;
        }
        let chunk = remaining.min(Duration::from_secs(60));
        tokio::select! {
            () = tokio::time::sleep(chunk) => {}
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return false;
                }
            }
        }
    }
}

async fn run_job(job: &CronJob, identity: &Identity, registry: &Registry) {
    let mut running = match exec::spawn(identity, &job.command) {
        Ok(r) => r,
        Err(e) => {
            logging::error!(r#type = "cron", line = job.line, error = %e, "failed to spawn job");
            return;
        }
    };
    lock(registry).insert(job.line, running.pgid);
    logging::info!(r#type = "cron", line = job.line, command = %job.command, "job started");

    let start = std::time::Instant::now();
    let status = running.child.wait().await;
    lock(registry).remove(&job.line);
    let duration_ms = start.elapsed().as_millis();

    match status {
        Ok(status) => logging::info!(
            r#type = "cron",
            line = job.line,
            exit_code = status.code(),
            signal = status.signal(),
            duration_ms,
            "job finished"
        ),
        Err(e) => logging::error!(
            r#type = "cron",
            line = job.line,
            error = %e,
            duration_ms,
            "failed to wait for job"
        ),
    }
}

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod tests;
