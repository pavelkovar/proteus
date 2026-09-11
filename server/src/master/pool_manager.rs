//! Prototype lifecycle and the worker pool: how the pool stays populated,
//! not what happens to one request.
//!
//! Retirement is deliberately split across both processes: a worker times
//! its own idle period, and master holds the `spare` floor, because neither
//! knows on its own both how long a worker has been idle and whether the
//! pool can afford to lose it.

use super::idle_stack::IdleStack;
use super::prototype_launch;
use super::worker_channel::WorkerChannel;
use crate::config::{Config, PhpOptions};
use crate::ipc::control;
use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio_seqpacket::UnixSeqpacket;

/// RAII cleanup for a spilled request body. Must outlive the response:
/// dropping it before the worker's done marker races the worker's `fopen()`.
pub struct TempBodyFile(std::path::PathBuf);

impl TempBodyFile {
    pub fn new(path: std::path::PathBuf) -> Self {
        TempBodyFile(path)
    }
}

fn remove_temp_body(path: &std::path::Path) {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(r#type = "controller", path = %path.display(), error = %e, "failed to remove temp body file");
        }
    }
}

/// The `unlink` is a filesystem round trip on whatever `TMPDIR` points at,
/// which need not be a tmpfs, so it goes to a background task rather than a
/// runtime thread.
///
/// `Handle::spawn`, not `spawn_blocking`: the latter panics once the runtime
/// is shutting down, and a panic in a `Drop` during unwind aborts the
/// process. Leaking a temp file for the OS to reap is the better failure.
impl Drop for TempBodyFile {
    fn drop(&mut self) {
        let path = std::mem::take(&mut self.0);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move { tokio::fs::remove_file(&path).await.or_else(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound { Ok(()) } else { Err(e) }
                }).unwrap_or_else(|e| {
                    tracing::warn!(r#type = "controller", path = %path.display(), error = %e, "failed to remove temp body file");
                }) });
            }
            // No runtime at all: inline is both safe and the only option.
            Err(_) => remove_temp_body(&path),
        }
    }
}

pub struct PoolManager {
    /// tokio mutex: `spawn_worker_once` holds this guard across an `.await`.
    control: Mutex<UnixSeqpacket>,
    /// Lock-free, being taken and returned on every request. Workable only
    /// because retirement is the worker's own decision, so nothing needs to
    /// inspect this from the far end.
    idle: IdleStack<PooledWorker>,
    /// Caps in-flight requests. `Arc` because the finish-watch task needs an
    /// `OwnedSemaphorePermit`.
    semaphore: Arc<Semaphore>,
    max_workers: usize,
    request_timeout: Duration,
    queue_timeout: Duration,
    /// Bounds the wait on the prototype. `queue_timeout` covers only
    /// acquiring a permit, so without this a wedged prototype hangs every
    /// dispatch forever.
    spawn_timeout: Duration,
    /// 0 disables the hard cap; otherwise `dispatch` rejects once
    /// `queue_depth` reaches it.
    queue_max_depth: usize,
    /// Only ever mutated through `QueueDepthGuard`.
    queue_depth: AtomicU64,
    started_at: Instant,
    /// `php.targets` names, sorted, for `status_json`.
    target_names: Vec<String>,
    requests_total: AtomicU64,
    watchdog_kills: AtomicU64,
    queue_timeouts: AtomicU64,
    dispatch_failed: AtomicU64,
    workers_spawned: AtomicU64,
    /// Clean retirement, as distinct from `dispatch_failed`.
    recycled_request_limit: AtomicU64,
    recycled_idle_timeout: AtomicU64,
    prototype_child: StdMutex<PrototypeHandle>,
    /// Retained so a respawn can reproduce the original launch exactly.
    php_mod_path: String,
    max_requests: u32,
    /// Seconds; 0 disables. Passed on so workers can retire themselves.
    idle_timeout: u64,
    uid: u32,
    gid: u32,
    /// False means keeping master's own identity, which requires skipping
    /// the `uid`/`gid` calls entirely rather than passing master's own.
    drop_privileges: bool,
    options: PhpOptions,
    environment: HashMap<String, String>,
    /// tokio mutex: held across `.await`. Not hot-path - only touched after
    /// a spawn already failed.
    respawn_backoff: Mutex<RespawnBackoff>,
    crash_loop_backoffs: AtomicU64,
    prototype_respawns: AtomicU64,
    /// Keyed by pid; presence doubles as "still one of ours". Touched twice
    /// in a worker's life plus once per `/status`, never per HTTP request -
    /// those updates go through `WorkerMeta` and take no lock here.
    workers: StdMutex<HashMap<u32, Arc<WorkerMeta>>>,
}

#[derive(Default)]
struct RespawnBackoff {
    last_attempt: Option<Instant>,
    consecutive_failures: u32,
}

/// RAII guard for one `queue_depth` slot. The check and increment are one
/// `fetch_update`, since a separate load and add would let a racing burst
/// past `max`. `Drop` also covers cancellation mid-wait, which would
/// otherwise leak the slot forever.
struct QueueDepthGuard<'a>(&'a AtomicU64);

impl<'a> QueueDepthGuard<'a> {
    fn try_new(counter: &'a AtomicU64, max: usize) -> Option<Self> {
        if max == 0 {
            counter.fetch_add(1, Relaxed);
            return Some(QueueDepthGuard(counter));
        }
        counter
            .try_update(Relaxed, Relaxed, |d| {
                if (d as usize) < max {
                    Some(d + 1)
                } else {
                    None
                }
            })
            .ok()?;
        Some(QueueDepthGuard(counter))
    }
}

impl Drop for QueueDepthGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

/// Carried together so the hot path never looks a worker up by pid.
pub(crate) struct PooledWorker {
    pub(crate) channel: WorkerChannel,
    pub(crate) pid: u32,
    /// Also carries the worker's idle-stack slot.
    pub(crate) meta: Arc<WorkerMeta>,
}

const STATE_IDLE: u8 = 0;
const STATE_BUSY: u8 = 1;

/// Per-worker counters reached through an `Arc` rather than the `workers`
/// map, so a per-request update never takes a global lock to reach one
/// worker's own fields.
///
/// `last_active_ms` rather than an `Instant`, there being no atomic
/// `Instant` and no consumer needing sub-second resolution.
pub(crate) struct WorkerMeta {
    /// Held for the worker's whole life, so parking and unparking touch only
    /// the idle head. Kept here, not on `PooledWorker`, so a caller holding
    /// just a pid can return it - and so it exists once rather than twice.
    pub(crate) slot: u32,
    state: std::sync::atomic::AtomicU8,
    request_count: std::sync::atomic::AtomicU32,
    started_at: Instant,
    last_active_ms: AtomicU64,
}

impl WorkerMeta {
    fn new(slot: u32, now: Instant, pool_started: Instant) -> Self {
        WorkerMeta {
            slot,
            state: std::sync::atomic::AtomicU8::new(STATE_IDLE),
            request_count: std::sync::atomic::AtomicU32::new(0),
            started_at: now,
            last_active_ms: AtomicU64::new(now.duration_since(pool_started).as_millis() as u64),
        }
    }

    fn mark_busy(&self, at: Instant, pool_started: Instant) {
        self.state.store(STATE_BUSY, Relaxed);
        self.request_count.fetch_add(1, Relaxed);
        self.last_active_ms
            .store(at.duration_since(pool_started).as_millis() as u64, Relaxed);
    }

    fn mark_idle(&self, at: Instant, pool_started: Instant) {
        self.state.store(STATE_IDLE, Relaxed);
        self.last_active_ms
            .store(at.duration_since(pool_started).as_millis() as u64, Relaxed);
    }

    fn state_str(&self) -> &'static str {
        match self.state.load(Relaxed) {
            STATE_BUSY => "busy",
            _ => "idle",
        }
    }
}

/// The prototype's pid, plus whether it has already been reaped.
///
/// An unreaped pid cannot be recycled, so signalling it stays safe right up
/// to the moment something reaps it - which is what this tracks.
struct PrototypeHandle {
    child: std::process::Child,
    reaped: bool,
}

impl PrototypeHandle {
    fn new(child: std::process::Child) -> Self {
        PrototypeHandle {
            child,
            reaped: false,
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// `None` once reaped: the only safe answer to what may be signalled.
    fn live_pid(&self) -> Option<u32> {
        (!self.reaped).then(|| self.child.id())
    }

    fn mark_reaped(&mut self) {
        self.reaped = true;
    }
}

/// Exponential: a bad php-mod path or config will not fix itself by being
/// retried faster.
fn respawn_backoff_delay(consecutive_failures: u32) -> Duration {
    Duration::from_secs((1u64 << consecutive_failures.min(6)).min(60))
}

/// SIGKILL only: every kill site is a worker already known to be wedged or
/// abandoned, where asking politely is what has been shown not to work.
///
/// Logs rather than panics on delivery failure; ESRCH on an already-dead pid
/// is the expected case.
pub(crate) fn sigkill(pid: u32, context: &str) {
    tracing::debug!(
        r#type = "controller",
        pid,
        context,
        "sending SIGKILL to worker"
    );
    if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGKILL) {
        tracing::warn!(r#type = "controller", pid, error = %e, "signal delivery failed");
    }
}

/// Installed where the dynamic linker already looks, so a bare `dlopen()`
/// finds it; `PROTEUS_PHP_MOD_PATH` overrides with an absolute path.
fn resolve_php_mod_path() -> String {
    std::env::var("PROTEUS_PHP_MOD_PATH").unwrap_or_else(|_| "libproteus-php-mod.so".to_string())
}

impl PoolManager {
    pub fn spawn_prototype(cfg: &Config) -> Self {
        // config::validate guarantees these are set together or not at all.
        let drop_to = match (&cfg.php.user, &cfg.php.group) {
            (Some(user), Some(group)) => {
                let uid = nix::unistd::User::from_name(user)
                    .expect("getpwnam failed")
                    .unwrap_or_else(|| panic!("user {user:?} not found"))
                    .uid
                    .as_raw();
                let gid = nix::unistd::Group::from_name(group)
                    .expect("getgrnam failed")
                    .unwrap_or_else(|| panic!("group {group:?} not found"))
                    .gid
                    .as_raw();
                Some((uid, gid))
            }
            _ => None,
        };
        let (uid, gid) = drop_to.unwrap_or_else(|| {
            (
                nix::unistd::getuid().as_raw(),
                nix::unistd::getgid().as_raw(),
            )
        });

        let php_mod_path = resolve_php_mod_path();
        let (control, child) = prototype_launch::spawn(
            &php_mod_path,
            cfg.php.limits.requests,
            cfg.php.processes.idle_timeout,
            drop_to,
            &cfg.php.options,
            &cfg.php.environment,
        )
        .expect("failed to spawn prototype");
        tracing::info!(
            r#type = "controller",
            pid = child.id(),
            uid,
            gid,
            dropped = drop_to.is_some(),
            "spawned prototype"
        );

        PoolManager {
            control: Mutex::new(control),
            idle: IdleStack::new(cfg.php.processes.max),
            semaphore: Arc::new(Semaphore::new(cfg.php.processes.max)),
            max_workers: cfg.php.processes.max,
            request_timeout: Duration::from_secs(cfg.php.limits.timeout),
            queue_timeout: Duration::from_secs(cfg.php.queue.timeout),
            spawn_timeout: Duration::from_secs(cfg.php.processes.spawn_timeout),
            queue_max_depth: cfg.php.queue.max_depth,
            queue_depth: AtomicU64::new(0),
            started_at: Instant::now(),
            target_names: {
                let mut names: Vec<String> = cfg.php.targets.keys().cloned().collect();
                names.sort();
                names
            },
            requests_total: AtomicU64::new(0),
            watchdog_kills: AtomicU64::new(0),
            queue_timeouts: AtomicU64::new(0),
            dispatch_failed: AtomicU64::new(0),
            workers_spawned: AtomicU64::new(0),
            recycled_request_limit: AtomicU64::new(0),
            recycled_idle_timeout: AtomicU64::new(0),
            prototype_child: StdMutex::new(PrototypeHandle::new(child)),
            php_mod_path,
            max_requests: cfg.php.limits.requests,
            idle_timeout: cfg.php.processes.idle_timeout,
            uid,
            gid,
            drop_privileges: drop_to.is_some(),
            options: PhpOptions {
                admin: cfg.php.options.admin.clone(),
                user: cfg.php.options.user.clone(),
            },
            environment: cfg.php.environment.clone(),
            respawn_backoff: Mutex::new(RespawnBackoff::default()),
            crash_loop_backoffs: AtomicU64::new(0),
            prototype_respawns: AtomicU64::new(0),
            workers: StdMutex::new(HashMap::new()),
        }
    }

    /// Rate-limited by `respawn_backoff_delay`. Returns whether it actually
    /// respawned.
    async fn try_respawn_prototype(&self) -> bool {
        let mut backoff = self.respawn_backoff.lock().await;
        let now = Instant::now();
        if let Some(last) = backoff.last_attempt {
            let required_delay = respawn_backoff_delay(backoff.consecutive_failures);
            if now.duration_since(last) < required_delay {
                self.crash_loop_backoffs.fetch_add(1, Relaxed);
                return false;
            }
        }
        backoff.last_attempt = Some(now);

        tracing::info!(
            r#type = "controller",
            consecutive_failures = backoff.consecutive_failures,
            "attempting to respawn the prototype"
        );
        // fork()+exec() blocks, and this one runs while the pool is live.
        let php_mod_path = self.php_mod_path.clone();
        let max_requests = self.max_requests;
        let drop_to = self.drop_privileges.then_some((self.uid, self.gid));
        let options = self.options.clone();
        let environment = self.environment.clone();
        let idle_timeout = self.idle_timeout;
        let spawn_result = tokio::task::spawn_blocking(move || {
            prototype_launch::spawn(
                &php_mod_path,
                max_requests,
                idle_timeout,
                drop_to,
                &options,
                &environment,
            )
        })
        .await
        .expect("prototype_launch::spawn blocking task panicked");
        match spawn_result {
            Ok((new_control, new_child)) => {
                let new_pid = new_child.id();
                *self.control.lock().await = new_control;
                self.replace_prototype_child(new_child);

                backoff.consecutive_failures = 0;
                self.prototype_respawns.fetch_add(1, Relaxed);
                tracing::info!(r#type = "controller", pid = new_pid, "prototype respawned");
                true
            }
            Err(e) => {
                backoff.consecutive_failures += 1;
                tracing::error!(r#type = "controller", error = %e, "failed to respawn prototype");
                false
            }
        }
    }

    /// Kills and reaps whatever it replaces. Not every respawn follows a
    /// death - a wedged prototype is replaced while still running, and
    /// dropping its `Child` would leave it orphaned with its whole PHP heap,
    /// tracked by nothing.
    fn replace_prototype_child(&self, new_child: std::process::Child) {
        let mut child_guard = self.prototype_child.lock().unwrap();
        if let Some(pid) = child_guard.live_pid() {
            sigkill(pid, "replacing a prototype that is still running");
            // The lock serialises this against the sweep, so exactly one of
            // the two reaps this pid.
            let _ = waitpid(Pid::from_raw(pid as i32), None);
        }
        *child_guard = PrototypeHandle::new(new_child);
    }

    /// Only ever runs on a prototype that has already stopped answering. Its
    /// workers follow via `PR_SET_PDEATHSIG`.
    fn kill_prototype(&self) {
        let pid = self.prototype_child.lock().unwrap().live_pid();
        if let Some(pid) = pid {
            sigkill(pid, "prototype stopped answering");
        }
    }

    /// Drops workers that retired themselves while parked here, releasing
    /// the memfd mappings their entries pin. `get_worker` skips such entries,
    /// but on a quiet pool nothing pops, so without this they would be
    /// reported idle forever and never released.
    ///
    /// Survivors are pushed back in reverse so the warmest stays on top. A
    /// concurrent pop or push just means this pass sees a slightly different
    /// set; nothing is lost either way.
    fn reap_retired_idle_workers(&self) {
        let to_check = self.idle.len();
        let mut keep = Vec::new();
        for _ in 0..to_check {
            let Some((_slot, worker)) = self.idle.pop() else {
                break;
            };
            if worker.channel.worker_has_exited() {
                tracing::debug!(
                    r#type = "controller",
                    pid = worker.pid,
                    "worker retired itself on idle timeout"
                );
                self.recycled_idle_timeout.fetch_add(1, Relaxed);
                self.remove_worker_meta(worker.pid);
                // Dropping releases the channel and its mapping.
            } else {
                keep.push(worker);
            }
        }
        for worker in keep.into_iter().rev() {
            self.return_worker(worker);
        }
    }

    /// Master's half of retirement: reap what workers left behind and hold
    /// the `spare` floor, without which the pool drains to zero on any quiet
    /// period and the next request pays a cold fork.
    ///
    /// Polls rather than reacting to each exit, which is observed by a
    /// per-worker task with no route back to the pool.
    pub async fn maintain_pool_loop(self: Arc<Self>, spare: usize, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            self.reap_retired_idle_workers();
            // Bounded by the total worker count, not the idle count: this
            // spawns without a semaphore permit, so topping up while others
            // are busy would push the pool past `processes.max` and past the
            // slot array sized for it.
            while self.idle.len() < spare && self.workers.lock().unwrap().len() < self.max_workers {
                match self.spawn_worker().await {
                    Ok(worker) => self.return_worker(worker),
                    Err(e) => {
                        tracing::warn!(r#type = "controller", error = %e, "failed to top the pool back up to spare");
                        break;
                    }
                }
            }
        }
    }

    /// `try_respawn_prototype` is reactive only: with enough spare workers,
    /// nothing would notice a dead prototype until the spares ran out.
    ///
    /// Also reaps anything `PR_SET_CHILD_SUBREAPER` reparents here.
    pub async fn watch_prototype_liveness(self: Arc<Self>, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            let mut prototype_exited = false;
            {
                // Held for the whole sweep so the pid compared against cannot
                // be replaced halfway through it.
                let mut child_guard = self.prototype_child.lock().unwrap();
                let prototype_pid = child_guard.pid();
                loop {
                    match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
                        Ok(WaitStatus::StillAlive) | Err(Errno::ECHILD) => break,
                        Ok(WaitStatus::Exited(pid, _)) | Ok(WaitStatus::Signaled(pid, _, _)) => {
                            if pid.as_raw() as u32 == prototype_pid {
                                child_guard.mark_reaped();
                                prototype_exited = true;
                            }
                        }
                        Err(Errno::EINTR) => continue,
                        Ok(_) | Err(_) => break,
                    }
                }
            }
            if prototype_exited {
                tracing::warn!(
                    r#type = "controller",
                    "prototype exited with no pending worker-spawn attempt to notice it - respawning proactively"
                );
                self.try_respawn_prototype().await;
            }
        }
    }

    /// Best-effort: fewer spares than asked for beats failing to start.
    pub async fn prespawn_spare(&self, spare: usize) {
        tracing::info!(r#type = "controller", spare, "pre-spawning spare workers");
        for _ in 0..spare {
            match self.spawn_worker().await {
                Ok(w) => self.return_worker(w),
                Err(e) => {
                    tracing::error!(r#type = "controller", error = %e, "failed to pre-spawn a spare worker")
                }
            }
        }
    }

    /// A dead prototype fails individual dispatches, never the whole master.
    async fn spawn_worker(&self) -> std::io::Result<PooledWorker> {
        match self.spawn_worker_once().await {
            Ok(w) => Ok(w),
            Err(e) => {
                tracing::warn!(r#type = "controller", error = %e, "spawn_worker failed, trying to respawn the prototype");
                if self.try_respawn_prototype().await {
                    self.spawn_worker_once().await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn spawn_worker_once(&self) -> std::io::Result<PooledWorker> {
        let control = self.control.lock().await;
        let request =
            tokio::time::timeout(self.spawn_timeout, control::request_worker(&control)).await;
        drop(control);
        let (fds, pid) = match request {
            Ok(result) => result?,
            Err(_elapsed) => {
                // SPAWN went out and no reply came, so this control channel
                // is desynced: a later request could read this one's stale
                // WORKER_READY and take fds for a worker it never asked for.
                // Killing the prototype is what retires that channel.
                tracing::error!(
                    r#type = "controller",
                    spawn_timeout = ?self.spawn_timeout,
                    "prototype did not answer a worker-spawn request in time, killing it"
                );
                self.kill_prototype();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "prototype did not answer a worker-spawn request in time",
                ));
            }
        };

        // The worker is already forked and parked on its rings, and nothing
        // else knows its pid yet, so failing to kill it here leaks it
        // permanently.
        let channel = WorkerChannel::new(fds, pid).inspect_err(|_| {
            sigkill(pid, "failed to set up the worker channel after the fork");
        })?;

        self.workers_spawned.fetch_add(1, Relaxed);
        let Some(slot) = self.idle.claim_slot() else {
            // Unreachable while the semaphore caps live workers at the slot
            // count; killing still beats leaking an untracked process.
            sigkill(pid, "no idle slot available for a freshly spawned worker");
            return Err(std::io::Error::other("idle pool has no free slot"));
        };
        let meta = Arc::new(WorkerMeta::new(slot, Instant::now(), self.started_at));
        self.workers.lock().unwrap().insert(pid, Arc::clone(&meta));
        tracing::debug!(r#type = "controller", pid, "spawned worker");
        Ok(PooledWorker { channel, pid, meta })
    }

    /// Forgets a worker and returns its slot. The worker must already be out
    /// of the idle stack.
    fn remove_worker_meta(&self, pid: u32) {
        if let Some(meta) = self.workers.lock().unwrap().remove(&pid) {
            self.idle.release_slot(meta.slot);
        }
    }

    /// LIFO, and load-bearing: FIFO would cycle evenly through every idle
    /// worker, so under steady traffic none would reach its own
    /// `idle_timeout` and the pool would never scale back down.
    async fn get_worker(&self) -> std::io::Result<PooledWorker> {
        // A worker can retire itself while still parked here.
        while let Some((_slot, worker)) = self.idle.pop() {
            if !worker.channel.worker_has_exited() {
                return Ok(worker);
            }
            tracing::debug!(
                r#type = "controller",
                pid = worker.pid,
                "discarding a worker that retired on idle timeout"
            );
            self.recycled_idle_timeout.fetch_add(1, Relaxed);
            self.remove_worker_meta(worker.pid);
        }
        self.spawn_worker().await
    }

    fn return_worker(&self, worker: PooledWorker) {
        self.idle.push(worker.meta.slot, worker);
    }

    /// A spilled body file must be chown()ed to the worker's identity, or it
    /// defaults to master's own.
    pub fn worker_uid_gid(&self) -> (u32, u32) {
        (self.uid, self.gid)
    }

    pub fn status_json(&self) -> serde_json::Value {
        let idle_count = self.idle.len();
        let busy = self.max_workers - self.semaphore.available_permits();
        let prototype_pid = self.prototype_child.lock().unwrap().pid();

        let now_ms = self.started_at.elapsed().as_millis() as u64;
        let workers: Vec<_> = self
            .workers
            .lock()
            .unwrap()
            .iter()
            .map(|(pid, meta)| {
                serde_json::json!({
                    "pid": pid,
                    "state": meta.state_str(),
                    "request_count": meta.request_count.load(Relaxed),
                    "started_ago_seconds": meta.started_at.elapsed().as_secs(),
                    "last_active_ago_seconds": now_ms.saturating_sub(meta.last_active_ms.load(Relaxed)) / 1000,
                })
            })
            .collect();

        serde_json::json!({
            "uptime_seconds": self.started_at.elapsed().as_secs(),
            "php": {
                "targets": self.target_names,
                "prototype_pid": prototype_pid,
                "processes": {
                    "idle": idle_count, "busy": busy, "total": idle_count + busy, "max": self.max_workers
                },
                "queue": {
                    "depth": self.queue_depth.load(Relaxed),
                    "max_depth": self.queue_max_depth,
                },
                "counters": {
                    "requests_total": self.requests_total.load(Relaxed),
                    "requests_failed": self.dispatch_failed.load(Relaxed),
                    "watchdog_kills": self.watchdog_kills.load(Relaxed),
                    "queue_timeouts": self.queue_timeouts.load(Relaxed),
                    "workers_spawned_total": self.workers_spawned.load(Relaxed),
                    "recycled_request_limit": self.recycled_request_limit.load(Relaxed),
                    "recycled_idle_timeout": self.recycled_idle_timeout.load(Relaxed),
                    "prototype_respawns_total": self.prototype_respawns.load(Relaxed),
                    "crash_loop_backoffs": self.crash_loop_backoffs.load(Relaxed),
                },
                "workers": workers,
            }
        })
    }
}

#[path = "pool_manager_dispatch.rs"]
mod dispatch;
pub(crate) use dispatch::{BodyStream, DispatchOutcome};

#[cfg(test)]
#[path = "pool_manager_tests.rs"]
mod tests;
