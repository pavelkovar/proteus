//! Prototype lifecycle and the worker pool: how the pool stays populated,
//! not what happens to one request.
//!
//! Retirement is deliberately split across both processes: a worker times
//! its own idle period, and master holds the `spare` floor, because neither
//! knows on its own both how long a worker has been idle and whether the
//! pool can afford to lose it.

use super::prototype_launch;
use super::worker_channel::WorkerChannel;
use crate::config::Config;
use crate::gauge::Gauge;
use crate::ipc::control;
use crate::logging;
use crate::prototype::ProtoConfig;
use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_seqpacket::UnixSeqpacket;

/// A spilled request body, held open after being unlinked: the worker gets
/// this fd, and closing it is what frees the space. Must outlive the response,
/// since the worker reads through it for the whole of the request.
pub struct TempBodyFile(std::fs::File);

impl TempBodyFile {
    pub fn new(file: std::fs::File) -> Self {
        TempBodyFile(file)
    }

    pub fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.0)
    }
}

pub struct PoolManager {
    /// tokio mutex: the guard is held across an `.await`.
    control: Mutex<UnixSeqpacket>,
    /// Where the control socket is registered. Everything that polls it has
    /// to run here, whatever runtime a request happens to arrive on.
    control_rt: tokio::runtime::Handle,
    /// Newest at the back, so dispatch takes the warmest and the sweep walks
    /// from the coldest.
    idle: StdMutex<VecDeque<PooledWorker>>,
    /// Raised whenever a worker is parked, so a caller waiting at
    /// `processes.max` hears about it rather than polling for it.
    worker_returned: Notify,
    /// Caps in-flight requests. `Arc` so a permit can outlive the dispatch
    /// that took it, as far as the response's own task.
    semaphore: Arc<Semaphore>,
    /// Caps how many workers exist. Taken before the fork and held by the
    /// worker's `WorkerMeta` until it is gone, so every exit frees a seat
    /// without anyone having to remember to.
    admission: Arc<Semaphore>,
    max_workers: usize,
    request_timeout: Duration,
    queue_timeout: Duration,
    /// Bounds the wait on the prototype. `queue_timeout` covers only
    /// acquiring a permit, so without this a wedged prototype hangs every
    /// dispatch forever.
    spawn_timeout: Duration,
    /// 0 disables the hard cap.
    queue_max_depth: usize,
    queue_depth: Gauge,
    started_at: Instant,
    /// `php.targets` names, kept sorted for a stable `/status`.
    target_names: Vec<String>,
    pub(crate) counters: Counters,
    prototype_child: StdMutex<PrototypeHandle>,
    prototype_spec: prototype_launch::PrototypeSpec,
    /// tokio mutex: the guard is held across an `.await`.
    respawn_backoff: Mutex<RespawnBackoff>,
    /// Presence doubles as "still one of ours". Off the request path, which
    /// reaches a worker's own counters through `WorkerMeta` instead.
    workers: StdMutex<HashMap<u32, Arc<WorkerMeta>>>,
}

/// Monotonic `/status` counters, all `Relaxed`: nothing reads one to decide
/// anything, so no ordering is owed to any other field.
#[derive(Default)]
pub(crate) struct Counters {
    requests_total: AtomicU64,
    watchdog_kills: AtomicU64,
    queue_timeouts: AtomicU64,
    dispatch_failed: AtomicU64,
    /// Refused for not fitting a request-ring frame. The head cap is meant to
    /// make that unreachable, so anything but zero says a configured path is
    /// longer than the ring was sized for.
    pub(crate) requests_too_large: AtomicU64,
    workers_spawned: AtomicU64,
    /// Clean retirement, as distinct from `dispatch_failed`.
    recycled_request_limit: AtomicU64,
    recycled_idle_timeout: AtomicU64,
    /// Non-zero means something else failed to release a worker.
    workers_reaped_dead: AtomicU64,
    /// Clients that went away while a worker was checked out.
    workers_abandoned: AtomicU64,
    prototype_respawns: AtomicU64,
    crash_loop_backoffs: AtomicU64,
}

#[derive(Default)]
struct RespawnBackoff {
    last_attempt: Option<Instant>,
    consecutive_failures: u32,
}

/// Carried together so the hot path never looks a worker up by pid.
pub(crate) struct PooledWorker {
    pub(crate) channel: WorkerChannel,
    pub(crate) pid: u32,
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
    /// The worker's seat in the pool, given up when this is dropped - which
    /// is once neither `workers` nor any `PooledWorker` holds it any more.
    _admission: OwnedSemaphorePermit,
    state: std::sync::atomic::AtomicU8,
    request_count: std::sync::atomic::AtomicU32,
    started_at: Instant,
    last_active_ms: AtomicU64,
}

impl WorkerMeta {
    fn new(admission: OwnedSemaphorePermit, now: Instant, pool_started: Instant) -> Self {
        WorkerMeta {
            _admission: admission,
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

/// The prototype's pid and everything that may be done to it.
///
/// A pid rather than a `std::process::Child`: master reaps through
/// `waitpid(-1)`, which a `Child::wait` would race for the status.
///
/// An unreaped pid cannot be recycled, so signalling it stays safe until
/// something reaps it - which is what `reaped` tracks.
struct PrototypeHandle {
    pid: u32,
    reaped: bool,
}

impl PrototypeHandle {
    fn new(pid: u32) -> Self {
        PrototypeHandle { pid, reaped: false }
    }

    fn pid(&self) -> u32 {
        self.pid
    }

    /// `None` once reaped: the only safe answer to what may be signalled.
    fn live_pid(&self) -> Option<u32> {
        (!self.reaped).then_some(self.pid)
    }

    /// Records a reaped child, reporting whether it was the prototype.
    fn note_exit(&mut self, pid: u32, status: WaitStatus) -> bool {
        if self.reaped || pid != self.pid {
            return false;
        }
        self.reaped = true;
        log_prototype_death(pid, status);
        true
    }

    /// Its workers need no separate kill: they follow via `PR_SET_PDEATHSIG`.
    fn kill(&self, context: &str) {
        if let Some(pid) = self.live_pid() {
            sigkill(pid, context);
        }
    }

    /// Kills and reaps whatever it replaces: not every respawn follows a
    /// death, and forgetting a wedged prototype's pid leaves it orphaned with
    /// its whole PHP heap, tracked by nothing.
    ///
    /// Reaps before killing, so a death that already happened keeps the status
    /// that explains it instead of reporting SIGKILL.
    fn replace(&mut self, new_pid: u32) {
        if let Some(pid) = self.live_pid() {
            match waitpid(Pid::from_raw(pid as i32), Some(WaitPidFlag::WNOHANG)) {
                Ok(status @ (WaitStatus::Exited(..) | WaitStatus::Signaled(..))) => {
                    log_prototype_death(pid, status);
                }
                // Reaped by something else, so the pid may name an unrelated
                // process by now.
                Err(Errno::ECHILD) => {}
                // A failed wait included: an unkilled prototype is the one
                // outcome that leaks a PHP heap.
                _ => {
                    sigkill(pid, "replacing a prototype that is still running");
                    let _ = waitpid(Pid::from_raw(pid as i32), None);
                }
            }
        }
        *self = PrototypeHandle::new(new_pid);
    }
}

/// Exponential: a bad php-mod path or config will not fix itself by being
/// retried faster.
fn respawn_backoff_delay(consecutive_failures: u32) -> Duration {
    Duration::from_secs((1u64 << consecutive_failures.min(6)).min(60))
}

/// Why a worker is leaving the pool.
#[derive(Clone, Copy)]
pub(crate) enum Retired {
    /// Retired itself after `processes.idle_timeout`.
    IdleTimeout,
    /// Retired itself after `limits.requests`.
    RequestLimit,
    /// The client went away while the worker was checked out.
    Abandoned,
    /// Overran `limits.timeout`, or broke the response protocol.
    Watchdog,
    /// Its channel failed mid-response.
    Failed,
    /// Would not take a request. Uncounted: only the caller knows whether the
    /// retry that follows went on to succeed.
    Unavailable,
}

impl Retired {
    /// A worker retiring itself exits on its own; anything else is still
    /// running and has to be stopped.
    fn needs_kill(self) -> bool {
        matches!(
            self,
            Retired::Watchdog | Retired::Failed | Retired::Unavailable
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            Retired::IdleTimeout => "idle timeout",
            Retired::RequestLimit => "request limit",
            Retired::Abandoned => "client abandoned the request",
            Retired::Watchdog => "watchdog",
            Retired::Failed => "channel failed",
            Retired::Unavailable => "worker would not take the request",
        }
    }
}

/// SIGKILL rather than SIGTERM: this is only ever reached for a process
/// already known to be wedged or abandoned, which will not shut itself down.
///
/// Logs rather than panics on delivery failure; ESRCH on an already-dead pid
/// is the expected case.
pub(crate) fn sigkill(pid: u32, context: &str) {
    logging::debug!(r#type = "controller", pid, context, "sending SIGKILL");
    if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGKILL) {
        logging::warn!(r#type = "controller", pid, error = %e, "signal delivery failed");
    }
}

fn log_prototype_death(pid: u32, status: WaitStatus) {
    match status {
        WaitStatus::Exited(_, code) => {
            logging::error!(
                r#type = "controller",
                pid,
                exit_code = code,
                "prototype exited"
            )
        }
        WaitStatus::Signaled(_, signal, core_dumped) => logging::error!(
            r#type = "controller",
            pid,
            signal = signal.as_str(),
            core_dumped,
            "prototype was killed by a signal"
        ),
        other => {
            logging::error!(r#type = "controller", pid, status = ?other, "prototype stopped")
        }
    }
}

/// Installed where the dynamic linker already looks, so a bare `dlopen()`
/// finds it; `PROTEUS_PHP_MOD_PATH` overrides with an absolute path.
fn resolve_php_mod_path() -> String {
    std::env::var("PROTEUS_PHP_MOD_PATH").unwrap_or_else(|_| "libproteus-php-mod.so".to_string())
}

/// The pool is at `processes.max`. `WouldBlock` so callers can tell it from
/// the failures that say something about the prototype's health.
fn pool_full_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "worker pool is at processes.max",
    )
}

fn is_pool_full(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::WouldBlock
}

/// How long one sweep may spend returning ring pages. A swept worker is
/// unavailable for the length of a `spawn_blocking` hop, and nothing here is
/// urgent: whatever is still due is due again a tick later.
const RECLAIM_BUDGET: Duration = Duration::from_millis(2);

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

        let spec = prototype_launch::PrototypeSpec {
            config: ProtoConfig {
                php_mod_path: resolve_php_mod_path(),
                max_requests: cfg.php.limits.requests,
                idle_timeout_seconds: cfg.php.processes.idle_timeout,
                options: cfg.php.options.clone(),
                environment: cfg.php.environment.clone(),
            },
            drop_to,
            no_new_privs: cfg.php.no_new_privs,
        };
        let (control, prototype_pid) =
            prototype_launch::spawn(&spec).expect("failed to spawn prototype");
        logging::info!(
            r#type = "controller",
            pid = prototype_pid,
            uid,
            gid,
            dropped = drop_to.is_some(),
            "spawned prototype"
        );

        PoolManager {
            control: Mutex::new(control),
            control_rt: tokio::runtime::Handle::current(),
            idle: StdMutex::new(VecDeque::with_capacity(cfg.php.processes.max)),
            worker_returned: Notify::new(),
            semaphore: Arc::new(Semaphore::new(cfg.php.processes.max)),
            admission: Arc::new(Semaphore::new(cfg.php.processes.max)),
            max_workers: cfg.php.processes.max,
            request_timeout: Duration::from_secs(cfg.php.limits.timeout),
            queue_timeout: Duration::from_secs(cfg.php.queue.timeout),
            spawn_timeout: Duration::from_secs(cfg.php.processes.spawn_timeout),
            queue_max_depth: cfg.php.queue.max_depth,
            queue_depth: Gauge::default(),
            started_at: Instant::now(),
            target_names: {
                let mut names: Vec<String> = cfg.php.targets.keys().cloned().collect();
                names.sort();
                names
            },
            counters: Counters::default(),
            prototype_child: StdMutex::new(PrototypeHandle::new(prototype_pid)),
            prototype_spec: spec,
            respawn_backoff: Mutex::new(RespawnBackoff::default()),
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
                self.counters.crash_loop_backoffs.fetch_add(1, Relaxed);
                return false;
            }
        }
        backoff.last_attempt = Some(now);

        logging::info!(
            r#type = "controller",
            consecutive_failures = backoff.consecutive_failures,
            "attempting to respawn the prototype"
        );
        // fork()+exec() blocks, and this one runs while the pool is live.
        let spec = self.prototype_spec.clone();
        let spawn_result = tokio::task::spawn_blocking(move || prototype_launch::spawn(&spec))
            .await
            .expect("prototype_launch::spawn blocking task panicked");
        match spawn_result {
            Ok((new_control, new_pid)) => {
                *self.control.lock().await = new_control;
                self.prototype_child.lock().unwrap().replace(new_pid);

                backoff.consecutive_failures = 0;
                self.counters.prototype_respawns.fetch_add(1, Relaxed);
                logging::info!(r#type = "controller", pid = new_pid, "prototype respawned");
                true
            }
            Err(e) => {
                backoff.consecutive_failures += 1;
                logging::error!(r#type = "controller", error = %e, "failed to respawn prototype");
                false
            }
        }
    }

    /// Retires the workers that timed themselves out and returns ring pages
    /// the rest have consumed.
    ///
    /// Taking each worker out for the length of its check is what gives
    /// `Ring::reclaim_if_due` the exclusion it needs.
    async fn sweep_idle_workers(&self) {
        // Rotating front to back: taking exactly as many as were parked leaves
        // the survivors in the order they started, and only one worker is out
        // of the pool at a time rather than all of them.
        let rounds = self.idle.lock().unwrap().len();
        let deadline = Instant::now() + RECLAIM_BUDGET;
        let mut reclaimed = 0usize;
        for _ in 0..rounds {
            let Some(worker) = self.idle.lock().unwrap().pop_front() else {
                break;
            };
            if worker.channel.worker_has_exited() {
                self.retire(worker.pid, Retired::IdleTimeout);
                continue;
            }
            if Instant::now() < deadline && worker.channel.reclaim_is_due() {
                let mapped = worker.channel.mapping();
                // fallocate is not guaranteed cheap and cannot safely overlap
                // itself, so this is awaited rather than left detached.
                let _ = tokio::task::spawn_blocking(move || mapped.reclaim_if_due()).await;
                reclaimed += 1;
            }
            // Not a bare push: a caller parked at the ceiling is owed the
            // wakeup, and a sweep is as much a return as a dispatch's is.
            self.return_worker(worker);
        }
        if reclaimed > 0 {
            logging::debug!(
                r#type = "controller",
                workers = reclaimed,
                "reclaimed ring pages from idle workers"
            );
        }
    }

    /// All of master's periodic process management. One task, so a respawn
    /// cannot run concurrently with a spawn against the control socket it is
    /// replacing.
    ///
    /// Polls rather than reacting to each exit: a prototype that dies while
    /// the pool still has spares is otherwise unnoticed until they run out.
    pub async fn maintain_loop(self: Arc<Self>, spare: usize, interval: Duration) {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            if self.reap_children() {
                logging::warn!(
                    r#type = "controller",
                    "prototype exited with no pending worker-spawn attempt to notice it - respawning proactively"
                );
                self.try_respawn_prototype().await;
            }
            self.sweep_idle_workers().await;
            // Seats rather than the worker map: a spawn holds its seat from
            // before the fork, so this cannot start one the pool has no room
            // for while another is still in flight.
            while self.idle.lock().unwrap().len() < spare && self.admission.available_permits() > 0
            {
                match self.spawn_worker().await {
                    Ok(worker) => self.return_worker(worker),
                    Err(e) => {
                        logging::warn!(r#type = "controller", error = %e, "failed to top the pool back up to spare");
                        break;
                    }
                }
            }
        }
    }

    /// Reaps everything `PR_SET_CHILD_SUBREAPER` has left here, reporting
    /// whether the prototype was among them.
    fn reap_children(&self) -> bool {
        // Held for the whole sweep so the pid compared against cannot be
        // replaced halfway through it.
        let mut child_guard = self.prototype_child.lock().unwrap();
        let mut prototype_died = false;
        control::reap_exited_children(|pid, status| {
            prototype_died |= child_guard.note_exit(pid.as_raw() as u32, status);
        });
        prototype_died
    }

    /// Best-effort: fewer spares than asked for beats failing to start.
    pub async fn prespawn_spare(self: &Arc<Self>, spare: usize) {
        logging::info!(r#type = "controller", spare, "pre-spawning spare workers");
        for _ in 0..spare {
            match self.spawn_worker().await {
                Ok(w) => self.return_worker(w),
                Err(e) => {
                    logging::error!(r#type = "controller", error = %e, "failed to pre-spawn a spare worker")
                }
            }
        }
    }

    /// Hops to the control runtime, which owns the control socket's
    /// registration whatever runtime the caller arrived on.
    ///
    /// A failed spawn escalates to a prototype respawn, but a full pool must
    /// not: that would kill a healthy prototype and fail every worker in
    /// flight, only to lose the same race again.
    async fn spawn_worker(self: &Arc<Self>) -> std::io::Result<PooledWorker> {
        let pool = Arc::clone(self);
        let spawned = self.control_rt.spawn(async move {
            match pool.spawn_worker_once().await {
                Ok(w) => Ok(w),
                Err(e) if is_pool_full(&e) => Err(e),
                Err(e) => {
                    logging::warn!(r#type = "controller", error = %e, "worker spawn failed, trying to respawn the prototype");
                    if pool.try_respawn_prototype().await {
                        pool.spawn_worker_once().await
                    } else {
                        Err(e)
                    }
                }
            }
        });
        spawned.await.unwrap_or_else(|e| {
            Err(std::io::Error::other(format!(
                "worker spawn task on the control runtime failed: {e}"
            )))
        })
    }

    async fn spawn_worker_once(&self) -> std::io::Result<PooledWorker> {
        // The only atomic admission gate: `workers.len() < max_workers` is a
        // check-then-act two spawns can pass at once. Dropped on any early
        // return below, so a failed spawn does not cost a seat for good.
        let Ok(admission) = Arc::clone(&self.admission).try_acquire_owned() else {
            return Err(pool_full_error());
        };

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
                logging::error!(
                    r#type = "controller",
                    spawn_timeout = ?self.spawn_timeout,
                    "prototype did not answer a worker-spawn request in time, killing it"
                );
                self.prototype_child
                    .lock()
                    .unwrap()
                    .kill("prototype stopped answering a worker-spawn request");
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

        self.counters.workers_spawned.fetch_add(1, Relaxed);
        // Past every fallible step, so the seat now belongs to a worker that
        // `workers` will account for.
        let meta = Arc::new(WorkerMeta::new(admission, Instant::now(), self.started_at));
        self.track_worker(pid, Arc::clone(&meta));
        logging::debug!(r#type = "controller", pid, "spawned worker");
        Ok(PooledWorker { channel, pid, meta })
    }

    /// Drops any entry this displaces, giving up its seat with it: the OS
    /// reuses pids, and a worker whose pid came round again before it was
    /// reaped would otherwise hold a seat for good.
    fn track_worker(&self, pid: u32, meta: Arc<WorkerMeta>) {
        // Bound first: as the scrutinee of an `if let`, the guard would live
        // for the whole arm and put the log call under the map lock.
        let displaced = self.workers.lock().unwrap().insert(pid, meta);
        if displaced.is_some() {
            // Gone for certain: the kernel does not hand out a live pid.
            logging::warn!(
                r#type = "controller",
                pid,
                "reusing the pid of a worker that was never reaped"
            );
            self.counters.workers_reaped_dead.fetch_add(1, Relaxed);
        }
    }

    /// The one way a worker leaves the pool. Its seat comes back when the
    /// last reference to its `WorkerMeta` goes, which is why a caller still
    /// holding the `PooledWorker` need not do anything else.
    pub(crate) fn retire(&self, pid: u32, why: Retired) {
        let counter = match why {
            Retired::IdleTimeout => Some(&self.counters.recycled_idle_timeout),
            Retired::RequestLimit => Some(&self.counters.recycled_request_limit),
            Retired::Abandoned => Some(&self.counters.workers_abandoned),
            Retired::Watchdog => Some(&self.counters.watchdog_kills),
            Retired::Failed => Some(&self.counters.dispatch_failed),
            Retired::Unavailable => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Relaxed);
        }
        if why.needs_kill() {
            sigkill(pid, why.as_str());
        } else {
            logging::debug!(
                r#type = "controller",
                pid,
                reason = why.as_str(),
                "retiring worker"
            );
        }
        self.workers.lock().unwrap().remove(&pid);
    }

    /// An idle worker, or a freshly spawned one.
    async fn get_worker(self: &Arc<Self>) -> std::io::Result<PooledWorker> {
        // A caller holding a permit with the pool at `processes.max` is owed a
        // worker that is merely busy or mid-return, so this waits for one
        // rather than failing the request.
        const POOL_FULL_WAIT: Duration = Duration::from_secs(1);
        let deadline = tokio::time::Instant::now() + POOL_FULL_WAIT;

        loop {
            let returned = self.worker_returned.notified();
            tokio::pin!(returned);

            // A worker can retire itself while still parked here.
            while let Some(worker) = self.take_idle() {
                if !worker.channel.worker_has_exited() {
                    return Ok(worker);
                }
                self.retire(worker.pid, Retired::IdleTimeout);
            }
            match self.spawn_worker().await {
                Err(e) if is_pool_full(&e) => {}
                other => return other,
            }
            if tokio::time::timeout_at(deadline, returned).await.is_err() {
                return Err(pool_full_error());
            }
        }
    }

    /// The warmest parked worker: its heap and OPcache are the least cold,
    /// and leaving the rest alone is what lets each reach its own idle
    /// timeout instead of being cycled evenly.
    fn take_idle(&self) -> Option<PooledWorker> {
        self.idle.lock().unwrap().pop_back()
    }

    fn return_worker(&self, worker: PooledWorker) {
        self.idle.lock().unwrap().push_back(worker);
        self.worker_returned.notify_one();
    }

    pub fn status_json(&self) -> serde_json::Value {
        let idle_count = self.idle.lock().unwrap().len();
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
                    "depth": self.queue_depth.get(),
                    "max_depth": self.queue_max_depth,
                },
                "counters": {
                    "requests_total": self.counters.requests_total.load(Relaxed),
                    "requests_failed": self.counters.dispatch_failed.load(Relaxed),
                    "requests_too_large": self.counters.requests_too_large.load(Relaxed),
                    "watchdog_kills": self.counters.watchdog_kills.load(Relaxed),
                    "queue_timeouts": self.counters.queue_timeouts.load(Relaxed),
                    "workers_spawned_total": self.counters.workers_spawned.load(Relaxed),
                    "recycled_request_limit": self.counters.recycled_request_limit.load(Relaxed),
                    "recycled_idle_timeout": self.counters.recycled_idle_timeout.load(Relaxed),
                    "workers_reaped_dead": self.counters.workers_reaped_dead.load(Relaxed),
                    "workers_abandoned": self.counters.workers_abandoned.load(Relaxed),
                    "prototype_respawns_total": self.counters.prototype_respawns.load(Relaxed),
                    "crash_loop_backoffs": self.counters.crash_loop_backoffs.load(Relaxed),
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
