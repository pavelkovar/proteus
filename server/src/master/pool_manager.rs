//! Prototype lifecycle and the worker pool: how the pool stays populated,
//! not what happens to one request.
//!
//! Master alone decides idle retirement, in `sweep_idle_workers`: it knows
//! both how long a worker has sat idle (`WorkerMeta`) and whether the pool
//! can afford to lose it (the `spare` floor) - a worker parked idle just waits.

mod dispatch;
mod prototype;
mod worker_channel;

use crate::config::Config;
use crate::ipc::control;
use crate::logging;
use crate::prototype::ProtoConfig;
use crate::utils::gauge::Gauge;
pub(crate) use dispatch::{BodyStream, DispatchOutcome};
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use prototype::Handle;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{
    AtomicBool, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_seqpacket::UnixSeqpacket;
use worker_channel::WorkerChannel;

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
    /// Dup of `control` for `KILL`s: one seqpacket message needs no lock and
    /// no `.await`, so `retire` and `Drop` can send it directly.
    kill_channel: StdMutex<Option<std::os::fd::OwnedFd>>,
    /// Lets concurrent spawns that failed on the same dead prototype reuse one respawn.
    prototype_generation: AtomicU64,
    /// Where the control socket is registered. Everything that polls it has
    /// to run here, whatever runtime a request happens to arrive on.
    control_rt: tokio::runtime::Handle,
    /// Newest at the back, so dispatch takes the warmest and the sweep walks
    /// from the coldest.
    idle: StdMutex<VecDeque<PooledWorker>>,
    /// Raised whenever a worker is parked, so a caller waiting at
    /// `processes.max` hears about it rather than polling for it.
    worker_returned: Notify,
    /// Wakes a settled `maintain_loop`: pool activity, or a worker's exit.
    maintenance: Arc<Notify>,
    /// Set by activity, cleared by each maintenance pass.
    active: AtomicBool,
    /// Caps in-flight requests. `Arc` so a permit can outlive the dispatch
    /// that took it, as far as the response's own task.
    semaphore: Arc<Semaphore>,
    /// Caps how many workers exist. Taken before the fork and held by the
    /// worker's `WorkerMeta` until it is gone, so every exit frees a seat
    /// without anyone having to remember to.
    admission: Arc<Semaphore>,
    max_workers: usize,
    /// `None` disables the watchdog; a request may then run indefinitely.
    request_timeout: Option<Duration>,
    /// `None` disables idle retirement; `sweep_idle_workers` never kills for it.
    idle_timeout: Option<Duration>,
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
    prototype_child: StdMutex<Handle>,
    prototype_spec: prototype::Spec,
    /// tokio mutex: the guard is held across an `.await`.
    respawn_backoff: Mutex<RespawnBackoff>,
    /// Presence doubles as "still one of ours". Off the request path, which
    /// reaches a worker's own counters through `WorkerMeta` instead.
    workers: StdMutex<HashMap<WorkerId, Arc<WorkerMeta>>>,
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
    /// Killed by master, unlike `recycled_request_limit` above - the worker
    /// was not given a chance to exit on its own.
    recycled_idle_timeout: AtomicU64,
    /// Found already exited while idle - a crash or an external kill.
    workers_vanished_idle: AtomicU64,
    /// Clients that went away while a worker was checked out.
    workers_abandoned: AtomicU64,
    prototype_respawns: AtomicU64,
    crash_loop_backoffs: AtomicU64,
    spawns_refused: AtomicU64,
}

#[derive(Default)]
struct RespawnBackoff {
    last_attempt: Option<Instant>,
    consecutive_failures: u32,
}

/// Identifies a worker for as long as the process runs. Never reused, unlike
/// the pid the kernel can hand to an unrelated process the moment this
/// worker is reaped.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct WorkerId(u64);

impl WorkerId {
    /// Unique process-wide, not just within one `PoolManager`.
    fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        WorkerId(NEXT.fetch_add(1, Relaxed))
    }
}

/// Carried together so the hot path never needs the `workers` map.
pub(crate) struct PooledWorker {
    pub(crate) channel: WorkerChannel,
    pub(crate) pid: u32,
    pub(crate) id: WorkerId,
    pub(crate) meta: Arc<WorkerMeta>,
}

/// A worker's id and pid, read together so nothing downstream can pair one
/// worker's id with another's pid.
#[derive(Clone, Copy)]
pub(crate) struct WorkerRef {
    pub(crate) id: WorkerId,
    pub(crate) pid: u32,
}

impl PooledWorker {
    pub(crate) fn as_ref(&self) -> WorkerRef {
        WorkerRef {
            id: self.id,
            pid: self.pid,
        }
    }
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
    /// Display only - `workers` is keyed by `WorkerId`, not this.
    pid: u32,
    state: std::sync::atomic::AtomicU8,
    request_count: std::sync::atomic::AtomicU32,
    started_at: Instant,
    last_active_ms: AtomicU64,
    /// `Some` only while busy. Exactly one task owns a worker at a time, so
    /// the only other contender for this lock is a rare `/status` read.
    current_request: StdMutex<Option<Arc<crate::ipc::data::PhpRequest<'static>>>>,
}

impl WorkerMeta {
    fn new(admission: OwnedSemaphorePermit, now: Instant, pool_started: Instant, pid: u32) -> Self {
        WorkerMeta {
            _admission: admission,
            pid,
            state: std::sync::atomic::AtomicU8::new(STATE_IDLE),
            request_count: std::sync::atomic::AtomicU32::new(0),
            started_at: now,
            last_active_ms: AtomicU64::new(now.duration_since(pool_started).as_millis() as u64),
            current_request: StdMutex::new(None),
        }
    }

    fn mark_busy(
        &self,
        at: Instant,
        pool_started: Instant,
        req: Arc<crate::ipc::data::PhpRequest<'static>>,
    ) {
        let previous = self.current_request.lock().unwrap().replace(req);
        self.state.store(STATE_BUSY, Relaxed);
        self.request_count.fetch_add(1, Relaxed);
        self.last_active_ms
            .store(at.duration_since(pool_started).as_millis() as u64, Relaxed);
        drop(previous);
    }

    fn mark_idle(&self, at: Instant, pool_started: Instant) {
        // Bound so the stale `Arc` - potentially the request's last
        // reference - drops after the guard, not under the lock.
        let previous = self.current_request.lock().unwrap().take();
        self.state.store(STATE_IDLE, Relaxed);
        self.last_active_ms
            .store(at.duration_since(pool_started).as_millis() as u64, Relaxed);
        drop(previous);
    }

    /// How long since this worker last went idle (or was spawned, if never
    /// claimed since).
    fn idle_for(&self, now: Instant, pool_started: Instant) -> Duration {
        let now_ms = now.duration_since(pool_started).as_millis() as u64;
        Duration::from_millis(now_ms.saturating_sub(self.last_active_ms.load(Relaxed)))
    }

    fn state_str(&self) -> &'static str {
        match self.state.load(Relaxed) {
            STATE_BUSY => "busy",
            _ => "idle",
        }
    }

    fn current_request_json(&self) -> serde_json::Value {
        match self.current_request.lock().unwrap().as_deref() {
            Some(req) => serde_json::json!({
                "method": req.method,
                "uri": req.uri,
                "script": req.script_path,
            }),
            None => serde_json::Value::Null,
        }
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
    /// Killed by master for sitting idle past `processes.idle_timeout`,
    /// beyond what `spare` needs kept warm.
    IdleTimeout,
    /// Retired itself after `limits.requests`.
    RequestLimit,
    /// The client went away while the worker was checked out.
    Abandoned,
    /// Overran `limits.timeout`, or broke the response protocol.
    Watchdog,
    /// Its channel failed mid-response.
    Failed,
    /// Found already exited while sitting idle - a crash or an external
    /// kill, not a retirement this server chose.
    Vanished,
    /// Would not take a request. Uncounted: only the caller knows whether the
    /// retry that follows went on to succeed.
    Unavailable,
}

impl Retired {
    /// A worker that already exited on its own needs no kill; every other
    /// reason is still running (or, for `IdleTimeout`, blocked idle) and has
    /// to be stopped.
    fn needs_kill(self) -> bool {
        matches!(
            self,
            Retired::IdleTimeout | Retired::Watchdog | Retired::Failed | Retired::Unavailable
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            Retired::IdleTimeout => "idle timeout",
            Retired::RequestLimit => "request limit",
            Retired::Abandoned => "client abandoned the request",
            Retired::Watchdog => "watchdog",
            Retired::Failed => "channel failed",
            Retired::Vanished => "found already exited while idle",
            Retired::Unavailable => "worker would not take the request",
        }
    }
}

/// For a wedged or abandoned prototype; workers go through
/// `PoolManager::kill_worker`. ESRCH on an already-dead pid is expected.
pub(crate) fn sigkill(pid: u32, context: &str) {
    logging::warn!(r#type = "controller", pid, context, "sending SIGKILL");
    if let Err(e) =
        crate::utils::process::kill_tolerating_esrch(Pid::from_raw(pid as i32), Signal::SIGKILL)
    {
        logging::warn!(r#type = "controller", pid, error = %e, "signal delivery failed");
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

fn dup_control(control: &UnixSeqpacket) -> Option<std::os::fd::OwnedFd> {
    use std::os::fd::AsFd;
    control
        .as_fd()
        .try_clone_to_owned()
        .inspect_err(|e| {
            logging::error!(r#type = "controller", error = %e, "could not duplicate the control socket, workers cannot be killed")
        })
        .ok()
}

/// A spawn's result on its way to a caller that may be cancelled first: a
/// worker already sent is returned to the pool here, one sent later by the
/// spawning task once it sees the channel closed.
struct SpawnReply {
    rx: tokio::sync::oneshot::Receiver<std::io::Result<PooledWorker>>,
    pool: Arc<PoolManager>,
}

impl Drop for SpawnReply {
    fn drop(&mut self) {
        self.rx.close();
        if let Ok(Ok(worker)) = self.rx.try_recv() {
            self.pool.return_worker(worker);
        }
    }
}

/// Only `Control` justifies a respawn: it kills every running worker
/// (PDEATHSIG), which fixes nothing when the prototype itself is fine.
enum SpawnFailure {
    PoolFull,
    /// Out of fds, pids or memory in the prototype.
    Refused(std::io::Error),
    /// Master could not set up its side of a spawned worker.
    Local(std::io::Error),
    Control(std::io::Error),
}

impl SpawnFailure {
    fn into_io(self) -> std::io::Error {
        match self {
            SpawnFailure::PoolFull => pool_full_error(),
            SpawnFailure::Refused(e) | SpawnFailure::Local(e) | SpawnFailure::Control(e) => e,
        }
    }
}

/// How long one sweep may spend returning ring pages. A swept worker is
/// unavailable for the length of a `spawn_blocking` hop, and nothing here is
/// urgent: whatever is still due is due again a tick later.
const RECLAIM_BUDGET: Duration = Duration::from_millis(2);

/// A settled pool's pass anyway, in case a wakeup was missed.
const SETTLED_RECHECK: Duration = Duration::from_secs(60);

impl PoolManager {
    pub fn spawn_prototype(cfg: &Config) -> Self {
        // config::validate guarantees these are set together or not at all.
        let drop_to = match (&cfg.php.user, &cfg.php.group) {
            (Some(user), Some(group)) => Some((
                crate::utils::process::resolve_user(user).uid.as_raw(),
                crate::utils::process::resolve_group(group).gid.as_raw(),
            )),
            _ => None,
        };
        let (uid, gid) = drop_to.unwrap_or_else(|| {
            (
                nix::unistd::getuid().as_raw(),
                nix::unistd::getgid().as_raw(),
            )
        });

        let spec = prototype::Spec {
            config: ProtoConfig {
                php_mod_path: resolve_php_mod_path(),
                max_requests: cfg.php.limits.requests,
                options: cfg.php.options.clone(),
                environment: cfg.php.environment.clone(),
                log_level: cfg.log_level.as_level(),
            },
            drop_to,
            no_new_privs: cfg.php.no_new_privs,
        };
        let (control, prototype_pid) = prototype::spawn(&spec).expect("failed to spawn prototype");
        logging::info!(
            r#type = "controller",
            pid = prototype_pid,
            uid,
            gid,
            dropped = drop_to.is_some(),
            "spawned prototype"
        );

        PoolManager {
            kill_channel: StdMutex::new(dup_control(&control)),
            prototype_generation: AtomicU64::new(0),
            control: Mutex::new(control),
            control_rt: tokio::runtime::Handle::current(),
            idle: StdMutex::new(VecDeque::with_capacity(cfg.php.processes.max)),
            worker_returned: Notify::new(),
            maintenance: Arc::new(Notify::new()),
            active: AtomicBool::new(true),
            semaphore: Arc::new(Semaphore::new(cfg.php.processes.max)),
            admission: Arc::new(Semaphore::new(cfg.php.processes.max)),
            max_workers: cfg.php.processes.max,
            request_timeout: (cfg.php.limits.timeout > 0)
                .then(|| Duration::from_secs(cfg.php.limits.timeout)),
            idle_timeout: (cfg.php.processes.idle_timeout > 0)
                .then(|| Duration::from_secs(cfg.php.processes.idle_timeout)),
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
            prototype_child: StdMutex::new(Handle::new(prototype_pid)),
            prototype_spec: spec,
            respawn_backoff: Mutex::new(RespawnBackoff::default()),
            workers: StdMutex::new(HashMap::new()),
        }
    }

    fn prototype_generation(&self) -> u64 {
        self.prototype_generation.load(Acquire)
    }

    /// Rate-limited by `respawn_backoff_delay`. Returns whether a prototype
    /// newer than `observed_generation` is in place, so the spawns queued
    /// behind the one that respawned are not refused by its backoff window.
    async fn try_respawn_prototype(&self, observed_generation: u64) -> bool {
        let mut backoff = self.respawn_backoff.lock().await;
        if self.prototype_generation() != observed_generation {
            return true;
        }
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
        let spawn_result = tokio::task::spawn_blocking(move || prototype::spawn(&spec))
            .await
            .expect("prototype::spawn blocking task panicked");
        match spawn_result {
            Ok((new_control, new_pid)) => {
                *self.kill_channel.lock().unwrap() = dup_control(&new_control);
                *self.control.lock().await = new_control;
                self.prototype_child.lock().unwrap().replace(new_pid);
                self.prototype_generation.fetch_add(1, Release);

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

    /// Retires idle workers past `processes.idle_timeout` beyond what `spare`
    /// needs kept warm, reaps ones found already gone, and returns ring pages
    /// the rest have consumed.
    ///
    /// Taking each worker out for the length of its check is what gives
    /// `Ring::reclaim_if_due` the exclusion it needs.
    async fn sweep_idle_workers(&self, spare: usize) {
        // Rotating front to back: taking exactly as many as were parked leaves
        // the survivors in the order they started, and only one worker is out
        // of the pool at a time rather than all of them.
        //
        // The front holds the coldest workers (see `take_idle`), so a sweep
        // that only ever pops the front and re-queues survivors at the back
        // reaches every worker exactly once per full rotation.
        let rounds = self.idle.lock().unwrap().len();
        let idle_timeout = self.idle_timeout;
        let deadline = Instant::now() + RECLAIM_BUDGET;
        let mut reclaimed = 0usize;
        for _ in 0..rounds {
            let Some(worker) = self.idle.lock().unwrap().pop_front() else {
                break;
            };
            if worker.channel.worker_has_exited() {
                self.retire(worker.as_ref(), Retired::Vanished);
                continue;
            }
            let now = Instant::now();
            // Re-checked live rather than against a headcount taken before the
            // loop: a concurrent `take_idle` shrinking the pool mid-sweep must
            // still leave `spare` behind, not whatever the count was at entry.
            if let Some(idle_timeout) = idle_timeout
                && worker.meta.idle_for(now, self.started_at) >= idle_timeout
                && self.idle.lock().unwrap().len() >= spare
            {
                self.retire(worker.as_ref(), Retired::IdleTimeout);
                continue;
            }
            if now < deadline && worker.channel.reclaim_is_due() {
                let mapped = worker.channel.mapping();
                // fallocate is not guaranteed cheap and cannot safely overlap
                // itself, so this is awaited rather than left detached.
                let _ = tokio::task::spawn_blocking(move || mapped.reclaim_if_due()).await;
                reclaimed += 1;
            }
            // Not a bare push: a caller parked at the ceiling is owed the
            // wakeup. Not `return_worker` either: that counts as activity.
            self.idle.lock().unwrap().push_back(worker);
            self.worker_returned.notify_one();
        }
        if reclaimed > 0 {
            logging::debug!(
                r#type = "controller",
                workers = reclaimed,
                "reclaimed ring pages from idle workers"
            );
        }
    }

    /// A load, and a write only on the idle-to-active edge, so a busy pool pays
    /// no shared-line write per request.
    fn note_activity(&self) {
        if !self.active.load(Relaxed) && !self.active.swap(true, Relaxed) {
            self.maintenance.notify_one();
        }
    }

    /// When a pass would find nothing to do until then: no activity since the
    /// last one, spares topped up, nothing to reclaim or reap. `None` while
    /// there is.
    fn settled_until(&self, spare: usize, now: Instant) -> Option<Instant> {
        if self.active.load(Relaxed) {
            return None;
        }
        let idle = self.idle.lock().unwrap();
        if idle.len() < spare && self.admission.available_permits() > 0 {
            return None;
        }
        if idle
            .iter()
            .any(|w| w.channel.reclaim_is_due() || w.channel.worker_has_exited())
        {
            return None;
        }
        let mut until = now + SETTLED_RECHECK;
        if let Some(idle_timeout) = self.idle_timeout
            && idle.len() > spare
        {
            for w in idle.iter() {
                let idle_for = w.meta.idle_for(now, self.started_at);
                until = until.min(now + idle_timeout.saturating_sub(idle_for));
            }
        }
        Some(until)
    }

    /// One task, so a respawn never races a spawn on the socket it replaces. Runs
    /// every `interval` while the pool is busy; once settled, only after activity,
    /// a worker's or the prototype's exit, or at the next idle timeout.
    pub async fn maintain_loop(self: Arc<Self>, spare: usize, interval: Duration) {
        let mut sigchld = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
        {
            Ok(s) => Some(s),
            Err(e) => {
                logging::warn!(r#type = "controller", error = %e, "no SIGCHLD handler, pool maintenance keeps polling");
                None
            }
        };
        loop {
            self.active.store(false, Relaxed);
            if self.reap_children() {
                logging::warn!(
                    r#type = "controller",
                    "prototype exited with no pending worker-spawn attempt to notice it - respawning proactively"
                );
                self.try_respawn_prototype(self.prototype_generation())
                    .await;
            }
            self.sweep_idle_workers(spare).await;
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
            let now = Instant::now();
            // Only a settled pool listens for activity: a busy one would
            // otherwise run a pass per request instead of per `interval`.
            let (wake_at, settled) = match (self.settled_until(spare, now), &sigchld) {
                (Some(until), Some(_)) => (until, true),
                _ => (now + interval, false),
            };
            let activity = async {
                if settled {
                    self.maintenance.notified().await
                } else {
                    std::future::pending().await
                }
            };
            let child_exit = async {
                match sigchld.as_mut() {
                    Some(s) => {
                        s.recv().await;
                    }
                    None => std::future::pending().await,
                }
            };
            let woken = tokio::select! {
                () = tokio::time::sleep_until(wake_at.into()) => false,
                () = activity => true,
                () = child_exit => true,
            };
            // A pass one `interval` after the event, as the fixed tick had it:
            // an immediate one would top up for a worker that is about to be
            // returned, or respawn under a request already doing so.
            if woken {
                tokio::time::sleep(interval).await;
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

    /// On the control runtime, which owns the control socket. Only a broken control
    /// channel respawns the prototype, never a full pool. Outlives a cancelled
    /// caller, whose worker then goes to the pool instead of holding a seat.
    async fn spawn_worker(self: &Arc<Self>) -> std::io::Result<PooledWorker> {
        let pool = Arc::clone(self);
        let (reply, spawned) = tokio::sync::oneshot::channel();
        self.control_rt.spawn(async move {
            // Before the attempt, so a respawn landing meanwhile is reused.
            let generation = pool.prototype_generation();
            let result = match pool.spawn_worker_once().await {
                Ok(w) => Ok(w),
                Err(SpawnFailure::Control(e)) => {
                    logging::warn!(r#type = "controller", error = %e, "worker spawn failed, trying to respawn the prototype");
                    if pool.try_respawn_prototype(generation).await {
                        pool.spawn_worker_once().await.map_err(SpawnFailure::into_io)
                    } else {
                        Err(e)
                    }
                }
                Err(SpawnFailure::Refused(e)) => {
                    pool.counters.spawns_refused.fetch_add(1, Relaxed);
                    logging::warn!(r#type = "controller", error = %e, "the prototype could not make a worker, leaving it running");
                    Err(e)
                }
                Err(other) => Err(other.into_io()),
            };
            if let Err(Ok(worker)) = reply.send(result) {
                pool.return_worker(worker);
            }
        });
        let mut spawned = SpawnReply {
            rx: spawned,
            pool: Arc::clone(self),
        };
        (&mut spawned.rx).await.unwrap_or_else(|_| {
            Err(std::io::Error::other(
                "worker spawn task on the control runtime failed",
            ))
        })
    }

    async fn spawn_worker_once(&self) -> Result<PooledWorker, SpawnFailure> {
        // The only atomic admission gate: `workers.len() < max_workers` is a
        // check-then-act two spawns can pass at once. Dropped on any early
        // return below, so a failed spawn does not cost a seat for good.
        let Ok(admission) = Arc::clone(&self.admission).try_acquire_owned() else {
            return Err(SpawnFailure::PoolFull);
        };

        let control = self.control.lock().await;
        let request =
            tokio::time::timeout(self.spawn_timeout, control::request_worker(&control)).await;
        drop(control);
        let (fds, pid) = match request {
            Ok(Ok(reply)) => reply,
            Ok(Err(e)) if control::is_spawn_refused(&e) => return Err(SpawnFailure::Refused(e)),
            Ok(Err(e)) => return Err(SpawnFailure::Control(e)),
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
                return Err(SpawnFailure::Control(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "prototype did not answer a worker-spawn request in time",
                )));
            }
        };

        // The worker is already forked and parked on its rings, and nothing
        // else knows its pid yet, so failing to kill it here leaks it
        // permanently.
        let channel = WorkerChannel::new(fds, pid, Some(Arc::clone(&self.maintenance)))
            .inspect_err(|_| {
                self.kill_worker(pid, "failed to set up the worker channel after the fork");
            })
            .map_err(SpawnFailure::Local)?;

        self.counters.workers_spawned.fetch_add(1, Relaxed);
        // Past every fallible step, so the seat now belongs to a worker that
        // `workers` will account for.
        let meta = Arc::new(WorkerMeta::new(
            admission,
            Instant::now(),
            self.started_at,
            pid,
        ));
        let id = WorkerId::next();
        self.track_worker(id, Arc::clone(&meta));
        logging::debug!(r#type = "controller", pid, "spawned worker");
        Ok(PooledWorker {
            channel,
            pid,
            id,
            meta,
        })
    }

    fn track_worker(&self, id: WorkerId, meta: Arc<WorkerMeta>) {
        self.workers.lock().unwrap().insert(id, meta);
    }

    /// The one way a worker leaves the pool. Its seat comes back when the
    /// last reference to its `WorkerMeta` goes, which is why a caller still
    /// holding the `PooledWorker` need not do anything else.
    pub(crate) fn retire(&self, who: WorkerRef, why: Retired) {
        self.note_activity();
        let counter = match why {
            Retired::IdleTimeout => Some(&self.counters.recycled_idle_timeout),
            Retired::RequestLimit => Some(&self.counters.recycled_request_limit),
            Retired::Abandoned => Some(&self.counters.workers_abandoned),
            Retired::Watchdog => Some(&self.counters.watchdog_kills),
            Retired::Failed => Some(&self.counters.dispatch_failed),
            Retired::Vanished => Some(&self.counters.workers_vanished_idle),
            Retired::Unavailable => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Relaxed);
        }
        if why.needs_kill() {
            self.kill_worker(who.pid, why.as_str());
        } else {
            logging::debug!(
                r#type = "controller",
                pid = who.pid,
                reason = why.as_str(),
                "retiring worker"
            );
        }
        self.workers.lock().unwrap().remove(&who.id);
    }

    /// Via the prototype: only the parent knows the pid was not already reaped
    /// and reused. An unreachable prototype is dead and PDEATHSIG does the job.
    pub(crate) fn kill_worker(&self, pid: u32, context: &str) {
        use nix::sys::socket::{MsgFlags, send};
        use std::os::fd::AsRawFd;
        logging::warn!(r#type = "controller", pid, context, "killing worker");
        let channel = self.kill_channel.lock().unwrap();
        let Some(fd) = channel.as_ref() else {
            return;
        };
        let msg = control::kill_command(pid);
        if let Err(e) = send(
            fd.as_raw_fd(),
            &msg,
            MsgFlags::MSG_DONTWAIT | MsgFlags::MSG_NOSIGNAL,
        ) {
            logging::warn!(r#type = "controller", pid, error = %e, "could not ask the prototype to kill a worker");
        }
    }

    /// An idle worker, or a freshly spawned one.
    async fn get_worker(self: &Arc<Self>) -> std::io::Result<PooledWorker> {
        // A caller holding a permit with the pool at `processes.max` is owed a
        // worker that is merely busy or mid-return, so this waits for one
        // rather than failing the request.
        const POOL_FULL_WAIT: Duration = Duration::from_secs(1);
        let deadline = tokio::time::Instant::now() + POOL_FULL_WAIT;
        let mut refused = None;

        loop {
            let returned = self.worker_returned.notified();
            tokio::pin!(returned);

            // A worker can vanish (crash, external kill) while parked here.
            while let Some(worker) = self.take_idle() {
                if !worker.channel.worker_has_exited() {
                    return Ok(worker);
                }
                self.retire(worker.as_ref(), Retired::Vanished);
            }
            let pool_full = match self.spawn_worker().await {
                Err(e) if is_pool_full(&e) => true,
                // No new worker for now, but a running one may free up soon.
                Err(e)
                    if control::is_spawn_refused(&e)
                        && !self.workers.lock().unwrap().is_empty() =>
                {
                    refused = Some(e);
                    false
                }
                other => return other,
            };
            // A recycled or killed worker frees a seat without returning. Not
            // after a refusal: the seat is free already and would be refused again.
            let seat_freed = async {
                if pool_full {
                    drop(self.admission.acquire().await);
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                () = &mut returned => {}
                () = seat_freed => {}
                () = tokio::time::sleep_until(deadline) => {
                    return Err(refused.unwrap_or_else(pool_full_error));
                }
            }
        }
    }

    /// The warmest parked worker (least cold heap and OPcache; the rest can reach
    /// their idle timeout), preferring one already registered with this runtime.
    fn take_idle(&self) -> Option<PooledWorker> {
        self.note_activity();
        let mut idle = self.idle.lock().unwrap();
        match idle.iter().rposition(|w| w.channel.registered_here()) {
            Some(i) => idle.remove(i),
            None => idle.pop_back(),
        }
    }

    fn return_worker(&self, worker: PooledWorker) {
        self.note_activity();
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
            .values()
            .map(|meta| {
                serde_json::json!({
                    "pid": meta.pid,
                    "state": meta.state_str(),
                    "request_count": meta.request_count.load(Relaxed),
                    "started_ago_seconds": meta.started_at.elapsed().as_secs(),
                    "last_active_ago_seconds": now_ms.saturating_sub(meta.last_active_ms.load(Relaxed)) / 1000,
                    "current_request": meta.current_request_json(),
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
                    "workers_vanished_idle": self.counters.workers_vanished_idle.load(Relaxed),
                    "workers_abandoned": self.counters.workers_abandoned.load(Relaxed),
                    "prototype_respawns_total": self.counters.prototype_respawns.load(Relaxed),
                    "crash_loop_backoffs": self.counters.crash_loop_backoffs.load(Relaxed),
                    "spawns_refused": self.counters.spawns_refused.load(Relaxed),
                },
                "workers": workers,
            }
        })
    }
}

#[cfg(test)]
#[path = "pool_manager_tests.rs"]
mod tests;
