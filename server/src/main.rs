//! One binary, two roles: master by default, prototype when re-exec'd as
//! `--internal-prototype` - which only master ever does. Everything the
//! prototype needs arrives over `CONFIG_FD` rather than argv.

mod config;
mod ipc;
mod logging;
mod master;
mod proctitle;
mod prototype;
mod worker;

use config::Config;
use master::http::{AppState, FsCache};
use master::pool_manager::PoolManager;
use std::sync::Arc;

/// The one source of truth for the name wherever it shows up at runtime.
pub(crate) const APP_NAME: &str = "proteus";

/// Per-thread heaps, avoiding the lock contention glibc's malloc sees under
/// concurrent alloc and free. Fork-safe here because the prototype is
/// re-exec'd and so starts single-threaded.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Otherwise a dead prototype's orphaned workers reparent past us to init,
/// which never reaps them - see `PoolManager::watch_prototype_liveness`.
#[cfg(target_os = "linux")]
fn enable_child_subreaper() {
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) } != 0 {
        logging::warn!(
            r#type = "controller",
            error = %std::io::Error::last_os_error(),
            "prctl(PR_SET_CHILD_SUBREAPER) failed - orphaned workers may outlive a killed prototype"
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn enable_child_subreaper() {}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.get(1).map(String::as_str) == Some(prototype::INTERNAL_PROTOTYPE_ARG) {
        prototype::run();
    }

    proctitle::set_title(&format!("{APP_NAME}: controller"));

    let config_path = args
        .get(1)
        .unwrap_or_else(|| panic!("usage: {APP_NAME} <config.json>"));
    let config_text = std::fs::read_to_string(config_path)
        .unwrap_or_else(|e| panic!("reading {config_path}: {e}"));
    let config: Config =
        config::parse(&config_text).unwrap_or_else(|e| panic!("{config_path}: {e}"));

    let validation_errors = config::validate(&config);
    if !validation_errors.is_empty() {
        for e in &validation_errors {
            eprintln!("[master] invalid config: {e}");
        }
        panic!(
            "invalid config ({} error(s)), see above",
            validation_errors.len()
        );
    }

    logging::init(true);
    enable_child_subreaper();

    // Two is enough: this runtime owns the prototype control socket, the pool's
    // background loops and the status endpoint, none of which are per-request.
    // Connections are accepted and served by the per-core runtimes.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    rt.block_on(run_master(config));

    logging::flush();
}

async fn run_master(config: Config) {
    let pool = Arc::new(PoolManager::spawn_prototype(&config));
    pool.prespawn_spare(config.php.processes.spare).await;

    // Reactive respawn alone cannot notice a dead prototype while the pool
    // still has idle workers to serve all traffic.
    tokio::spawn(Arc::clone(&pool).watch_prototype_liveness(std::time::Duration::from_secs(2)));

    // Scale-down is the worker's own decision; master only holds the floor,
    // since a worker cannot know whether the pool can spare it.
    tokio::spawn(Arc::clone(&pool).maintain_pool_loop(
        config.php.processes.spare,
        std::time::Duration::from_secs(1),
    ));

    let fs_cache = FsCache::new(
        config.fs_cache.max_entries,
        std::time::Duration::from_millis(config.fs_cache.ttl_ms),
    );
    let state = Arc::new(AppState::new(pool, config, fs_cache));

    // `None` where the mask could not be read: the count is still worth having,
    // but pinning to invented ids would land threads on CPUs this process may
    // not run on.
    let cpus: Vec<Option<usize>> = {
        let allowed = master::http::allowed_cpus();
        if allowed.is_empty() {
            let n = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            logging::warn!(
                r#type = "controller",
                cores = n,
                "could not read this process's CPU affinity, serving unpinned"
            );
            vec![None; n]
        } else {
            allowed.into_iter().map(Some).collect()
        }
    };

    // Raced against accept(), so it bounds how long the accept loops run
    // rather than guaranteeing nothing more is taken. The drain that follows
    // waits on requests in flight, not connections.
    let (shutdown_tx, shutdown) = master::http::Shutdown::channel();
    // Shared, so `connection.max` stays a whole-process cap. Zero means no
    // cap, expressed as a huge permit count so the acquire path stays
    // branchless.
    let conn_cap = state.config.connection.max;
    let connection_slots = Arc::new(tokio::sync::Semaphore::new(if conn_cap == 0 {
        tokio::sync::Semaphore::MAX_PERMITS
    } else {
        tokio::sync::Semaphore::MAX_PERMITS.min(conn_cap)
    }));

    // One runtime per core, each accepting on its own share of every address.
    let (exit_tx, exit) = master::http::Shutdown::channel();
    // All of them before any thread starts: one failing half way through would
    // otherwise leave a process serving on some cores and not others.
    let mut per_core: Vec<Vec<(std::net::TcpListener, Arc<str>)>> = Vec::with_capacity(cpus.len());
    for _ in &cpus {
        let mut listeners = Vec::with_capacity(state.config.listen.len());
        for listen in &state.config.listen {
            let socket = master::http::reuseport_listener(listen)
                .unwrap_or_else(|e| panic!("cannot listen on {listen}: {e}"));
            listeners.push((socket, Arc::from(listen.as_str())));
        }
        per_core.push(listeners);
    }

    let mut threads = Vec::with_capacity(cpus.len());
    for (cpu, listeners) in cpus.iter().copied().zip(per_core) {
        let state = Arc::clone(&state);
        let shutdown = shutdown.clone();
        let exit = exit.clone();
        let connection_slots = Arc::clone(&connection_slots);
        threads.push(std::thread::spawn(move || {
            if let Some(cpu) = cpu {
                master::http::pin_to_cpu(cpu);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build a serving runtime");
            rt.block_on(master::http::serve_core(
                listeners,
                state,
                shutdown,
                exit,
                connection_slots,
            ));
        }));
    }
    for listen in &state.config.listen {
        logging::info!(r#type = "controller", %listen, cores = cpus.len(), "listening");
    }

    master::http::serve_control(state, shutdown_tx).await;

    // Only now: until the drain is over, the serving runtimes still own live
    // connections.
    let _ = exit_tx.send(true);
    for t in threads {
        let _ = t.join();
    }
}
