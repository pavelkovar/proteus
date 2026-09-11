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
        tracing::warn!(
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

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    rt.block_on(run_master(config));
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
    master::http::serve(state).await;
}
