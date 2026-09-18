//! One binary, two roles: master by default, prototype when re-exec'd as
//! `--internal-prototype` - which only master ever does. Everything the
//! prototype needs arrives over `CONFIG_FD` rather than argv.

mod config;
mod cron;
mod ipc;
mod logging;
mod master;
mod prototype;
mod utils;
mod worker;

use clap::{CommandFactory, Parser};
use config::Config;
use master::http::AppState;
use master::pool_manager::PoolManager;
use std::sync::Arc;
use utils::fs_cache::FsCache;

pub(crate) const APP_NAME: &str = "proteus";

#[derive(clap::Parser)]
#[command(arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// Path to the JSON config; runs the HTTP+PHP server. Required unless a
    /// subcommand is given instead.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Reads stdin, expands $VAR/${VAR} references, and writes the result to stdout.
    Envsubst,
    /// Runs the jobs in a crontab file, each on its own schedule, until SIGTERM or SIGINT.
    Cron {
        /// User to run jobs as; give together with --group, or omit both to
        /// inherit this process's own identity.
        #[arg(long)]
        user: Option<String>,
        /// Group to run jobs as; give together with --user, or omit both.
        #[arg(long)]
        group: Option<String>,
        /// How long a running job gets after the shutdown signal before SIGKILL.
        #[arg(long, default_value = "15s")]
        shutdown_grace: humantime::Duration,
        /// Path to the crontab file to run.
        crontab: std::path::PathBuf,
    },
}

/// Bounds how long a dead prototype goes unnoticed while the pool still has
/// spares to serve every request, and how long a worker that exited on its
/// own keeps its seat in the pool.
const POOL_MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Per-thread heaps, avoiding the lock contention glibc's malloc sees under
/// concurrent alloc and free. Fork-safe here because the prototype is
/// re-exec'd and so starts single-threaded.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Otherwise a dead prototype's orphaned workers reparent past us to init,
/// which never reaps them - see `PoolManager::maintain_loop`.
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

/// `proteus envsubst < input > output` - the same `${VAR}` expansion `parse`
/// runs before JSON, exposed standalone so an entrypoint can template any
/// file (e.g. a php.ini snippet shared with the PHP CLI) with identical rules.
fn run_envsubst() -> ! {
    use std::io::{Read, Write};
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .unwrap_or_else(|e| panic!("reading stdin: {e}"));
    let output = utils::envsubst::substitute(&input).unwrap_or_else(|e| panic!("{e}"));
    std::io::stdout()
        .write_all(output.as_bytes())
        .unwrap_or_else(|e| panic!("writing stdout: {e}"));
    std::process::exit(0);
}

fn run_cron(
    user: Option<String>,
    group: Option<String>,
    shutdown_grace: std::time::Duration,
    crontab_path: std::path::PathBuf,
) -> ! {
    utils::proctitle::set_title(&format!("{APP_NAME}: cron"));
    logging::init(true);
    enable_child_subreaper();

    let text = std::fs::read_to_string(&crontab_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", crontab_path.display()));
    let jobs = cron::parse(&text).unwrap_or_else(|e| panic!("{}: {e}", crontab_path.display()));
    let identity = cron::Identity::resolve(user.as_deref(), group.as_deref());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    rt.block_on(cron::run(jobs, identity, shutdown_grace));

    logging::flush();
    std::process::exit(0);
}

fn run_server(config_path: std::path::PathBuf) {
    utils::proctitle::set_title(&format!("{APP_NAME}: controller"));

    let config_text = std::fs::read_to_string(&config_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", config_path.display()));
    let mut config: Config =
        config::parse(&config_text).unwrap_or_else(|e| panic!("{}: {e}", config_path.display()));

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
    config::apply_conditions(&mut config);

    logging::set_min_level(config.log_level.as_level());
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

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Raw argv[1] check, ahead of clap: an internal re-exec contract, not a
    // user-facing subcommand.
    if args.get(1).map(String::as_str) == Some(prototype::INTERNAL_PROTOTYPE_ARG) {
        prototype::run();
    }

    let cli = Cli::parse();
    match cli.command {
        Some(Command::Envsubst) => run_envsubst(),
        Some(Command::Cron {
            user,
            group,
            shutdown_grace,
            crontab,
        }) => run_cron(user, group, shutdown_grace.into(), crontab),
        None => match cli.config {
            Some(config_path) => run_server(config_path),
            None => Cli::command()
                .error(
                    clap::error::ErrorKind::MissingRequiredArgument,
                    "--config <CONFIG> is required when no subcommand is given",
                )
                .exit(),
        },
    }
}

async fn run_master(config: Config) {
    let pool = Arc::new(PoolManager::spawn_prototype(&config));
    pool.prespawn_spare(config.php.processes.spare).await;

    tokio::spawn(
        Arc::clone(&pool).maintain_loop(config.php.processes.spare, POOL_MAINTENANCE_INTERVAL),
    );

    let fs_cache = FsCache::new(
        config.fs_cache.max_entries,
        std::time::Duration::from_millis(config.fs_cache.ttl_ms),
    );
    let state = Arc::new(AppState::new(pool, config, fs_cache));

    // `None` where the mask could not be read: the count is still worth having,
    // but pinning to invented ids would land threads on CPUs this process may
    // not run on.
    let cpus: Vec<Option<usize>> = {
        let allowed = utils::cpu::allowed_cpus();
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

    // One runtime per core, each accepting its own reuseport share of the
    // one listening address.
    let (exit_tx, exit) = master::http::Shutdown::channel();
    // All of them before any thread starts: one failing half way through would
    // otherwise leave a process serving on some cores and not others.
    let listen_addr: Arc<str> = Arc::from(state.config.listen.as_str());
    let mut per_core = Vec::with_capacity(cpus.len());
    for _ in &cpus {
        let socket = master::http::reuseport_listener(&state.config.listen)
            .unwrap_or_else(|e| panic!("cannot listen on {}: {e}", state.config.listen));
        per_core.push((socket, Arc::clone(&listen_addr)));
    }

    let mut threads = Vec::with_capacity(cpus.len());
    for (cpu, listener) in cpus.iter().copied().zip(per_core) {
        let state = Arc::clone(&state);
        let shutdown = shutdown.clone();
        let exit = exit.clone();
        let connection_slots = Arc::clone(&connection_slots);
        threads.push(std::thread::spawn(move || {
            if let Some(cpu) = cpu {
                utils::cpu::pin_to_cpu(cpu);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build a serving runtime");
            rt.block_on(master::http::serve_core(
                listener,
                state,
                shutdown,
                exit,
                connection_slots,
            ));
        }));
    }
    logging::info!(r#type = "controller", listen = %state.config.listen, cores = cpus.len(), "listening");

    master::http::serve_control(state, shutdown_tx).await;

    // Only now: until the drain is over, the serving runtimes still own live
    // connections.
    let _ = exit_tx.send(true);
    for t in threads {
        let _ = t.join();
    }
}
