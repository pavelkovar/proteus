//! JSON configuration schema.

use crate::utils::envsubst;
use crate::utils::match_pattern::{MatchPattern, matches_any};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A JSON parse error's line/column still points at the real config file.
pub fn parse(text: &str) -> Result<Config, String> {
    let substituted = envsubst::substitute(text)?;
    serde_json::from_str(&substituted).map_err(|e| e.to_string())
}

/// Must run after `validate` - a route disabled in this environment still
/// needs its own config checked now, not only once the condition later
/// flips true.
pub fn apply_conditions(cfg: &mut Config) {
    cfg.routes
        .retain(|r| r.when.as_ref().is_none_or(Condition::eval));
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub listen: String,
    #[serde(default)]
    pub routes: Vec<Route>,
    pub php: PhpConfig,
    #[serde(default)]
    pub status: StatusConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Existence and type only, never content.
    #[serde(default)]
    pub fs_cache: FsCacheConfig,
    /// Bytes of request body to accept before a 413.
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,
    /// CIDRs of the direct TCP peers whose `X-Forwarded-*` is believed; empty
    /// (default) trusts none. An entry lets everything it covers name the
    /// client identity, so list proxy addresses as narrowly as known.
    #[serde(default)]
    pub trusted_proxies: Vec<ipnetwork::IpNetwork>,
    #[serde(default)]
    pub connection: ConnectionConfig,
    /// `None` (the default) disables rate limiting entirely.
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    #[serde(default)]
    pub log_level: LogLevel,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub(crate) fn as_level(self) -> u8 {
        match self {
            LogLevel::Debug => crate::logging::level::DEBUG,
            LogLevel::Info => crate::logging::level::INFO,
            LogLevel::Warn => crate::logging::level::WARN,
            LogLevel::Error => crate::logging::level::ERROR,
        }
    }
}

/// Per-client-IP request cap, scoped by `user_agent` so it can target only
/// known crawlers rather than every visitor.
#[derive(Debug, Deserialize)]
pub struct RateLimitConfig {
    /// Burst capacity: how many requests one client may fire immediately,
    /// and the ceiling it can never exceed even after refilling.
    pub requests: u32,
    /// How long it takes to refill that same capacity from empty; sustained
    /// throughput settles at `requests / period_seconds`.
    pub period_seconds: u64,
    /// Same matching rules as `match.uri`/`match.method`/`match.host`.
    /// Empty matches every client; non-empty limits only matching ones.
    #[serde(default)]
    pub user_agent: Vec<MatchPattern>,
}

/// Caps on what one client can tie up before ever reaching a worker - the
/// `php.queue`/`php.processes` limits bound only work already accepted.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ConnectionConfig {
    /// Concurrently open connections; 0 disables the cap. Set well above
    /// real sustained use, but low enough that a flood hits a bounded
    /// refusal rather than the process's fd limit or its memory.
    pub max: usize,
    /// How long a connection may take to send its complete request head. The
    /// slowloris defence: a client dribbling a byte at a time otherwise holds
    /// a slot forever, having sent nothing the server could act on.
    pub header_read_timeout: u64,
    /// How long a connection may sit with no request in flight; 0 disables.
    /// Without this the connection cap becomes its own denial of service, an
    /// attacker filling every slot with keep-alives gone quiet.
    pub idle_timeout: u64,
    /// How long a request body may stall between reads; 0 disables. Bounds
    /// the gap, never the whole upload, so a slow but honest client still
    /// completes.
    pub body_read_timeout: u64,
}

/// Per core, so the cap tracks the size of the machine the way the runtime's
/// own worker-thread count already does. On a large host the product can
/// outgrow `RLIMIT_NOFILE`, which has to be raised alongside it.
const MAX_CONNECTIONS_PER_CORE: usize = 512;

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            max: std::thread::available_parallelism().map_or(1, |n| n.get())
                * MAX_CONNECTIONS_PER_CORE,
            header_read_timeout: 10,
            idle_timeout: 65,
            body_read_timeout: 60,
        }
    }
}

fn default_script_extensions() -> Vec<String> {
    vec!["php".to_string()]
}

fn default_max_body_size() -> usize {
    64 * 1024 * 1024 // 64 MiB
}

#[derive(Debug, Deserialize)]
pub struct Route {
    /// `None` always keeps the route. Re-evaluated only if the process
    /// restarts - there is no live config reload.
    #[serde(default)]
    pub when: Option<Condition>,
    /// Absent is a catch-all.
    #[serde(rename = "match", default)]
    pub matcher: RouteMatch,
    /// Flattened, so a `Php` route missing `target` fails to parse outright.
    #[serde(flatten)]
    pub action: RouteActionConfig,
}

/// Environment-driven predicate deciding whether a route is kept.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    Env {
        name: String,
        /// Absent just requires the variable to be set, to any value.
        #[serde(default)]
        equals: Option<String>,
    },
    All(Vec<Condition>),
    Any(Vec<Condition>),
    Not(Box<Condition>),
}

impl Condition {
    fn eval(&self) -> bool {
        match self {
            Condition::Env { name, equals } => match (std::env::var(name), equals) {
                (Ok(v), Some(expected)) => &v == expected,
                (Ok(_), None) => true,
                (Err(_), _) => false,
            },
            Condition::All(cs) => cs.iter().all(Condition::eval),
            Condition::Any(cs) => cs.iter().any(Condition::eval),
            Condition::Not(c) => !c.eval(),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum RouteActionConfig {
    Static {
        root: String,
        /// Fallback when `root` has no matching file; `None` is a 404. A
        /// chain may fall through several roots before reaching `Php`.
        #[serde(default)]
        fallback: Option<Box<RouteActionConfig>>,
    },

    Php {
        target: Arc<str>,
    },
    /// Bare status, no body. `u16` so a bad value fails `validate` with a
    /// real message rather than a byte-offset parse error.
    Return {
        status: u16,
    },
}

#[derive(Debug, Default, Deserialize)]
pub struct RouteMatch {
    /// Same rule as `method`, against the request path.
    #[serde(default)]
    pub uri: Vec<MatchPattern>,
    /// Matches if no non-negated pattern exists or one hits, and no negated
    /// pattern hits. ANDed with `uri`.
    #[serde(default)]
    pub method: Vec<MatchPattern>,
    /// Same rule, against the resolved Host. Lowercased first, hostnames not
    /// being case-sensitive, so patterns must be written lowercase.
    #[serde(default)]
    pub host: Vec<MatchPattern>,
}

impl RouteMatch {
    pub(crate) fn matches(&self, path: &str, method: &str, host: &str) -> bool {
        matches_any(&self.uri, path)
            && matches_any(&self.method, method)
            && matches_any(&self.host, host)
    }
}

#[derive(Debug, Deserialize)]
pub struct PhpConfig {
    /// Named entrypoints sharing this pool's processes and limits.
    #[serde(default)]
    pub targets: HashMap<String, Target>,
    /// Injected into the real environment before `fork()`, so it always
    /// reaches `getenv()` but `$_ENV` only with 'E' in `variables_order`.
    #[serde(default)]
    pub environment: HashMap<String, String>,
    /// Omitting both inherits master's identity with no setuid/setgid, for a
    /// deployment already launched unprivileged.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub group: Option<String>,
    /// ZEND_INI_SYSTEM (admin) / ZEND_INI_USER (user), applied by php-mod.
    #[serde(default)]
    pub options: PhpOptions,
    /// Extensions a request may execute, as PHP-FPM's
    /// `security.limit_extensions`. Without it an uploaded `.png` holding PHP
    /// is remote code execution.
    #[serde(default = "default_script_extensions")]
    pub script_extensions: Vec<String>,
    pub limits: Limits,
    pub processes: Processes,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    /// Refuses privilege gained through `execve`: a setuid binary a script
    /// shells out to runs as the worker instead. Exception: `mail()` needs
    /// an MTA submission helper such as `postdrop` to run setgid.
    #[serde(default = "default_true")]
    pub no_new_privs: bool,
}

fn default_true() -> bool {
    true
}

/// `Serialize` because it crosses `exec()`, which leaves the new process no
/// access to master's in-memory config.
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PhpOptions {
    #[serde(default)]
    pub admin: HashMap<String, String>,
    #[serde(default)]
    pub user: HashMap<String, String>,
}

/// `script` wins over `index` when both are set.
#[derive(Debug, Deserialize, Clone)]
pub struct Target {
    pub root: String,
    /// One front-controller for every request, with the whole path as
    /// PATH_INFO.
    #[serde(default)]
    pub script: Option<String>,
    /// URL maps to a `.php` under `root`, trailing segments becoming
    /// PATH_INFO.
    #[serde(default)]
    pub index: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Limits {
    /// Worker self-retires after this many requests.
    pub requests: u32,
    /// Watchdog SIGKILLs a worker exceeding this many seconds on one request.
    pub timeout: u64,
}

#[derive(Debug, Deserialize)]
pub struct Processes {
    /// Concurrently running workers, not the queue-depth cap.
    pub max: usize,
    /// Pre-spawned at startup; idle floor once a worker is claimed.
    pub spare: usize,
    /// Master kills a worker idle this long, once above the `spare` floor;
    /// 0 never. The floor itself is never touched, however long it sits idle.
    #[serde(default)]
    pub idle_timeout: u64,
    /// How long the prototype may take to answer a spawn request before it
    /// is treated as wedged. Its own knob rather than `queue.timeout`, since
    /// the first spawn after a restart waits out the prototype's PHP init.
    #[serde(default = "default_spawn_timeout")]
    pub spawn_timeout: u64,
}

fn default_spawn_timeout() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    /// Requests that may wait for a permit; 0 is unbounded, which under
    /// overload only delays the inevitable 503 while burning memory.
    pub max_depth: usize,
    /// How long to wait for a permit before a 503.
    pub timeout: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig {
            max_depth: 512,
            timeout: 5,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ShutdownConfig {
    /// SIGTERM drain wait: long enough to finish a request, short enough not
    /// to stall a rolling deploy.
    pub grace_period_seconds: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        ShutdownConfig {
            grace_period_seconds: 15,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct StatusConfig {
    pub listen: String,
}

impl Default for StatusConfig {
    fn default() -> Self {
        StatusConfig {
            listen: "127.0.0.1:8081".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    pub min_size_bytes: usize,
    /// Compression allowlist; empty means no restriction. Already-compressed
    /// formats would only burn CPU.
    pub mime_types: Vec<String>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig {
            min_size_bytes: 1024,
            mime_types: [
                "application/javascript",
                "application/json",
                "application/rss+xml",
                "application/vnd.ms-fontobject",
                "application/x-font-ttf",
                "application/xml",
                "font/opentype",
                "image/svg+xml",
                "image/x-icon",
                "text/css",
                "text/html",
                "text/javascript",
                "text/plain",
                "text/xml",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct FsCacheConfig {
    /// TTL for a cached verdict; 0 disables the cache.
    pub ttl_ms: u64,
    /// Paths kept at once; once full, admitting one evicts the coldest.
    pub max_entries: usize,
}

impl Default for FsCacheConfig {
    fn default() -> Self {
        FsCacheConfig {
            ttl_ms: 150,
            max_entries: 4096,
        }
    }
}

/// Cross-field checks serde's structural parsing cannot express.
pub fn validate(cfg: &Config) -> Vec<String> {
    let mut errors = Vec::new();
    // Settings whose zero is a degenerate value rather than an off switch.
    // Each entry completes "<name> must be at least 1 (0 would ...)".
    let rate_limit = cfg.rate_limit.as_ref();
    for (name, is_zero, consequence) in [
        (
            "php.processes.max",
            cfg.php.processes.max == 0,
            "leave no worker to serve anything",
        ),
        (
            "php.limits.timeout",
            cfg.php.limits.timeout == 0,
            "time out every request immediately",
        ),
        (
            "php.limits.requests",
            cfg.php.limits.requests == 0,
            "recycle every worker after its first request",
        ),
        (
            "php.queue.timeout",
            cfg.php.queue.timeout == 0,
            "time out every request immediately",
        ),
        (
            "php.processes.spawn_timeout",
            cfg.php.processes.spawn_timeout == 0,
            "fail every worker spawn immediately",
        ),
        (
            "connection.header_read_timeout",
            cfg.connection.header_read_timeout == 0,
            "reject every request before it arrives",
        ),
        (
            "rate_limit.requests",
            rate_limit.is_some_and(|r| r.requests == 0),
            "reject every matching request immediately",
        ),
        (
            "rate_limit.period_seconds",
            rate_limit.is_some_and(|r| r.period_seconds == 0),
            "never refill",
        ),
    ] {
        if is_zero {
            errors.push(format!("{name} must be at least 1 (0 would {consequence})"));
        }
    }
    if cfg.php.processes.spare > cfg.php.processes.max {
        errors.push(format!(
            "php.processes.spare ({}) must not exceed php.processes.max ({})",
            cfg.php.processes.spare, cfg.php.processes.max
        ));
    }
    if cfg.php.user.is_some() != cfg.php.group.is_some() {
        errors.push(
            "php.user and php.group must be set together or not at all (a half-drop would leave the other \
             at whatever identity master happened to start as)"
                .to_string(),
        );
    }
    if cfg.php.script_extensions.is_empty() {
        errors.push(
            "php.script_extensions must list at least one extension (an empty list can never run a script)"
                .to_string(),
        );
    }
    for ext in &cfg.php.script_extensions {
        if ext.is_empty() || ext.starts_with('.') || ext.contains('/') {
            errors.push(format!(
                "php.script_extensions entry {ext:?} must be a bare extension such as \"php\", without a leading dot or any path separator"
            ));
        }
    }
    // Caught here rather than as a puzzling 404 on every request.
    for (name, target) in &cfg.php.targets {
        for (field, value) in [("script", &target.script), ("index", &target.index)] {
            if let Some(value) = value
                && !extension_is_listed(value, &cfg.php.script_extensions)
            {
                errors.push(format!(
                    "php.targets.{name}.{field} {value:?} does not end in one of php.script_extensions ({:?}), so it could never be executed",
                    cfg.php.script_extensions
                ));
            }
        }
    }
    for net in &cfg.trusted_proxies {
        if net.prefix() == 0 {
            errors.push(format!(
                "trusted_proxies entry {net} covers every address, which would let any client pick its own \
                 X-Forwarded-For identity and so bypass the per-client rate limit; list the proxy addresses \
                 explicitly"
            ));
        }
    }
    for route in &cfg.routes {
        validate_action(&route.action, &cfg.php.targets, &mut errors);
    }
    errors
}

/// The single gate deciding what may be executed, shared by request
/// resolution and this validation so the two cannot drift apart.
///
/// Case-sensitive, so `.PHP` is refused: a case-insensitive filesystem
/// reaches the same file either way, and an upload filter that only rejected
/// `php` must not be undone here.
pub fn extension_is_listed(path: &str, allowed: &[String]) -> bool {
    std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|ext| allowed.iter().any(|a| a == ext))
}

/// A target may be named at any depth of a `fallback` chain.
fn validate_action(
    action: &RouteActionConfig,
    targets: &HashMap<String, Target>,
    errors: &mut Vec<String>,
) {
    match action {
        RouteActionConfig::Php { target } => {
            if !targets.contains_key(&**target) {
                errors.push(format!(
                    "route target {target:?} is not defined in php.targets"
                ));
            }
        }
        RouteActionConfig::Static {
            fallback: Some(next),
            ..
        } => validate_action(next, targets, errors),
        RouteActionConfig::Static { fallback: None, .. } => {}
        RouteActionConfig::Return { status } => {
            if hyper::StatusCode::from_u16(*status).is_err() {
                errors.push(format!(
                    "route return status {status} is not a valid HTTP status code (100-999)"
                ));
            }
        }
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
