//! JSON configuration schema.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Braced form only, so a regex like `~\.php$` in `match.uri` is never
/// mistaken for a placeholder. Expanding raw text rather than a parsed tree
/// keeps line and column accurate in parse errors.
pub fn parse(text: &str) -> Result<Config, String> {
    let substituted = substitute_env(text)?;
    serde_json::from_str(&substituted).map_err(|e| e.to_string())
}

fn substitute_env(text: &str) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| "unterminated \"${\" in config (missing closing brace)".to_string())?;
        let body = &after[..end];
        let (name, default) = match body.split_once(':') {
            Some((name, default)) => (name, Some(default)),
            None => (body, None),
        };
        let value = match (std::env::var(name), default) {
            (Ok(v), _) => v,
            (Err(_), Some(default)) => default.to_string(),
            (Err(_), None) => {
                return Err(format!(
                    "environment variable {name:?} is not set (referenced as \"${{{body}}}\" in config)"
                ));
            }
        };
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub listen: Vec<String>,
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
    /// (the default) believes none of them. An entry grants everything it
    /// covers the power to name the client identity behind it, so list the
    /// proxy's own addresses as narrowly as they are known.
    #[serde(default)]
    pub trusted_proxies: Vec<ipnetwork::IpNetwork>,
    #[serde(default)]
    pub connection: ConnectionConfig,
    /// `None` (the default) disables rate limiting entirely.
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
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

/// Caps on what one client can tie up before ever reaching a worker.
///
/// The `php.queue` and `php.processes` limits bound work already accepted. A
/// connection commits a task, a socket buffer and, once a body arrives, heap
/// and then temp space up to `max_body_size` - all before either limit sees it.
#[derive(Debug, Deserialize)]
pub struct ConnectionConfig {
    /// Concurrently open connections; 0 disables the cap. Set well above
    /// real sustained use, but low enough that a flood hits a bounded
    /// refusal rather than the process's fd limit or its memory.
    #[serde(default = "default_max_connections")]
    pub max: usize,
    /// How long a connection may take to send its complete request head. The
    /// slowloris defence: a client dribbling a byte at a time otherwise holds
    /// a slot forever, having sent nothing the server could act on.
    #[serde(default = "default_header_read_timeout")]
    pub header_read_timeout: u64,
    /// How long a connection may sit with no request in flight; 0 disables.
    ///
    /// Without this the connection cap becomes its own denial of service, an
    /// attacker filling every slot with keep-alives that completed one cheap
    /// request and then went quiet. Set just above the keep-alive most
    /// clients and load balancers use, so ordinary reuse is never cut off.
    #[serde(default = "default_connection_idle_timeout")]
    pub idle_timeout: u64,
    /// How long a request body may stall between reads; 0 disables. Bounds
    /// the gap, never the whole upload, so a slow but honest client still
    /// completes.
    #[serde(default = "default_body_read_timeout")]
    pub body_read_timeout: u64,
}

/// Per core, so the cap tracks the size of the machine the way the runtime's
/// own worker-thread count already does. On a large host the product can
/// outgrow `RLIMIT_NOFILE`, which has to be raised alongside it.
const MAX_CONNECTIONS_PER_CORE: usize = 512;

fn default_max_connections() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get()) * MAX_CONNECTIONS_PER_CORE
}

fn default_header_read_timeout() -> u64 {
    10
}

fn default_connection_idle_timeout() -> u64 {
    65
}

fn default_body_read_timeout() -> u64 {
    60
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            max: default_max_connections(),
            header_read_timeout: default_header_read_timeout(),
            idle_timeout: default_connection_idle_timeout(),
            body_read_timeout: default_body_read_timeout(),
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
    /// Absent is a catch-all.
    #[serde(rename = "match", default)]
    pub matcher: RouteMatch,
    /// Flattened, so a `Php` route missing `target` fails to parse outright.
    #[serde(flatten)]
    pub action: RouteActionConfig,
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

/// Matches if no non-negated pattern exists or one hits, and no negated
/// pattern hits.
pub(crate) fn matches_any(patterns: &[MatchPattern], value: &str) -> bool {
    let mut has_positive = false;
    let mut positive_matched = false;
    for pattern in patterns {
        if let MatchPattern::Not(inner) = pattern {
            if inner.matches(value) {
                return false;
            }
        } else {
            has_positive = true;
            positive_matched = positive_matched || pattern.matches(value);
        }
    }
    !has_positive || positive_matched
}

/// `~pattern` is a regex, which is linear-time and so safe on hostile input;
/// anything else is a glob. A leading `!` negates. Compiled once at load.
#[derive(Debug)]
pub enum MatchPattern {
    /// No `*`.
    Exact(String),
    /// Matches anything.
    Any,
    /// `min_length` lets a short value be rejected in O(1).
    Glob {
        leading: bool,
        trailing: bool,
        parts: Vec<String>,
        min_length: usize,
    },
    Regex(regex::Regex),
    Not(Box<MatchPattern>),
}

impl MatchPattern {
    pub(crate) fn matches(&self, value: &str) -> bool {
        match self {
            MatchPattern::Exact(s) => value == s,
            MatchPattern::Any => true,
            MatchPattern::Regex(re) => re.is_match(value),
            MatchPattern::Not(inner) => !inner.matches(value),
            MatchPattern::Glob {
                leading,
                trailing,
                parts,
                min_length,
            } => {
                if value.len() < *min_length {
                    return false;
                }
                let last = parts.len() - 1;
                let mut rest = value;
                for (i, part) in parts.iter().enumerate() {
                    if i == 0 && !leading {
                        match rest.strip_prefix(part.as_str()) {
                            Some(after) => rest = after,
                            None => return false,
                        }
                    } else if i == last && !trailing {
                        return rest.ends_with(part.as_str());
                    } else {
                        match rest.find(part.as_str()) {
                            Some(offset) => rest = &rest[offset + part.len()..],
                            None => return false,
                        }
                    }
                }
                true
            }
        }
    }
}

impl TryFrom<String> for MatchPattern {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        // Never matches anything real: a typo, not an intent.
        if s.is_empty() {
            return Err("empty match pattern (use \"*\" to match everything)".to_string());
        }
        if let Some(rest) = s.strip_prefix('!') {
            return MatchPattern::try_from(rest.to_string())
                .map(|p| MatchPattern::Not(Box::new(p)));
        }
        if let Some(pattern) = s.strip_prefix('~') {
            // ASCII-only, which drops the sizeable `unicode-*` features from
            // the release binary; \d, \w and \s still work.
            return regex::RegexBuilder::new(pattern)
                .unicode(false)
                .build()
                .map(MatchPattern::Regex)
                .map_err(|e| format!("invalid match regex {pattern:?}: {e}"));
        }
        if !s.contains('*') {
            return Ok(MatchPattern::Exact(s));
        }
        if s.chars().all(|c| c == '*') {
            return Ok(MatchPattern::Any);
        }
        let leading = s.starts_with('*');
        let trailing = s.ends_with('*');
        let parts: Vec<String> = s
            .split('*')
            .filter(|p| !p.is_empty())
            .map(String::from)
            .collect();
        let min_length = parts.iter().map(String::len).sum();
        Ok(MatchPattern::Glob {
            leading,
            trailing,
            parts,
            min_length,
        })
    }
}

impl<'de> Deserialize<'de> for MatchPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
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
    /// is remote code execution, and any file under a target's root is
    /// readable through the interpreter that echoes it.
    #[serde(default = "default_script_extensions")]
    pub script_extensions: Vec<String>,
    pub limits: Limits,
    pub processes: Processes,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub shutdown: ShutdownConfig,
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
    /// A worker retires itself after this long with no request; 0 never.
    ///
    /// Enforced by the worker rather than by master sweeping an idle list,
    /// which is what lets the idle pool be a lock-free stack. Master keeps
    /// `spare` topped up, so the pool settles at the floor, not at zero.
    #[serde(default)]
    pub idle_timeout: u64,
    /// How long the prototype may take to answer a spawn request before it
    /// is treated as wedged. Generous, because the fork is instant but the
    /// first spawn after a restart waits out the prototype's whole PHP init.
    /// Its own knob rather than `queue.timeout`, which is sized for how long
    /// a client should wait.
    #[serde(default = "default_spawn_timeout")]
    pub spawn_timeout: u64,
}

fn default_spawn_timeout() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
pub struct QueueConfig {
    /// Requests that may wait for a permit; 0 is unbounded, which under
    /// overload only delays the inevitable 503 while burning memory.
    #[serde(default = "default_queue_max_depth")]
    pub max_depth: usize,
    /// How long to wait for a permit before a 503.
    #[serde(default = "default_queue_timeout")]
    pub timeout: u64,
}

fn default_queue_max_depth() -> usize {
    512
}

fn default_queue_timeout() -> u64 {
    5
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig {
            max_depth: default_queue_max_depth(),
            timeout: default_queue_timeout(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ShutdownConfig {
    /// SIGTERM drain wait: long enough to finish a request, short enough not
    /// to stall a rolling deploy.
    #[serde(default = "default_shutdown_grace_period_seconds")]
    pub grace_period_seconds: u64,
}

fn default_shutdown_grace_period_seconds() -> u64 {
    15
}

// Not derived: a field-level `serde(default)` fires only when the struct is
// present and the field missing, not when the whole object is absent.
impl Default for ShutdownConfig {
    fn default() -> Self {
        ShutdownConfig {
            grace_period_seconds: default_shutdown_grace_period_seconds(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StatusConfig {
    /// Defaults to loopback-only.
    #[serde(default = "default_status_listen")]
    pub listen: String,
}

fn default_status_listen() -> String {
    "127.0.0.1:8081".to_string()
}

// Not derived, for the reason above: an absent object would silently default
// `listen` to "".
impl Default for StatusConfig {
    fn default() -> Self {
        StatusConfig {
            listen: default_status_listen(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CompressionConfig {
    #[serde(default = "default_min_compress_size")]
    pub min_size_bytes: usize,
    /// Compression allowlist; empty means no restriction. Already-compressed
    /// formats would only burn CPU.
    #[serde(default = "default_compress_mime_types")]
    pub mime_types: Vec<String>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        CompressionConfig {
            min_size_bytes: default_min_compress_size(),
            mime_types: default_compress_mime_types(),
        }
    }
}

fn default_min_compress_size() -> usize {
    1024
}

fn default_compress_mime_types() -> Vec<String> {
    [
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
    .collect()
}

#[derive(Debug, Deserialize)]
pub struct FsCacheConfig {
    /// TTL for a cached verdict; 0 disables the cache.
    #[serde(default = "default_fs_cache_ttl_ms")]
    pub ttl_ms: u64,
    /// Once full, further paths go uncached rather than evicting.
    #[serde(default = "default_fs_cache_max_entries")]
    pub max_entries: usize,
}

impl Default for FsCacheConfig {
    fn default() -> Self {
        FsCacheConfig {
            ttl_ms: default_fs_cache_ttl_ms(),
            max_entries: default_fs_cache_max_entries(),
        }
    }
}

fn default_fs_cache_ttl_ms() -> u64 {
    150
}

fn default_fs_cache_max_entries() -> usize {
    4096
}

/// Cross-field checks serde's structural parsing cannot express.
pub fn validate(cfg: &Config) -> Vec<String> {
    let mut errors = Vec::new();
    if cfg.php.processes.max == 0 {
        errors.push("php.processes.max must be at least 1".to_string());
    }
    if cfg.php.processes.spare > cfg.php.processes.max {
        errors.push(format!(
            "php.processes.spare ({}) must not exceed php.processes.max ({})",
            cfg.php.processes.spare, cfg.php.processes.max
        ));
    }
    // 0 here times out immediately rather than disabling.
    if cfg.php.limits.timeout == 0 {
        errors.push(
            "php.limits.timeout must be at least 1 (0 would time out every request immediately)"
                .to_string(),
        );
    }
    if cfg.php.limits.requests == 0 {
        errors.push("php.limits.requests must be at least 1 (0 would recycle every worker after its first request)".to_string());
    }
    if cfg.php.user.is_some() != cfg.php.group.is_some() {
        errors.push(
            "php.user and php.group must be set together or not at all (a half-drop would leave the other \
             at whatever identity master happened to start as)"
                .to_string(),
        );
    }
    if cfg.php.queue.timeout == 0 {
        errors.push(
            "php.queue.timeout must be at least 1 (0 would time out every request immediately)"
                .to_string(),
        );
    }
    if cfg.connection.header_read_timeout == 0 {
        errors.push(
            "connection.header_read_timeout must be at least 1 (0 would reject every request before it arrives)"
                .to_string(),
        );
    }
    if cfg.php.processes.spawn_timeout == 0 {
        errors.push(
            "php.processes.spawn_timeout must be at least 1 (0 would fail every worker spawn immediately)".to_string(),
        );
    }
    if let Some(rate_limit) = &cfg.rate_limit {
        if rate_limit.requests == 0 {
            errors.push("rate_limit.requests must be at least 1 (0 would reject every matching request immediately)".to_string());
        }
        if rate_limit.period_seconds == 0 {
            errors.push(
                "rate_limit.period_seconds must be at least 1 (0 would never refill)".to_string(),
            );
        }
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
