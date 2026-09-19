# Configuration

Proteus is configured with a single JSON file, given on the command line:

```console
$ proteus --config /etc/proteus/config.json
```

The file is read once, at startup — there's no live reload, so after editing it, restart the process (or have your process supervisor restart it) to pick up the change. Startup validation also catches cross-field problems, like a route naming a PHP target that doesn't exist, not just invalid JSON.

The format is plain JSON: no comments, no trailing commas.

- [Environment variables](#environment-variables)
- [Listening](#listening) — `listen`
- [Routes](#routes) — `routes`
  - [Matching](#matching) — `match`
  - [Match patterns](#match-patterns)
  - [Conditional routes](#conditional-routes) — `when`
  - [Actions](#actions) — `static`, `php`, `return`
- [PHP](#php) — `php`
  - [Targets](#targets) — `php.targets`
  - [General options](#general-options)
  - [Limits](#limits) — `php.limits`
  - [Processes](#processes) — `php.processes`
  - [Queueing](#queueing) — `php.queue`
  - [Shutdown](#shutdown) — `php.shutdown`
- [Connections](#connections) — `connection`
- [Rate limiting](#rate-limiting) — `rate_limit`
- [Trusted proxies](#trusted-proxies) — `trusted_proxies`
- [Compression](#compression) — `compression`
- [Filesystem cache](#filesystem-cache) — `fs_cache`
- [Request body size](#request-body-size) — `max_body_size`
- [Logging](#logging) — `log_level`
- [Status endpoint](#status-endpoint) — `status`

Below is a complete example, followed by a full reference of every option. Every option not marked *(required)* has a default and may be omitted.

```json
{
  "listen": "0.0.0.0:8080",
  "log_level": "info",
  "max_body_size": 26214400,
  "trusted_proxies": ["10.0.0.0/8"],

  "routes": [
    { "match": { "uri": ["/uploads/*"] }, "action": "static", "root": "/var/www/uploads" },
    {
      "match": {},
      "action": "static",
      "root": "/var/www/public",
      "fallback": { "action": "php", "target": "app" }
    }
  ],

  "php": {
    "user": "phpapp",
    "group": "phpapp",
    "targets": {
      "app": { "root": "/var/www", "script": "index.php" }
    },
    "limits": { "requests": 500, "timeout": 30 },
    "processes": { "max": 20, "spare": 4 }
  },

  "connection": {
    "max": 8192
  },

  "rate_limit": {
    "enabled": true,
    "requests": 100,
    "period_seconds": 60,
    "user_agent": ["~(?i)bot|crawler|spider"]
  }
}
```

## Environment variables

Before the file is parsed as JSON, `$NAME` and `${NAME}` references anywhere in it are expanded against the process environment, so secrets and per-environment values don't have to be hardcoded into the file:

```json
{
  "listen": "0.0.0.0:${PORT}",
  "log_level": "${LOG_LEVEL:-info}"
}
```

A bare `$NAME` expands only when immediately followed by an identifier character, so it's safe to use inside a regex like `~\.php$` — there, the `$` is a plain end-of-string anchor, not a variable reference. `$$` escapes to a literal `$`, so `$${uri}` survives as `${uri}`, useful when a [match pattern](#match-patterns) needs a literal dollar sign.

Shell-style defaulting is supported too:

| Form | Expands to |
|---|---|
| `${NAME}` | The variable's value. Startup fails if `NAME` is unset. |
| `${NAME:-default}` | The variable's value, or `default` if `NAME` is unset or empty. |
| `${NAME-default}` | The variable's value, or `default` if `NAME` is unset (empty counts as set). |
| `${NAME:=default}` | Same as `:-`, but does not write `default` back to the environment — it only affects this expansion. |
| `${NAME:+alt}` | `alt` if `NAME` is set and non-empty, otherwise empty. |

An unset variable referenced without a default (`$NAME` or `${NAME}`) is a startup error naming the missing variable, not a silent empty string — a typo in a variable name fails loudly instead of producing a config that parses but means something else.

This same expansion is available standalone as [`proteus envsubst`](commands.md#envsubst), for templating other files (an `.ini` snippet shared with the PHP CLI, say) with identical rules.

## Listening

| Option | Default | Description |
|---|---|---|
| `listen` *(required)* | — | String; the TCP address to accept requests on, as `host:port` (e.g. `"0.0.0.0:8080"`, `"127.0.0.1:8080"`). |

```json
{ "listen": "0.0.0.0:8080" }
```

## Routes

`routes` is an ordered array. Each incoming request is matched against the array in order, and the **first** route whose `match` matches wins — later routes are never consulted for that request. A route with no `match` at all matches everything, so it's typically last, as a catch-all.

```json
{
  "routes": [
    { "match": { "uri": ["/health"] }, "action": "return", "status": 200 },
    { "match": { "uri": ["*.php"] }, "action": "return", "status": 404 },
    { "match": {}, "action": "static", "root": "/var/www/public",
      "fallback": { "action": "php", "target": "app" } }
  ]
}
```

Each route has:

| Option | Default | Description |
|---|---|---|
| `match` | matches everything | Object; see [Matching](#matching) below. |
| `when` | always kept | Object; see [Conditional routes](#conditional-routes) below. |
| `action` *(required)* | — | String; one of `static`, `php`, or `return`, with fields as described in [Actions](#actions). |

### Matching

The `match` object narrows a route to specific requests:

| Option | Default | Description |
|---|---|---|
| `uri` | matches any | Array of [match patterns](#match-patterns), tested against the request path. |
| `method` | matches any | Array of [match patterns](#match-patterns), tested against the HTTP method. |
| `host` | matches any | Array of [match patterns](#match-patterns), tested against the resolved `Host`, lowercased first — write host patterns in lowercase. |

A request matches a route only if `uri`, `method`, and `host` all match (each field is independent; they're ANDed together). Omitting a field, or leaving `match` out entirely, matches any value for that field.

#### Match patterns

The same pattern syntax is used everywhere a config value matches against a string — route `uri`/`method`/`host` and [`rate_limit.user_agent`](#rate-limiting). A pattern is one of:

- **A glob**, the default form: `*` matches any run of characters. `/api/*` matches `/api/` and everything under it; `*.php` matches any path ending in `.php`.
- **A regex**, written with a leading `~`: `~^/api/v[0-9]+/` matches by a full regular expression.
- **A negation**, written with a leading `!`: `!/admin/*` matches everything *except* paths under `/admin/`. `!` can prefix either a glob or a regex (`!~...`).

A list of patterns matches a value if no non-negated pattern is present, or one of the non-negated patterns matches — **and** no negated pattern matches. An empty list matches everything.

### Conditional routes

`when` decides at startup whether a route exists at all — a route whose condition fails is left out of the routing table entirely, as if deleted from the file. It's checked once, against the environment at that moment, and never re-evaluated afterward.

```json
{
  "match": { "uri": ["/debug/*"] },
  "when": { "env": { "name": "APP_ENV", "equals": "development" } },
  "action": "php",
  "target": "app"
}
```

| Form | Description |
|---|---|
| `{ "env": { "name": "VAR" } }` | True if the environment variable `VAR` is set, to any value. |
| `{ "env": { "name": "VAR", "equals": "value" } }` | True if `VAR` is set and equal to `value`. |
| `{ "all": [ <condition>, ... ] }` | True if every nested condition is true. |
| `{ "any": [ <condition>, ... ] }` | True if at least one nested condition is true. |
| `{ "not": <condition> }` | True if the nested condition is false. |

A route disabled by `when` still has its own config checked at startup — an environment-gated route with a typo in its PHP target name fails startup immediately, rather than only once someone flips the environment variable that turns it on.

### Actions

**`static`** serves files from disk.

| Option | Default | Description |
|---|---|---|
| `root` *(required)* | — | String; directory the request path is resolved against. |
| `fallback` | none (404) | Action object, tried when the request doesn't resolve to a file under `root`. Fallbacks can chain (a `static` falling back to another `static` falling back to a `php`), commonly used to serve static assets straight from disk and hand everything else to a PHP front controller. |

**`php`** dispatches the request to a PHP worker.

| Option | Default | Description |
|---|---|---|
| `target` *(required)* | — | String; the name of an entry in [`php.targets`](#targets). Startup fails if it doesn't exist. |

**`return`** answers with a status code and no body — useful for health checks, fixed redirects to elsewhere, or closing off a route entirely (e.g. `404` for direct requests to `.php` files that should only ever be reached through routing).

| Option | Default | Description |
|---|---|---|
| `status` *(required)* | — | Integer; the HTTP status code to return (100–999). |

## PHP

The `php` object configures the pool of PHP worker processes and the applications they run.

### Targets

`php.targets` names one or more front controllers, referenced by [`php` routes](#actions). Every target shares the same pool of worker processes, the same `limits`, and the same `environment` — there's one PHP process pool per Proteus instance, not one per target.

```json
{
  "php": {
    "targets": {
      "app": { "root": "/var/www", "script": "index.php" },
      "legacy": { "root": "/var/www/legacy", "index": "app.php" }
    }
  }
}
```

| Option | Default | Description |
|---|---|---|
| `root` *(required)* | — | String; the application's document root. |
| `script` | none | String; a single front-controller script (relative to `root`) that handles every request to this target, with the full request path passed as `PATH_INFO`. |
| `index` | `"index.php"` | String; a `.php` file (relative to `root`) that requests resolve into — `/blog/post` resolves to `index` under `blog/`, with the trailing segment passed as `PATH_INFO`. Ignored when `script` is set. |

### General options

| Option | Default | Description |
|---|---|---|
| `environment` | `{}` | Object of string key/value pairs, injected into each worker's environment before it's forked. Reaches `getenv()` always, and `$_ENV` only if PHP's `variables_order` includes `E`. |
| `user` | inherited | String; the OS user PHP workers run as. Must be set together with `group`. |
| `group` | inherited | String; the OS group PHP workers run as. Must be set together with `user`. Omitting both keeps the workers running as whatever identity started the master process. |
| `options` | `{}` | Object with `admin` and `user` sub-objects, each a map of `php.ini` setting names to values. Settings under `admin` can't be changed by a script at runtime (`ini_set()`); settings under `user` can. |
| `script_extensions` | `["php"]` | Array of strings; the file extensions a request is allowed to execute, matched case-sensitively. Applies to `script`/`index` in every target, and is worth keeping deliberately narrow — without it, a file upload that smuggles PHP into an unexpected extension becomes remote code execution. |
| `no_new_privs` | `true` | Boolean; stops worker processes — and anything they `exec()`/`shell_exec()` into — from gaining new privileges through a setuid/setgid program (the Linux `PR_SET_NO_NEW_PRIVS` flag). Turn off only if something you run genuinely needs that, e.g. `mail()` delivering through a setgid helper like `postdrop`. |

```json
{
  "php": {
    "user": "phpapp",
    "group": "phpapp",
    "script_extensions": ["php"]
  }
}
```

### Limits

`php.limits` bounds how long a single request or worker may run.

| Option | Default | Description |
|---|---|---|
| `requests` | `0` (never) | Integer; number of requests a worker handles before it's recycled — it finishes what it's doing, then exits and is replaced. Keeps a slow memory leak from growing unbounded. |
| `timeout` | `0` (disabled) | Integer; maximum seconds a single request may run before the worker handling it is killed. |

```json
{ "php": { "limits": { "requests": 500, "timeout": 30 } } }
```

### Processes

`php.processes` sizes the worker pool.

| Option | Default | Description |
|---|---|---|
| `max` | `1` | Integer; maximum number of PHP workers running at once. |
| `spare` | `1` | Integer; workers pre-spawned at startup and kept as an idle floor, so a burst of traffic doesn't have to pay process-spawn latency on the critical path. Must not exceed `max`. |
| `idle_timeout` | `0` (never) | Integer; seconds an idle worker may sit unclaimed before master kills it, once the pool has more than `spare` idle workers — `spare` itself is protected. |
| `spawn_timeout` | `30` | Integer; seconds a new worker may take to start up and signal readiness before it's treated as wedged and killed. |

```json
{ "php": { "processes": { "max": 20, "spare": 4, "idle_timeout": 120 } } }
```

### Queueing

`php.queue` bounds how a request waits for a worker once none is immediately free — as opposed to [`connection`](#connections), which bounds the connection itself before a request is even accepted for processing.

| Option | Default | Description |
|---|---|---|
| `max_depth` | `512` | Integer; requests allowed to wait for a free worker at once. `0` is unbounded, which under sustained overload only delays an eventual `503` while consuming more memory — a bounded queue turns overload into fast, cheap failures instead. |
| `timeout` | `5` | Integer; seconds a request waits in the queue for a worker before it gets a `503`. |

```json
{ "php": { "queue": { "max_depth": 1000, "timeout": 10 } } }
```

### Shutdown

`php.shutdown` controls how long a graceful shutdown waits before forcing an exit.

| Option | Default | Description |
|---|---|---|
| `grace_period_seconds` | `15` | Integer; on `SIGTERM`, how long Proteus waits for in-flight requests to finish before forcing an exit. Long enough to let a normal request complete, short enough not to stall a rolling deploy. |

```json
{ "php": { "shutdown": { "grace_period_seconds": 30 } } }
```

## Connections

The `connection` object bounds what a single client connection may cost before a request ever reaches a PHP worker at all — open sockets, pending headers, idle keep-alives. These protect the server process itself (file descriptors, memory), independently of PHP; see [Queueing](#queueing) above for limits on work already accepted.

| Option | Default | Description |
|---|---|---|
| `max` | 512 × CPU cores | Integer; maximum number of concurrently open connections. `0` disables the cap. |
| `header_read_timeout` | `10` | Integer; seconds a connection may take to send a complete request head, counted from the first byte received. Without this, a client that dribbles a request one byte at a time (a slowloris attack) can hold a connection slot forever. |
| `idle_timeout` | `65` | Integer; seconds a keep-alive connection may sit with no request in flight before it's closed. `0` disables the timeout. |
| `body_read_timeout` | `60` | Integer; seconds a request body may stall between reads before the connection is closed. Bounds the gap between reads, not the whole upload, so a slow but honest client can still finish. `0` disables the timeout. |

> [!WARNING]
> Disabling `idle_timeout` turns `max` into a denial-of-service vector of its own: a client can open every available connection slot, send one request on each with `Connection: keep-alive`, and then go silent, locking out every other client indefinitely. Leave it enabled unless every client that can reach this listener is trusted.

## Rate limiting

The `rate_limit` object caps how many requests a single client IP may make, using a token bucket: each client starts with a full bucket of `requests` tokens, spends one per request, and refills continuously, reaching full again after `period_seconds`. A client that empties its bucket gets `429 Too Many Requests` until it refills. There's no per-route rate limiting, only an optional restriction to matching clients via `user_agent`.

| Option | Default | Description |
|---|---|---|
| `enabled` | `false` | Boolean; turns the feature on. |
| `requests` *(required)* | — | Integer; the bucket's capacity — how many requests a client may fire in a burst, and the ceiling it never exceeds even fully refilled. |
| `period_seconds` *(required)* | — | Integer; how long a fully empty bucket takes to refill completely. Sustained throughput per client settles at `requests / period_seconds` requests per second. |
| `user_agent` | `[]` | Array of [match patterns](#match-patterns); restricts the limit to requests whose `User-Agent` matches. Empty applies the limit to every client, regardless of `User-Agent`. |

```json
{
  "rate_limit": {
    "enabled": true,
    "requests": 100,
    "period_seconds": 60,
    "user_agent": ["~(?i)bot|crawler|spider"]
  }
}
```

> [!NOTE]
> The limit is keyed by client IP. Behind a reverse proxy, every request otherwise appears to come from the proxy's own IP — see [Trusted proxies](#trusted-proxies) to make Proteus trust the real client address instead.

## Trusted proxies

| Option | Default | Description |
|---|---|---|
| `trusted_proxies` | `[]` | Array of CIDR strings (e.g. `"10.0.0.0/8"`). When a request arrives directly from an address in this list, its `X-Forwarded-For` header is trusted to name the real client — used for [rate limiting](#rate-limiting) and for logging. Direct connections from any other address are trusted only for their own peer address, and any `X-Forwarded-For` they send is ignored. |

An entry lets everything it covers assign its own client identity via `X-Forwarded-For`, so list only the actual addresses of your reverse proxies, as narrowly as you know them. A `/0` entry (trusting every address) is rejected at startup, since it would let any client pick its own identity and bypass the per-client rate limit entirely.

```json
{ "trusted_proxies": ["10.0.0.0/8", "172.16.0.0/12"] }
```

## Compression

The `compression` object controls on-the-fly response compression (gzip, brotli, or zstd, negotiated by `Accept-Encoding`).

| Option | Default | Description |
|---|---|---|
| `enabled` | `true` | Boolean; turns compression off entirely when `false`, regardless of `min_size_bytes` or `mime_types`. |
| `min_size_bytes` | `1024` | Integer; responses smaller than this are sent uncompressed — below a few hundred bytes, compression overhead can exceed the savings. |
| `mime_types` | see below | Array of strings; content types eligible for compression. An empty array removes the restriction and compresses every response body regardless of size. |

<details>
<summary>Default <code>mime_types</code> list</summary>

```json
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
  "text/xml"
]
```

</details>

```json
{ "compression": { "min_size_bytes": 512 } }
```

## Filesystem cache

The `fs_cache` object caches filesystem existence/type checks, so a route resolving through several fallbacks doesn't re-stat the same path on every request.

| Option | Default | Description |
|---|---|---|
| `ttl_ms` | `150` | Integer; how long a cached "does this path exist, and what kind is it" verdict is trusted before being re-checked. `0` disables the cache. Only existence and file type are cached — never file contents. |
| `max_entries` | `4096` | Integer; number of paths kept in the cache at once. Once full, admitting a new entry evicts the coldest one. |

```json
{ "fs_cache": { "ttl_ms": 500, "max_entries": 16384 } }
```

## Request body size

| Option | Default | Description |
|---|---|---|
| `max_body_size` | `67108864` (64 MiB) | Integer; maximum request body size, in bytes. A larger body is rejected with `413 Payload Too Large` before it's read into memory. |

```json
{ "max_body_size": 26214400 }
```

## Logging

| Option | Default | Description |
|---|---|---|
| `log_level` | `"info"` | String; one of `"debug"`, `"info"`, `"warn"`, `"error"`. Only messages at or above this level are emitted. Logs are written as structured lines to stdout. |

```json
{ "log_level": "warn" }
```

## Status endpoint

The `status` object configures a second HTTP listener, separate from the main one, that reports pool and connection metrics as JSON.

| Option | Default | Description |
|---|---|---|
| `listen` | `"127.0.0.1:8081"` | String; `host:port` for this listener. Kept apart from the top-level `listen` so it can be bound to localhost only, even when the main listener faces the internet. |

```json
{ "status": { "listen": "127.0.0.1:9090" } }
```

A `GET` to that listener returns a snapshot like this:

```json
{
  "uptime_seconds": 3600,
  "php": {
    "targets": ["app"],
    "prototype_pid": 1234,
    "processes": { "idle": 3, "busy": 1, "total": 4, "max": 20 },
    "queue": { "depth": 0, "max_depth": 512 },
    "counters": {
      "requests_total": 48213,
      "requests_failed": 0,
      "requests_too_large": 0,
      "watchdog_kills": 0,
      "queue_timeouts": 0,
      "workers_spawned_total": 7,
      "recycled_request_limit": 2,
      "recycled_idle_timeout": 1,
      "workers_reaped_dead": 0,
      "workers_vanished_idle": 0,
      "workers_abandoned": 0,
      "prototype_respawns_total": 0,
      "crash_loop_backoffs": 0
    },
    "workers": [
      { "pid": 1240, "state": "idle", "request_count": 812, "started_ago_seconds": 3500, "last_active_ago_seconds": 12 }
    ]
  }
}
```

| Field | Description |
|---|---|
| `processes` | Current worker counts: `idle`, `busy`, `total`, and the configured `max`. |
| `queue` | Requests currently waiting for a worker (`depth`), against `max_depth`. |
| `counters` | Cumulative totals since startup: request outcomes and worker lifecycle events. See below for what each one means. |
| `workers` | One entry per live worker, with its state, requests served, and how long ago it started or was last active. |

<details>
<summary>Counter meanings</summary>

| Counter | Meaning |
|---|---|
| `requests_total` | Requests dispatched to a PHP worker. |
| `requests_failed` | The worker's channel failed mid-response; the worker is killed. |
| `requests_too_large` | Refused because the request didn't fit in one request-ring frame. Should stay `0` — anything else means a route's paths are longer than the ring was sized for. |
| `watchdog_kills` | Workers killed for overrunning `limits.timeout`, or for breaking the response protocol. |
| `queue_timeouts` | Requests that waited longer than `queue.timeout` for a free worker and got a `503`. |
| `workers_spawned_total` | Workers forked over the process's whole lifetime. |
| `recycled_request_limit` | Workers retired cleanly after reaching `limits.requests`. |
| `recycled_idle_timeout` | Workers killed by master for sitting idle past `processes.idle_timeout`, beyond the `spare` floor. |
| `workers_reaped_dead` | A worker's pid was reused before the old one was reaped. Should stay `0`. |
| `workers_vanished_idle` | A worker was found already gone while idle — a crash or an external kill, not a decision Proteus made. |
| `workers_abandoned` | The client disconnected while a worker was still handling its request. |
| `prototype_respawns_total` | Times the PHP prototype process was respawned after dying. |
| `crash_loop_backoffs` | Respawn attempts skipped by the backoff after repeated prototype failures. |

</details>
