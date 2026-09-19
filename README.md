# Proteus

[![Tests](https://github.com/pavelkovar/proteus/actions/workflows/test.yml/badge.svg)](https://github.com/pavelkovar/proteus/actions/workflows/test.yml)
[![Rust](https://img.shields.io/badge/rust-2024-orange)](https://www.rust-lang.org/)
[![Platform](https://img.shields.io/badge/platform-linux-blue)](#building)

**A PHP application server for containers, meant to run behind a reverse proxy.**

Proteus keeps a pool of persistent PHP worker processes running — no per-request fork/exec — behind a single Rust binary, talking to workers over shared-memory rings instead of FastCGI.

## Features

- **Multi-core** — one listener socket per CPU core, each pinned to its own thread.
- **Persistent workers** — PHP, OPcache, and APCu state are initialized once and forked into every worker, not reloaded per request.
- **Shared-memory IPC** — requests and responses cross the master/worker boundary over memory-mapped rings, not a FastCGI socket.
- **Built-in protections** — a watchdog that actually kills a hung worker (not just the client-facing response), privilege-escalation guards (`no_new_privs`) on every worker, and trusted-proxy-gated client IPs.
- **Token-bucket rate limiting** and **on-the-fly compression** (gzip, brotli, zstd), negotiated per request.
- **Range and conditional requests** for static files — `ETag`/`If-None-Match` and byte-range (`Range`/`Content-Range`) support.
- **A JSON config file** with shell-style `${VAR}` substitution, conditional routes, and a live `/status` metrics endpoint.
- **A built-in `cron` runner**, so a container doesn't need a separate cron daemon installed.

## Quick start

```json
{
  "listen": "0.0.0.0:8080",
  "routes": [
    { "match": {}, "action": "php", "target": "app" }
  ],
  "php": {
    "targets": { "app": { "root": "/var/www", "script": "index.php" } },
    "limits": { "requests": 500, "timeout": 30 },
    "processes": { "max": 20, "spare": 4 }
  }
}
```

```console
$ proteus --config config.json
```

## Documentation

- [Configuration](docs/configuration.md)
- [Command line](docs/commands.md)

## Building

Linux only.

### php-mod

The embed-SAPI bridge to PHP. Needs PHP's headers on the build machine (`php-config` on `PATH`):

```console
$ cd php-mod
$ make
```

### server

```console
$ cd server
$ cargo build            # debug build
$ cargo build --release  # optimized build
```
