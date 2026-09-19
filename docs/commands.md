# Command line

The `proteus` binary has one primary mode — run the HTTP+PHP server — plus two standalone utility subcommands that share code with it but don't start a server at all.

```console
$ proteus --config <path>  # run the server
$ proteus envsubst         # expand $VAR/${VAR} on stdin
$ proteus cron <crontab>   # run a crontab's jobs until stopped
```

Exactly one of `--config` or a subcommand is required; running `proteus` with neither prints usage and exits.

- [Running the server](#running-the-server)
- [envsubst](#envsubst)
- [cron](#cron)
  - [Crontab format](#crontab-format)
  - [Job environment](#job-environment)
  - [Scheduling and overlap](#scheduling-and-overlap)
  - [Shutdown](#shutdown)

## Running the server

```console
$ proteus --config /etc/proteus/config.json
```

Reads and validates the JSON config, then runs until stopped. See [the configuration reference](configuration.md) for the file format; a config that fails validation is reported on stderr and the process exits without serving any traffic.

By default it loads its PHP engine bridge as `libproteus-php-mod.so`, found via the dynamic linker's normal search path. Set `PROTEUS_PHP_MOD_PATH` to an absolute path to point at a specific file instead.

## envsubst

```console
$ proteus envsubst < input > output
```

Reads stdin to completion, expands `$VAR` and `${VAR}` references against the process environment, and writes the result to stdout. This is the exact same substitution the config file itself goes through before being parsed as JSON — see [Environment variables](configuration.md#environment-variables) for the full syntax (defaults, `$$` escaping, and so on).

It exists standalone so an entrypoint script can template any file with identical rules — a `php.ini` snippet shared with the PHP CLI, for example — not just `proteus`'s own config:

```console
$ echo 'memory_limit = ${PHP_MEMORY_LIMIT:-256M}' | proteus envsubst
memory_limit = 256M
```

A reference to an unset variable with no default fails the same way it would in the config file: the process exits with an error naming the missing variable, rather than substituting an empty string.

## cron

```console
$ proteus cron [--user <user> --group <group>] [--shutdown-grace <duration>] <crontab>
```

Runs every job in a crontab file, each on its own schedule, until it receives `SIGTERM` or `SIGINT`. This is a small, self-contained cron daemon meant to run as the one PID in a container alongside (or instead of) `proteus --config`, not a wrapper around the system's own `cron`/`crond`.

| Option | Default | Description |
|---|---|---|
| `<crontab>` *(required)* | — | Path to the crontab file to run. |
| `--user` | inherited | OS user to run every job as. Must be given together with `--group`. |
| `--group` | inherited | OS group to run every job as. Must be given together with `--user`. Omitting both runs jobs as whatever identity `proteus cron` itself has — the same rule as `php.user`/`php.group`. |
| `--shutdown-grace` | `15s` | How long a running job gets, after the shutdown signal, before it's `SIGKILL`ed. Accepts a human duration such as `30s`, `2m`. |

### Crontab format

One job per line: five whitespace-separated schedule fields, then the command as the rest of the line, verbatim.

```
# minute hour day-of-month month day-of-week command
*/5 *    *   *     *              /usr/local/bin/example-job.sh
0   3    *   *     *              php /var/www/bin/nightly-cleanup.php
```

Blank lines and lines starting with `#` are skipped. The schedule fields use standard cron syntax — `*`, ranges (`1-5`), lists (`1,15`), and steps (`*/5`).

A malformed line fails startup with its line number, rather than silently skipping a broken job.

### Job environment

Each job runs as `sh -c "<command>"`, in a fresh, minimal environment — not the one `proteus cron` itself was started with:

| Variable | Value |
|---|---|
| `HOME` | The identity's home directory. |
| `LOGNAME`, `USER` | The identity's username. |
| `SHELL` | `/bin/sh` |
| `PATH` | `/usr/bin:/bin` |
| `TZ` | Passed through from `proteus cron`'s own environment, if set. |

The job's working directory is the identity's home directory. Its stdout and stderr are inherited directly, not captured into the structured log.

> [!NOTE]
> Each job also runs in its own process group with `PR_SET_NO_NEW_PRIVS` set, the same privilege-escalation guard `php.no_new_privs` gives PHP workers.

### Scheduling and overlap

Every job has its own independent loop: compute the next occurrence from *now*, sleep to it, run the command, then repeat — there's no shared per-minute tick. One consequence falls out of this for free: a job's next occurrence is only ever computed after its current run has finished, so **the same job never overlaps itself**. If a run takes long enough that one or more of its own scheduled occurrences would have fallen inside it, those occurrences are simply skipped, not queued up to run back-to-back afterward.

Two different jobs, on the other hand, run fully independently and can overlap each other freely.

### Shutdown

On `SIGTERM` or `SIGINT`, every job currently running is sent `SIGCONT` (in case it was stopped) and then `SIGTERM`, addressed to its whole process group so a shell pipeline's children are reached too, not just `sh` itself. `proteus cron` then waits up to `--shutdown-grace` for them to exit before `SIGKILL`ing whatever is left. New jobs are not started once shutdown begins.
