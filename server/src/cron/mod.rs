//! Crontab parsing: `minute hour day-of-month month day-of-week command`,
//! one job per line, blank lines and `#` comments skipped, and `NAME=value`
//! lines setting a variable for the jobs below them, as cronie does.

mod exec;
mod scheduler;

pub(crate) use exec::Identity;
pub(crate) use scheduler::run;

use std::str::FromStr;

#[derive(Debug)]
pub(crate) struct CronJob {
    /// 1-based, for error messages and as the overlap-tracking identity.
    pub(crate) line: usize,
    pub(crate) schedule: croner::Cron,
    pub(crate) command: String,
    /// The crontab's `NAME=value` lines above this job.
    pub(crate) env: Vec<(String, String)>,
}

pub(crate) fn parse(text: &str) -> Result<Vec<CronJob>, String> {
    let mut jobs = Vec::new();
    let mut env: Vec<(String, String)> = Vec::new();
    for (i, raw_line) in text.lines().enumerate() {
        let line = i + 1;
        let trimmed = raw_line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((name, value)) = parse_env_line(trimmed) {
            if name == "SHELL" && value != "/bin/sh" {
                return Err(format!(
                    "line {line}: jobs always run under /bin/sh, so SHELL={value} would only mislead them"
                ));
            }
            match env.iter_mut().find(|(n, _)| *n == name) {
                Some(slot) => slot.1 = value,
                None => env.push((name, value)),
            }
            continue;
        }
        let (expr, command) = split_schedule_and_command(trimmed)
            .ok_or_else(|| format!("line {line}: expected 5 schedule fields and a command"))?;
        if expr.contains('+') {
            return Err(format!(
                "line {line}: '+' in the day-of-week field is a croner-specific extension \
                 (opts into AND instead of the usual OR with day-of-month) and is not accepted here"
            ));
        }
        let schedule = croner::Cron::from_str(&expr)
            .map_err(|e| format!("line {line}: invalid schedule {expr:?}: {e}"))?;
        jobs.push(CronJob {
            line,
            schedule,
            command: command.to_string(),
            env: env.clone(),
        });
    }
    Ok(jobs)
}

/// cronie's rules: blanks around `=`, matching quotes stripped. A job line never
/// matches, as no schedule field starts like a variable name.
fn parse_env_line(line: &str) -> Option<(String, String)> {
    let (name, value) = line.split_once('=')?;
    let name = name.trim_end();
    let mut chars = name.chars();
    let first = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return None;
    }
    let value = value.trim();
    let value = ['"', '\'']
        .iter()
        .find_map(|&q| value.strip_prefix(q)?.strip_suffix(q))
        .unwrap_or(value);
    Some((name.to_string(), value.to_string()))
}

/// Splits `line` into its 5 whitespace-separated schedule fields (rejoined
/// with single spaces) and the remaining command text, verbatim. `None` if
/// fewer than 5 fields or no command follows.
fn split_schedule_and_command(line: &str) -> Option<(String, &str)> {
    let mut rest = line;
    let mut fields = Vec::with_capacity(5);
    for _ in 0..5 {
        let (field, remainder) = rest.split_once(char::is_whitespace)?;
        if field.is_empty() {
            return None;
        }
        fields.push(field);
        rest = remainder.trim_start();
    }
    if rest.is_empty() {
        return None;
    }
    Some((fields.join(" "), rest))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
