use super::*;
use std::os::unix::process::ExitStatusExt;

#[test]
fn resolve_with_neither_user_nor_group_uses_the_current_identity() {
    let identity = Identity::resolve(None, None);
    assert_eq!(identity.uid, None);
    assert_eq!(identity.gid, None);
    assert!(!identity.name.is_empty());
    assert!(!identity.home.is_empty());
}

#[test]
fn resolve_with_both_user_and_group_looks_up_real_ids() {
    // root always exists and is always uid/gid 0, so this is safe in any
    // environment these tests run in (they already run as root in the
    // Docker dev image the integration suite requires).
    let identity = Identity::resolve(Some("root"), Some("root"));
    assert_eq!(identity.uid, Some(0));
    assert_eq!(identity.gid, Some(0));
    assert_eq!(identity.name, "root");
}

#[test]
#[should_panic(expected = "--user and --group must be set together")]
fn resolve_rejects_user_without_group() {
    Identity::resolve(Some("root"), None);
}

#[test]
#[should_panic(expected = "--user and --group must be set together")]
fn resolve_rejects_group_without_user() {
    Identity::resolve(None, Some("root"));
}

#[test]
#[should_panic(expected = "not found")]
fn resolve_panics_on_an_unknown_user() {
    Identity::resolve(Some("proteus-test-no-such-user"), Some("root"));
}

#[tokio::test]
async fn spawn_runs_the_command_and_reports_its_own_process_group() {
    let identity = Identity::resolve(None, None);
    let running = spawn(&identity, "exit 0", &[]).expect("should spawn");
    // A session leader is its own process group leader.
    assert_eq!(running.pgid.as_raw(), running.child.id().unwrap() as i32);
}

#[tokio::test]
async fn spawned_command_runs_with_the_fresh_minimal_environment() {
    let identity = Identity::resolve(None, None);
    let mut running = spawn(
        &identity,
        "[ \"$PATH\" = /usr/local/bin:/usr/bin:/bin ] && [ -n \"$HOME\" ] && [ \"$SHELL\" = /bin/sh ] \
         && [ -z \"$CARGO\" ]",
        &[],
    )
    .expect("should spawn");
    let status = running.child.wait().await.expect("should wait");
    assert!(status.success(), "job did not see the expected environment");
}

#[tokio::test]
async fn signal_group_delivers_to_a_real_process() {
    let identity = Identity::resolve(None, None);
    let mut running = spawn(&identity, "sleep 30", &[]).expect("should spawn");
    signal_group(running.pgid, Signal::SIGTERM);
    let status = running.child.wait().await.expect("should wait");
    assert_eq!(status.signal(), Some(Signal::SIGTERM as i32));
}

#[test]
fn signal_group_tolerates_a_pid_that_is_already_gone() {
    // A pid this large is never a real running process; this must not panic
    // or log anything louder than the tolerated-ESRCH path.
    signal_group(Pid::from_raw(i32::MAX - 1), Signal::SIGTERM);
}

fn env_of(
    identity: &Identity,
    inherited: &[(&str, &str)],
    crontab: &[(&str, &str)],
) -> BTreeMap<String, String> {
    let crontab: Vec<(String, String)> = crontab
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    job_environment(
        identity,
        inherited.iter().map(|(k, v)| (k.into(), v.into())),
        &crontab,
    )
    .into_iter()
    .map(|(k, v)| (k.into_string().unwrap(), v.into_string().unwrap()))
    .collect()
}

#[test]
fn an_inherited_environment_keeps_its_variables_but_not_another_users_identity() {
    let identity = Identity::resolve(Some("root"), Some("root"));
    let env = env_of(
        &identity,
        &[
            ("APP_ENV", "prod"),
            ("PATH", "/opt/php/bin:/usr/bin"),
            ("HOME", "/home/deploy"),
            ("USER", "deploy"),
            ("SHELL", "/bin/bash"),
        ],
        &[],
    );
    assert_eq!(env["APP_ENV"], "prod");
    assert_eq!(env["PATH"], "/opt/php/bin:/usr/bin");
    assert_eq!(env["HOME"], identity.home);
    assert_eq!(env["USER"], "root");
    assert_eq!(env["LOGNAME"], "root");
    assert_eq!(env["SHELL"], "/bin/sh");
}

#[test]
fn crontab_lines_override_everything_below_them() {
    let identity = Identity::resolve(None, None);
    let env = env_of(
        &identity,
        &[("APP_ENV", "prod")],
        &[
            ("APP_ENV", "staging"),
            ("PATH", "/srv/bin"),
            ("HOME", "/srv"),
        ],
    );
    assert_eq!(env["APP_ENV"], "staging");
    assert_eq!(env["PATH"], "/srv/bin");
    assert_eq!(env["HOME"], "/srv");
}

#[tokio::test]
async fn inherit_env_hands_jobs_this_processs_environment() {
    assert!(std::env::var_os("CARGO").is_some());
    let identity = Identity::resolve(None, None).inheriting_env(true);
    let mut running = spawn(
        &identity,
        "[ -n \"$CARGO\" ] && [ \"$FROM_CRONTAB\" = yes ]",
        &[("FROM_CRONTAB".into(), "yes".into())],
    )
    .expect("should spawn");
    assert!(running.child.wait().await.unwrap().success());
}
