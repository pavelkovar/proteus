use super::*;

#[test]
fn parses_a_simple_job() {
    let jobs = parse("*/5 * * * * cd /var/www/public && php bin/console app:process-orders\n")
        .expect("should parse");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].line, 1);
    assert_eq!(
        jobs[0].command,
        "cd /var/www/public && php bin/console app:process-orders"
    );
}

#[test]
fn skips_blank_lines_and_comments() {
    let jobs =
        parse("\n# a comment\n   \n* * * * * echo hi\n  # indented comment\n0 0 * * * echo bye\n")
            .expect("should parse");
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].line, 4);
    assert_eq!(jobs[1].line, 6);
}

#[test]
fn rejects_a_line_with_too_few_fields() {
    let err = parse("* * * * echo hi\n").unwrap_err();
    assert!(err.contains("line 1"), "unexpected error: {err}");
}

#[test]
fn rejects_a_line_with_no_command() {
    let err = parse("* * * * *\n").unwrap_err();
    assert!(err.contains("line 1"), "unexpected error: {err}");
}

#[test]
fn rejects_a_plus_prefixed_day_of_week() {
    let err = parse("* * * 1 +MON echo hi\n").unwrap_err();
    assert!(
        err.contains("croner-specific extension"),
        "unexpected error: {err}"
    );
}

#[test]
fn rejects_an_invalid_schedule_expression() {
    let err = parse("99 * * * * echo hi\n").unwrap_err();
    assert!(err.contains("line 1"), "unexpected error: {err}");
}

#[test]
fn reports_the_correct_line_number_for_a_later_malformed_line() {
    let err = parse("* * * * * echo hi\nnot-enough-fields\n").unwrap_err();
    assert!(err.contains("line 2"), "unexpected error: {err}");
}

#[test]
fn preserves_extra_whitespace_within_the_command() {
    let jobs = parse("* * * * *   echo   hi  there\n").expect("should parse");
    assert_eq!(jobs[0].command, "echo   hi  there");
}

#[test]
fn env_lines_apply_to_the_jobs_below_them_only() {
    let jobs = parse(
        "* * * * * first\n\
         APP_ENV=prod\n\
         * * * * * second\n\
         APP_ENV = \"staging\"\n\
         GREETING='hello world'\n\
         EMPTY=\n\
         * * * * * third\n",
    )
    .expect("should parse");
    assert!(jobs[0].env.is_empty());
    assert_eq!(jobs[1].env, vec![("APP_ENV".into(), "prod".into())]);
    assert_eq!(
        jobs[2].env,
        vec![
            ("APP_ENV".into(), "staging".into()),
            ("GREETING".into(), "hello world".into()),
            ("EMPTY".into(), String::new()),
        ]
    );
}

#[test]
fn only_a_valid_name_before_the_equals_sign_makes_an_env_line() {
    assert_eq!(
        parse_env_line("PATH=/a:/b"),
        Some(("PATH".into(), "/a:/b".into()))
    );
    assert_eq!(
        parse_env_line("_X1 = 'a=b'"),
        Some(("_X1".into(), "a=b".into()))
    );
    assert_eq!(parse_env_line("Q=\"a'"), Some(("Q".into(), "\"a'".into())));
    assert_eq!(parse_env_line("1X=a"), None);
    assert_eq!(parse_env_line("*/5 * * * * FOO=bar cmd"), None);
    assert_eq!(parse_env_line("0 3 * * * php a.php --x=1"), None);
}

#[test]
fn a_shell_other_than_bin_sh_is_refused() {
    assert!(parse("SHELL=/bin/sh\n* * * * * true\n").is_ok());
    let err = parse("SHELL=/bin/bash\n* * * * * true\n").unwrap_err();
    assert!(err.contains("line 1"), "unexpected error: {err}");
}
