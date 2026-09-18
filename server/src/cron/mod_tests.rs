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
