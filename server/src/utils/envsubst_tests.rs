use super::*;

/// The operator is matched before the closing brace, so a default containing
/// a colon of its own must survive whole.
#[test]
fn default_value_containing_a_colon_survives_whole() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_COLON") };
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_COLON:-http://localhost:8080}");
    assert_eq!(result.unwrap(), "http://localhost:8080");
}

/// `-` (no colon) only falls back when the variable is entirely unset; a
/// set-but-empty value is kept as-is, unlike the `:-` form.
#[test]
fn dash_default_is_used_only_when_unset_not_when_empty() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_DASH") };
    assert_eq!(
        substitute("${PROTEUS_TEST_INTERPOLATE_DASH-fallback}").unwrap(),
        "fallback"
    );

    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_DASH", "") };
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_DASH-fallback}");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_DASH") };
    assert_eq!(
        result.unwrap(),
        "",
        "set-but-empty must not trigger `-`'s default"
    );
}

/// `:-` treats a set-but-empty value the same as unset.
#[test]
fn colon_dash_default_is_used_when_unset_or_empty() {
    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_CDASH", "") };
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_CDASH:-fallback}");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_CDASH") };
    assert_eq!(result.unwrap(), "fallback");
}

/// `=`/`:=` choose the same value as `-`/`:-` - the distinct bash meaning
/// (assign the variable) has nothing to assign to in a one-shot expansion.
#[test]
fn equals_forms_behave_like_dash_forms() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_EQ") };
    assert_eq!(
        substitute("${PROTEUS_TEST_INTERPOLATE_EQ=fallback}").unwrap(),
        "fallback"
    );

    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_EQ", "") };
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_EQ:=fallback}");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_EQ") };
    assert_eq!(result.unwrap(), "fallback");
}

#[test]
fn plus_form_substitutes_other_only_when_set() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_PLUS") };
    assert_eq!(
        substitute("${PROTEUS_TEST_INTERPOLATE_PLUS+other}").unwrap(),
        ""
    );

    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_PLUS", "anything") };
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_PLUS+other}");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_PLUS") };
    assert_eq!(result.unwrap(), "other");
}

/// `:+` additionally treats a set-but-empty value as absent, unlike `+`.
#[test]
fn colon_plus_form_treats_an_empty_value_as_absent() {
    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_CPLUS", "") };
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_CPLUS:+other}");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_CPLUS") };
    assert_eq!(result.unwrap(), "");
}

#[test]
fn substitute_rejects_an_unrecognised_operator() {
    let err = substitute("${NAME:x}").unwrap_err();
    assert!(
        err.contains("invalid substitution"),
        "unexpected error: {err}"
    );
}

#[test]
fn substitute_errors_on_unterminated_placeholder() {
    let err = substitute("${UNCLOSED").unwrap_err();
    assert!(err.contains("closing brace"), "unexpected error: {err}");
}

/// The scan advances past each match in the *original* text; a substituted
/// value is never rescanned for a `${...}` of its own.
#[test]
fn substitute_does_not_recursively_expand_a_substituted_value() {
    unsafe {
        std::env::set_var(
            "PROTEUS_TEST_INTERPOLATE_INNER",
            "${PROTEUS_TEST_INTERPOLATE_OUTER}",
        );
        std::env::remove_var("PROTEUS_TEST_INTERPOLATE_OUTER");
    }
    let result = substitute("${PROTEUS_TEST_INTERPOLATE_INNER}")
        .expect("the unset OUTER variable must not be resolved");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_INNER") };
    assert_eq!(result, "${PROTEUS_TEST_INTERPOLATE_OUTER}");
}

#[test]
fn bare_dollar_var_expands_like_braced_form() {
    unsafe { std::env::set_var("PROTEUS_TEST_BARE", "world") };
    let result = substitute("hello $PROTEUS_TEST_BARE!");
    unsafe { std::env::remove_var("PROTEUS_TEST_BARE") };
    assert_eq!(result.unwrap(), "hello world!");
}

#[test]
fn bare_dollar_var_errors_when_unset() {
    unsafe { std::env::remove_var("PROTEUS_TEST_BARE_UNSET") };
    let err = substitute("$PROTEUS_TEST_BARE_UNSET").unwrap_err();
    assert!(
        err.contains("PROTEUS_TEST_BARE_UNSET"),
        "unexpected error: {err}"
    );
}

/// A regex end-of-string anchor like `~\.php$`, or any other `$` not
/// immediately followed by an identifier character, must not be mistaken for
/// a placeholder - this is what keeps route `match.uri` patterns safe.
#[test]
fn dollar_not_followed_by_an_identifier_stays_literal() {
    for text in ["price: $5", "trailing $", "spaced $ out", r"~\.php$"] {
        assert_eq!(substitute(text).unwrap(), text);
    }
}

#[test]
fn double_dollar_escapes_a_bare_variable() {
    unsafe { std::env::remove_var("PROTEUS_TEST_ESCAPED_BARE") };
    assert_eq!(
        substitute("$$PROTEUS_TEST_ESCAPED_BARE").unwrap(),
        "$PROTEUS_TEST_ESCAPED_BARE"
    );
}

#[test]
fn double_dollar_escapes_a_braced_variable() {
    unsafe { std::env::remove_var("PROTEUS_TEST_ESCAPED_BRACED") };
    assert_eq!(
        substitute("$${PROTEUS_TEST_ESCAPED_BRACED}").unwrap(),
        "${PROTEUS_TEST_ESCAPED_BRACED}"
    );
}

/// The scanner must keep advancing past each match rather than stopping at
/// the first.
#[test]
fn substitutes_multiple_placeholders_in_one_value() {
    unsafe {
        std::env::set_var("PROTEUS_TEST_INTERPOLATE_HOST", "example.test");
        std::env::set_var("PROTEUS_TEST_INTERPOLATE_PORT", "9090");
    }
    let result =
        substitute("http://${PROTEUS_TEST_INTERPOLATE_HOST}:${PROTEUS_TEST_INTERPOLATE_PORT}/");
    unsafe {
        std::env::remove_var("PROTEUS_TEST_INTERPOLATE_HOST");
        std::env::remove_var("PROTEUS_TEST_INTERPOLATE_PORT");
    }
    assert_eq!(result.unwrap(), "http://example.test:9090/");
}
