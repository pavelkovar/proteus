use super::*;

#[test]
fn uri_pattern_matches_suffix_prefix_contains_exact_and_any() {
    let suffix: MatchPattern = "*.php".to_string().try_into().unwrap();
    assert!(suffix.matches("/index.php"));
    assert!(!suffix.matches("/index.phtml"));

    let contains: MatchPattern = "*.php/*".to_string().try_into().unwrap();
    assert!(contains.matches("/index.php/extra"));
    assert!(!contains.matches("/index.php"));

    let prefix: MatchPattern = "/admin*".to_string().try_into().unwrap();
    assert!(prefix.matches("/admin/panel"));
    assert!(!prefix.matches("/other"));

    let exact: MatchPattern = "/administrator/".to_string().try_into().unwrap();
    assert!(exact.matches("/administrator/"));
    assert!(!exact.matches("/administrator/x"));

    let any: MatchPattern = "*".to_string().try_into().unwrap();
    assert!(any.matches("/anything"));
}

#[test]
fn uri_pattern_glob_supports_multiple_and_middle_wildcards() {
    let admin_php: MatchPattern = "/admin/*/*.php".to_string().try_into().unwrap();
    assert!(admin_php.matches("/admin/x/y.php"));
    assert!(!admin_php.matches("/admin/x/y.html"));
    assert!(!admin_php.matches("/other/x/y.php"));

    // Asset-passthrough shape: the segment between the wildcards varies.
    let module_assets: MatchPattern = "/modules/*/assets/*".to_string().try_into().unwrap();
    assert!(module_assets.matches("/modules/blog/assets/style.css"));
    assert!(module_assets.matches("/modules/backend/assets/js/app.js"));
    assert!(!module_assets.matches("/modules/blog/resources/style.css"));
}

/// A wildcard matching zero characters, exactly at the boundary the
/// `min_length` short-circuit is sized for. A change requiring a non-empty
/// span would silently reject these.
#[test]
fn uri_pattern_glob_wildcard_can_match_zero_characters_at_a_boundary() {
    let trailing: MatchPattern = "/admin*".to_string().try_into().unwrap();
    assert!(
        trailing.matches("/admin"),
        "a trailing wildcard must also match zero extra characters"
    );

    let middle: MatchPattern = "ab*cd".to_string().try_into().unwrap();
    assert!(
        middle.matches("abcd"),
        "a middle wildcard must also match zero characters between two literal parts"
    );
    assert!(middle.matches("abXYcd"));
    assert!(
        !middle.matches("ab"),
        "shorter than min_length must still be rejected"
    );
}

#[test]
fn uri_pattern_regex_matches_and_rejects_an_invalid_pattern() {
    let dotfile: MatchPattern = "~/\\.".to_string().try_into().unwrap();
    assert!(dotfile.matches("/.env"));
    assert!(dotfile.matches("/some/.git/config"));
    assert!(!dotfile.matches("/plain/path"));

    let err = MatchPattern::try_from("~(".to_string()).unwrap_err();
    assert!(
        err.contains("invalid match regex"),
        "unexpected error: {err}"
    );
}

#[test]
fn uri_pattern_regex_supports_ascii_perl_classes() {
    let ip: MatchPattern = r"~^/\d+\.\d+\.\d+\.\d+$".to_string().try_into().unwrap();
    assert!(ip.matches("/1.2.3.4"));
    assert!(!ip.matches("/not-an-ip"));
}

#[test]
fn regex_match_pattern_digit_class_is_ascii_only_not_unicode() {
    let pattern = MatchPattern::try_from("~^\\d+$".to_string()).unwrap();
    assert!(pattern.matches("123"));
    // A real Unicode decimal digit, which `\d` matches in Unicode mode.
    assert!(!pattern.matches("\u{0663}\u{0663}\u{0663}"));
}
