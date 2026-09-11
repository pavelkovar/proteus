use super::*;

#[test]
fn parses_a_full_config() {
    let json = r#"
    {
      "listen": ["0.0.0.0:8080"],
      "routes": [
        { "match": { "uri": ["/uploads/*"] }, "action": "static", "root": "/var/www/uploads" },
        { "match": {}, "action": "static", "root": "/var/www/public",
          "fallback": { "action": "php", "target": "app" } }
      ],
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "targets": { "app": { "root": "/var/www", "script": "index.php" } },
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 },
        "queue": { "timeout": 5 }
      }
    }"#;
    let cfg: Config = serde_json::from_str(json).expect("should parse");
    assert_eq!(cfg.listen, vec!["0.0.0.0:8080"]);
    assert_eq!(cfg.routes.len(), 2);
    assert_eq!(
        cfg.routes[1].action,
        RouteActionConfig::Static {
            root: "/var/www/public".into(),
            fallback: Some(Box::new(RouteActionConfig::Php {
                target: "app".into()
            })),
        }
    );
    assert_eq!(cfg.php.limits.requests, 500);
    assert_eq!(cfg.status.listen, "127.0.0.1:8081"); // default applied
    assert_eq!(cfg.compression.min_size_bytes, 1024); // default applied
    assert_eq!(cfg.max_body_size, 64 * 1024 * 1024); // default applied
}

#[test]
fn queue_and_shutdown_default_when_entirely_omitted() {
    let json = r#"
    {
      "listen": ["0.0.0.0:8080"],
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 }
      }
    }"#;
    let cfg: Config = serde_json::from_str(json).expect("should parse");
    assert_eq!(cfg.php.queue.timeout, 5);
    assert_eq!(cfg.php.queue.max_depth, 512);
    assert_eq!(cfg.php.shutdown.grace_period_seconds, 15);
}

#[test]
fn queue_and_shutdown_respect_explicit_values() {
    let json = r#"
    {
      "listen": ["0.0.0.0:8080"],
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 },
        "queue": { "timeout": 30, "max_depth": 0 },
        "shutdown": { "grace_period_seconds": 0 }
      }
    }"#;
    let cfg: Config = serde_json::from_str(json).expect("should parse");
    assert_eq!(cfg.php.queue.timeout, 30);
    assert_eq!(cfg.php.queue.max_depth, 0);
    assert_eq!(cfg.php.shutdown.grace_period_seconds, 0);
}

#[test]
fn max_body_size_respects_an_explicit_value() {
    let json = r#"
    {
      "listen": ["0.0.0.0:8080"],
      "max_body_size": 1048576,
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 },
        "queue": { "timeout": 5 }
      }
    }"#;
    let cfg: Config = serde_json::from_str(json).expect("should parse");
    assert_eq!(cfg.max_body_size, 1048576);
}

#[test]
fn trusted_proxies_defaults_to_empty_and_parses_cidrs() {
    let json = r#"
    {
      "listen": ["0.0.0.0:8080"],
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 },
        "queue": { "timeout": 5 }
      }
    }"#;
    let cfg: Config = serde_json::from_str(json).expect("should parse");
    assert!(cfg.trusted_proxies.is_empty());

    let json_with_proxies = json.replace(
        r#""listen": ["0.0.0.0:8080"],"#,
        r#""listen": ["0.0.0.0:8080"], "trusted_proxies": ["10.0.0.0/8", "192.168.1.1"],"#,
    );
    let cfg: Config = serde_json::from_str(&json_with_proxies).expect("should parse");
    assert_eq!(cfg.trusted_proxies.len(), 2);
    assert!(cfg.trusted_proxies[0].contains("10.1.2.3".parse().unwrap()));
    assert!(cfg.trusted_proxies[1].contains("192.168.1.1".parse().unwrap()));
}

#[test]
fn rejects_an_invalid_cidr_in_trusted_proxies() {
    // Must fail config parsing outright rather than yield a default network
    // that silently matches everything or nothing at request time.
    let json = r#"
    {
      "listen": ["0.0.0.0:8080"],
      "trusted_proxies": ["not-a-cidr"],
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 },
        "queue": { "timeout": 5 }
      }
    }"#;
    let result: Result<Config, _> = serde_json::from_str(json);
    assert!(
        result.is_err(),
        "a malformed CIDR must fail config parsing, not parse into some default network"
    );
}

#[test]
fn rejects_missing_required_field() {
    let json = r#"{ "listen": ["0.0.0.0:8080"], "php": {} }"#;
    let result: Result<Config, _> = serde_json::from_str(json);
    assert!(result.is_err(), "php.user/group/limits/... are required");
}

#[test]
fn rejects_php_route_with_no_target() {
    let json = r#"{ "match": {}, "action": "php" }"#;
    let result: Result<Route, _> = serde_json::from_str(json);
    assert!(
        result.is_err(),
        "Php.target is required, not Option<String>"
    );
}

fn base_config_json(routes: &str, targets: &str) -> String {
    format!(
        r#"{{
          "listen": ["0.0.0.0:8080"],
          "routes": [{routes}],
          "php": {{
            "user": "phpapp",
            "group": "phpapp",
            "targets": {{ {targets} }},
            "limits": {{ "requests": 500, "timeout": 30 }},
            "processes": {{ "max": 20, "spare": 4 }},
            "queue": {{ "timeout": 5 }}
          }}
        }}"#
    )
}

#[test]
fn parses_targets_with_script_and_index() {
    let json = base_config_json(
        "",
        r#""api": { "root": "/var/www/api/public", "script": "index.php" },
           "legacy": { "root": "/var/www/legacy", "index": "main.php" }"#,
    );
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.targets["api"].root, "/var/www/api/public");
    assert_eq!(cfg.php.targets["api"].script.as_deref(), Some("index.php"));
    assert_eq!(cfg.php.targets["api"].index, None);
    assert_eq!(cfg.php.targets["legacy"].script, None);
    assert_eq!(cfg.php.targets["legacy"].index.as_deref(), Some("main.php"));
}

#[test]
fn validate_accepts_route_target_defined_in_php_targets() {
    let json = base_config_json(
        r#"{ "match": { "uri": ["/api/*"] }, "action": "php", "target": "api" }"#,
        r#""api": { "root": "/var/www/api/public", "script": "index.php" }"#,
    );
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(validate(&cfg), Vec::<String>::new());
}

#[test]
fn validate_rejects_route_target_missing_from_php_targets() {
    let json = base_config_json(
        r#"{ "match": { "uri": ["/api/*"] }, "action": "php", "target": "typo" }"#,
        r#""api": { "root": "/var/www/api/public", "script": "index.php" }"#,
    );
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("typo"),
        "unexpected error: {}",
        errors[0]
    );
}

#[test]
fn validate_rejects_a_zero_limits_timeout() {
    let json = base_config_json("", "").replace(r#""timeout": 30"#, r#""timeout": 0"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("php.limits.timeout")),
        "expected a php.limits.timeout error, got: {errors:?}"
    );
}

#[test]
fn validate_rejects_a_zero_limits_requests() {
    let json = base_config_json("", "").replace(r#""requests": 500"#, r#""requests": 0"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("php.limits.requests")),
        "expected a php.limits.requests error, got: {errors:?}"
    );
}

/// A `/0` entry hands every client on the Internet the power to set its own
/// `X-Forwarded-For` identity, which is exactly the rate-limit bypass the
/// trusted-proxy gate exists to close.
#[test]
fn validate_rejects_a_trusted_proxies_entry_covering_every_address() {
    for cidr in ["0.0.0.0/0", "::/0"] {
        let json = base_config_json("", "").replace(
            r#""listen""#,
            &format!(r#""trusted_proxies": ["{cidr}"], "listen""#),
        );
        let cfg: Config = serde_json::from_str(&json).expect("should parse");
        let errors = validate(&cfg);
        assert!(
            errors.iter().any(|e| e.contains("trusted_proxies")),
            "expected a trusted_proxies error for {cidr}, got: {errors:?}"
        );
    }
}

#[test]
fn validate_accepts_a_narrow_trusted_proxies_entry() {
    let json = base_config_json("", "").replace(
        r#""listen""#,
        r#""trusted_proxies": ["10.0.0.0/8", "192.168.0.0/16"], "listen""#,
    );
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(validate(&cfg), Vec::<String>::new());
}

#[test]
fn validate_rejects_a_zero_queue_timeout() {
    let json = base_config_json("", "").replace(r#""timeout": 5"#, r#""timeout": 0"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("php.queue.timeout")),
        "expected a php.queue.timeout error, got: {errors:?}"
    );
}

#[test]
fn php_user_and_group_default_to_absent_and_parse_together() {
    let json = base_config_json("", "")
        .replace(r#""user": "phpapp","#, "")
        .replace(r#""group": "phpapp","#, "");
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.user, None);
    assert_eq!(cfg.php.group, None);
    assert_eq!(
        validate(&cfg),
        Vec::<String>::new(),
        "omitting both together must not be a validation error"
    );
}

#[test]
fn validate_rejects_specifying_only_one_of_php_user_and_group() {
    let json = base_config_json("", "").replace(r#""group": "phpapp","#, "");
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.user.as_deref(), Some("phpapp"));
    assert_eq!(cfg.php.group, None);
    let errors = validate(&cfg);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("php.user") && e.contains("php.group")),
        "expected a php.user/php.group mismatch error, got: {errors:?}"
    );
}

#[test]
fn validate_walks_into_a_nested_fallback_target() {
    let json = base_config_json(
        r#"{ "match": {}, "action": "static", "root": "/var/www/public",
             "fallback": { "action": "php", "target": "typo" } }"#,
        r#""api": { "root": "/var/www/api/public", "script": "index.php" }"#,
    );
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].contains("typo"),
        "unexpected error: {}",
        errors[0]
    );
}

#[test]
fn route_match_parses_uri_patterns() {
    let json = r#"{ "match": { "uri": ["*.php", "~^/\\.", "/administrator/", "*"] },
                     "action": "return", "status": 403 }"#;
    let route: Route = serde_json::from_str(json).expect("should parse");
    assert_eq!(route.matcher.uri.len(), 4);
    assert!(matches!(
        &route.matcher.uri[0],
        MatchPattern::Glob { leading: true, trailing: false, parts, .. } if parts.as_slice() == [".php"]
    ));
    assert!(matches!(&route.matcher.uri[1], MatchPattern::Regex(_)));
    assert!(matches!(&route.matcher.uri[2], MatchPattern::Exact(s) if s == "/administrator/"));
    assert!(matches!(&route.matcher.uri[3], MatchPattern::Any));
    assert_eq!(route.action, RouteActionConfig::Return { status: 403 });
}

#[test]
fn route_match_parses_negated_uri_patterns() {
    let json = r#"{ "match": { "uri": ["!/admin/secret*", "!~^/\\.", "/admin/*"] },
                     "action": "return", "status": 403 }"#;
    let route: Route = serde_json::from_str(json).expect("should parse");
    assert!(matches!(
        &route.matcher.uri[0],
        MatchPattern::Not(inner) if matches!(&**inner, MatchPattern::Glob { leading: false, trailing: true, parts, .. } if parts.as_slice() == ["/admin/secret"])
    ));
    assert!(
        matches!(&route.matcher.uri[1], MatchPattern::Not(inner) if matches!(**inner, MatchPattern::Regex(_)))
    );
    assert!(matches!(
        &route.matcher.uri[2],
        MatchPattern::Glob { leading: false, trailing: true, parts, .. } if parts.as_slice() == ["/admin/"]
    ));
}

#[test]
fn uri_pattern_rejects_an_empty_string() {
    let err = MatchPattern::try_from(String::new()).unwrap_err();
    assert!(err.contains("empty"), "unexpected error: {err}");
}

#[test]
fn uri_pattern_rejects_a_bare_negation_with_nothing_to_negate() {
    let err = MatchPattern::try_from("!".to_string()).unwrap_err();
    assert!(err.contains("empty"), "unexpected error: {err}");
}

#[test]
fn route_match_parses_method_alongside_uri() {
    let json = r#"{ "match": { "uri": ["/api/*"], "method": ["POST", "PUT"] },
                     "action": "return", "status": 403 }"#;
    let route: Route = serde_json::from_str(json).expect("should parse");
    assert_eq!(route.matcher.uri.len(), 1);
    assert_eq!(route.matcher.method.len(), 2);
    assert!(matches!(&route.matcher.method[0], MatchPattern::Exact(s) if s == "POST"));
    assert!(matches!(&route.matcher.method[1], MatchPattern::Exact(s) if s == "PUT"));
}

#[test]
fn route_match_parses_host_alongside_uri_and_method() {
    let json = r#"{ "match": { "uri": ["/*"], "method": ["GET"], "host": ["example.test", "!admin.example.test"] },
                     "action": "return", "status": 403 }"#;
    let route: Route = serde_json::from_str(json).expect("should parse");
    assert_eq!(route.matcher.host.len(), 2);
    assert!(matches!(&route.matcher.host[0], MatchPattern::Exact(s) if s == "example.test"));
    assert!(matches!(&route.matcher.host[1], MatchPattern::Not(_)));
}

#[test]
fn route_match_can_be_omitted_entirely_as_a_catch_all() {
    let json = r#"{ "action": "return", "status": 403 }"#;
    let route: Route = serde_json::from_str(json).expect("should parse");
    assert!(route.matcher.uri.is_empty());
    assert!(route.matcher.host.is_empty());
}

/// `\d` must stay ASCII-only. Re-enabling Unicode mode compiles fine and
/// passes every ASCII-input test, silently matching a non-ASCII digit and
/// pulling the `unicode-*` features back into the binary.
#[test]
fn regex_match_pattern_digit_class_is_ascii_only_not_unicode() {
    let pattern = MatchPattern::try_from("~^\\d+$".to_string()).unwrap();
    assert!(pattern.matches("123"));
    // A real Unicode decimal digit, which `\d` matches in Unicode mode.
    assert!(!pattern.matches("\u{0663}\u{0663}\u{0663}"));
}

#[test]
fn route_match_rejects_an_invalid_uri_regex() {
    let json = r#"{ "match": { "uri": ["~("] }, "action": "return", "status": 403 }"#;
    let result: Result<Route, _> = serde_json::from_str(json);
    assert!(result.is_err(), "expected an invalid regex to fail parsing");
}

#[test]
fn validate_rejects_an_out_of_range_return_status() {
    let json = base_config_json(r#"{ "match": {}, "action": "return", "status": 1000 }"#, "");
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("return status")),
        "expected a return-status validation error, got: {errors:?}"
    );
}

/// `top_level_extra` is spliced in as a sibling of `listen`/`php`, so it
/// must be its own complete `"key": value,` entries (or empty).
fn config_json(php_extra: &str, top_level_extra: &str) -> String {
    format!(
        r#"{{
          "listen": ["0.0.0.0:8080"],
          {top_level_extra}
          "php": {{
            {php_extra}
            "limits": {{ "requests": 500, "timeout": 30 }},
            "processes": {{ "max": 20, "spare": 4 }},
            "queue": {{ "timeout": 5 }}
          }}
        }}"#
    )
}

fn minimal_config_json(php_extra: &str) -> String {
    config_json(php_extra, "")
}

#[test]
fn parse_substitutes_a_set_variable_outside_uri() {
    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_A", "secret123") };
    let json =
        minimal_config_json(r#""environment": { "SECRET": "${PROTEUS_TEST_INTERPOLATE_A}" },"#);
    let cfg = parse(&json).expect("should parse");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_A") };
    assert_eq!(cfg.php.environment["SECRET"], "secret123");
}

#[test]
fn parse_falls_back_to_default_and_prefers_a_set_value() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_B") };
    let json =
        minimal_config_json(r#""environment": { "PORT": "${PROTEUS_TEST_INTERPOLATE_B:8080}" },"#);
    let cfg = parse(&json).expect("should parse");
    assert_eq!(cfg.php.environment["PORT"], "8080");

    unsafe { std::env::set_var("PROTEUS_TEST_INTERPOLATE_B", "9090") };
    let cfg = parse(&json).expect("should parse");
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_B") };
    assert_eq!(cfg.php.environment["PORT"], "9090");
}

/// The first colon separates name from default, so a default containing one
/// of its own must survive whole.
#[test]
fn parse_default_value_containing_a_colon_survives_whole() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_COLON") };
    let json = minimal_config_json(
        r#""environment": { "BASE_URL": "${PROTEUS_TEST_INTERPOLATE_COLON:http://localhost:8080}" },"#,
    );
    let cfg = parse(&json).expect("should parse");
    assert_eq!(cfg.php.environment["BASE_URL"], "http://localhost:8080");
}

/// The scanner must keep advancing past each match rather than stopping at
/// the first.
#[test]
fn parse_substitutes_multiple_placeholders_in_one_value() {
    unsafe {
        std::env::set_var("PROTEUS_TEST_INTERPOLATE_HOST", "example.test");
        std::env::set_var("PROTEUS_TEST_INTERPOLATE_PORT", "9090");
    }
    let json = minimal_config_json(
        r#""environment": { "URL": "http://${PROTEUS_TEST_INTERPOLATE_HOST}:${PROTEUS_TEST_INTERPOLATE_PORT}/" },"#,
    );
    let cfg = parse(&json).expect("should parse");
    unsafe {
        std::env::remove_var("PROTEUS_TEST_INTERPOLATE_HOST");
        std::env::remove_var("PROTEUS_TEST_INTERPOLATE_PORT");
    }
    assert_eq!(cfg.php.environment["URL"], "http://example.test:9090/");
}

#[test]
fn parse_errors_on_missing_variable_without_default() {
    unsafe { std::env::remove_var("PROTEUS_TEST_INTERPOLATE_D") };
    let json = minimal_config_json(r#""environment": { "X": "${PROTEUS_TEST_INTERPOLATE_D}" },"#);
    let err = parse(&json).expect_err("should error");
    assert!(
        err.contains("PROTEUS_TEST_INTERPOLATE_D"),
        "unexpected error: {err}"
    );
}

#[test]
fn substitute_env_errors_on_unterminated_placeholder() {
    let err = substitute_env("${UNCLOSED").unwrap_err();
    assert!(err.contains("closing brace"), "unexpected error: {err}");
}

#[test]
fn parse_leaves_text_without_placeholders_untouched() {
    let cfg = parse(&minimal_config_json("")).expect("should parse");
    assert_eq!(cfg.listen, vec!["0.0.0.0:8080"]);
}

#[test]
fn parse_does_not_mistake_uri_regex_syntax_for_a_placeholder() {
    let json = r#"{
      "listen": ["0.0.0.0:8080"],
      "routes": [
        { "match": { "uri": ["~\\.php$"] }, "action": "return", "status": 403 }
      ],
      "php": {
        "limits": { "requests": 500, "timeout": 30 },
        "processes": { "max": 20, "spare": 4 },
        "queue": { "timeout": 5 }
      }
    }"#;
    let cfg = parse(json)
        .expect("a literal regex $ anchor in match.uri must not be treated as an env var");
    let pattern = &cfg.routes[0].matcher.uri[0];
    assert!(pattern.matches("/index.php"));
    assert!(!pattern.matches("/index.phtml"));
}

#[test]
fn parse_errors_still_report_a_line_and_column() {
    let json = minimal_config_json("\"user\": 12345,");
    let err = parse(&json).expect_err("a number where a string is expected must fail to parse");
    assert!(
        err.contains("line") && err.contains("column"),
        "unexpected error (no location?): {err}"
    );
}

/// 0 reads as disabled but is the opposite here, failing every spawn
/// instantly.
#[test]
fn validate_rejects_a_zero_spawn_timeout() {
    let json = minimal_config_json("").replace(
        r#""processes": { "max": 20, "spare": 4 }"#,
        r#""processes": { "max": 20, "spare": 4, "spawn_timeout": 0 }"#,
    );
    let cfg = parse(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("spawn_timeout")),
        "expected a spawn_timeout error, got: {errors:?}"
    );
}

/// An absent `connection` block must reach these defaults, not zeroes: a 0
/// would disable `body_read_timeout` silently rather than fail.
#[test]
fn connection_timeouts_default_when_the_whole_block_is_absent() {
    let cfg = parse(&minimal_config_json("")).expect("should parse");
    assert_eq!(cfg.connection.body_read_timeout, 60);
    assert_eq!(cfg.connection.header_read_timeout, 10);
    assert_eq!(cfg.connection.idle_timeout, 65);
    // Derived from the core count, so only the zero case is worth asserting:
    // a 0 there would disable the cap outright.
    assert_ne!(cfg.connection.max, 0);
    assert!(
        validate(&cfg).is_empty(),
        "the defaults must themselves be valid"
    );
}

/// Omitted, it must land on the generous default: a short one makes a cold
/// start's PHP init look like a wedged prototype.
#[test]
fn spawn_timeout_defaults_when_absent() {
    let cfg = parse(&minimal_config_json("")).expect("should parse");
    assert_eq!(cfg.php.processes.spawn_timeout, 30);
    assert!(
        validate(&cfg).is_empty(),
        "the default must itself be valid"
    );
}

fn config_json_with_rate_limit(rate_limit_json: &str) -> String {
    config_json("", &format!(r#""rate_limit": {rate_limit_json},"#))
}

#[test]
fn rate_limit_is_none_when_the_section_is_absent() {
    let cfg = parse(&minimal_config_json("")).expect("should parse");
    assert!(cfg.rate_limit.is_none());
    assert!(validate(&cfg).is_empty());
}

#[test]
fn rate_limit_parses_with_and_without_user_agent() {
    let cfg = parse(&config_json_with_rate_limit(
        r#"{ "requests": 100, "period_seconds": 60 }"#,
    ))
    .expect("should parse");
    let rl = cfg.rate_limit.expect("rate_limit should be Some");
    assert_eq!(rl.requests, 100);
    assert_eq!(rl.period_seconds, 60);
    assert!(rl.user_agent.is_empty());

    let cfg = parse(&config_json_with_rate_limit(
        r#"{ "requests": 100, "period_seconds": 60, "user_agent": ["*GPTBot*", "*ClaudeBot*"] }"#,
    ))
    .expect("should parse");
    assert_eq!(
        cfg.rate_limit
            .expect("rate_limit should be Some")
            .user_agent
            .len(),
        2
    );
}

#[test]
fn validate_rejects_a_zero_rate_limit_requests() {
    let cfg = parse(&config_json_with_rate_limit(
        r#"{ "requests": 0, "period_seconds": 60 }"#,
    ))
    .expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("rate_limit.requests")),
        "expected a rate_limit.requests error, got: {errors:?}"
    );
}

#[test]
fn validate_rejects_a_zero_rate_limit_period_seconds() {
    let cfg = parse(&config_json_with_rate_limit(
        r#"{ "requests": 100, "period_seconds": 0 }"#,
    ))
    .expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("rate_limit.period_seconds")),
        "expected a rate_limit.period_seconds error, got: {errors:?}"
    );
}
