use super::*;

#[test]
fn parses_a_full_config() {
    let json = r#"
    {
      "listen": "0.0.0.0:8080",
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
    assert_eq!(cfg.listen, "0.0.0.0:8080");
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
    assert!(cfg.compression.enabled); // default applied
    assert_eq!(cfg.max_body_size, 64 * 1024 * 1024); // default applied
}

#[test]
fn processes_max_and_spare_default_to_one() {
    let json = r#"
    {
      "listen": "0.0.0.0:8080",
      "php": {
        "user": "phpapp",
        "group": "phpapp",
        "limits": { "requests": 500, "timeout": 30 },
        "processes": {}
      }
    }"#;
    let cfg: Config = serde_json::from_str(json).expect("should parse");
    assert_eq!(cfg.php.processes.max, 1);
    assert_eq!(cfg.php.processes.spare, 1);
}

#[test]
fn queue_and_shutdown_default_when_entirely_omitted() {
    let json = r#"
    {
      "listen": "0.0.0.0:8080",
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
      "listen": "0.0.0.0:8080",
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
      "listen": "0.0.0.0:8080",
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
      "listen": "0.0.0.0:8080",
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
        r#""listen": "0.0.0.0:8080","#,
        r#""listen": "0.0.0.0:8080", "trusted_proxies": ["10.0.0.0/8", "192.168.1.1"],"#,
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
      "listen": "0.0.0.0:8080",
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
    let json = r#"{ "listen": "0.0.0.0:8080", "php": {} }"#;
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
          "listen": "0.0.0.0:8080",
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
    assert_eq!(cfg.php.targets["api"].index, "index.php"); // default applied, unused since `script` wins
    assert_eq!(cfg.php.targets["legacy"].script, None);
    assert_eq!(cfg.php.targets["legacy"].index, "main.php");
}

#[test]
fn target_index_defaults_to_index_php_when_omitted() {
    let json = base_config_json("", r#""app": { "root": "/var/www" }"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.targets["app"].index, "index.php");
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
fn limits_timeout_of_zero_disables_the_watchdog() {
    let json = base_config_json("", "").replace(r#""timeout": 30"#, r#""timeout": 0"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(validate(&cfg), Vec::<String>::new());
}

#[test]
fn limits_timeout_defaults_to_zero_when_omitted() {
    let json = base_config_json("", "").replace(r#", "timeout": 30"#, "");
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.limits.timeout, 0);
}

#[test]
fn limits_requests_defaults_to_zero_when_omitted() {
    let json = base_config_json("", "").replace(r#""requests": 500, "#, "");
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.limits.requests, 0);
}

#[test]
fn limits_object_can_be_omitted_entirely() {
    let json =
        base_config_json("", "").replace(r#""limits": { "requests": 500, "timeout": 30 },"#, "");
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(cfg.php.limits.requests, 0);
    assert_eq!(cfg.php.limits.timeout, 0);
}

#[test]
fn limits_requests_of_zero_disables_recycling() {
    let json = base_config_json("", "").replace(r#""requests": 500"#, r#""requests": 0"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert_eq!(validate(&cfg), Vec::<String>::new());
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

/// An operator writing `".php"` or an empty entry gets a startup error, not a
/// gate that quietly matches nothing.
#[test]
fn validate_rejects_malformed_script_extensions() {
    for bad in [r#"[".php"]"#, r#"[""]"#, r#"["php/x"]"#] {
        let json = base_config_json("", "").replace(
            r#""limits""#,
            &format!(r#""script_extensions": {bad}, "limits""#),
        );
        let cfg: Config = serde_json::from_str(&json).expect("should parse");
        let errors = validate(&cfg);
        assert!(
            errors.iter().any(|e| e.contains("script_extensions")),
            "expected an error for {bad}, got: {errors:?}"
        );
    }
}

#[test]
fn validate_rejects_an_empty_script_extensions_list() {
    let json =
        base_config_json("", "").replace(r#""limits""#, r#""script_extensions": [], "limits""#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    assert!(
        validate(&cfg)
            .iter()
            .any(|e| e.contains("script_extensions"))
    );
}

/// A target naming a script the gate would refuse can never serve a request,
/// so it is a startup error rather than a 404 on every hit.
#[test]
fn validate_rejects_a_target_script_the_gate_would_refuse() {
    let json = base_config_json("", r#""api": { "root": "/var/www", "script": "app.inc" }"#);
    let cfg: Config = serde_json::from_str(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("script_extensions")),
        "got: {errors:?}"
    );
}

#[test]
fn script_extensions_defaults_to_php_only() {
    let cfg: Config = serde_json::from_str(&base_config_json("", "")).expect("should parse");
    assert_eq!(cfg.php.script_extensions, vec!["php".to_string()]);
}

/// The names an attacker reaches for when a gate compares suffixes by hand:
/// a dotfile has no extension at all, and a trailing dot or space is a
/// different file that a sloppy `ends_with` would wave through.
#[test]
fn extension_gate_admits_only_exact_listed_extensions() {
    let allowed = vec!["php".to_string()];
    for ok in ["/r/index.php", "/r/a.b/c.php", "/r/..php"] {
        assert!(extension_is_listed(ok, &allowed), "{ok} should be allowed");
    }
    for bad in [
        "/r/uploads/avatar.png",
        "/r/.env",
        "/r/README",
        "/r/a.PHP",
        "/r/a.pHp",
        "/r/a.php.",
        "/r/a.php ",
        "/r/a.phtml",
        "/r/a.php.txt",
    ] {
        assert!(!extension_is_listed(bad, &allowed), "{bad} must be refused");
    }
}

#[test]
fn extension_gate_refuses_everything_when_the_list_is_empty() {
    assert!(!extension_is_listed("/r/index.php", &[]));
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
          "listen": "0.0.0.0:8080",
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
fn parse_leaves_text_without_placeholders_untouched() {
    let cfg = parse(&minimal_config_json("")).expect("should parse");
    assert_eq!(cfg.listen, "0.0.0.0:8080");
}

#[test]
fn parse_does_not_mistake_uri_regex_syntax_for_a_placeholder() {
    let json = r#"{
      "listen": "0.0.0.0:8080",
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

/// A spare floor above the pool's own ceiling could never be satisfied.
#[test]
fn validate_rejects_spare_exceeding_max() {
    let json = minimal_config_json("").replace(
        r#""processes": { "max": 20, "spare": 4 }"#,
        r#""processes": { "max": 4, "spare": 20 }"#,
    );
    let cfg = parse(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("spare") && e.contains("max")),
        "expected a spare-exceeds-max error, got: {errors:?}"
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

/// Absent, this must default to `Info`, matching `logging::MIN_LEVEL`'s own
/// compile-time default - an operator who never sets it must see the same
/// verbosity as before this field existed.
#[test]
fn log_level_defaults_to_info_when_absent() {
    let cfg = parse(&minimal_config_json("")).expect("should parse");
    assert_eq!(cfg.log_level, LogLevel::Info);
    assert_eq!(cfg.log_level.as_level(), crate::logging::level::INFO);
}

/// Each named level must reach the matching numeric ordinal the logging
/// macros compare against, or a config asking for `"warn"` could silently
/// run at some other verbosity.
#[test]
fn log_level_parses_every_named_value() {
    for (name, level, ordinal) in [
        ("debug", LogLevel::Debug, crate::logging::level::DEBUG),
        ("info", LogLevel::Info, crate::logging::level::INFO),
        ("warn", LogLevel::Warn, crate::logging::level::WARN),
        ("error", LogLevel::Error, crate::logging::level::ERROR),
    ] {
        let json = config_json("", &format!(r#""log_level": "{name}","#));
        let cfg = parse(&json).unwrap_or_else(|e| panic!("{name} should parse: {e}"));
        assert_eq!(cfg.log_level, level, "got the wrong variant for {name:?}");
        assert_eq!(
            cfg.log_level.as_level(),
            ordinal,
            "wrong ordinal for {name:?}"
        );
    }
}

/// An unrecognised level must fail to parse rather than silently fall back to
/// a default - a typo in config should never pass as "just use info".
#[test]
fn log_level_rejects_an_unknown_value() {
    let json = config_json("", r#""log_level": "verbose","#);
    assert!(parse(&json).is_err(), "an unknown log level must not parse");
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
fn rate_limit_enabled_defaults_to_false() {
    let cfg = parse(&config_json_with_rate_limit(
        r#"{ "requests": 100, "period_seconds": 60 }"#,
    ))
    .expect("should parse");
    assert!(!cfg.rate_limit.expect("rate_limit should be Some").enabled);

    let cfg = parse(&config_json_with_rate_limit(
        r#"{ "enabled": true, "requests": 100, "period_seconds": 60 }"#,
    ))
    .expect("should parse");
    assert!(cfg.rate_limit.expect("rate_limit should be Some").enabled);
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
fn route_with_no_when_is_always_kept() {
    let json = base_config_json(r#"{ "match": {}, "action": "return", "status": 403 }"#, "");
    let mut cfg = parse(&json).expect("should parse");
    apply_conditions(&mut cfg);
    assert_eq!(cfg.routes.len(), 1);
}

/// Sets/clears `env` (name, value), parses a single route gated by
/// `condition_json`, and asserts the route survives `apply_conditions` iff
/// `expect_kept`.
fn assert_route_kept(condition_json: &str, env: &[(&str, Option<&str>)], expect_kept: bool) {
    for (name, value) in env {
        match value {
            Some(v) => unsafe { std::env::set_var(name, v) },
            None => unsafe { std::env::remove_var(name) },
        }
    }
    let json = base_config_json(
        &format!(
            r#"{{ "when": {condition_json}, "match": {{}}, "action": "return", "status": 404 }}"#
        ),
        "",
    );
    let mut cfg = parse(&json).expect("should parse");
    apply_conditions(&mut cfg);
    for (name, _) in env {
        unsafe { std::env::remove_var(name) };
    }
    assert_eq!(
        cfg.routes.len(),
        expect_kept as usize,
        "condition {condition_json} with env {env:?}: expected kept={expect_kept}"
    );
}

#[test]
fn route_when_env_equals_keeps_the_route_only_on_a_match() {
    let condition = r#"{ "env": { "name": "PROTEUS_TEST_WHEN_A", "equals": "true" } }"#;
    assert_route_kept(condition, &[("PROTEUS_TEST_WHEN_A", None)], false);
    assert_route_kept(condition, &[("PROTEUS_TEST_WHEN_A", Some("false"))], false);
    assert_route_kept(condition, &[("PROTEUS_TEST_WHEN_A", Some("true"))], true);
}

#[test]
fn route_when_env_without_equals_only_requires_the_variable_to_be_set() {
    let condition = r#"{ "env": { "name": "PROTEUS_TEST_WHEN_B" } }"#;
    assert_route_kept(condition, &[("PROTEUS_TEST_WHEN_B", None)], false);
    assert_route_kept(
        condition,
        &[("PROTEUS_TEST_WHEN_B", Some("anything"))],
        true,
    );
}

#[test]
fn route_when_not_inverts_the_inner_condition() {
    let condition = r#"{ "not": { "env": { "name": "PROTEUS_TEST_WHEN_C" } } }"#;
    assert_route_kept(condition, &[("PROTEUS_TEST_WHEN_C", None)], true);
    assert_route_kept(condition, &[("PROTEUS_TEST_WHEN_C", Some("x"))], false);
}

#[test]
fn route_when_all_requires_every_condition() {
    let condition = r#"{ "all": [
        { "env": { "name": "PROTEUS_TEST_WHEN_D1", "equals": "1" } },
        { "env": { "name": "PROTEUS_TEST_WHEN_D2", "equals": "1" } }
    ] }"#;
    assert_route_kept(
        condition,
        &[
            ("PROTEUS_TEST_WHEN_D1", Some("1")),
            ("PROTEUS_TEST_WHEN_D2", None),
        ],
        false,
    );
    assert_route_kept(
        condition,
        &[
            ("PROTEUS_TEST_WHEN_D1", Some("1")),
            ("PROTEUS_TEST_WHEN_D2", Some("1")),
        ],
        true,
    );
}

#[test]
fn route_when_any_requires_at_least_one_condition() {
    let condition = r#"{ "any": [
        { "env": { "name": "PROTEUS_TEST_WHEN_E1", "equals": "1" } },
        { "env": { "name": "PROTEUS_TEST_WHEN_E2", "equals": "1" } }
    ] }"#;
    assert_route_kept(
        condition,
        &[
            ("PROTEUS_TEST_WHEN_E1", None),
            ("PROTEUS_TEST_WHEN_E2", None),
        ],
        false,
    );
    assert_route_kept(
        condition,
        &[
            ("PROTEUS_TEST_WHEN_E1", Some("1")),
            ("PROTEUS_TEST_WHEN_E2", None),
        ],
        true,
    );
}

#[test]
fn validate_still_catches_an_error_in_a_route_that_when_will_later_drop() {
    let json = base_config_json(
        r#"{ "when": { "env": { "name": "PROTEUS_TEST_WHEN_F" } },
             "match": {}, "action": "php", "target": "typo" }"#,
        "",
    );
    unsafe { std::env::remove_var("PROTEUS_TEST_WHEN_F") };
    let cfg = parse(&json).expect("should parse");
    let errors = validate(&cfg);
    assert!(
        errors.iter().any(|e| e.contains("typo")),
        "expected the disabled route's bad target to still be caught, got: {errors:?}"
    );
}

#[test]
fn apply_conditions_runs_after_validate_would_have_passed() {
    let json = base_config_json(
        r#"{ "when": { "env": { "name": "PROTEUS_TEST_WHEN_G" } },
             "match": {}, "action": "php", "target": "api" }"#,
        r#""api": { "root": "/var/www/api/public", "script": "index.php" }"#,
    );
    unsafe { std::env::remove_var("PROTEUS_TEST_WHEN_G") };
    let mut cfg = parse(&json).expect("should parse");
    assert_eq!(validate(&cfg), Vec::<String>::new());
    apply_conditions(&mut cfg);
    assert!(
        cfg.routes.is_empty(),
        "the condition was false, so the route must not survive"
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
