use super::*;
use crate::config::{
    Config, Limits, PhpConfig, Processes, QueueConfig, Route, RouteActionConfig, RouteMatch,
};
use crate::utils::match_pattern::MatchPattern;

fn test_config(routes: Vec<Route>) -> Config {
    Config {
        listen: "127.0.0.1:0".into(),
        routes,
        php: PhpConfig {
            targets: Default::default(),
            environment: Default::default(),
            user: Some("phpapp".into()),
            group: Some("phpapp".into()),
            options: Default::default(),
            script_extensions: vec!["php".to_string()],
            limits: Limits {
                requests: 500,
                timeout: 30,
            },
            processes: Processes {
                max: 4,
                spare: 2,
                idle_timeout: 0,
                spawn_timeout: 30,
            },
            queue: QueueConfig {
                max_depth: 0,
                timeout: 5,
            },
            shutdown: Default::default(),
            no_new_privs: true,
        },
        status: Default::default(),
        compression: Default::default(),
        fs_cache: Default::default(),
        max_body_size: 64 * 1024 * 1024,
        trusted_proxies: Vec::new(),
        connection: Default::default(),
        rate_limit: None,
        log_level: Default::default(),
    }
}

fn static_route(prefix: &str, root: &str) -> Route {
    Route {
        matcher: RouteMatch {
            uri: vec![MatchPattern::try_from(format!("{prefix}*")).unwrap()],
            ..Default::default()
        },
        action: RouteActionConfig::Static {
            root: root.into(),
            fallback: None,
        },
    }
}

#[test]
fn matches_static_route_by_prefix() {
    let cfg = test_config(vec![static_route("/static/", "/var/www/public")]);
    match match_route(&cfg, "/static/logo.png", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Static { root, fallback },
        } => {
            assert_eq!(root, "/var/www/public");
            assert_eq!(*fallback, None);
        }
        other => panic!("expected Static, got {other:?}"),
    }
}

#[test]
fn falls_through_to_no_match_when_nothing_fits() {
    let cfg = test_config(vec![static_route("/static/", "/var/www/public")]);
    assert_eq!(
        match_route(&cfg, "/api/whatever", "GET", ""),
        RouteDecision::NoMatch
    );
}

#[test]
fn first_matching_route_wins() {
    let cfg = test_config(vec![
        static_route("/uploads/", "/var/www/uploads"),
        Route {
            matcher: RouteMatch::default(),
            action: RouteActionConfig::Static {
                root: "/var/www/public".into(),
                fallback: Some(Box::new(RouteActionConfig::Php {
                    target: "app".into(),
                })),
            },
        },
    ]);
    match match_route(&cfg, "/uploads/x.jpg", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Static { root, .. },
        } => {
            assert_eq!(root, "/var/www/uploads")
        }
        other => panic!("expected uploads route, got {other:?}"),
    }
    match match_route(&cfg, "/anything-else", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Static { root, fallback },
        } => {
            assert_eq!(root, "/var/www/public");
            assert_eq!(
                fallback.as_deref(),
                Some(&RouteActionConfig::Php {
                    target: "app".into()
                })
            );
        }
        other => panic!("expected catch-all route, got {other:?}"),
    }
}

#[test]
fn php_target_is_carried_into_the_decision() {
    let cfg = test_config(vec![
        Route {
            matcher: RouteMatch {
                uri: vec![MatchPattern::try_from("/api/*".to_string()).unwrap()],
                ..Default::default()
            },
            action: RouteActionConfig::Php {
                target: "api".into(),
            },
        },
        Route {
            matcher: RouteMatch {
                uri: vec![MatchPattern::try_from("/legacy/*".to_string()).unwrap()],
                ..Default::default()
            },
            action: RouteActionConfig::Static {
                root: "/var/www/legacy".into(),
                fallback: Some(Box::new(RouteActionConfig::Php {
                    target: "legacy".into(),
                })),
            },
        },
    ]);
    match match_route(&cfg, "/api/whoami", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Php { target },
        } => assert_eq!(&**target, "api"),
        other => panic!("expected Php, got {other:?}"),
    }
    match match_route(&cfg, "/legacy/x.php", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Static { fallback, .. },
        } => {
            assert_eq!(
                fallback.as_deref(),
                Some(&RouteActionConfig::Php {
                    target: "legacy".into()
                })
            );
        }
        other => panic!("expected Static, got {other:?}"),
    }
}

fn uri_route(uri_patterns: &[&str], action: RouteActionConfig) -> Route {
    Route {
        matcher: RouteMatch {
            uri: uri_patterns
                .iter()
                .map(|p| MatchPattern::try_from(p.to_string()).unwrap())
                .collect(),
            ..Default::default()
        },
        action,
    }
}

#[test]
fn match_route_requires_a_uri_pattern_to_match_when_present() {
    let cfg = test_config(vec![uri_route(
        &["*.php", "*.php/*", "/administrator/"],
        RouteActionConfig::Php {
            target: "joomla".into(),
        },
    )]);
    match match_route(&cfg, "/index.php", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Php { target },
            ..
        } => assert_eq!(&**target, "joomla"),
        other => panic!("expected Php, got {other:?}"),
    }
    assert_eq!(
        match_route(&cfg, "/index.html", "GET", ""),
        RouteDecision::NoMatch
    );
}

#[test]
fn match_route_supports_a_return_action() {
    let cfg = test_config(vec![uri_route(
        &["~/\\.", "~^\\."],
        RouteActionConfig::Return { status: 403 },
    )]);
    match match_route(&cfg, "/.env", "GET", "") {
        RouteDecision::Matched {
            action: RouteActionConfig::Return { status },
            ..
        } => assert_eq!(*status, 403),
        other => panic!("expected Return, got {other:?}"),
    }
    assert_eq!(
        match_route(&cfg, "/plain", "GET", ""),
        RouteDecision::NoMatch
    );
}

#[test]
fn match_route_combines_negated_and_positive_uri_patterns() {
    let cfg = test_config(vec![uri_route(
        &["/admin/*", "!/admin/secret*"],
        RouteActionConfig::Return { status: 200 },
    )]);
    assert!(matches!(
        match_route(&cfg, "/admin/panel", "GET", ""),
        RouteDecision::Matched { .. }
    ));
    assert_eq!(
        match_route(&cfg, "/admin/secret/keys", "GET", ""),
        RouteDecision::NoMatch
    );
    assert_eq!(
        match_route(&cfg, "/other", "GET", ""),
        RouteDecision::NoMatch
    );
}

#[test]
fn match_route_treats_a_purely_negative_uri_list_as_p_empty() {
    // With nothing non-negated, the rule collapses to a pure exclusion list.
    let cfg = test_config(vec![uri_route(
        &["!*.css", "!*.js"],
        RouteActionConfig::Return { status: 200 },
    )]);
    assert!(matches!(
        match_route(&cfg, "/index.html", "GET", ""),
        RouteDecision::Matched { .. }
    ));
    assert_eq!(
        match_route(&cfg, "/app.js", "GET", ""),
        RouteDecision::NoMatch
    );
}

/// The same collapse for `method`: a pure exclusion list must match
/// everything but what is negated, not silently match nothing.
#[test]
fn match_route_treats_a_purely_negative_method_list_as_p_empty() {
    let cfg = test_config(vec![Route {
        matcher: RouteMatch {
            method: vec!["!OPTIONS".to_string().try_into().unwrap()],
            ..Default::default()
        },
        action: RouteActionConfig::Return { status: 200 },
    }]);
    assert!(matches!(
        match_route(&cfg, "/", "GET", ""),
        RouteDecision::Matched { .. }
    ));
    assert!(matches!(
        match_route(&cfg, "/", "POST", ""),
        RouteDecision::Matched { .. }
    ));
    assert_eq!(
        match_route(&cfg, "/", "OPTIONS", ""),
        RouteDecision::NoMatch
    );
}

#[test]
fn match_route_matches_on_method() {
    let cfg = test_config(vec![Route {
        matcher: RouteMatch {
            method: vec![
                "GET".to_string().try_into().unwrap(),
                "HEAD".to_string().try_into().unwrap(),
            ],
            ..Default::default()
        },
        action: RouteActionConfig::Return { status: 200 },
    }]);
    assert!(matches!(
        match_route(&cfg, "/", "GET", ""),
        RouteDecision::Matched { .. }
    ));
    assert!(matches!(
        match_route(&cfg, "/", "HEAD", ""),
        RouteDecision::Matched { .. }
    ));
    assert_eq!(match_route(&cfg, "/", "POST", ""), RouteDecision::NoMatch);
}

#[test]
fn match_route_ands_uri_and_method() {
    let cfg = test_config(vec![Route {
        matcher: RouteMatch {
            uri: vec!["/api/*".to_string().try_into().unwrap()],
            method: vec!["POST".to_string().try_into().unwrap()],
            ..Default::default()
        },
        action: RouteActionConfig::Return { status: 200 },
    }]);
    assert!(matches!(
        match_route(&cfg, "/api/widgets", "POST", ""),
        RouteDecision::Matched { .. }
    ));

    assert_eq!(
        match_route(&cfg, "/other", "POST", ""),
        RouteDecision::NoMatch
    );

    assert_eq!(
        match_route(&cfg, "/api/widgets", "GET", ""),
        RouteDecision::NoMatch
    );
}

#[test]
fn match_route_matches_on_host() {
    let cfg = test_config(vec![Route {
        matcher: RouteMatch {
            host: vec!["example.test".to_string().try_into().unwrap()],
            ..Default::default()
        },
        action: RouteActionConfig::Return { status: 200 },
    }]);
    assert!(matches!(
        match_route(&cfg, "/", "GET", "example.test"),
        RouteDecision::Matched { .. }
    ));
    assert_eq!(
        match_route(&cfg, "/", "GET", "other.test"),
        RouteDecision::NoMatch
    );
    // A configured list must reject a miss, not read it as no restriction.
    assert_eq!(match_route(&cfg, "/", "GET", ""), RouteDecision::NoMatch);
}

/// `host` arrives pre-lowercased, so patterns must be written lowercase.
/// This covers only the AND with the other fields; the lowercasing itself is
/// the caller's job and is covered at the integration level.
#[test]
fn match_route_ands_host_with_uri_and_method() {
    let cfg = test_config(vec![Route {
        matcher: RouteMatch {
            uri: vec!["/api/*".to_string().try_into().unwrap()],
            method: vec!["GET".to_string().try_into().unwrap()],
            host: vec!["api.example.test".to_string().try_into().unwrap()],
        },
        action: RouteActionConfig::Return { status: 200 },
    }]);
    assert!(matches!(
        match_route(&cfg, "/api/widgets", "GET", "api.example.test"),
        RouteDecision::Matched { .. }
    ));
    assert_eq!(
        match_route(&cfg, "/api/widgets", "GET", "other.example.test"),
        RouteDecision::NoMatch
    );
}

/// Routing runs on the decoded path, so no pattern can be dodged by spelling
/// a character as an escape.
#[test]
fn route_matching_cannot_be_dodged_by_encoding_a_character() {
    let cfg = test_config(vec![Route {
        matcher: RouteMatch {
            uri: vec![MatchPattern::try_from("~\\.php$".to_string()).unwrap()],
            ..Default::default()
        },
        action: RouteActionConfig::Php {
            target: "app".into(),
        },
    }]);
    let raw = "/index%2Ephp";
    let decoded = percent_decode_path(raw).unwrap();

    assert!(
        matches!(
            match_route(&cfg, &decoded, "GET", ""),
            RouteDecision::Matched { .. }
        ),
        "the decoded path must match the route"
    );
    assert_eq!(
        match_route(&cfg, raw, "GET", ""),
        RouteDecision::NoMatch,
        "sanity: matching the raw path is exactly the bypass this closes"
    );
}

#[tokio::test]
async fn wait_returns_at_once_if_already_fired() {
    let (tx, mut shutdown) = Shutdown::channel();
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), shutdown.wait())
        .await
        .expect("wait() must not block once the signal already fired");
}

#[tokio::test]
async fn wait_unblocks_once_the_signal_fires() {
    let (tx, mut shutdown) = Shutdown::channel();
    let waited = tokio::spawn(async move {
        shutdown.wait().await;
    });
    // Give the spawned task a chance to start waiting before firing.
    tokio::task::yield_now().await;
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), waited)
        .await
        .expect("wait() must unblock once the signal fires")
        .expect("task panicked");
}

/// Without this, an accept loop blocked on `wait()` after master itself has
/// gone would never return, and the process could not exit.
#[tokio::test]
async fn wait_returns_when_the_sender_is_dropped() {
    let (tx, mut shutdown) = Shutdown::channel();
    drop(tx);
    tokio::time::timeout(std::time::Duration::from_millis(100), shutdown.wait())
        .await
        .expect("wait() must not hang once the sender is gone");
}

/// Every accept loop clones its own `Shutdown`; all of them must see one
/// signal, not just the original.
#[tokio::test]
async fn every_clone_observes_the_same_signal() {
    let (tx, shutdown) = Shutdown::channel();
    let mut a = shutdown.clone();
    let mut b = shutdown;
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), a.wait())
        .await
        .expect("clone a must see the signal");
    tokio::time::timeout(std::time::Duration::from_millis(100), b.wait())
        .await
        .expect("clone b must see the signal");
}
