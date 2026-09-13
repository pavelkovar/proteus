use super::compression::*;
use super::fs_cache::{FsCache, FsKind};
use super::*;
use crate::config::{
    Config, Limits, MatchPattern, PhpConfig, Processes, QueueConfig, Route, RouteActionConfig,
    RouteMatch, extension_is_listed,
};
use hyper::HeaderMap;
use std::net::IpAddr;

fn test_config(routes: Vec<Route>) -> Config {
    Config {
        listen: vec!["127.0.0.1:0".into()],
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

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("proteus-openat2-{}-{name}", std::process::id()))
}

/// A file just written has a warm dentry, which is the whole point: the open
/// is answered inline and the blocking pool is never involved.
#[test]
fn open_cached_answers_a_warm_dentry_inline() {
    let path = temp_path("warm");
    std::fs::write(&path, b"x").unwrap();
    let result = open_cached(&path);
    let _ = std::fs::remove_file(&path);
    match result {
        Some(Ok(_)) => {}
        Some(Err(e)) => panic!("a readable file must open, got {e}"),
        // Only legitimate when the kernel has no RESOLVE_CACHED at all.
        None => assert!(!openat2_usable_for_test(), "a warm dentry must not defer"),
    }
}

/// The errno distinction that keeps a miss from being opened twice: a
/// genuinely absent file is an answer, not a reason to consult the pool.
#[test]
fn open_cached_reports_a_missing_file_rather_than_deferring_it() {
    let path = temp_path("missing");
    let _ = std::fs::remove_file(&path);
    match open_cached(&path) {
        Some(Err(e)) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
        Some(Ok(_)) => panic!("a file that does not exist must not open"),
        None => assert!(
            !openat2_usable_for_test(),
            "ENOENT is the real answer and must not be deferred to the blocking pool"
        ),
    }
}

/// `open()` succeeds on a directory, so the caller - not this helper - is
/// what keeps one from being streamed as a body.
#[test]
fn open_cached_opens_a_directory_which_stat_then_classifies() {
    let dir = temp_path("dir");
    std::fs::create_dir_all(&dir).unwrap();
    let result = open_cached(&dir);
    let _ = std::fs::remove_dir(&dir);
    if let Some(Ok(file)) = result {
        let (_, meta) = stat_and_advise(file).unwrap();
        assert!(meta.is_dir());
    }
}

/// The inline path and the `stat` fallback must classify identically, or
/// which one answered would change what the worker is handed.
#[tokio::test]
async fn stat_kind_classifies_the_same_however_it_was_answered() {
    let cache = FsCache::new(64, std::time::Duration::from_secs(60));
    let dir = temp_path("statkind");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("script.php");
    std::fs::write(&file, b"<?php").unwrap();
    let absent = dir.join("nope.php");

    assert_eq!(stat_kind(&cache, &file).await, FsKind::File);
    assert_eq!(stat_kind(&cache, &dir).await, FsKind::Dir);
    assert_eq!(stat_kind(&cache, &absent).await, FsKind::Missing);

    // Same verdicts with the fast path out of the picture.
    let cold = FsCache::new(64, std::time::Duration::from_secs(60));
    assert_eq!(kind_of(std::fs::metadata(&file)), FsKind::File);
    assert_eq!(kind_of(std::fs::metadata(&dir)), FsKind::Dir);
    assert_eq!(kind_of(std::fs::metadata(&absent)), FsKind::Missing);
    assert_eq!(stat_kind(&cold, &file).await, FsKind::File);

    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ip_is_trusted_proxy_matches_a_configured_cidr() {
    let trusted_proxies: Vec<ipnetwork::IpNetwork> = vec!["10.0.0.0/8".parse().unwrap()];
    assert!(ip_is_trusted_proxy(
        "10.1.2.3".parse().unwrap(),
        &trusted_proxies
    ));
    assert!(!ip_is_trusted_proxy(
        "127.0.0.1".parse().unwrap(),
        &trusted_proxies
    ));
    assert!(!ip_is_trusted_proxy(
        "203.0.113.1".parse().unwrap(),
        &trusted_proxies
    ));
}

/// Nothing is trusted by default - loopback included, or any local process
/// able to open a socket could pick its own `REMOTE_ADDR` and rate-limit
/// bucket.
#[test]
fn ip_is_trusted_proxy_trusts_nobody_when_the_list_is_empty() {
    for ip in ["127.0.0.1", "::1", "10.0.0.1", "203.0.113.1"] {
        assert!(!ip_is_trusted_proxy(ip.parse().unwrap(), &[]), "{ip}");
    }
}

/// A dual-stack listener reports an IPv4 peer in mapped form; an operator's
/// IPv4 CIDR has to keep matching it.
#[test]
fn ip_is_trusted_proxy_matches_an_ipv4_cidr_against_an_ipv4_mapped_peer() {
    let trusted_proxies: Vec<ipnetwork::IpNetwork> = vec!["10.0.0.0/8".parse().unwrap()];
    assert!(ip_is_trusted_proxy(
        "::ffff:10.0.0.20".parse().unwrap(),
        &trusted_proxies
    ));
}

fn trusted_nets(cidrs: &[&str]) -> Vec<ipnetwork::IpNetwork> {
    cidrs.iter().map(|c| c.parse().unwrap()).collect()
}

/// Mirrors `handle()`, config list included, so a test cannot pass against a
/// gate the live call site never applies.
fn resolve(peer: &str, xff: &[&str], trusted: &[&str]) -> IpAddr {
    let trusted = trusted_nets(trusted);
    let peer = Peer::resolve(peer.parse().unwrap(), &trusted);
    let mut headers = HeaderMap::new();
    for line in xff {
        headers.append("x-forwarded-for", line.parse().unwrap());
    }
    resolve_client_ip(peer, &headers, &trusted).ip()
}

/// The bypass this whole gate exists to prevent.
#[test]
fn a_direct_client_cannot_choose_its_own_identity_with_x_forwarded_for() {
    assert_eq!(
        resolve("203.0.113.10", &["198.51.100.20"], &["10.0.0.0/8"]),
        "203.0.113.10".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn a_trusted_proxys_x_forwarded_for_is_honoured() {
    assert_eq!(
        resolve("10.0.0.20", &["198.51.100.20"], &["10.0.0.0/8"]),
        "198.51.100.20".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn the_walk_passes_every_trusted_hop_and_stops_at_the_client() {
    assert_eq!(
        resolve(
            "10.0.0.20",
            &["198.51.100.20, 10.0.0.10, 10.0.0.20"],
            &["10.0.0.0/8"]
        ),
        "198.51.100.20".parse::<IpAddr>().unwrap()
    );
}

/// Everything left of the first untrusted hop was written by something this
/// server has no reason to believe, so the walk must stop there rather than
/// run on to the leftmost entry.
#[test]
fn the_walk_stops_at_the_first_untrusted_hop_not_the_leftmost_one() {
    assert_eq!(
        resolve(
            "10.0.0.20",
            &["1.1.1.1, 203.0.113.9, 10.0.0.10"],
            &["10.0.0.0/8"]
        ),
        "203.0.113.9".parse::<IpAddr>().unwrap(),
        "a client-supplied prefix must not be reachable past an untrusted hop"
    );
}

/// hyper keeps repeated field lines separate, so reading only the first would
/// hand the identity to whichever line the client sent - its own, ahead of
/// the one the proxy appends.
#[test]
fn a_client_prepended_header_line_cannot_outrank_the_one_its_proxy_appends() {
    assert_eq!(
        resolve("10.0.0.20", &["1.1.1.1", "198.51.100.20"], &["10.0.0.0/8"]),
        "198.51.100.20".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn a_malformed_hop_falls_back_to_the_peer() {
    let peer = "10.0.0.20".parse::<IpAddr>().unwrap();
    for xff in [
        "not-an-ip",
        "9.9.9.9, not-an-ip",
        "",
        "   ",
        "198.51.100.20:443",
        "198.51.100.20, ,10.0.0.10",
        "for=198.51.100.20",
    ] {
        assert_eq!(
            resolve("10.0.0.20", &[xff], &["10.0.0.0/8"]),
            peer,
            "{xff:?}"
        );
    }
}

#[test]
fn a_missing_header_falls_back_to_the_peer() {
    assert_eq!(
        resolve("10.0.0.20", &[], &["10.0.0.0/8"]),
        "10.0.0.20".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn surrounding_whitespace_in_the_chain_is_tolerated() {
    assert_eq!(
        resolve(
            "10.0.0.20",
            &["  198.51.100.20 ,\t10.0.0.10  "],
            &["10.0.0.0/8"]
        ),
        "198.51.100.20".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn ipv6_hops_resolve_against_an_ipv6_trusted_cidr() {
    assert_eq!(
        resolve(
            "fd00::20",
            &["2001:db8::50, fd00::10"],
            &["fd00::/8", "10.0.0.0/8"]
        ),
        "2001:db8::50".parse::<IpAddr>().unwrap()
    );
}

/// Both forms of the same address must land on one identity, or a client
/// reaching a dual-stack listener would get a second rate-limit bucket.
#[test]
fn ipv4_mapped_addresses_collapse_onto_their_ipv4_form() {
    assert_eq!(
        resolve("::ffff:203.0.113.10", &[], &["10.0.0.0/8"]),
        "203.0.113.10".parse::<IpAddr>().unwrap()
    );
    assert_eq!(
        resolve("10.0.0.20", &["::ffff:198.51.100.20"], &["10.0.0.0/8"]),
        "198.51.100.20".parse::<IpAddr>().unwrap()
    );
}

/// The client entry sits past the cap, so an uncapped walk reaches it and a
/// capped one must not.
#[test]
fn an_overlong_chain_falls_back_to_the_peer() {
    let padding = std::iter::repeat_n("10.0.0.1", 200).collect::<Vec<_>>();
    let chain = format!("203.0.113.50,{}", padding.join(","));
    assert_eq!(
        resolve("10.0.0.20", &[&chain], &["10.0.0.0/8"]),
        "10.0.0.20".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn server_name_port_prefers_trusted_x_forwarded_host_over_host() {
    let mut headers = HeaderMap::new();
    headers.insert("host", "internal-lb:8080".parse().unwrap());
    headers.insert("x-forwarded-host", "public.example.com".parse().unwrap());
    assert_eq!(
        resolve_server_name_port(&headers, true, "0.0.0.0:80"),
        ("public.example.com".to_string(), 80)
    );
}

#[test]
fn server_name_port_ignores_x_forwarded_host_from_untrusted_peer() {
    let mut headers = HeaderMap::new();
    headers.insert("host", "internal-lb:8080".parse().unwrap());
    headers.insert("x-forwarded-host", "spoofed.example.com".parse().unwrap());
    assert_eq!(
        resolve_server_name_port(&headers, false, "0.0.0.0:80"),
        ("internal-lb".to_string(), 8080)
    );
}

#[test]
fn server_name_port_falls_back_to_listen_addr_when_no_headers_sent() {
    let headers = HeaderMap::new();
    assert_eq!(
        resolve_server_name_port(&headers, true, "0.0.0.0:8080"),
        ("0.0.0.0".to_string(), 8080)
    );
}

/// A bracketed IPv6 host carries colons of its own, so a rightmost-colon
/// split finds one of those instead of a port separator. Real clients send
/// exactly this form on the default port.
#[test]
fn server_name_port_handles_bracketed_ipv6_host_with_and_without_a_port() {
    let mut headers = HeaderMap::new();
    headers.insert("host", "[::1]".parse().unwrap());
    assert_eq!(
        resolve_server_name_port(&headers, true, "0.0.0.0:80"),
        ("[::1]".to_string(), 80)
    );

    headers.insert("host", "[::1]:8080".parse().unwrap());
    assert_eq!(
        resolve_server_name_port(&headers, true, "0.0.0.0:80"),
        ("[::1]".to_string(), 8080)
    );
}

#[test]
fn https_trusted_peer_with_matching_header_is_true() {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "https".parse().unwrap());
    assert!(resolve_https(&headers, true));
}

#[test]
fn https_header_value_is_case_insensitive() {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "HTTPS".parse().unwrap());
    assert!(resolve_https(&headers, true));
}

#[test]
fn https_untrusted_peer_is_ignored_even_with_the_header_set() {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "https".parse().unwrap());
    assert!(
        !resolve_https(&headers, false),
        "an untrusted peer must never be able to spoof HTTPS"
    );
}

#[test]
fn https_defaults_to_false_with_no_header() {
    assert!(!resolve_https(&HeaderMap::new(), true));
}

#[test]
fn https_wrong_value_is_false() {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "http".parse().unwrap());
    assert!(!resolve_https(&headers, true));
}

#[test]
fn compression_respects_size_threshold() {
    assert_eq!(
        pick_encoding(100, "gzip", 1024, "text/plain", &[]),
        None,
        "below threshold"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "text/plain", &[]),
        Some(Encoding::Gzip),
        "above threshold, gzip accepted"
    );
    assert_eq!(
        pick_encoding(2000, "", 1024, "text/plain", &[]),
        None,
        "above threshold but no accept-encoding"
    );
    assert_eq!(
        pick_encoding(2000, "br", 1024, "text/plain", &[]),
        Some(Encoding::Brotli),
        "client wants brotli"
    );
}

#[test]
fn compression_prefers_zstd_then_brotli_then_gzip() {
    assert_eq!(
        pick_encoding(2000, "gzip, br, zstd", 1024, "text/plain", &[]),
        Some(Encoding::Zstd),
        "zstd preferred when the client accepts all three"
    );
    assert_eq!(
        pick_encoding(2000, "gzip, br", 1024, "text/plain", &[]),
        Some(Encoding::Brotli),
        "brotli preferred over gzip when zstd isn't accepted"
    );
    assert_eq!(
        pick_encoding(2000, "deflate", 1024, "text/plain", &[]),
        None,
        "no supported encoding accepted"
    );
}

#[test]
fn compression_respects_weighted_accept_encoding() {
    // An explicit veto beats our own top preference.
    assert_eq!(
        pick_encoding(
            2000,
            "zstd;q=0, br;q=0.8, gzip;q=0.5",
            1024,
            "text/plain",
            &[]
        ),
        Some(Encoding::Brotli)
    );
    // A q value alone does not re-rank against our own priority order;
    // acceptable is all that is asked of it.
    assert_eq!(
        pick_encoding(2000, "zstd;q=0.1, gzip;q=1.0", 1024, "text/plain", &[]),
        Some(Encoding::Zstd)
    );

    assert_eq!(
        pick_encoding(2000, "*;q=1", 1024, "text/plain", &[]),
        Some(Encoding::Zstd)
    );
    // An explicit entry overrides the wildcard in either direction.
    assert_eq!(
        pick_encoding(2000, "*;q=1, zstd;q=0", 1024, "text/plain", &[]),
        Some(Encoding::Brotli),
        "explicit zstd;q=0 overrides the permissive wildcard"
    );
    assert_eq!(
        pick_encoding(2000, "*;q=0, gzip;q=1", 1024, "text/plain", &[]),
        Some(Encoding::Gzip),
        "explicit gzip;q=1 overrides the blanket wildcard veto"
    );
    assert_eq!(
        pick_encoding(2000, "*;q=0", 1024, "text/plain", &[]),
        None,
        "wildcard veto with no explicit overrides"
    );
}

#[test]
fn compression_mime_types_allowlist() {
    let allowed = vec!["text/html".to_string(), "application/json".to_string()];
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "text/html", &allowed),
        Some(Encoding::Gzip),
        "text/html is on the allowlist"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "image/png", &allowed),
        None,
        "image/png is not on the allowlist"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "text/html; charset=utf-8", &allowed),
        Some(Encoding::Gzip),
        "charset suffix must not defeat the match"
    );
    assert_eq!(
        pick_encoding(2000, "gzip", 1024, "image/png", &[]),
        Some(Encoding::Gzip),
        "empty allowlist (the default) means no restriction at all"
    );
}

/// Framing always comes from the real body: forwarding a script's own
/// `Content-Length` desyncs the client, or a reused connection's next
/// response, the moment the two disagree.
#[test]
fn framing_headers_from_a_php_script_are_never_forwarded() {
    let mut headers = crate::ipc::data::HeaderBlob::default();
    headers.push("Content-Length", "999999");
    headers.push("Transfer-Encoding", "chunked");
    headers.push("Connection", "keep-alive");
    headers.push("X-Custom", "kept");
    let resp = build_response(StatusCode::OK, b"hello".to_vec(), &headers);
    assert!(
        resp.headers().get("content-length").is_none(),
        "a script-set Content-Length must never reach the client"
    );
    assert!(
        resp.headers().get("transfer-encoding").is_none(),
        "a script-set Transfer-Encoding must never reach the client"
    );
    assert!(
        resp.headers().get("connection").is_none(),
        "a script-set Connection must never reach the client"
    );
    assert_eq!(
        resp.headers().get("x-custom").unwrap(),
        "kept",
        "non-framing headers must still pass through untouched"
    );
}

#[test]
fn rejects_parent_dir_traversal() {
    // Live-exploitable before this check existed.
    assert!(path_escapes_root("/../../../../etc/passwd"));
    assert!(path_escapes_root("/static/../../etc/passwd"));
    assert!(path_escapes_root("/a/../../b"));
    assert!(!path_escapes_root("/static/logo.png"));
    assert!(!path_escapes_root("/"));
    // Not a traversal component, and must not be rejected.
    assert!(!path_escapes_root("/file..name.txt"));
}

// --- percent-decoding ---

#[test]
fn percent_decode_leaves_an_unencoded_path_borrowed() {
    let decoded = percent_decode_path("/a/b/c.txt").unwrap();
    assert!(
        matches!(decoded, std::borrow::Cow::Borrowed(_)),
        "the common case must not allocate"
    );
    assert_eq!(decoded, "/a/b/c.txt");
}

#[test]
fn percent_decode_handles_spaces_and_multibyte() {
    assert_eq!(
        percent_decode_path("/my%20file.txt").unwrap(),
        "/my file.txt"
    );
    // One codepoint spread over two escapes: validating per-escape rejects it.
    assert_eq!(percent_decode_path("/%C3%A1%C4%8D.php").unwrap(), "/áč.php");
    assert_eq!(percent_decode_path("/a%2Bb%3Dc").unwrap(), "/a+b=c");
    assert_eq!(percent_decode_path("/%2e").unwrap(), "/.");
}

/// Why decoding must precede the traversal check: the raw path hides these.
#[test]
fn decoded_traversal_is_caught_by_the_traversal_check() {
    for raw in [
        "/%2e%2e/etc/passwd",
        "/a/%2E%2E/%2E%2E/etc/passwd",
        "/%2e%2e",
    ] {
        let decoded = percent_decode_path(raw).expect("these decode fine, they're just hostile");
        assert!(
            path_escapes_root(&decoded),
            "{raw} decodes to {decoded:?}, which must be rejected as traversal"
        );
        assert!(
            !path_escapes_root(raw),
            "sanity: {raw} is exactly the shape the raw check misses"
        );
    }
}

/// An encoded separator invents path segments after routing and the
/// traversal check have already run on a different shape.
#[test]
fn percent_decode_rejects_encoded_separators() {
    for raw in ["/a%2Fb", "/a%2fb", "/%2e%2e%2fetc/passwd", "/a%5Cb"] {
        assert_eq!(
            percent_decode_path(raw),
            Err(PathDecodeError::EncodedSeparator),
            "{raw} must be refused"
        );
    }
}

#[test]
fn percent_decode_rejects_nul_malformed_and_non_utf8() {
    assert_eq!(percent_decode_path("/a%00b"), Err(PathDecodeError::Nul));
    assert_eq!(
        percent_decode_path("/a%zzb"),
        Err(PathDecodeError::Malformed)
    );
    assert_eq!(percent_decode_path("/a%2"), Err(PathDecodeError::Malformed));
    assert_eq!(percent_decode_path("/a%"), Err(PathDecodeError::Malformed));
    // Valid percent-encoding, invalid UTF-8.
    assert_eq!(
        percent_decode_path("/%FF%FE"),
        Err(PathDecodeError::NotUtf8)
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

/// A restricted cpuset is what catches a count standing in for identities:
/// `available_parallelism` answers 2 for `{1,3}`, so pinning to `0..2` misses
/// both. Affinity is per-thread, so this restricts a thread of its own.
#[test]
fn allowed_cpus_reports_real_ids_under_a_restricted_cpuset() {
    let all = allowed_cpus();
    if all.len() < 2 {
        return; // nothing to restrict
    }
    // Deliberately not the lowest two: a bug that returns `0..n` passes on
    // a contiguous set starting at zero.
    let picked = vec![all[all.len() - 2], all[all.len() - 1]];
    let expected = picked.clone();

    std::thread::spawn(move || {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for &cpu in &picked {
                libc::CPU_SET(cpu, &mut set);
            }
            assert_eq!(
                libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
                0,
                "could not restrict this thread's affinity"
            );
        }
        assert_eq!(allowed_cpus(), expected);

        pin_to_cpu(expected[1]);
        assert_eq!(allowed_cpus(), vec![expected[1]]);
    })
    .join()
    .expect("affinity thread panicked");
}
