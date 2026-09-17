use super::*;
use hyper::HeaderMap;

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
