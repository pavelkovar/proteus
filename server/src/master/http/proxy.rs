//! Trusted-proxy-gated resolution of the real client IP, Host and scheme.

use hyper::HeaderMap;
use std::net::IpAddr;

/// Trusts the header only if `peer_ip` is itself trusted.
///
/// Walks right to left and stops at the first untrusted hop: everything to
/// its right was appended by proxies already trusted, everything to its left
/// is as client-controlled as that hop. An unparseable hop stops the walk
/// rather than being skipped, since skipping would keep walking left into
/// client-controlled territory on the word of a hop that failed validation.
pub(crate) fn resolve_client_ip(
    peer_ip: IpAddr,
    is_peer_trusted: bool,
    headers: &HeaderMap,
    trusted_check: impl Fn(IpAddr) -> bool,
) -> IpAddr {
    if !is_peer_trusted {
        return peer_ip;
    }
    let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) else {
        return peer_ip;
    };
    for hop in xff.split(',').rev() {
        let Ok(ip) = hop.trim().parse::<IpAddr>() else {
            return peer_ip;
        };
        if !trusted_check(ip) {
            return ip;
        }
    }
    peer_ip
}

/// An empty `trusted_proxies` means loopback only.
pub(crate) fn ip_is_trusted_proxy(ip: IpAddr, trusted_proxies: &[ipnetwork::IpNetwork]) -> bool {
    if trusted_proxies.is_empty() {
        return ip.is_loopback();
    }
    trusted_proxies.iter().any(|net| net.contains(ip))
}

/// `X-Forwarded-Host`, from a trusted peer only, beats `Host`, which in turn
/// falls back to this listener's own bind address.
pub(crate) fn resolve_server_name_port(headers: &HeaderMap, is_trusted_peer: bool, listen_addr: &str) -> (String, u16) {
    let fallback_port = listen_addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
        .unwrap_or(80);
    let fallback_name = listen_addr.rsplit_once(':').map_or(listen_addr, |(h, _)| h);

    let host = is_trusted_peer
        .then(|| headers.get("x-forwarded-host").and_then(|v| v.to_str().ok()))
        .flatten()
        .or_else(|| headers.get(hyper::header::HOST).and_then(|v| v.to_str().ok()));

    let Some(host) = host else {
        return (fallback_name.to_string(), fallback_port);
    };
    let (name, port) = split_host_port(host);
    (name.to_string(), port.and_then(|p| p.parse().ok()).unwrap_or(fallback_port))
}

/// A bracketed IPv6 literal carries colons of its own, so splitting on the
/// last colon finds one of those rather than the port separator, garbling a
/// bare `[::1]`. Only a colon after the closing bracket is a separator.
fn split_host_port(host: &str) -> (&str, Option<&str>) {
    if host.starts_with('[')
        && let Some(bracket_end) = host.find(']')
    {
        let name = &host[..=bracket_end];
        let port = host[bracket_end + 1..].strip_prefix(':');
        return (name, port);
    }
    match host.rsplit_once(':') {
        Some((name, port)) => (name, Some(port)),
        None => (host, None),
    }
}

/// This server never terminates TLS, so the answer comes from
/// `X-Forwarded-Proto` and only from a trusted peer.
pub(crate) fn resolve_https(headers: &HeaderMap, is_trusted_peer: bool) -> bool {
    is_trusted_peer
        && headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("https"))
}
