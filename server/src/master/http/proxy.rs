//! Trusted-proxy-gated resolution of the real client IP, Host and scheme.

use hyper::HeaderMap;
use std::net::IpAddr;

/// A client IP that has passed the trusted-proxy gate. Keep `resolve_client_ip`
/// its only constructor: a second one is a way to hand a consumer an address
/// that never went through the gate.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ClientIdentity(IpAddr);

impl ClientIdentity {
    pub(crate) fn ip(self) -> IpAddr {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(ip: IpAddr) -> Self {
        ClientIdentity(canonical(ip))
    }
}

impl std::fmt::Display for ClientIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// The connection's own end, fixed for as long as it stays open: neither the
/// peer address nor `trusted_proxies` can change under it.
#[derive(Clone, Copy)]
pub(crate) struct Peer {
    identity: ClientIdentity,
    is_trusted_proxy: bool,
}

impl Peer {
    pub(crate) fn resolve(ip: IpAddr, trusted_proxies: &[ipnetwork::IpNetwork]) -> Self {
        Peer {
            identity: ClientIdentity(canonical(ip)),
            is_trusted_proxy: ip_is_trusted_proxy(ip, trusted_proxies),
        }
    }

    pub(crate) fn is_trusted_proxy(self) -> bool {
        self.is_trusted_proxy
    }
}

/// Past this a chain is padding, not a deployment.
const MAX_FORWARDED_HOPS: usize = 32;

/// The real client behind `peer`, taken from `X-Forwarded-For` only if
/// `peer` is a trusted proxy. Every exit but one fails closed rather than
/// skipping an unparseable/overlong/unvouched-for entry to keep walking.
pub(crate) fn resolve_client_ip(
    peer: Peer,
    headers: &HeaderMap,
    trusted_proxies: &[ipnetwork::IpNetwork],
) -> ClientIdentity {
    let Peer {
        identity: peer,
        is_trusted_proxy,
    } = peer;
    if !is_trusted_proxy {
        return peer;
    }
    let mut hops = 0;
    // RFC 9110 §5.3: repeated field lines are one list in the order received.
    // Reading only the first lets a client send its own line ahead of the one
    // its proxy appends and so choose its own identity.
    for value in headers.get_all("x-forwarded-for").iter().rev() {
        let Ok(list) = value.to_str() else {
            return peer;
        };
        for hop in list.split(',').rev() {
            hops += 1;
            if hops > MAX_FORWARDED_HOPS {
                return peer;
            }
            let Ok(ip) = hop.trim().parse::<IpAddr>() else {
                return peer;
            };
            if !ip_is_trusted_proxy(ip, trusted_proxies) {
                return ClientIdentity(canonical(ip));
            }
        }
    }
    peer
}

/// Empty trusts nothing, so no configuration is the safe configuration.
pub(crate) fn ip_is_trusted_proxy(ip: IpAddr, trusted_proxies: &[ipnetwork::IpNetwork]) -> bool {
    let ip = canonical(ip);
    trusted_proxies.iter().any(|net| net.contains(ip))
}

/// One address, one spelling: a dual-stack listener reports an IPv4 peer as
/// `::ffff:a.b.c.d`, silently failing an operator's IPv4 CIDR otherwise.
/// Not `to_ipv4`, which also turns the deprecated `::1` into `0.0.0.1`.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => ip,
        },
        IpAddr::V4(_) => ip,
    }
}

/// `X-Forwarded-Host`, from a trusted peer only, beats `Host`, which in turn
/// falls back to this listener's own bind address.
pub(crate) fn resolve_server_name_port(
    headers: &HeaderMap,
    is_trusted_peer: bool,
    listen_addr: &str,
) -> (String, u16) {
    let fallback_port = listen_addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
        .unwrap_or(80);
    let fallback_name = listen_addr.rsplit_once(':').map_or(listen_addr, |(h, _)| h);

    let host = is_trusted_peer
        .then(|| {
            headers
                .get("x-forwarded-host")
                .and_then(|v| v.to_str().ok())
        })
        .flatten()
        .or_else(|| {
            headers
                .get(hyper::header::HOST)
                .and_then(|v| v.to_str().ok())
        });

    let Some(host) = host else {
        return (fallback_name.to_string(), fallback_port);
    };
    let (name, port) = split_host_port(host);
    (
        name.to_string(),
        port.and_then(|p| p.parse().ok()).unwrap_or(fallback_port),
    )
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

#[cfg(test)]
#[path = "proxy_tests.rs"]
mod tests;
