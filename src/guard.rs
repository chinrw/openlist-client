//! Keeps file transfers off loopback and link-local addresses. A link comes
//! from OpenList, so a hostile or compromised OpenList could otherwise make
//! the client fetch a service on this machine, or a cloud metadata
//! endpoint, and hand the answer to its caller. OpenList's own origin stays
//! reachable, because a local storage links back to it.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

/// Loopback, link-local and "this network" addresses, also inside IPv6
/// forms that carry an IPv4 address (mapped, compatible, NAT64). Private
/// LAN ranges are allowed: a storage on a NAS links there.
pub(crate) fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_link_local() || v4.octets()[0] == 0,
        IpAddr::V6(v6) => {
            let segments = v6.segments();
            let nat64 = segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0];
            let compatible = segments[..6] == [0; 6];
            let embedded = v6.to_ipv4_mapped().or_else(|| {
                // The low 32 bits are the IPv4 address.
                #[allow(clippy::cast_possible_truncation)]
                let low = v6.to_bits() as u32;
                (nat64 || compatible).then(|| Ipv4Addr::from(low))
            });
            match embedded {
                // `::1` is also the compatible form of 0.0.0.1.
                Some(v4) => is_local(IpAddr::V4(v4)) || v6.is_loopback(),
                None => (segments[0] & 0xffc0) == 0xfe80,
            }
        }
    }
}

/// Which transfer targets are allowed. Host names are checked when they are
/// resolved, by [`Resolver`]. IP literals never reach a resolver, so
/// [`Guard::allows`] checks them before each request and redirect.
#[derive(Clone)]
pub(crate) struct Guard {
    /// OpenList's host and port, or `None` when local links are allowed
    /// everywhere.
    openlist: Option<(String, Option<u16>)>,
}

impl Guard {
    pub(crate) fn new(openlist: &Url, local_links: bool) -> Self {
        Self {
            openlist: (!local_links).then(|| {
                let host = openlist.host_str().unwrap_or_default().to_ascii_lowercase();
                (host, openlist.port_or_known_default())
            }),
        }
    }

    /// Whether a request to `url` may be sent. OpenList's own origin is
    /// exempt. Another port on its host is not, or an OpenList on this
    /// machine could reach every other service here. That part only holds
    /// for IP literals and `localhost`: a name that resolves locally is
    /// exempt as a whole, since the resolver never sees the port.
    pub(crate) fn allows(&self, url: &Url) -> bool {
        let Some((openlist_host, openlist_port)) = &self.openlist else {
            return true;
        };
        let same_host = url
            .host_str()
            .is_some_and(|h| h.eq_ignore_ascii_case(openlist_host));
        if same_host && url.port_or_known_default() == *openlist_port {
            return true;
        }
        match url.host() {
            Some(Host::Ipv4(v4)) => !is_local(IpAddr::V4(v4)),
            Some(Host::Ipv6(v6)) => !is_local(IpAddr::V6(v6)),
            Some(Host::Domain(name)) => !(same_host && is_localhost(name)),
            None => true,
        }
    }

    /// The resolver for transfer clients, or `None` when nothing is
    /// filtered.
    pub(crate) fn resolver(&self) -> Option<Resolver> {
        self.openlist
            .clone()
            .map(|(openlist_host, _)| Resolver { openlist_host })
    }
}

fn is_localhost(name: &str) -> bool {
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    name == "localhost" || name.ends_with(".localhost")
}

/// Drops local addresses from every host but OpenList's.
pub(crate) struct Resolver {
    openlist_host: String,
}

impl Resolve for Resolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        let trusted = host.eq_ignore_ascii_case(&self.openlist_host);
        Box::pin(async move {
            let found: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let kept: Vec<SocketAddr> = if trusted {
                found
            } else {
                found.into_iter().filter(|a| !is_local(a.ip())).collect()
            };
            if kept.is_empty() {
                return Err(format!("{host} has no address outside this machine").into());
            }
            let addrs: Addrs = Box::new(kept.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_addresses_include_ipv4_inside_ipv6() {
        for ip in [
            "127.0.0.1",
            "127.8.9.10",
            "169.254.169.254",
            "0.0.0.0",
            "0.1.2.3",
            "::1",
            "fe80::1",
            "::",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::127.0.0.1",
            "64:ff9b::127.0.0.1",
            "64:ff9b::a9fe:a9fe",
        ] {
            assert!(is_local(ip.parse().unwrap()), "{ip}");
        }
        for ip in [
            "192.168.1.10",
            "10.0.0.1",
            "8.8.8.8",
            "fd00::1",
            "2001:db8::1",
            "64:ff9b::808:808",
        ] {
            assert!(!is_local(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn only_openlist_s_own_origin_may_be_local() {
        let guard = Guard::new(&"http://127.0.0.1:5244/".parse().unwrap(), false);
        let allows = |url: &str| guard.allows(&url.parse().unwrap());
        assert!(allows("http://127.0.0.1:5244/d/file"));
        assert!(
            !allows("http://127.0.0.1:9000/other-service"),
            "another port on OpenList's machine"
        );
        assert!(!allows("http://169.254.169.254/latest/meta-data/"));
        assert!(!allows("http://[::1]:8080/"));
        assert!(allows("http://192.168.1.10/nas"));
        assert!(
            allows("https://cdn.example/file"),
            "names go to the resolver"
        );

        let named = Guard::new(&"http://localhost:5244/".parse().unwrap(), false);
        assert!(named.allows(&"http://localhost:5244/d/file".parse().unwrap()));
        assert!(!named.allows(&"http://localhost:6379/".parse().unwrap()));

        let open = Guard::new(&"http://127.0.0.1:5244/".parse().unwrap(), true);
        assert!(open.allows(&"http://169.254.169.254/".parse().unwrap()));
        assert!(open.resolver().is_none());
    }
}
