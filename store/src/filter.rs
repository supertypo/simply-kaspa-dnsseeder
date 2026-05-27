use crate::record::{NetAddress, PeerRecord};
use semver::Version;

/// Address family used for DNS filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    V4,
    V6,
}

/// Predicate over a [`PeerRecord`].
///
/// Used by both the HTTP and DNS surfaces; the HTTP surface typically passes
/// `family = None`, `min_protocol_version = None`, `min_user_agent = None`,
/// `default_port_only = false` and only sets `dead_after_ms`. The DNS surface
/// fills all fields.
#[derive(Debug, Clone)]
pub struct Filter {
    pub now_ms: i64,
    pub dead_after_ms: i64,
    pub stale_good_ms: Option<i64>,
    pub family: Option<Family>,
    pub min_protocol_version: Option<u32>,
    pub min_user_agent: Option<Version>,
    pub default_port: Option<u16>,
}

impl Filter {
    /// Returns true iff the record passes all configured criteria.
    #[must_use]
    pub fn matches(&self, rec: &PeerRecord) -> bool {
        // Dead
        if self.now_ms - rec.last_seen_ms > self.dead_after_ms {
            return false;
        }
        if let Some(stale_good) = self.stale_good_ms {
            if self.now_ms - rec.last_success_ms > stale_good {
                return false;
            }
        }
        if let Some(family) = self.family {
            let ok = match (family, &rec.address.ip) {
                (Family::V4, std::net::IpAddr::V4(_)) => true,
                (Family::V6, std::net::IpAddr::V6(_)) => true,
                _ => false,
            };
            if !ok {
                return false;
            }
        }
        if let Some(min) = self.min_protocol_version {
            if rec.protocol_version < min {
                return false;
            }
        }
        if let Some(min) = self.min_user_agent.as_ref() {
            match PeerRecord::parse_kaspad_version(&rec.user_agent) {
                Some(v) if &v >= min => {}
                _ => return false,
            }
        }
        if let Some(port) = self.default_port {
            if rec.address.port != port {
                return false;
            }
        }
        let _ = NetAddress { ip: rec.address.ip, port: rec.address.port }; // type witness
        true
    }
}
