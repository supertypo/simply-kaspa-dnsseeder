use crate::record::PeerRecord;
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
    /// Build the standard "serve this peer to clients" filter shared by the
    /// HTTP and DNS surfaces. Always enforces the stale-good window (which
    /// also implicitly hides stubs) and skips the dead cutoff — `prune_dead`
    /// removes those records out of band. Caller fills in `family` and
    /// `default_port` per surface.
    #[must_use]
    pub fn serving(
        now_ms: i64,
        stale_good_ms: i64,
        min_protocol_version: Option<u32>,
        min_user_agent: Option<Version>,
        family: Option<Family>,
        default_port: Option<u16>,
    ) -> Self {
        Self {
            now_ms,
            dead_after_ms: i64::MAX,
            stale_good_ms: Some(stale_good_ms),
            family,
            min_protocol_version,
            min_user_agent,
            default_port,
        }
    }

    /// Returns true iff the record passes all configured criteria.
    #[must_use]
    pub fn matches(&self, rec: &PeerRecord) -> bool {
        if self.now_ms - rec.last_seen_ms > self.dead_after_ms {
            return false;
        }
        if let Some(stale_good) = self.stale_good_ms
            && self.now_ms - rec.last_success_ms > stale_good
        {
            return false;
        }
        if let Some(family) = self.family {
            let ok = matches!(
                (family, &rec.address.ip),
                (Family::V4, std::net::IpAddr::V4(_)) | (Family::V6, std::net::IpAddr::V6(_))
            );
            if !ok {
                return false;
            }
        }
        if let Some(min) = self.min_protocol_version
            && rec.protocol_version < min
        {
            return false;
        }
        if let Some(min) = self.min_user_agent.as_ref() {
            match PeerRecord::parse_kaspad_version(&rec.user_agent) {
                Some(v) if (v.major, v.minor, v.patch) >= (min.major, min.minor, min.patch) => {}
                _ => return false,
            }
        }
        if let Some(port) = self.default_port
            && rec.address.port != port
        {
            return false;
        }
        true
    }
}
