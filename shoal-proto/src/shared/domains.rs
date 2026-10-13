//! Where a member stands, for placement to spread copies over
//!
//! A node names its failure domains in its own configuration, and the control group commits them
//! on its member record when the node observes itself
//! ([F81](../../../docs/src/features/failure-domains.md)). There are three levels, nested widest
//! first: a **zone** holds racks, a **rack** holds hosts, and a **host** is the machine the node
//! runs on ([F83](../../../docs/src/features/rack-and-zone-domains.md)). The host defaults to the
//! name the operating system gives that machine, so nodes on one machine truthfully share a host
//! and a test that needs several hosts names them. A rack and a zone are named or absent.
//!
//! ```yaml
//! cluster:
//!   failure_domains:
//!     host: titan
//!     rack: shelf-1
//!     zone: lab-east
//! ```
//!
//! The types here are what a node's configuration, `shoaladm`'s inventory and the fixture all
//! hold, so none of them can spell a domain another way. A pool's `failure_domain`
//! ([`FailureDomain`](super::pools::FailureDomain)) is the other half: the level no two chunks of a
//! stripe may share, which a member's domains are read against.
//!
//! # Invariants
//!
//! **A record holds a map of levels to names**, so a level is another key and not a format change,
//! and a build that knows fewer levels keeps the ones it reads when it writes the record back.
//!
//! **Unknown keys are refused in a configuration**, so a level no build acts on is never accepted
//! and ignored.
//!
//! **A member that names no domain at a level stands in the nearest one below it that it names**
//! ([`domain_at`]): a node naming no rack is its host's rack, so a cluster that names no rack or
//! zone spreads exactly as it did by host alone. The fallback carries the level it came from, so
//! a host's name never equals a rack's.
//!
//! **A name is the cluster's at its level**, never scoped to the level above it: two zones' racks
//! named alike are one rack. A mistake in the hierarchy can then only make domains look more
//! shared than they are, never claim a spread that does not exist.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The level a member's host is recorded under
pub const HOST: &str = "host";

/// The level a member's rack is recorded under: every host of one rack is one domain
pub const RACK: &str = "rack";

/// The level a member's zone is recorded under: every rack of one zone is one domain
pub const ZONE: &str = "zone";

/// Every level a member can stand in, widest first
///
/// The order is the order a spread is judged in: a new zone before a new rack, a new rack before
/// a new host ([F83](../../../docs/src/features/rack-and-zone-domains.md)).
pub const LEVELS: [&str; 3] = [ZONE, RACK, HOST];

/// The longest name a domain may have, in bytes
///
/// A hostname is at most 253 bytes, so this is room for any machine's own name and for one an
/// operator writes by hand.
pub const MAX_DOMAIN_NAME: usize = 255;

/// Where a node stands, as its configuration names it
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDomains {
    /// The machine this node runs on; the name the operating system gives it when absent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The rack its host stands in; absent, its host is a rack of its own
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
    /// The zone its rack stands in; absent, its rack (or its host) is a zone of its own
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
}

impl FailureDomains {
    /// Domains naming a host
    ///
    /// # Arguments
    ///
    /// * `name` - The host this node stands on
    #[must_use]
    pub fn host<T: Into<String>>(name: T) -> Self {
        FailureDomains {
            host: Some(name.into()),
            ..FailureDomains::default()
        }
    }

    /// These domains with a rack named
    ///
    /// # Arguments
    ///
    /// * `name` - The rack this node's host stands in
    #[must_use]
    pub fn rack<T: Into<String>>(mut self, name: T) -> Self {
        self.rack = Some(name.into());
        self
    }

    /// These domains with a zone named
    ///
    /// # Arguments
    ///
    /// * `name` - The zone this node's rack stands in
    #[must_use]
    pub fn zone<T: Into<String>>(mut self, name: T) -> Self {
        self.zone = Some(name.into());
        self
    }

    /// Every name given, by level, widest first
    #[must_use]
    pub fn named(&self) -> Vec<(&'static str, &str)> {
        // the levels in their order, each only when it is named
        [(ZONE, &self.zone), (RACK, &self.rack), (HOST, &self.host)]
            .into_iter()
            .filter_map(|(level, name)| name.as_deref().map(|name| (level, name)))
            .collect()
    }

    /// Whether every name given can be one, and why not
    ///
    /// # Errors
    ///
    /// The first name that is empty, too long, or holds whitespace or a control character.
    pub fn check(&self) -> Result<(), String> {
        // only what is named is judged; an absent host is the machine's own
        for (level, name) in self.named() {
            check_name(level, name)?;
        }
        Ok(())
    }

    /// The domains this node stands in, keyed by level: the host it names, or the machine's own,
    /// and the rack and zone when they are named
    ///
    /// # Arguments
    ///
    /// * `hostname` - The name the operating system gives the machine, if it gave one
    ///
    /// # Errors
    ///
    /// A named host that cannot be one, or no host at all: none named and none given by the
    /// machine, or one given by the machine that cannot be one.
    pub fn resolve(&self, hostname: Option<String>) -> Result<BTreeMap<String, String>, String> {
        // a name the operator gave is judged before anything falls back
        self.check()?;
        let host = match &self.host {
            Some(host) => host.clone(),
            None => {
                // the machine's own name, which every node on it shares
                let name = hostname.ok_or_else(|| {
                    "the operating system gave no hostname; name this node's host in \
                     cluster.failure_domains.host"
                        .to_string()
                })?;
                check_name(HOST, &name).map_err(|why| {
                    format!(
                        "the operating system's hostname cannot be a failure domain ({why}); \
                         name this node's host in cluster.failure_domains.host"
                    )
                })?;
                name
            }
        };
        // the host always, and the levels above it only when they are named
        let mut domains = BTreeMap::from([(HOST.to_string(), host)]);
        for (level, name) in [(RACK, &self.rack), (ZONE, &self.zone)] {
            if let Some(name) = name {
                domains.insert(level.to_string(), name.clone());
            }
        }
        Ok(domains)
    }
}

/// The domain a member stands in at one level, and the level its name was given at
///
/// The name the member gives at `level`, or else at the nearest level below it that it names: a
/// member naming no rack stands in its host's rack, and one naming no zone in its rack's zone, or
/// its host's. The level returned is the one the name came from, so a host standing in as its own
/// rack never equals a rack of that name. `None` when the member names nothing at or below the
/// level, as a record from before F81 or a test's literal one does; such a member stands in a
/// domain of its own ([F83](../../../docs/src/features/rack-and-zone-domains.md)).
///
/// # Arguments
///
/// * `domains` - The member's domains, as its record holds them
/// * `level` - The level to read, one of [`LEVELS`]
#[must_use]
pub fn domain_at<'a>(
    domains: &'a BTreeMap<String, String>,
    level: &str,
) -> Option<(&'static str, &'a str)> {
    // the levels from this one down to the host, the first one named
    LEVELS
        .iter()
        .skip_while(|wider| **wider != level)
        .find_map(|named| domains.get(*named).map(|name| (*named, name.as_str())))
}

/// Whether a domain's name can be one, and why not
///
/// # Arguments
///
/// * `level` - The level it names, for the refusal
/// * `name` - The name
///
/// # Errors
///
/// A name that is empty, longer than [`MAX_DOMAIN_NAME`], or holds whitespace or a control
/// character.
fn check_name(level: &str, name: &str) -> Result<(), String> {
    // an empty name would put every node that gives one in one domain by accident
    if name.trim().is_empty() {
        // what leaving it out means differs: the machine's name for a host, the level below's
        // for a rack or a zone
        let absent = if level == HOST {
            "for the machine's own name"
        } else {
            "to stand in the level below's"
        };
        return Err(format!(
            "cluster.failure_domains.{level} is empty; name the {level} this node stands in, or \
             leave it out {absent}"
        ));
    }
    // a name past any hostname's length is a mistake, not a machine
    if name.len() > MAX_DOMAIN_NAME {
        return Err(format!(
            "cluster.failure_domains.{level} is {} bytes long; at most {MAX_DOMAIN_NAME}",
            name.len()
        ));
    }
    // whitespace and control characters make two names that print alike compare unlike
    if name
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(format!(
            "cluster.failure_domains.{level} {name:?} holds whitespace or a control character"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{domain_at, FailureDomains, HOST, LEVELS, MAX_DOMAIN_NAME, RACK, ZONE};

    /// The documented shape is read, an empty block is the default, and a level no build acts on
    /// is refused by name
    #[test]
    fn the_documented_shape_is_read_and_unknown_levels_refused() {
        // a host named
        let named: FailureDomains = serde_yaml::from_str("host: titan").expect("a host reads");
        assert_eq!(named, FailureDomains::host("titan"));
        // nothing named is the machine's own name
        let empty: FailureDomains = serde_yaml::from_str("{}").expect("an empty block reads");
        assert_eq!(empty, FailureDomains::default());
        // every level, as the module's documentation writes them (F83)
        let every: FailureDomains =
            serde_yaml::from_str("host: titan\nrack: shelf-1\nzone: lab-east")
                .expect("every level reads");
        assert_eq!(
            every,
            FailureDomains::host("titan")
                .rack("shelf-1")
                .zone("lab-east")
        );
        // a rack alone leaves the host to the machine
        let rack: FailureDomains = serde_yaml::from_str("rack: r1").expect("a rack alone reads");
        assert_eq!(rack, FailureDomains::default().rack("r1"));
        // a level no build places by is refused by name
        for unknown in ["host: titan\nrow: r1", "region: eu"] {
            let error = serde_yaml::from_str::<FailureDomains>(unknown)
                .expect_err("an unknown level was accepted");
            assert!(
                error.to_string().contains("row") || error.to_string().contains("region"),
                "{error}"
            );
        }
        // an absent level is written as nothing at all, and the whole block writes back as read
        assert_eq!(
            serde_yaml::to_string(&FailureDomains::default()).unwrap(),
            "{}\n"
        );
        let again: FailureDomains =
            serde_yaml::from_str(&serde_yaml::to_string(&every).unwrap()).unwrap();
        assert_eq!(again, every);
        // and the levels are named widest first, as a spread is judged
        assert_eq!(LEVELS, [ZONE, RACK, HOST]);
        assert_eq!(
            every.named(),
            vec![(ZONE, "lab-east"), (RACK, "shelf-1"), (HOST, "titan")]
        );
    }

    /// A name that is empty, too long, or holds whitespace or a control character is refused
    #[test]
    fn a_host_that_cannot_be_one_is_refused() {
        for bad in ["", "   ", "two words", "tab\there", "line\nbreak"] {
            assert!(FailureDomains::host(bad).check().is_err(), "{bad:?}");
        }
        assert!(FailureDomains::host("x".repeat(MAX_DOMAIN_NAME + 1))
            .check()
            .is_err());
        // a host at the bound, and a plain one, are both fine
        assert!(FailureDomains::host("x".repeat(MAX_DOMAIN_NAME))
            .check()
            .is_ok());
        assert!(FailureDomains::host("titan").check().is_ok());
        assert!(FailureDomains::default().check().is_ok());
        // a rack and a zone are held to the same rule, and refused naming their own key
        for (domains, key) in [
            (
                FailureDomains::host("titan").rack(" "),
                "cluster.failure_domains.rack",
            ),
            (
                FailureDomains::default().zone("lab east"),
                "cluster.failure_domains.zone",
            ),
        ] {
            let refused = domains.check().unwrap_err();
            assert!(refused.contains(key), "{refused}");
        }
        assert!(FailureDomains::host("titan")
            .rack("shelf-1")
            .zone("lab-east")
            .check()
            .is_ok());
    }

    /// A named host wins over the machine's, an absent one is the machine's, and no name at all
    /// is refused
    #[test]
    fn a_host_resolves_to_the_named_one_or_the_machines() {
        let host = |name: &str| BTreeMap::from([(HOST.to_string(), name.to_string())]);
        // the operator's name, whatever the machine says
        assert_eq!(
            FailureDomains::host("rack-one-a").resolve(Some("europa".to_string())),
            Ok(host("rack-one-a"))
        );
        // the machine's own name when none is given
        assert_eq!(
            FailureDomains::default().resolve(Some("europa".to_string())),
            Ok(host("europa"))
        );
        // neither is refused, naming the key that fixes it
        let refused = FailureDomains::default().resolve(None).unwrap_err();
        assert!(
            refused.contains("cluster.failure_domains.host"),
            "{refused}"
        );
        // and so is a machine name that cannot be one
        let refused = FailureDomains::default()
            .resolve(Some(" ".to_string()))
            .unwrap_err();
        assert!(
            refused.contains("cluster.failure_domains.host"),
            "{refused}"
        );
        // a bad name given is refused before the machine's is looked at
        assert!(FailureDomains::host("")
            .resolve(Some("europa".to_string()))
            .is_err());
        // a rack and a zone named are resolved beside the host, the machine's or the named one
        let every = FailureDomains::default()
            .rack("shelf-1")
            .zone("lab-east")
            .resolve(Some("europa".to_string()))
            .expect("every level resolves");
        assert_eq!(
            every,
            BTreeMap::from([
                (HOST.to_string(), "europa".to_string()),
                (RACK.to_string(), "shelf-1".to_string()),
                (ZONE.to_string(), "lab-east".to_string()),
            ])
        );
        // and a bad rack is refused, though the machine gave a good host
        assert!(FailureDomains::default()
            .rack("")
            .resolve(Some("europa".to_string()))
            .is_err());
    }

    /// A member stands at each level in the name it gives there or the nearest one below, tagged
    /// with the level it came from, and in none when it names nothing at or below (F83)
    #[test]
    fn a_level_left_out_is_the_nearest_named_below() {
        let record = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(level, name)| ((*level).to_string(), (*name).to_string()))
                .collect()
        };
        // every level named is read as it is
        let every = record(&[(HOST, "titan"), (RACK, "r1"), (ZONE, "east")]);
        assert_eq!(domain_at(&every, ZONE), Some((ZONE, "east")));
        assert_eq!(domain_at(&every, RACK), Some((RACK, "r1")));
        assert_eq!(domain_at(&every, HOST), Some((HOST, "titan")));
        // no zone: the rack stands in as the zone
        let racked = record(&[(HOST, "titan"), (RACK, "r1")]);
        assert_eq!(domain_at(&racked, ZONE), Some((RACK, "r1")));
        // a host alone stands in as its own rack and zone, tagged as a host, so a host called r1
        // never equals a rack called r1
        let hosted = record(&[(HOST, "r1")]);
        assert_eq!(domain_at(&hosted, ZONE), Some((HOST, "r1")));
        assert_eq!(domain_at(&hosted, RACK), Some((HOST, "r1")));
        assert_ne!(domain_at(&hosted, RACK), domain_at(&racked, RACK));
        // a zone with no rack: the rack level falls to the host, never up to the zone
        let zoned = record(&[(HOST, "titan"), (ZONE, "east")]);
        assert_eq!(domain_at(&zoned, RACK), Some((HOST, "titan")));
        assert_eq!(domain_at(&zoned, ZONE), Some((ZONE, "east")));
        // a record naming nothing stands nowhere, which a caller reads as a domain of its own
        let none = BTreeMap::new();
        for level in LEVELS {
            assert_eq!(domain_at(&none, level), None);
        }
        // a level no build knows is never read, though a record may carry it
        let unknown = record(&[(HOST, "titan"), ("row", "r9")]);
        assert_eq!(domain_at(&unknown, RACK), Some((HOST, "titan")));
    }
}
