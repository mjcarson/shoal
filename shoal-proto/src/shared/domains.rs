//! Where a member stands, for placement to spread copies over
//!
//! A node names its failure domains in its own configuration, and the control group commits them
//! on its member record when the node observes itself
//! ([F81](../../../docs/src/features/failure-domains.md)). The one level today is the host: the
//! machine the node runs on, which defaults to the name the operating system gives that machine,
//! so nodes on one machine truthfully share a host and a test that needs several hosts names them.
//!
//! ```yaml
//! cluster:
//!   failure_domains:
//!     host: titan
//! ```
//!
//! The types here are what a node's configuration, `shoaladm`'s inventory and the fixture all
//! hold, so none of them can spell a domain another way. A pool's `failure_domain`
//! ([`FailureDomain`](super::pools::FailureDomain)) is the other half: the level no two chunks of a
//! stripe may share, which a member's domains are read against.
//!
//! # Invariants
//!
//! **A record holds a map of levels to names**, so a rack is another key and not a format change,
//! and a build that knows no rack keeps one it reads when it writes the record back.
//!
//! **Unknown keys are refused in a configuration**, so a level no build acts on yet is never
//! accepted and ignored.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The level a member's host is recorded under
pub const HOST: &str = "host";

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
        }
    }

    /// Whether every name given can be one, and why not
    ///
    /// # Errors
    ///
    /// The first name that is empty, too long, or holds whitespace or a control character.
    pub fn check(&self) -> Result<(), String> {
        // only what is named is judged; an absent host is the machine's own
        if let Some(host) = &self.host {
            check_name(HOST, host)?;
        }
        Ok(())
    }

    /// The domains this node stands in, keyed by level: the host it names, or the machine's own
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
        Ok(BTreeMap::from([(HOST.to_string(), host)]))
    }
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
        return Err(format!(
            "cluster.failure_domains.{level} is empty; name the {level} this node stands on, or \
             leave it out for the machine's own name"
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

    use super::{FailureDomains, HOST, MAX_DOMAIN_NAME};

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
        // a rack is refused until a build places by one
        let error = serde_yaml::from_str::<FailureDomains>("host: titan\nrack: r1")
            .expect_err("a rack is refused");
        assert!(error.to_string().contains("rack"), "{error}");
        // and an absent host is written as nothing at all
        assert_eq!(
            serde_yaml::to_string(&FailureDomains::default()).unwrap(),
            "{}\n"
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
    }
}
