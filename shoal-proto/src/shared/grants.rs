//! What a principal may do to the tables a schema serves
//!
//! [F12](../../../docs/src/features/authentication.md) gave a connection a principal and stopped
//! there. This is what a principal is held to ([F77](../../../docs/src/features/authorization.md)):
//! a catalog of grants, by principal and then by table, each naming the privileges it allows. A
//! server reads it from `auth.grants`; a cluster seeds it from the bootstrapper's file and commits
//! every change to it after, so every coordinator holds the same catalog.
//!
//! The catalog is keyed by the kind of thing granted, and a table is the first kind. A bucket is
//! the second ([S2](../../../docs/src/object-storage/buckets.md)): a `buckets` map beside `tables`,
//! holding the same [`Privileges`] under the bucket's name, so the object store is authorized by
//! this catalog and not by a second one ([F79](../../../docs/src/features/bucket-schema.md)). The
//! tables a bucket generated are reached through the bucket alone: no table grant can name one.
//!
//! # Invariants
//!
//! **Nothing is allowed that no grant names.** A principal the catalog does not name holds
//! nothing, and a table its grants do not name, under its own name or `*`, is refused. Only a
//! connection with no principal at all, on a server that requires no authentication, is not held
//! to the catalog, since there is nobody to hold.
//!
//! **A table is named as the schema spells it, or `*` for every table, and a bucket the same
//! way.** A name the schema does not serve, and a table a bucket generated, are refused where the
//! grant is made - at startup for a file, at the proposal for an admin request - and never kept
//! as a grant on nothing.
//!
//! **Read and write are separate, and neither implies the other.** A writer that may not read
//! learns no rows. It does learn whether a conditional write's condition held, one bit a write,
//! which is what the condition is for.
//!
//! **A revoke takes back what a grant on the same name gave.** There are no negative grants, so a
//! revoke of one table never subtracts from what `*` holds.

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

/// The name a grant uses for every table the schema serves
pub const EVERY_TABLE: &str = "*";

/// The name a grant uses for every bucket the schema declares
pub const EVERY_BUCKET: &str = "*";

/// The kind of thing a grant is made on
///
/// The catalog holds a principal's grants by kind, and every grant names one of these beside the
/// name it is made on ([F79](../../../docs/src/features/bucket-schema.md)).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GrantOn {
    /// A table the schema declares, by the name it spells it
    #[default]
    Table,
    /// A bucket the schema declares, by its marker's name
    Bucket,
}

impl GrantOn {
    /// What a grant on this kind names, for a message
    #[must_use]
    pub fn noun(self) -> &'static str {
        match self {
            GrantOn::Table => "table",
            GrantOn::Bucket => "bucket",
        }
    }
}

impl fmt::Display for GrantOn {
    /// Write the kind as an operator types it
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.noun())
    }
}

/// What a schema serves that a grant can name
///
/// Built by [`QuerySupport::served`](crate::shared::traits::QuerySupport::served) from a schema's
/// own lists, or from what a cluster's control state recorded at its initialization.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Served<'a> {
    /// Every table a client can query, which a table grant can name
    pub tables: Vec<&'a str>,
    /// Every table a bucket generated, paired with its bucket, which no table grant can name
    pub generated: Vec<(&'a str, &'a str)>,
    /// Every bucket, which a bucket grant can name
    pub buckets: Vec<&'a str>,
}

impl<'a> Served<'a> {
    /// What a schema with these tables and no bucket serves
    ///
    /// # Arguments
    ///
    /// * `tables` - The tables it serves, as it spells them
    #[must_use]
    pub fn tables(tables: &[&'a str]) -> Self {
        Served {
            tables: tables.to_vec(),
            generated: Vec::new(),
            buckets: Vec::new(),
        }
    }
}

/// What a grant allows: reading, writing, both, or neither
///
/// A set of two bits rather than an enum of four, so a check is one mask. It is written in a file
/// and on the wire as a list of names, `[read, write]`, and read from that or from one string of
/// names separated by commas, `read, write`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Privileges(u8);

impl Privileges {
    /// Nothing at all
    pub const NONE: Privileges = Privileges(0);
    /// Reading rows: a get, an exists, a filter
    pub const READ: Privileges = Privileges(1);
    /// Changing rows: an insert, an update, a delete, conditional or not
    pub const WRITE: Privileges = Privileges(1 << 1);
    /// Reading and writing
    pub const ALL: Privileges = Privileges(Privileges::READ.0 | Privileges::WRITE.0);

    /// The privilege a query needs, by whether it changes its table
    ///
    /// # Arguments
    ///
    /// * `write` - Whether the query changes its table
    #[must_use]
    pub const fn for_query(write: bool) -> Self {
        if write {
            Privileges::WRITE
        } else {
            Privileges::READ
        }
    }

    /// Whether these privileges include every one of another set
    ///
    /// # Arguments
    ///
    /// * `other` - The privileges that have to be held
    #[must_use]
    pub const fn contains(self, other: Privileges) -> bool {
        self.0 & other.0 == other.0
    }

    /// These privileges and another set's
    ///
    /// # Arguments
    ///
    /// * `other` - The privileges to add
    #[must_use]
    pub const fn union(self, other: Privileges) -> Self {
        Privileges(self.0 | other.0)
    }

    /// These privileges without another set's
    ///
    /// # Arguments
    ///
    /// * `other` - The privileges to take away
    #[must_use]
    pub const fn without(self, other: Privileges) -> Self {
        Privileges(self.0 & !other.0)
    }

    /// Whether these privileges allow nothing
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The names of the privileges in this set, in the order they are written
    #[must_use]
    pub fn names(self) -> Vec<&'static str> {
        // each privilege this set holds, read before write
        let mut names = Vec::with_capacity(2);
        if self.contains(Privileges::READ) {
            names.push("read");
        }
        if self.contains(Privileges::WRITE) {
            names.push("write");
        }
        names
    }

    /// Parse one privilege's name
    ///
    /// `all` is accepted as both, since it is what an operator types for a principal that may do
    /// anything to a table.
    ///
    /// # Arguments
    ///
    /// * `name` - The name, in any case
    ///
    /// # Errors
    ///
    /// When the name is not a privilege, naming the ones that are.
    pub fn parse_one(name: &str) -> Result<Self, GrantError> {
        match name.trim().to_ascii_lowercase().as_str() {
            "read" => Ok(Privileges::READ),
            "write" => Ok(Privileges::WRITE),
            "all" => Ok(Privileges::ALL),
            _ => Err(GrantError::UnknownPrivilege(name.trim().to_string())),
        }
    }

    /// Parse a list of privileges separated by commas, such as `read,write`
    ///
    /// # Arguments
    ///
    /// * `list` - The names
    ///
    /// # Errors
    ///
    /// When any name is not a privilege, or the list names none.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_proto::shared::grants::Privileges;
    ///
    /// assert_eq!(Privileges::parse("read, write").unwrap(), Privileges::ALL);
    /// assert!(Privileges::parse("delete").is_err());
    /// ```
    pub fn parse(list: &str) -> Result<Self, GrantError> {
        // every name in the list, empty pieces skipped so a trailing comma is not a privilege
        let mut privileges = Privileges::NONE;
        for name in list.split(',').filter(|name| !name.trim().is_empty()) {
            privileges = privileges.union(Privileges::parse_one(name)?);
        }
        // a list naming nothing grants nothing, which is never what was meant
        if privileges.is_empty() {
            return Err(GrantError::NoPrivileges);
        }
        Ok(privileges)
    }
}

impl fmt::Display for Privileges {
    /// Write these privileges as the names a file and a command spell them
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // nothing has no names, so it is written as a word of its own
        if self.is_empty() {
            return write!(f, "nothing");
        }
        write!(f, "{}", self.names().join(", "))
    }
}

impl Serialize for Privileges {
    /// Write these privileges as a list of their names
    ///
    /// # Arguments
    ///
    /// * `serializer` - Where to write them
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.names().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Privileges {
    /// Read privileges from a list of names, or one string of names separated by commas
    ///
    /// # Arguments
    ///
    /// * `deserializer` - Where to read them from
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(PrivilegesVisitor)
    }
}

/// Reads [`Privileges`] in either of the two shapes a file writes them in
struct PrivilegesVisitor;

impl<'de> Visitor<'de> for PrivilegesVisitor {
    type Value = Privileges;

    /// Say what this visitor reads, for a refusal
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a list of privileges, such as [read, write]")
    }

    /// Read one string of names separated by commas
    ///
    /// # Arguments
    ///
    /// * `value` - The string
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Privileges, E> {
        Privileges::parse(value).map_err(E::custom)
    }

    /// Read a list of names
    ///
    /// # Arguments
    ///
    /// * `seq` - The list
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Privileges, A::Error> {
        // every name in the list, any one of which may be refused by name
        let mut privileges = Privileges::NONE;
        while let Some(name) = seq.next_element::<String>()? {
            privileges = privileges.union(Privileges::parse_one(&name).map_err(de::Error::custom)?);
        }
        // an empty list grants nothing, which is never what was meant
        if privileges.is_empty() {
            return Err(de::Error::custom(GrantError::NoPrivileges));
        }
        Ok(privileges)
    }
}

/// Why a grant could not be made, or a catalog could not be read
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantError {
    /// A privilege's name is not one this build knows
    UnknownPrivilege(String),
    /// A grant or a revoke named no privilege at all
    NoPrivileges,
    /// A grant named no principal
    NoPrincipal,
    /// A grant named a table the schema does not serve
    UnknownTable {
        /// The principal the grant was for
        principal: String,
        /// The table it named
        table: String,
        /// The tables the schema serves
        served: Vec<String>,
    },
    /// A grant named a table a bucket generated, which is reached through its bucket alone
    GeneratedTable {
        /// The principal the grant was for
        principal: String,
        /// The table it named
        table: String,
        /// The bucket that generated it
        bucket: String,
    },
    /// A grant named a bucket the schema does not declare
    UnknownBucket {
        /// The principal the grant was for
        principal: String,
        /// The bucket it named
        bucket: String,
        /// The buckets the schema declares
        served: Vec<String>,
    },
}

impl fmt::Display for GrantError {
    /// Write a legible description of this refusal
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrantError::UnknownPrivilege(name) => {
                write!(f, "{name} is not a privilege; read, write or all")
            }
            GrantError::NoPrivileges => write!(f, "a grant has to name read, write or both"),
            GrantError::NoPrincipal => write!(f, "a grant has to name a principal"),
            GrantError::UnknownTable {
                principal,
                table,
                served,
            } => write!(
                f,
                "the grant to {principal} names {table}, which the schema does not serve; \
                 it serves {served:?}, or {EVERY_TABLE} for every table"
            ),
            GrantError::GeneratedTable {
                principal,
                table,
                bucket,
            } => write!(
                f,
                "the grant to {principal} names {table}, which bucket {bucket} generated and \
                 which is reached through that bucket alone; grant on the bucket instead"
            ),
            GrantError::UnknownBucket {
                principal,
                bucket,
                served,
            } => write!(
                f,
                "the grant to {principal} names bucket {bucket}, which the schema does not \
                 declare; it declares {served:?}, or {EVERY_BUCKET} for every bucket"
            ),
        }
    }
}

impl std::error::Error for GrantError {}

/// What one principal is granted
///
/// Unknown keys are refused, so a key from a build that grants more - a `roles` map, say - is
/// never read as a principal holding nothing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalGrants {
    /// What this principal holds on each table, by the name the schema spells it, or `*` for
    /// every table
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tables: BTreeMap<String, Privileges>,
    /// What this principal holds on each bucket, by its marker's name, or `*` for every bucket
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub buckets: BTreeMap<String, Privileges>,
}

impl PrincipalGrants {
    /// What this principal holds on a table: its grant on that name and its grant on `*`
    ///
    /// # Arguments
    ///
    /// * `table` - The table, as the schema spells it
    #[must_use]
    pub fn on_table(&self, table: &str) -> Privileges {
        // a grant on the name, and one on every table, each count
        let named = self.tables.get(table).copied().unwrap_or_default();
        let every = self.tables.get(EVERY_TABLE).copied().unwrap_or_default();
        named.union(every)
    }

    /// What this principal holds on a bucket: its grant on that name and its grant on `*`
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket, by its marker's name
    #[must_use]
    pub fn on_bucket(&self, bucket: &str) -> Privileges {
        // a grant on the name, and one on every bucket, each count
        let named = self.buckets.get(bucket).copied().unwrap_or_default();
        let every = self.buckets.get(EVERY_BUCKET).copied().unwrap_or_default();
        named.union(every)
    }

    /// The grants this principal holds on one kind of thing, by name
    ///
    /// # Arguments
    ///
    /// * `on` - The kind of thing
    #[must_use]
    pub fn of(&self, on: GrantOn) -> &BTreeMap<String, Privileges> {
        match on {
            GrantOn::Table => &self.tables,
            GrantOn::Bucket => &self.buckets,
        }
    }

    /// The grants this principal holds on one kind of thing, to change
    ///
    /// # Arguments
    ///
    /// * `on` - The kind of thing
    fn of_mut(&mut self, on: GrantOn) -> &mut BTreeMap<String, Privileges> {
        match on {
            GrantOn::Table => &mut self.tables,
            GrantOn::Bucket => &mut self.buckets,
        }
    }

    /// Whether this principal holds nothing on any table or bucket
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables
            .values()
            .chain(self.buckets.values())
            .all(|privileges| privileges.is_empty())
    }
}

/// Every principal's grants: the catalog a node authorizes queries against
///
/// Ordered maps throughout, so the catalog serializes the same way on every node and a committed
/// copy compares equal to what was proposed.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Grants {
    /// Each principal's grants, by the name it authenticates as
    pub principals: BTreeMap<String, PrincipalGrants>,
}

impl Grants {
    /// Grant a principal privileges on a table, on top of what it already holds there
    ///
    /// Returns whether anything changed, so a grant of what is already held moves no version.
    ///
    /// # Arguments
    ///
    /// * `principal` - Who is granted
    /// * `table` - The table, as the schema spells it, or `*`
    /// * `privileges` - What is granted
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_proto::shared::grants::{Grants, Privileges};
    ///
    /// let mut grants = Grants::default();
    /// assert!(grants.grant("reader", "Movie", Privileges::READ));
    /// assert!(!grants.grant("reader", "Movie", Privileges::READ));
    /// assert_eq!(grants.on_table("reader", "Movie"), Privileges::READ);
    /// ```
    pub fn grant(&mut self, principal: &str, table: &str, privileges: Privileges) -> bool {
        // a grant on a table is a grant on the first kind of thing
        self.grant_on(principal, GrantOn::Table, table, privileges)
    }

    /// Grant a principal privileges on a table or a bucket, on top of what it already holds there
    ///
    /// Returns whether anything changed, so a grant of what is already held moves no version.
    ///
    /// # Arguments
    ///
    /// * `principal` - Who is granted
    /// * `on` - Whether the name is a table's or a bucket's
    /// * `name` - The table or bucket, as the schema spells it, or `*`
    /// * `privileges` - What is granted
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_proto::shared::grants::{GrantOn, Grants, Privileges};
    ///
    /// let mut grants = Grants::default();
    /// assert!(grants.grant_on("reader", GrantOn::Bucket, "Posters", Privileges::READ));
    /// assert_eq!(grants.on_bucket("reader", "Posters"), Privileges::READ);
    /// assert_eq!(grants.on_table("reader", "Posters"), Privileges::NONE);
    /// ```
    pub fn grant_on(
        &mut self,
        principal: &str,
        on: GrantOn,
        name: &str,
        privileges: Privileges,
    ) -> bool {
        // what the principal holds on this name now, if anything
        let held = self
            .principals
            .entry(principal.to_string())
            .or_default()
            .of_mut(on)
            .entry(name.to_string())
            .or_default();
        let granted = held.union(privileges);
        let changed = granted != *held;
        *held = granted;
        // a grant of nothing leaves no empty entry behind
        self.prune(principal);
        changed
    }

    /// Take privileges a grant on a table gave a principal back
    ///
    /// Returns whether anything changed. A revoke on one table takes nothing from what `*`
    /// grants, since there are no negative grants.
    ///
    /// # Arguments
    ///
    /// * `principal` - Who is revoked
    /// * `table` - The name the grant was made on, as the schema spells it, or `*`
    /// * `privileges` - What is taken back
    pub fn revoke(&mut self, principal: &str, table: &str, privileges: Privileges) -> bool {
        // a revoke on a table is a revoke on the first kind of thing
        self.revoke_on(principal, GrantOn::Table, table, privileges)
    }

    /// Take privileges a grant on a table or a bucket gave a principal back
    ///
    /// Returns whether anything changed. A revoke on one name takes nothing from what `*`
    /// grants, since there are no negative grants.
    ///
    /// # Arguments
    ///
    /// * `principal` - Who is revoked
    /// * `on` - Whether the name is a table's or a bucket's
    /// * `name` - The name the grant was made on, as the schema spells it, or `*`
    /// * `privileges` - What is taken back
    pub fn revoke_on(
        &mut self,
        principal: &str,
        on: GrantOn,
        name: &str,
        privileges: Privileges,
    ) -> bool {
        // a principal or a name with no grant has nothing to take back
        let Some(held) = self
            .principals
            .get_mut(principal)
            .and_then(|grants| grants.of_mut(on).get_mut(name))
        else {
            return false;
        };
        let kept = held.without(privileges);
        let changed = kept != *held;
        *held = kept;
        // a grant left holding nothing is removed, and so is a principal left with none
        self.prune(principal);
        changed
    }

    /// Remove the grants a principal holds nothing under, and the principal if none are left
    ///
    /// # Arguments
    ///
    /// * `principal` - The principal to tidy
    fn prune(&mut self, principal: &str) {
        if let Some(grants) = self.principals.get_mut(principal) {
            // a grant holding nothing goes, of either kind
            grants.tables.retain(|_, privileges| !privileges.is_empty());
            grants
                .buckets
                .retain(|_, privileges| !privileges.is_empty());
            // and a principal goes only once it holds nothing of any kind
            if grants.tables.is_empty() && grants.buckets.is_empty() {
                self.principals.remove(principal);
            }
        }
    }

    /// What a principal holds on a table, through its grant on the name and on `*`
    ///
    /// # Arguments
    ///
    /// * `principal` - The principal
    /// * `table` - The table, as the schema spells it
    #[must_use]
    pub fn on_table(&self, principal: &str, table: &str) -> Privileges {
        self.principals
            .get(principal)
            .map_or(Privileges::NONE, |grants| grants.on_table(table))
    }

    /// What a principal holds on a bucket, through its grant on the name and on `*`
    ///
    /// # Arguments
    ///
    /// * `principal` - The principal
    /// * `bucket` - The bucket, by its marker's name
    #[must_use]
    pub fn on_bucket(&self, principal: &str, bucket: &str) -> Privileges {
        self.principals
            .get(principal)
            .map_or(Privileges::NONE, |grants| grants.on_bucket(bucket))
    }

    /// Whether the catalog grants nothing to anybody
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.principals.values().all(PrincipalGrants::is_empty)
    }

    /// Check every grant names a principal, and a table or a bucket the schema serves, or `*`
    ///
    /// # Arguments
    ///
    /// * `served` - What the schema serves that a grant can name
    ///
    /// # Errors
    ///
    /// The first grant that names nobody, a table or bucket the schema does not serve, or a
    /// table a bucket generated.
    pub fn check(&self, served: &Served<'_>) -> Result<(), GrantError> {
        for (principal, grants) in &self.principals {
            // a grant to the empty name could never be held by a connection
            if principal.trim().is_empty() {
                return Err(GrantError::NoPrincipal);
            }
            // every table it names, and every bucket
            for table in grants.tables.keys() {
                check_table(principal, table, served)?;
            }
            for bucket in grants.buckets.keys() {
                check_bucket(principal, bucket, served)?;
            }
        }
        Ok(())
    }
}

/// Check a name a grant is made on is one the schema serves, of the kind the grant says, or `*`
///
/// # Arguments
///
/// * `principal` - The principal the grant is for, for the refusal
/// * `on` - Whether the name is a table's or a bucket's
/// * `name` - The name the grant is made on
/// * `served` - What the schema serves that a grant can name
///
/// # Errors
///
/// When the name is not one the schema serves of that kind, or is a table a bucket generated.
pub fn check_grant(
    principal: &str,
    on: GrantOn,
    name: &str,
    served: &Served<'_>,
) -> Result<(), GrantError> {
    match on {
        GrantOn::Table => check_table(principal, name, served),
        GrantOn::Bucket => check_bucket(principal, name, served),
    }
}

/// Check a table a grant names is one a client can query, or `*`
///
/// # Arguments
///
/// * `principal` - The principal the grant is for, for the refusal
/// * `table` - The table the grant names
/// * `served` - What the schema serves that a grant can name
///
/// # Errors
///
/// When the name is a table a bucket generated, or neither a served table nor `*`.
pub fn check_table(principal: &str, table: &str, served: &Served<'_>) -> Result<(), GrantError> {
    // every table, or one the schema declares under exactly this name
    if table == EVERY_TABLE || served.tables.contains(&table) {
        return Ok(());
    }
    // a table a bucket generated is refused by name, with the bucket to grant on instead
    if let Some((_, bucket)) = served.generated.iter().find(|(name, _)| *name == table) {
        return Err(GrantError::GeneratedTable {
            principal: principal.to_string(),
            table: table.to_string(),
            bucket: (*bucket).to_string(),
        });
    }
    Err(GrantError::UnknownTable {
        principal: principal.to_string(),
        table: table.to_string(),
        served: served
            .tables
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    })
}

/// Check a bucket a grant names is one the schema declares, or `*`
///
/// # Arguments
///
/// * `principal` - The principal the grant is for, for the refusal
/// * `bucket` - The bucket the grant names
/// * `served` - What the schema serves that a grant can name
///
/// # Errors
///
/// When the name is neither a declared bucket nor `*`.
pub fn check_bucket(principal: &str, bucket: &str, served: &Served<'_>) -> Result<(), GrantError> {
    // every bucket, or one the schema declares under exactly this name
    if bucket == EVERY_BUCKET || served.buckets.contains(&bucket) {
        return Ok(());
    }
    Err(GrantError::UnknownBucket {
        principal: principal.to_string(),
        bucket: bucket.to_string(),
        served: served
            .buckets
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every spelling a file or a command uses parses to the set it names, and nothing else does
    #[test]
    fn privileges_parse_from_their_names() {
        assert_eq!(Privileges::parse("read").unwrap(), Privileges::READ);
        assert_eq!(Privileges::parse("write").unwrap(), Privileges::WRITE);
        assert_eq!(Privileges::parse("read,write").unwrap(), Privileges::ALL);
        assert_eq!(
            Privileges::parse(" Write , READ ").unwrap(),
            Privileges::ALL
        );
        assert_eq!(Privileges::parse("all").unwrap(), Privileges::ALL);
        // a name that is not a privilege is refused by name, never read as nothing
        assert_eq!(
            Privileges::parse("read,delete"),
            Err(GrantError::UnknownPrivilege("delete".to_string()))
        );
        assert_eq!(Privileges::parse(""), Err(GrantError::NoPrivileges));
        assert_eq!(Privileges::parse(" , "), Err(GrantError::NoPrivileges));
    }

    /// Neither privilege implies the other, and a query asks for exactly one of them
    #[test]
    fn read_and_write_are_separate() {
        assert!(!Privileges::READ.contains(Privileges::WRITE));
        assert!(!Privileges::WRITE.contains(Privileges::READ));
        assert!(Privileges::ALL.contains(Privileges::READ));
        assert!(Privileges::ALL.contains(Privileges::WRITE));
        assert!(!Privileges::NONE.contains(Privileges::READ));
        assert_eq!(Privileges::for_query(false), Privileges::READ);
        assert_eq!(Privileges::for_query(true), Privileges::WRITE);
        assert_eq!(Privileges::ALL.without(Privileges::WRITE), Privileges::READ);
        assert_eq!(Privileges::NONE.to_string(), "nothing");
        assert_eq!(Privileges::ALL.to_string(), "read, write");
    }

    /// A catalog reads from the YAML a config file writes, in both shapes, and refuses a key it
    /// does not know rather than reading a principal that holds nothing
    #[test]
    fn a_catalog_reads_from_yaml() {
        let grants: Grants = serde_yaml::from_str(
            "reader:\n  tables:\n    Movie: [read]\n    Review: read, write\nloader:\n  tables:\n    '*': [read, write]\n",
        )
        .expect("a catalog");
        assert_eq!(grants.on_table("reader", "Movie"), Privileges::READ);
        assert_eq!(grants.on_table("reader", "Review"), Privileges::ALL);
        assert_eq!(grants.on_table("reader", "Credit"), Privileges::NONE);
        assert_eq!(grants.on_table("loader", "Credit"), Privileges::ALL);
        assert_eq!(grants.on_table("nobody", "Movie"), Privileges::NONE);
        // a list written back reads back the same
        let written = serde_yaml::to_string(&grants).expect("written");
        let read: Grants = serde_yaml::from_str(&written).expect("read back");
        assert_eq!(read, grants);
        // a privilege nobody knows, and a key nobody knows, are refused
        assert!(
            serde_yaml::from_str::<Grants>("reader:\n  tables:\n    Movie: [delete]\n").is_err()
        );
        assert!(serde_yaml::from_str::<Grants>("reader:\n  roles:\n    admin: [read]\n").is_err());
        assert!(serde_yaml::from_str::<Grants>("reader:\n  tables:\n    Movie: []\n").is_err());
    }

    /// A bucket's grants read beside a table's, and a principal holding only them is kept
    ///
    /// Before F79 a `buckets` key was refused, as a key a later build would write. It is the
    /// second kind now, read the way `tables` is.
    #[test]
    fn a_catalog_holds_bucket_grants_beside_table_grants() {
        let grants: Grants = serde_yaml::from_str(
            "viewer:\n  buckets:\n    Posters: [read]\nloader:\n  tables:\n    Movie: [write]\n  buckets:\n    '*': all\n",
        )
        .expect("a catalog with buckets");
        assert_eq!(grants.on_bucket("viewer", "Posters"), Privileges::READ);
        assert_eq!(grants.on_bucket("viewer", "Stills"), Privileges::NONE);
        assert_eq!(grants.on_bucket("loader", "Stills"), Privileges::ALL);
        // a bucket's grant gives nothing on a table of the same name, and the other way round
        assert_eq!(grants.on_table("viewer", "Posters"), Privileges::NONE);
        assert_eq!(grants.on_bucket("loader", "Movie"), Privileges::ALL);
        assert_eq!(grants.on_table("loader", "Movie"), Privileges::WRITE);
        // a catalog of bucket grants alone is not empty, and writes back what it read
        assert!(!grants.is_empty());
        let written = serde_yaml::to_string(&grants).expect("written");
        let read: Grants = serde_yaml::from_str(&written).expect("read back");
        assert_eq!(read, grants);
        // a principal holding only a bucket grant is kept by a revoke of nothing else
        let mut held = Grants::default();
        assert!(held.grant_on("viewer", GrantOn::Bucket, "Posters", Privileges::READ));
        assert!(!held.revoke("viewer", "Posters", Privileges::READ));
        assert_eq!(held.on_bucket("viewer", "Posters"), Privileges::READ);
        // and leaves the catalog once its bucket grant is taken back
        assert!(held.revoke_on("viewer", GrantOn::Bucket, "Posters", Privileges::READ));
        assert!(held.principals.is_empty());
    }

    /// A grant adds to what is held, a revoke takes back what a grant on the same name gave and
    /// never what `*` gives, and each says whether anything changed
    #[test]
    fn grants_and_revokes_change_only_what_they_name() {
        let mut grants = Grants::default();
        assert!(grants.grant("reader", "Movie", Privileges::READ));
        assert!(grants.grant("reader", "Movie", Privileges::WRITE));
        assert!(!grants.grant("reader", "Movie", Privileges::ALL));
        assert_eq!(grants.on_table("reader", "Movie"), Privileges::ALL);
        assert!(grants.revoke("reader", "Movie", Privileges::WRITE));
        assert_eq!(grants.on_table("reader", "Movie"), Privileges::READ);
        // nothing held is nothing to take back
        assert!(!grants.revoke("reader", "Movie", Privileges::WRITE));
        assert!(!grants.revoke("nobody", "Movie", Privileges::READ));
        // a grant on every table is not subtracted from by a revoke of one
        assert!(grants.grant("reader", EVERY_TABLE, Privileges::READ));
        assert!(grants.revoke("reader", "Movie", Privileges::READ));
        assert_eq!(grants.on_table("reader", "Movie"), Privileges::READ);
        // and a principal left holding nothing is gone from the catalog
        assert!(grants.revoke("reader", EVERY_TABLE, Privileges::ALL));
        assert!(grants.principals.is_empty());
        assert!(grants.is_empty());
    }

    /// A grant naming a table the schema does not serve is refused by name, and `*` is not one
    #[test]
    fn a_grant_on_a_table_nobody_serves_is_refused() {
        let mut grants = Grants::default();
        grants.grant("reader", "Movie", Privileges::READ);
        grants.grant("reader", EVERY_TABLE, Privileges::READ);
        let served = Served::tables(&["Movie", "Review"]);
        assert_eq!(grants.check(&served), Ok(()));
        grants.grant("reader", "Moive", Privileges::READ);
        match grants.check(&served) {
            Err(GrantError::UnknownTable { table, .. }) => assert_eq!(table, "Moive"),
            other => panic!("a misspelled table was accepted: {other:?}"),
        }
        let mut nobody = Grants::default();
        nobody.grant(" ", "Movie", Privileges::READ);
        assert_eq!(
            nobody.check(&Served::tables(&["Movie"])),
            Err(GrantError::NoPrincipal)
        );
    }

    /// A table grant on a table a bucket generated is refused with the bucket to grant on, and
    /// a bucket grant naming a bucket the schema does not declare is refused by name
    #[test]
    fn a_grant_on_a_generated_table_or_an_unknown_bucket_is_refused() {
        let served = Served {
            tables: vec!["Movie"],
            generated: vec![
                ("PostersObjectMeta", "Posters"),
                ("PostersStripeMeta", "Posters"),
            ],
            buckets: vec!["Posters"],
        };
        // a table, a bucket and every bucket are each what a grant may name
        let mut grants = Grants::default();
        grants.grant("reader", "Movie", Privileges::READ);
        grants.grant_on("reader", GrantOn::Bucket, "Posters", Privileges::READ);
        grants.grant_on("reader", GrantOn::Bucket, EVERY_BUCKET, Privileges::READ);
        assert_eq!(grants.check(&served), Ok(()));
        // a generated table names the bucket it is reached through
        let mut generated = Grants::default();
        generated.grant("reader", "PostersStripeMeta", Privileges::WRITE);
        match generated.check(&served) {
            Err(GrantError::GeneratedTable { table, bucket, .. }) => {
                assert_eq!(table, "PostersStripeMeta");
                assert_eq!(bucket, "Posters");
            }
            other => panic!("a grant on a generated table was accepted: {other:?}"),
        }
        // a bucket's name is not a table's, and a misspelled bucket is nobody's
        assert!(matches!(
            check_grant("reader", GrantOn::Table, "Posters", &served),
            Err(GrantError::UnknownTable { .. })
        ));
        assert!(matches!(
            check_grant("reader", GrantOn::Bucket, "Postres", &served),
            Err(GrantError::UnknownBucket { .. })
        ));
        // and a table's name is not a bucket's
        assert!(matches!(
            check_grant("reader", GrantOn::Bucket, "Movie", &served),
            Err(GrantError::UnknownBucket { .. })
        ));
    }
}
