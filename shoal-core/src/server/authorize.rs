//! Holding a client's principal to the grants in force
//! ([F77](../../../docs/src/features/authorization.md))
//!
//! The coordinating shard is where a client's bundle enters this node's routing, so it is where a
//! principal is held to its grants: a query on a table its principal holds no grant for is
//! answered `Unauthorized` by name before anything is routed. A node that receives a forwarded
//! share trusts the coordinator that sent it, as it trusts that coordinator's routing.
//!
//! The grants are compiled against the schema whenever they change, into one [`Privileges`] a
//! table by the table's position, so the check on a query is an index and a mask. A bucket's are
//! compiled the same way, by the bucket's position, for its operations to be checked against
//! ([F79](../../../docs/src/features/bucket-schema.md)).
//!
//! # Invariants
//!
//! **A connection with no principal is never held to the grants.** It exists only on a server
//! that requires no authentication, where nobody can be named, and that is every benchmark and
//! test that does not ask for authentication; the check costs such a bundle one load of an empty
//! map's length.
//!
//! **An admin holds every table.** `cluster.admins` can grant itself anything, so refusing it a
//! table would protect nothing; it is compiled to [`Access::Everything`], which skips the check.
//!
//! **A principal named nowhere holds nothing.** It compiles to a table of `NONE`, never to a
//! missing entry the check could read as "not restricted".
//!
//! **A table a bucket generated compiles to nothing for every principal but an admin**, whatever
//! `*` grants. The front door refuses a client's query of one before this check is reached, so this
//! is the second wall, not the first: a grant can never be what lets a client write a bucket's row.

use std::collections::HashMap;
use std::rc::Rc;

use crate::shared::grants::{Grants, Privileges};
use crate::shared::traits::QuerySupport;

/// What one principal may do to this node's tables and buckets, as a shard compiled it
#[derive(Debug, Clone)]
pub enum Access {
    /// Anything to every table and every bucket: an admin's
    Everything,
    /// What it holds on each table and each bucket, by position among the schema's
    Granted {
        /// What it holds on each table, by the table's position among the schema's tables
        tables: Rc<[Privileges]>,
        /// What it holds on each bucket, by the bucket's position among the schema's buckets
        buckets: Rc<[Privileges]>,
    },
}

impl Access {
    /// What this access holds on a table
    ///
    /// # Arguments
    ///
    /// * `position` - The table's position among the schema's tables
    #[must_use]
    pub fn on(&self, position: usize) -> Privileges {
        match self {
            Access::Everything => Privileges::ALL,
            // a position past the schema is a table this build does not serve: nothing
            Access::Granted { tables, .. } => tables.get(position).copied().unwrap_or_default(),
        }
    }

    /// What this access holds on a bucket
    ///
    /// What the object store's operations are checked against, by the coordinator that checks a
    /// table's query ([F79](../../../docs/src/features/bucket-schema.md)).
    ///
    /// # Arguments
    ///
    /// * `position` - The bucket's position among the schema's buckets
    #[must_use]
    pub fn on_bucket(&self, position: usize) -> Privileges {
        match self {
            Access::Everything => Privileges::ALL,
            // a position past the schema is a bucket this build does not declare: nothing
            Access::Granted { buckets, .. } => buckets.get(position).copied().unwrap_or_default(),
        }
    }

    /// Whether this access allows what an operation on a bucket needs
    ///
    /// # Arguments
    ///
    /// * `position` - The bucket's position among the schema's buckets
    /// * `needed` - What the operation needs: `read` for a get or a stat, `write` for the rest
    #[must_use]
    pub fn allows_bucket(&self, position: usize, needed: Privileges) -> bool {
        self.on_bucket(position).contains(needed)
    }

    /// Whether this access allows everything, so no query of it needs checking
    #[must_use]
    pub const fn is_everything(&self) -> bool {
        matches!(self, Access::Everything)
    }
}

/// The tables and buckets of a schema, in the order their positions index
#[derive(Debug, Clone, Copy)]
pub struct Positions<'a> {
    /// Every table, generated ones included, in the order their positions index
    pub tables: &'a [&'a str],
    /// The tables buckets generated, each paired with its bucket
    pub generated: &'a [(&'a str, &'a str)],
    /// Every bucket, in the order their positions index
    pub buckets: &'a [&'a str],
}

impl Positions<'static> {
    /// The tables and buckets a schema declares, in the order their positions index
    #[must_use]
    pub fn of<S: QuerySupport>() -> Self {
        Positions {
            tables: S::table_names(),
            generated: S::generated_tables(),
            buckets: S::bucket_names(),
        }
    }
}

impl<'a> Positions<'a> {
    /// The tables of a schema with no bucket
    ///
    /// # Arguments
    ///
    /// * `tables` - Every table, in the order their positions index
    #[must_use]
    pub fn tables(tables: &'a [&'a str]) -> Self {
        Positions {
            tables,
            generated: &[],
            buckets: &[],
        }
    }
}

/// Who may do what to this node's tables, compiled from the grants and the admins in force
#[derive(Debug, Clone)]
pub struct Authorizer {
    /// Each principal the grants or the admins name, and what it may do
    principals: HashMap<String, Access>,
    /// What a principal named nowhere holds: nothing on any table or bucket
    nothing: Access,
}

impl Authorizer {
    /// Compile what every principal may do to each of the schema's tables and buckets
    ///
    /// # Arguments
    ///
    /// * `grants` - The grants in force
    /// * `admins` - The principals that may change the cluster, which hold everything
    /// * `positions` - The schema's tables and buckets, in the order their positions index
    #[must_use]
    pub fn compile(grants: &Grants, admins: &[String], positions: Positions<'_>) -> Self {
        // each granted principal's privileges on each table and bucket, the name's grant and
        // `*`'s together, and nothing on a table a bucket generated
        let mut principals = HashMap::with_capacity(grants.principals.len() + admins.len());
        for (principal, held) in &grants.principals {
            let tables: Rc<[Privileges]> = positions
                .tables
                .iter()
                .map(|table| {
                    // a generated table is reached through its bucket alone, `*` or not
                    if positions.generated.iter().any(|(name, _)| name == table) {
                        Privileges::NONE
                    } else {
                        held.on_table(table)
                    }
                })
                .collect();
            let buckets: Rc<[Privileges]> = positions
                .buckets
                .iter()
                .map(|bucket| held.on_bucket(bucket))
                .collect();
            principals.insert(principal.clone(), Access::Granted { tables, buckets });
        }
        // an admin can grant itself anything, so it holds everything whatever it was granted
        for admin in admins {
            principals.insert(admin.clone(), Access::Everything);
        }
        Authorizer {
            principals,
            nothing: Access::Granted {
                tables: vec![Privileges::NONE; positions.tables.len()].into(),
                buckets: vec![Privileges::NONE; positions.buckets.len()].into(),
            },
        }
    }

    /// What a principal may do to each table
    ///
    /// # Arguments
    ///
    /// * `principal` - The name the connection authenticated as
    #[must_use]
    pub fn access(&self, principal: &str) -> Access {
        self.principals
            .get(principal)
            .unwrap_or(&self.nothing)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::grants::EVERY_TABLE;

    /// A principal holds what its grants name on each table, `*` adding to every one of them, an
    /// admin holds everything, and a principal named nowhere holds nothing
    #[test]
    fn grants_compile_to_a_privilege_a_table() {
        let tables = ["Movie", "Review", "Credit"];
        let mut grants = Grants::default();
        grants.grant("reader", "Movie", Privileges::READ);
        grants.grant("reader", "Review", Privileges::WRITE);
        grants.grant("auditor", EVERY_TABLE, Privileges::READ);
        grants.grant("auditor", "Credit", Privileges::WRITE);
        let authorizer =
            Authorizer::compile(&grants, &["admin".to_string()], Positions::tables(&tables));
        let reader = authorizer.access("reader");
        assert_eq!(reader.on(0), Privileges::READ);
        assert_eq!(reader.on(1), Privileges::WRITE);
        assert_eq!(reader.on(2), Privileges::NONE);
        let auditor = authorizer.access("auditor");
        assert_eq!(auditor.on(0), Privileges::READ);
        assert_eq!(auditor.on(2), Privileges::ALL);
        // the admin skips the check whatever it was granted
        assert!(authorizer.access("admin").is_everything());
        // nobody named holds nothing, on every table and past the last
        let stranger = authorizer.access("stranger");
        assert!(!stranger.is_everything());
        for position in 0..=tables.len() {
            assert_eq!(stranger.on(position), Privileges::NONE);
        }
    }

    /// A bucket's grants compile beside a table's, by the bucket's position, and a table a
    /// bucket generated compiles to nothing for every principal but an admin, `*` or not (F79)
    #[test]
    fn grants_compile_to_a_privilege_a_bucket_and_nothing_on_a_generated_table() {
        use crate::shared::grants::{GrantOn, EVERY_BUCKET};
        // a schema of one table and one bucket, its two tables in the bucket's place
        let tables = ["Movie", "PostersObjectMeta", "PostersStripeMeta"];
        let generated = [
            ("PostersObjectMeta", "Posters"),
            ("PostersStripeMeta", "Posters"),
        ];
        let buckets = ["Posters", "Stills"];
        let positions = Positions {
            tables: &tables,
            generated: &generated,
            buckets: &buckets,
        };
        let mut grants = Grants::default();
        grants.grant("everything", EVERY_TABLE, Privileges::ALL);
        grants.grant_on("viewer", GrantOn::Bucket, "Posters", Privileges::READ);
        grants.grant_on("loader", GrantOn::Bucket, EVERY_BUCKET, Privileges::ALL);
        let authorizer = Authorizer::compile(&grants, &["admin".to_string()], positions);
        // `*` on every table reaches the declared one and neither generated one
        let everything = authorizer.access("everything");
        assert_eq!(everything.on(0), Privileges::ALL);
        assert_eq!(everything.on(1), Privileges::NONE);
        assert_eq!(everything.on(2), Privileges::NONE);
        assert_eq!(everything.on_bucket(0), Privileges::NONE);
        // a bucket's grant holds on its bucket alone, and `*` on every bucket
        let viewer = authorizer.access("viewer");
        assert!(viewer.allows_bucket(0, Privileges::READ));
        assert!(!viewer.allows_bucket(0, Privileges::WRITE));
        assert!(!viewer.allows_bucket(1, Privileges::READ));
        let loader = authorizer.access("loader");
        assert!(loader.allows_bucket(0, Privileges::WRITE));
        assert!(loader.allows_bucket(1, Privileges::WRITE));
        // nobody named holds nothing on any bucket, past the last included
        let stranger = authorizer.access("stranger");
        for position in 0..=buckets.len() {
            assert_eq!(stranger.on_bucket(position), Privileges::NONE);
        }
        // and an admin holds every bucket
        assert!(authorizer.access("admin").allows_bucket(1, Privileges::ALL));
    }
}
