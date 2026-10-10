//! Holding a client's principal to the grants in force
//! ([F77](../../../docs/src/features/authorization.md))
//!
//! The coordinating shard is where a client's bundle enters this node's routing, so it is where a
//! principal is held to its grants: a query on a table its principal holds no grant for is
//! answered `Unauthorized` by name before anything is routed. A node that receives a forwarded
//! share trusts the coordinator that sent it, as it trusts that coordinator's routing.
//!
//! The grants are compiled against the schema whenever they change, into one [`Privileges`] a
//! table by the table's position, so the check on a query is an index and a mask.
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

use std::collections::HashMap;
use std::rc::Rc;

use crate::shared::grants::{Grants, Privileges};

/// What one principal may do to this node's tables, as a shard compiled it
#[derive(Debug, Clone)]
pub enum Access {
    /// Anything to every table: an admin's
    Everything,
    /// What it holds on each table, by the table's position among the schema's tables
    Tables(Rc<[Privileges]>),
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
            Access::Tables(tables) => tables.get(position).copied().unwrap_or_default(),
        }
    }

    /// Whether this access allows everything, so no query of it needs checking
    #[must_use]
    pub const fn is_everything(&self) -> bool {
        matches!(self, Access::Everything)
    }
}

/// Who may do what to this node's tables, compiled from the grants and the admins in force
#[derive(Debug, Clone)]
pub struct Authorizer {
    /// Each principal the grants or the admins name, and what it may do
    principals: HashMap<String, Access>,
    /// What a principal named nowhere holds: nothing on any table
    nothing: Access,
}

impl Authorizer {
    /// Compile what every principal may do to each of the schema's tables
    ///
    /// # Arguments
    ///
    /// * `grants` - The grants in force
    /// * `admins` - The principals that may change the cluster, which hold every table
    /// * `tables` - The schema's tables, in the order their positions index
    #[must_use]
    pub fn compile(grants: &Grants, admins: &[String], tables: &[&str]) -> Self {
        // each granted principal's privileges on each table, the name's grant and `*`'s together
        let mut principals = HashMap::with_capacity(grants.principals.len() + admins.len());
        for (principal, held) in &grants.principals {
            let access: Rc<[Privileges]> =
                tables.iter().map(|table| held.on_table(table)).collect();
            principals.insert(principal.clone(), Access::Tables(access));
        }
        // an admin can grant itself anything, so it holds everything whatever it was granted
        for admin in admins {
            principals.insert(admin.clone(), Access::Everything);
        }
        Authorizer {
            principals,
            nothing: Access::Tables(vec![Privileges::NONE; tables.len()].into()),
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
        let authorizer = Authorizer::compile(&grants, &["admin".to_string()], &tables);
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
}
