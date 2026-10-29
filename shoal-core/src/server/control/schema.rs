//! The schema a cluster serves, as its control group commits it
//!
//! A cluster's schema was its initializer's, recorded once at `Initialize` as the tables and
//! buckets it served, and compared exactly at every door after. Since
//! [F93](../../../../docs/src/features/operations-and-the-real-cluster.md) it is a record of its
//! own: the schema's id, its lineage and each table's own fingerprint. It is committed at
//! `Initialize` and moved by `ExtendSchema` alone, which a cluster applies only once every member's
//! build holds the schema it moves to, so a schema that appends tables or buckets is rolled onto a
//! running cluster and nothing else is.
//!
//! # Invariants
//!
//! **A committed schema only ever extends.** Its lineage is a prefix of every lineage it moves
//! to, its tables are a prefix of the tables it moves to, and no member's build that does not
//! hold it is served: such a build is refused at the hello and stops itself at start.
//!
//! **A table is judged by its own fingerprint**, never by the schema around it, so a restore and
//! a snapshot's install of one table are admitted between builds whose schemas share it.

use serde::{Deserialize, Serialize};

use crate::shared::identity::TableId;
use crate::shared::lineage;
use crate::shared::traits::QuerySupport;

use super::types::BucketRecord;

/// A schema, as the control group commits it and as a build serves it
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SchemaRecord {
    /// The schema's structural id, the last element of its lineage
    pub id: u64,
    /// The schema's id after each of its fields, in declaration order
    pub lineage: Vec<u64>,
    /// Each table's own fingerprint, by identity, in the order the schema serves them
    pub fingerprints: Vec<(TableId, u64)>,
}

impl SchemaRecord {
    /// The record of the schema a client type describes
    #[must_use]
    pub fn of<C: QuerySupport>() -> Self {
        SchemaRecord {
            id: C::SCHEMA_ID,
            lineage: C::SCHEMA_LINEAGE.to_vec(),
            // every table, generated ones included, by the identity its name hashes to
            fingerprints: C::table_fingerprints()
                .iter()
                .map(|(name, fingerprint)| (TableId::of(name), *fingerprint))
                .collect(),
        }
    }

    /// The fingerprint this schema holds one table at, if it has the table
    ///
    /// # Arguments
    ///
    /// * `table` - The table's identity
    #[must_use]
    pub fn fingerprint(&self, table: TableId) -> Option<u64> {
        self.fingerprints
            .iter()
            .find(|(id, _)| *id == table)
            .map(|(_, fingerprint)| *fingerprint)
    }

    /// Whether this schema is the other one with fields appended, or the other one itself
    ///
    /// # Arguments
    ///
    /// * `base` - The schema this one may extend
    #[must_use]
    pub fn extends(&self, base: &SchemaRecord) -> bool {
        // the base's lineage is where this one's began, and its tables are this one's first
        self.lineage.len() >= base.lineage.len()
            && lineage::compatible(&self.lineage, &base.lineage)
            && self.fingerprints.len() >= base.fingerprints.len()
            && self.fingerprints[..base.fingerprints.len()] == base.fingerprints[..]
    }
}

/// The schema a build serves, as its control plane is handed it at start
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchemaBuild {
    /// The schema's record
    pub record: SchemaRecord,
    /// The tables it serves, with their stable identities, in the order it serves them
    pub tables: Vec<(String, TableId)>,
    /// The buckets it declares, and the tables each generated
    pub buckets: Vec<BucketRecord>,
}

impl SchemaBuild {
    /// What the build of a client type serves
    #[must_use]
    pub fn of<C: QuerySupport>() -> Self {
        SchemaBuild {
            record: SchemaRecord::of::<C>(),
            tables: C::table_ids()
                .into_iter()
                .map(|(name, id)| (name.to_string(), id))
                .collect(),
            buckets: BucketRecord::of_schema(&C::bucket_ids(), C::generated_tables()),
        }
    }

    /// Whether this build serves every table of a committed schema, as it does when its own
    /// schema is that one or extends it
    ///
    /// # Arguments
    ///
    /// * `ancestor` - The committed schema this build may extend
    #[must_use]
    pub fn holds(&self, ancestor: &SchemaRecord) -> bool {
        self.record.extends(ancestor)
    }
}

/// Judge one backup file's table against the schema a restore puts it into
///
/// A table the destination serves at the fingerprint the file was cut at is restored; one it
/// serves at another fingerprint is a table whose row changed, which is not an addition; one
/// a bucket generated is a bucket's rows, which never travel as table files; and one it does
/// not serve at all was removed or renamed. Each is named, so an operator knows which.
///
/// # Arguments
///
/// * `schema` - The schema the destination committed
/// * `generated` - The tables the destination's buckets generated, by name
/// * `table_name` - The file's table, as the schema that cut it named it
/// * `table` - Its identity
/// * `fingerprint` - The fingerprint the file was cut at
///
/// # Errors
///
/// Why the file cannot be restored into this schema.
pub fn judge_table(
    schema: &SchemaRecord,
    generated: &[&str],
    table_name: &str,
    table: TableId,
    fingerprint: u64,
) -> Result<(), String> {
    // a bucket's rows are never restored as table files, whichever schema cut them
    if generated.contains(&table_name) {
        return Err(format!(
            "the backup holds table {table_name}, a bucket's rows: a bucket is backed up as its \
             objects, and no restore carries its rows as table files"
        ));
    }
    match schema.fingerprint(table) {
        Some(served) if served == fingerprint => Ok(()),
        Some(served) => Err(format!(
            "the backup holds table {table_name} shaped {fingerprint:#018x} and this cluster \
             serves it shaped {served:#018x}: its row changed, which is a new cluster and an \
             import, not a restore"
        )),
        None => Err(format!(
            "the backup holds table {table_name}, which this cluster's schema does not serve"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record of the given lineage and tables
    ///
    /// # Arguments
    ///
    /// * `lineage` - The schema's lineage
    /// * `tables` - Each table's name and fingerprint
    fn record(lineage: &[u64], tables: &[(&str, u64)]) -> SchemaRecord {
        SchemaRecord {
            id: *lineage.last().unwrap(),
            lineage: lineage.to_vec(),
            fingerprints: tables
                .iter()
                .map(|(name, fingerprint)| (TableId::of(name), *fingerprint))
                .collect(),
        }
    }

    /// An append extends its base and nothing else does
    #[test]
    fn an_append_extends_its_base() {
        let base = record(&[1, 2], &[("A", 10), ("B", 20)]);
        let appended = record(&[1, 2, 3], &[("A", 10), ("B", 20), ("C", 30)]);
        let changed = record(&[1, 4, 5], &[("A", 10), ("B", 21), ("C", 30)]);
        assert!(appended.extends(&base));
        assert!(base.extends(&base));
        assert!(!base.extends(&appended));
        assert!(!changed.extends(&base));
    }

    /// A restore's file is judged by its own table, and each refusal names why
    #[test]
    fn a_file_is_judged_by_its_own_table() {
        let schema = record(&[1, 2], &[("A", 10), ("PostersObjectMeta", 11)]);
        let generated = ["PostersObjectMeta"];
        assert!(judge_table(&schema, &generated, "A", TableId::of("A"), 10).is_ok());
        let changed = judge_table(&schema, &generated, "A", TableId::of("A"), 99).unwrap_err();
        assert!(changed.contains("its row changed"), "{changed}");
        let rows = judge_table(
            &schema,
            &generated,
            "PostersObjectMeta",
            TableId::of("PostersObjectMeta"),
            11,
        )
        .unwrap_err();
        assert!(rows.contains("a bucket's rows"), "{rows}");
        let gone = judge_table(&schema, &generated, "Z", TableId::of("Z"), 1).unwrap_err();
        assert!(gone.contains("does not serve"), "{gone}");
    }
}
