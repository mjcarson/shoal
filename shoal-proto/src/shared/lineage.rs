//! A schema's lineage, and the one rule two lineages are judged by
//!
//! A schema's lineage is its id after each of its fields, in declaration order
//! ([`QuerySupport::SCHEMA_LINEAGE`](crate::shared::traits::QuerySupport::SCHEMA_LINEAGE)). A
//! schema that only appends tables or buckets to another has the other's lineage as a prefix of
//! its own, and nothing else does short of a collision of the fingerprint's hash. So an append
//! is told from every other change by comparing lineages, which is how a rolling addition is
//! admitted at every door a schema id was compared at exactly before it
//! ([F93](../../../docs/src/features/operations-and-the-real-cluster.md)).
//!
//! # Invariants
//!
//! **A lineage's last element is its schema's id**, so a lineage of one element and a bare id
//! are the same thing: a peer that sent only its id is judged as a lineage of that id alone.
//!
//! **The judge is symmetric.** Either side may be the newer build; only a lineage that is not a
//! prefix of the other's is another schema.
//!
//! **Two lineages are judged where they overlap, never from their first ids.** A hello carries a
//! lineage cut to its last [`MAX_LINEAGE`](crate::shared::protocol::peer::hello::MAX_LINEAGE)
//! ids, and a bare id is a lineage of one: either is a window that ends where its schema does, so
//! the judge finds one side's last id in the other and compares the ids that end there
//! ([Resolved #271](../../../docs/src/appendix/resolved/lineage-judged-where-it-overlaps.md)).

/// Get whether one lineage is a prefix of the other, either way round
///
/// Two schemas whose fields one extends the other's: an append, in whichever direction. Either
/// lineage may be a window of its schema's, its last ids alone, so one side's last id is found in
/// the other and the ids that end there compared, as far back as both hold. An empty lineage is
/// no schema and is compatible with nothing.
///
/// # Arguments
///
/// * `ours` - One side's lineage
/// * `theirs` - The other side's lineage
pub fn compatible(ours: &[u64], theirs: &[u64]) -> bool {
    // an empty lineage names no schema at all
    if ours.is_empty() || theirs.is_empty() {
        return false;
    }
    // one side ends where the other passes through, with the same ids before it
    ends_within(ours, theirs) || ends_within(theirs, ours)
}

/// Get whether a lineage ends at an id another holds, the two the same as far back as both reach
///
/// # Arguments
///
/// * `shorter` - The lineage whose end is looked for
/// * `longer` - The lineage it is looked for in
fn ends_within(shorter: &[u64], longer: &[u64]) -> bool {
    // where the shorter one's schema sits in the longer
    let Some(last) = shorter.last() else {
        return false;
    };
    let Some(at) = longer.iter().position(|id| id == last) else {
        return false;
    };
    // the ids both hold, back from there
    let overlap = (at + 1).min(shorter.len());
    longer[at + 1 - overlap..=at] == shorter[shorter.len() - overlap..]
}

/// Get whether a lineage holds a schema's id, so a build of it serves that schema's tables
///
/// # Arguments
///
/// * `lineage` - The lineage of a build
/// * `id` - The id of the schema asked about
pub fn holds(lineage: &[u64], id: u64) -> bool {
    lineage.contains(&id)
}

/// Get the lineage a schema's id stands for once it is known to be an ancestor of a lineage
///
/// The prefix of `lineage` that ends at `id`, or `None` when `lineage` does not hold it.
///
/// # Arguments
///
/// * `lineage` - The lineage of a build
/// * `id` - The id of a schema the build may extend
pub fn ancestor<'a>(lineage: &'a [u64], id: u64) -> Option<&'a [u64]> {
    // find where the id sits and keep everything up to it
    lineage
        .iter()
        .position(|element| *element == id)
        .map(|at| &lineage[..=at])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An append is compatible either way round, and a change is not
    #[test]
    fn a_prefix_is_compatible_and_a_change_is_not() {
        // a schema, an append to it, and a change to its second field
        let base = [1, 2];
        let appended = [1, 2, 3];
        let changed = [1, 4, 5];
        assert!(compatible(&base, &appended));
        assert!(compatible(&appended, &base));
        assert!(compatible(&base, &base));
        assert!(!compatible(&base, &changed));
        assert!(!compatible(&appended, &changed));
        // nothing is compatible with no schema at all
        assert!(!compatible(&[], &base));
    }

    /// A bare id is the lineage of that id alone, and a lineage cut to its last ids is judged
    /// where it overlaps the other, an append or a change either way round (item 271)
    #[test]
    fn a_bare_id_and_a_cut_lineage_are_judged_where_they_overlap() {
        // a bare id against the lineage it ends
        assert!(compatible(&[1, 2, 9], &[9]));
        assert!(compatible(&[9], &[1, 2, 9]));
        assert!(!compatible(&[1, 2, 9], &[8]));
        // a lineage of three hundred fields, and its last 255 as a hello carries them
        let full: Vec<u64> = (0..300).collect();
        let cut = &full[45..];
        assert!(compatible(&full, cut));
        assert!(compatible(cut, &full));
        // an append of eleven fields, cut, against the build before it, whole or cut
        let appended: Vec<u64> = (0..311).collect();
        let appended_cut = &appended[56..];
        assert!(compatible(&full, appended_cut));
        assert!(compatible(cut, appended_cut));
        // a change to field ten moves every id after it, cut or not
        let changed: Vec<u64> = (0..10).chain(1_000..1_290).collect();
        assert!(!compatible(&full, &changed[35..]));
        assert!(!compatible(cut, &changed[35..]));
    }

    /// A lineage holds every id along it and an ancestor ends at that id
    #[test]
    fn a_lineage_holds_its_ancestors() {
        let lineage = [7, 8, 9];
        assert!(holds(&lineage, 8));
        assert!(!holds(&lineage, 6));
        assert_eq!(ancestor(&lineage, 8), Some(&lineage[..2]));
        assert_eq!(ancestor(&lineage, 6), None);
    }
}
