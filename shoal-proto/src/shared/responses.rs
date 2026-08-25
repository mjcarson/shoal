//! A response from a set of queries
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    client::{Errors, QuerySuceededOpts},
    shared::protocol::error::ErrorCode,
    shared::traits::ShoalSortedTable,
};

/// The different response kind types
///
/// `Error` is appended rather than inserted, and every variant after it must be too. These are
/// derived by rkyv, so the order of this enum is its wire representation.
#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum ResponseActionNames {
    /// A response to an insert query
    Insert,
    /// A response to a get query
    Get,
    /// A response to a delete query
    Delete,
    /// A response to an update query
    Update,
    /// A response to an exists query
    Exists,
    /// A query that failed rather than answering
    Error,
}

/// A failure the server is answering a query with
///
/// The code is held as the raw number it is written as rather than as an [`ErrorCode`], because
/// this type is archived and `ErrorCode` lives in a module that has no rkyv dependency and must
/// not gain one — the protocol module exists to frame the serialization format, so it cannot
/// depend on it. Read the class of failure with [`ResponseError::code`] rather than by comparing
/// the field, which also gets a code a newer server knows about and this build does not read back
/// as [`ErrorCode::Unknown`] instead of as a number nothing matches.
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct ResponseError {
    /// What class of failure this is, as the number it is written as
    pub code: u16,
    /// What the server said about it, in one line, for a person
    pub msg: String,
}

impl ResponseError {
    /// Create a new failure to answer a query with
    ///
    /// # Arguments
    ///
    /// * `code` - What class of failure this is
    /// * `msg` - What to say about it
    pub fn new<M: Into<String>>(code: ErrorCode, msg: M) -> Self {
        ResponseError {
            code: code.as_u16(),
            msg: msg.into(),
        }
    }

    /// Get what class of failure this is
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.code)
    }
}

impl ArchivedResponseError {
    /// Get what class of failure this is
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.code.to_native())
    }

    /// Get what the server said about this failure
    pub fn msg(&self) -> &str {
        &self.msg
    }
}

/// Which partition a run of a get's rows came from, and how many rows it gave
///
/// Both fields are eight bytes, so this struct has no padding and rkyv can copy a run of them
/// in one go. Widening `len` down to a `u32` would save nothing and cost that.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub struct RowGroup {
    /// The partition these rows came from
    pub partition: u64,
    /// How many of [`GetRows::rows`] this group covers
    pub len: u64,
}

/// The rows a get found, and which partition each run of them came from
///
/// **`rows` is laid out in `groups` order**: group *i* covers the rows starting at the sum of
/// the lengths of the groups before it. Every operation here preserves that, and
/// [`GetRows::is_consistent`] is what says so out loud.
///
/// The index exists because the shard collecting the shares of a split get has to put the rows
/// back into the order the query named their partitions in, and a bare `Vec<T>` gave it no way
/// to ask a row where it came from except to hash the row's partition key again — once per row,
/// for information every shard already had and threw away
/// ([O18](../../../docs/src/appendix/optimizations.md)). With the index that reorder is a sort of
/// the groups, which there are as many of as the query named partitions rather than as many of as
/// it found rows.
///
/// **The rows stay flat rather than nested inside their groups.** A `Vec<(u64, Vec<T>)>` would
/// carry the same information and would make this type simpler, at the cost of every caller: the
/// payload a client walks is an `ArchivedVec<Archived<T>>` today, and keeping it one is what lets
/// `FromShoal::retrieve` and every `access::<T>()` in the tree go on meaning what they meant.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct GetRows<T> {
    /// Every row this get found, in the order the query named their partitions
    pub rows: Vec<T>,
    /// The partitions those rows came from, in the same order, one entry per partition
    pub groups: Vec<RowGroup>,
}

impl<T> GetRows<T> {
    /// Answer with the rows of a single partition
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition these rows came from
    /// * `rows` - The rows it gave
    #[must_use]
    pub fn single(partition: u64, rows: Vec<T>) -> Self {
        // one partition is one group covering every row of it
        let groups = vec![RowGroup {
            partition,
            len: rows.len() as u64,
        }];
        GetRows { rows, groups }
    }

    /// Answer with the rows of several partitions, in the order they were named
    ///
    /// A partition that gave nothing contributes no group, so an empty run never appears in the
    /// index and a reader never has to decide what a zero length group means.
    ///
    /// **The first run is taken rather than copied**, which is
    /// [O36](../../../docs/src/appendix/optimizations.md): a get that read one partition — much
    /// the commonest kind — used to have every row it found moved into a second `Vec` for no
    /// reason beyond the shape of the code that built it. It now hands over the `Vec` the scan
    /// already filled, and only a get that really did read several partitions concatenates
    /// anything.
    ///
    /// # Arguments
    ///
    /// * `slots` - The rows each named partition gave, paired with its key, in named order
    #[must_use]
    pub fn from_slots<I: IntoIterator<Item = (u64, Vec<T>)>>(slots: I) -> Self {
        // collect the runs, dropping the partitions that had nothing to give
        let mut rows = Vec::new();
        let mut groups = Vec::new();
        for (partition, mut found) in slots {
            // a partition that gave no rows is not a group, it is an absence
            if found.is_empty() {
                continue;
            }
            groups.push(RowGroup {
                partition,
                len: found.len() as u64,
            });
            // the first run this get found becomes the answer rather than being copied into it
            if rows.is_empty() {
                rows = found;
            } else {
                rows.append(&mut found);
            }
        }
        GetRows { rows, groups }
    }

    /// How many rows this answer holds
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether this answer holds no rows at all
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Whether the index still covers every row exactly once
    ///
    /// The client cannot check this for itself — it reads the rows through the index — so it is
    /// checked here, by the tests, on every operation that touches either half.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        // the groups have to account for every row and no more
        let counted: u64 = self.groups.iter().map(|group| group.len).sum();
        counted == self.rows.len() as u64
    }

    /// Take another shard's share of this answer into ours
    ///
    /// Appending keeps `rows` in `groups` order, because both halves of the share are appended
    /// together and each was already ordered within itself. The result is in arrival order, which
    /// [`Self::order_by`] is what fixes.
    ///
    /// # Arguments
    ///
    /// * `other` - The share to take in
    pub fn absorb(&mut self, mut other: Self) {
        // both halves move together, so the index goes on describing the rows
        self.rows.append(&mut other.rows);
        self.groups.append(&mut other.groups);
    }

    /// Drop any rows past a limit, and the part of the index that described them
    ///
    /// # Arguments
    ///
    /// * `limit` - The most rows to keep
    pub fn truncate(&mut self, limit: usize) {
        // an answer already inside the limit keeps every row and every group
        if self.rows.len() <= limit {
            return;
        }
        self.rows.truncate(limit);
        // walk the index alongside the rows that survived, clipping the group the limit lands in
        let mut kept = 0usize;
        let mut groups = 0usize;
        for group in &mut self.groups {
            // every group past the limit goes, along with the rows it described
            if kept >= limit {
                break;
            }
            // the group the limit falls inside keeps only the rows in front of it
            let room = limit - kept;
            if group.len as usize > room {
                group.len = room as u64;
            }
            kept += group.len as usize;
            groups += 1;
        }
        self.groups.truncate(groups);
    }

    /// Put the rows back into the order the query named their partitions in
    ///
    /// One lookup per **partition** and one move per row, where a bare `Vec<T>` needed one hash
    /// of a row's partition key per **row**. Nothing is hashed here at all.
    ///
    /// The sort is stable, so rows within one partition keep the order their shard gave them,
    /// which for a sorted table is their sort key order.
    ///
    /// # Arguments
    ///
    /// * `rank` - What place a partition was named in, or nothing if the query never named it
    pub fn order_by<F: Fn(u64) -> Option<usize>>(&mut self, rank: F) {
        // a single run is already in whatever order it is going to be in
        if self.groups.len() < 2 {
            return;
        }
        // rank each group once
        //
        // a group from a partition this query did not name cannot happen, but placing it last
        // keeps this total rather than panicking on a key we cannot rank
        let mut order: Vec<(usize, usize)> = self
            .groups
            .iter()
            .enumerate()
            .map(|(at, group)| (rank(group.partition).unwrap_or(usize::MAX), at))
            .collect();
        // sorting by rank alone would be enough, but pairing it with the position keeps the
        // ordering total when two groups somehow rank the same
        order.sort_unstable();
        // nothing to move if the groups already sit in the order they were named
        if order.iter().enumerate().all(|(at, (_, from))| at == *from) {
            return;
        }
        // cut the rows into their runs, from the back so each cut is the tail of what is left
        let mut rows = std::mem::take(&mut self.rows);
        let mut runs: Vec<Vec<T>> = Vec::with_capacity(self.groups.len());
        for group in self.groups.iter().rev() {
            let at = rows.len() - group.len as usize;
            runs.push(rows.split_off(at));
        }
        runs.reverse();
        // and lay them back down in the order the query named them
        let mut ordered_rows = Vec::with_capacity(runs.iter().map(Vec::len).sum());
        let mut ordered_groups = Vec::with_capacity(self.groups.len());
        for (_, from) in order {
            ordered_rows.append(&mut runs[from]);
            ordered_groups.push(self.groups[from]);
        }
        self.rows = ordered_rows;
        self.groups = ordered_groups;
    }
}

/// The different response kinds from a query
///
/// `Error` is appended rather than inserted, for the same reason it is in [`ResponseActionNames`]:
/// rkyv derives this enum's wire representation from its order.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum ResponseAction<T> {
    /// Whether an insert was successful or not
    Insert(bool),
    /// The response from a get query
    ///
    /// The rows carry the partitions they came from beside them, which is what lets a split get
    /// be put back in order without asking every row where it belongs
    /// ([O18](../../../docs/src/appendix/optimizations.md)).
    Get(Option<GetRows<T>>),
    /// The response from a delete
    Delete(bool),
    /// The response from an update
    Update(bool),
    /// The response from an exists query - true if data exists
    Exists(bool),
    /// This query failed rather than answering
    ///
    /// This is what separates "the row is not there" from "the copy of it could not be read".
    /// Before it existed both were `Get(None)` and the client had no way to tell them apart.
    Error(ResponseError),
}

impl<T> ResponseAction<T> {
    /// Merge another shards share of this queries answer into ours
    ///
    /// Only a get and an exists can be split across shards, since every other query
    /// names a single partition and so has a single owner. A write that somehow got
    /// here keeps the answer it already had rather than inventing one.
    ///
    /// **A failed share makes the whole answer a failure.** This is the one place where three
    /// shards worth of rows can hide a fourth shard's failure, and presenting a partial answer as
    /// a complete one is precisely the defect the error channel exists to remove — the client
    /// cannot tell "these are all the rows" from "these are the rows we could read" unless the
    /// merge refuses to make that choice for it.
    ///
    /// # Arguments
    ///
    /// * `other` - The other shards share of this queries answer
    pub fn merge(&mut self, other: Self) {
        // combine our two shares by the kind of query they answer
        match (self, other) {
            // a share that failed takes over the answer, whatever we had found so far
            (ours, ResponseAction::Error(theirs)) => *ours = ResponseAction::Error(theirs),
            // and an answer that has already failed stays failed, whatever anyone else found
            (ResponseAction::Error(_), _) => (),
            // a get is the union of the rows each shard found
            (ResponseAction::Get(ours), ResponseAction::Get(theirs)) => {
                // pull out the rows they found, if they found any
                let Some(theirs) = theirs else {
                    return;
                };
                // add their rows to ours, or take theirs if we found none
                //
                // their index comes with their rows, so the answer goes on knowing which
                // partition every row in it came from however many shares are folded in
                match ours {
                    Some(ours) => ours.absorb(theirs),
                    None => *ours = Some(theirs),
                }
            }
            // a row exists if any shard we asked found it
            (ResponseAction::Exists(ours), ResponseAction::Exists(theirs)) => *ours |= theirs,
            // every other query names a single partition, so it is never split
            _ => (),
        }
    }

    /// Drop any rows past this queries limit
    ///
    /// A limit is applied on each shard as it scans, so every share that arrives here
    /// is already no longer than the limit. Their union can still be longer, which is
    /// what this trims.
    ///
    /// A failure is left alone: a limit bounds rows, and a failure has none. It cannot reach here
    /// as anything but the whole answer anyway, since a failed share wins the merge.
    ///
    /// # Arguments
    ///
    /// * `limit` - The most rows this query asked for
    pub fn truncate(&mut self, limit: usize) {
        // only a get returns rows that could be over a limit
        if let ResponseAction::Get(Some(rows)) = self {
            // drop everything past our limit, and the part of the index that described it
            rows.truncate(limit);
            // an emptied get answers None, the same as a get that found nothing
            if rows.is_empty() {
                *self = ResponseAction::Get(None);
            }
        }
    }
}

impl<T> ResponseAction<T> {
    /// Put our rows back into the order the query named their partitions in
    ///
    /// Each shard answers with the rows of its own partitions, in the order the query named
    /// them, but the shares are merged in whatever order they arrive. Sorting by where each
    /// rows partition was named undoes that, and leaves a get whose answer depends only on
    /// the query and not on which shard happened to reply first.
    ///
    /// The sort is stable, so rows within one partition keep the order their shard gave them,
    /// which for a sorted table is their sort key order.
    ///
    /// A failure is left alone, for the same reason a [`ResponseAction::truncate`] leaves one
    /// alone: there are no rows to put in an order.
    ///
    /// **This used to hash every row.** A share carried rows and nothing else, so the only way
    /// to ask a row which partition it came from was to hash its partition key again — once per
    /// row, for something every shard already knew and discarded. The rows now arrive with the
    /// index of the partitions they came from, so this ranks the groups instead: as many lookups
    /// as the query named partitions, and no hashing at all
    /// ([O18](../../../docs/src/appendix/optimizations.md)). It is also why this impl block no
    /// longer requires `T: PartitionKeySupport`, and therefore why a projection is free to leave
    /// its table's partition key out ([F2](../../../docs/src/features/projections.md)).
    ///
    /// # Arguments
    ///
    /// * `order` - The partitions this query named, in the order it named them
    pub fn order_by_partitions(&mut self, order: &[u64]) {
        // only a get comes back as rows there is an order to
        let ResponseAction::Get(Some(rows)) = self else {
            return;
        };
        // a query naming one partition has nothing to interleave
        if order.len() < 2 {
            return;
        }
        // build the rank of each partition this query named
        let ranks: HashMap<u64, usize> = order
            .iter()
            .enumerate()
            .map(|(rank, key)| (*key, rank))
            .collect();
        // and put each run of rows where the query asked for it
        rows.order_by(|partition| ranks.get(&partition).copied());
    }
}

/// A response from a query
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct Response<T> {
    /// The id for the query we are responding too
    pub id: Uuid,
    /// This response index in the queries vec
    pub index: usize,
    /// The response data
    pub data: ResponseAction<T>,
    /// Whether this is the last response for a query or not
    pub end: bool,
}

impl<T> Response<T> {
    /// Merge another shards share of this queries answer into ours
    ///
    /// # Arguments
    ///
    /// * `other` - The other shards share of this queries answer
    pub fn merge(&mut self, other: Self) {
        self.data.merge(other.data);
    }

    /// Drop any rows past this queries limit
    ///
    /// # Arguments
    ///
    /// * `limit` - The most rows this query asked for
    pub fn truncate(&mut self, limit: usize) {
        self.data.truncate(limit);
    }
}

impl<T> Response<T> {
    /// Put our rows back into the order the query named their partitions in
    ///
    /// # Arguments
    ///
    /// * `order` - The partitions this query named, in the order it named them
    pub fn order_by_partitions(&mut self, order: &[u64]) {
        self.data.order_by_partitions(order);
    }
}

/// Check if a response succeeded or not
///
/// # Arguments
///
/// * `should_check` - Whether this response should be checked
/// * `value` - The value to check
/// * `kind` - The kind of query that is being checked
macro_rules! check_response {
    ($should_check:expr, $value:expr, $kind:expr) => {
        // only check queries that we are requried too
        if $should_check {
            // check if this query failed or not
            if $should_check == $value {
                return Ok(());
            }
            // this query failed so return its kind name
            $kind
        } else {
            return Ok(());
        }
    };
}

impl<T: Archive> ArchivedResponse<T> {
    /// Check if this query succeeded according to our criteria
    ///
    /// A query that failed is never a success, whatever the options say. The options decide which
    /// *outcomes* count as success — whether an insert that inserted nothing is a problem, whether
    /// a get that matched nothing is — and a failure is not an outcome. A caller that passed
    /// `get: false` is saying it can live with an empty answer, not that it wants an unreadable
    /// partition reported as one.
    ///
    /// # Arguments
    ///
    /// * `opts` - Which kinds of query have to have done something to count as succeeding
    pub fn succeeded(&self, opts: QuerySuceededOpts) -> Result<(), Errors> {
        // check if this query failed or not
        let kind = match &self.data {
            // a failure short circuits every option, and answers with what the server said
            ArchivedResponseAction::Error(error) => {
                return Err(Errors::Server {
                    query_id: Some(self.id),
                    index: Some(self.index.to_native() as usize),
                    code: error.code(),
                    msg: error.msg().to_owned(),
                })
            }
            ArchivedResponseAction::Insert(inserted) => {
                check_response!(opts.insert, *inserted, ResponseActionNames::Insert)
            }
            ArchivedResponseAction::Get(got) => {
                check_response!(opts.get, got.is_some(), ResponseActionNames::Get)
            }
            ArchivedResponseAction::Update(updated) => {
                check_response!(opts.update, *updated, ResponseActionNames::Update)
            }
            ArchivedResponseAction::Delete(deleted) => {
                check_response!(opts.delete, *deleted, ResponseActionNames::Delete)
            }
            ArchivedResponseAction::Exists(exists) => {
                check_response!(opts.exists, *exists, ResponseActionNames::Exists)
            }
        };
        // this query failed and it was required to succeed
        // build a nice descriptive error for it
        let error = Errors::QueryDidNotSucceed {
            id: self.id,
            index: self.index.to_native() as usize,
            kind,
            end: self.end,
        };
        Err(error)
    }

    /// Get the kind of query this is a response to
    pub fn kind(&self) -> ResponseActionNames {
        match &self.data {
            ArchivedResponseAction::Insert(_) => ResponseActionNames::Insert,
            ArchivedResponseAction::Get(_) => ResponseActionNames::Get,
            ArchivedResponseAction::Update(_) => ResponseActionNames::Update,
            ArchivedResponseAction::Delete(_) => ResponseActionNames::Delete,
            ArchivedResponseAction::Exists(_) => ResponseActionNames::Exists,
            ArchivedResponseAction::Error(_) => ResponseActionNames::Error,
        }
    }

    /// Get the failure this query answered with, if it failed
    ///
    /// Returns `Some` only for a query that failed, so a caller can tell a failure from a result
    /// without going through [`ArchivedResponse::succeeded`] and its options.
    pub fn error(&self) -> Option<&ArchivedResponseError> {
        match &self.data {
            ArchivedResponseAction::Error(error) => Some(error),
            _ => None,
        }
    }

    /// Mark this response as the last one
    pub fn end(&mut self) {
        self.end = true;
    }

    /// Get the exists result if this is an Exists response
    ///
    /// Returns `Some(bool)` if this is an Exists response, `None` otherwise
    pub fn get_exists(&self) -> Option<bool> {
        match &self.data {
            ArchivedResponseAction::Exists(exists) => Some(*exists),
            _ => None,
        }
    }
}

/// The responses from a set of queries
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct Responses<D: ShoalSortedTable> {
    /// The responses for our queries
    pub responses: Vec<Response<D>>,
}

impl<D: ShoalSortedTable> Responses<D> {
    /// Create an empty responses object of the correct size
    ///
    /// # Arguments
    ///
    /// * `len` - The number of queries we are responding too
    pub fn with_capacity(len: usize) -> Self {
        // build our responses bundle
        Responses {
            responses: Vec::with_capacity(len),
        }
    }

    /// A new response to this response bundle
    ///
    /// # Arguments
    ///
    /// * `response` - The response to add
    pub fn add(&mut self, response: Response<D>) {
        // add this response
        self.responses.push(response);
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{
        ArchivedResponse, ErrorCode, GetRows, QuerySuceededOpts, Response, ResponseAction,
        ResponseError, RowGroup,
    };
    use crate::client::Errors;
    use std::collections::HashMap;

    /// Archive a response so the checks that only exist on the archived side can be run against it
    ///
    /// # Arguments
    ///
    /// * `data` - The answer to wrap in a response and archive
    fn archived(data: ResponseAction<u64>) -> rkyv::util::AlignedVec {
        // build a response around this answer, at an index that is not zero so a lost index shows
        let response = Response {
            id: Uuid::from_u128(0x1234),
            index: 3,
            data,
            end: false,
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&response).unwrap()
    }

    /// A share that failed takes over the whole answer
    ///
    /// This is the one place rows from three shards can hide a fourth shard's failure. A merge
    /// that let the rows win would hand the client a partial answer with nothing to say it was
    /// partial, which is the defect the error channel exists to remove.
    #[test]
    fn an_error_share_wins_a_merge() {
        // a share that found rows, merged with a share that failed
        let mut ours = ResponseAction::Get(Some(GetRows::single(7, vec![1u64, 2, 3])));
        ours.merge(ResponseAction::Error(ResponseError::new(
            ErrorCode::StorageRead,
            "could not read partition 7",
        )));
        assert!(matches!(ours, ResponseAction::Error(_)));
        // and the other way round, so the answer stays failed however the shares arrive
        let mut ours = ResponseAction::Error(ResponseError::new(
            ErrorCode::StorageRead,
            "could not read partition 7",
        ));
        ours.merge(ResponseAction::Get(Some(GetRows::single(7, vec![1u64, 2, 3]))));
        assert!(matches!(ours, ResponseAction::Error(_)));
        // an exists is merged by the same rule, since it is the other query that can be split
        let mut ours = ResponseAction::<u64>::Exists(true);
        ours.merge(ResponseAction::Error(ResponseError::new(
            ErrorCode::ArchiveMissing,
            "archive is gone",
        )));
        assert!(matches!(ours, ResponseAction::Error(_)));
    }

    /// A query that failed is never a success, whatever the options say
    ///
    /// The options say which outcomes count as success. A failure is not an outcome, so a caller
    /// that turned every check off must still be told that its query did not work.
    #[test]
    fn an_error_response_never_succeeds_whatever_the_opts_say() {
        // archive a response that failed
        let buff = archived(ResponseAction::Error(ResponseError::new(
            ErrorCode::CorruptArchive,
            "partition 7 of TestRecord could not be read",
        )));
        let response = rkyv::access::<ArchivedResponse<u64>, rkyv::rancor::Error>(&buff).unwrap();
        // turn every check off, which is the most permissive thing a caller can ask for
        let permissive = QuerySuceededOpts {
            insert: false,
            update: false,
            get: false,
            delete: false,
            exists: false,
        };
        // it still fails, and it fails with what the server said rather than with a kind name
        match response.succeeded(permissive) {
            Err(Errors::Server { code, msg, index, .. }) => {
                assert_eq!(code, ErrorCode::CorruptArchive);
                assert!(msg.contains("partition 7"));
                assert_eq!(index, Some(3));
            }
            other => panic!("a failed query was not reported as a failure: {other:?}"),
        }
        // and the same response says so through the accessor a caller can ask directly
        assert_eq!(
            response.error().map(super::ArchivedResponseError::code),
            Some(ErrorCode::CorruptArchive)
        );
        // while a get that merely found nothing is still the other kind of answer
        let buff = archived(ResponseAction::Get(None));
        let response = rkyv::access::<ArchivedResponse<u64>, rkyv::rancor::Error>(&buff).unwrap();
        assert!(response.error().is_none());
        assert!(response.succeeded(permissive).is_ok());
    }

    /// A row that knows which partition it came from, for checking the index against
    ///
    /// The whole point of the index is that a row no longer has to be asked this. The test row
    /// answers anyway, so the new reorder can be checked against the hashing one it replaced.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Placed {
        /// The partition this row came from
        partition: u64,
        /// Which row of that partition this is, so a reorder that shuffles within a run shows
        offset: u64,
    }

    /// Build the rows of one partition
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition these rows came from
    /// * `count` - How many rows it gave
    fn run(partition: u64, count: u64) -> (u64, Vec<Placed>) {
        let rows = (0..count).map(|offset| Placed { partition, offset }).collect();
        (partition, rows)
    }

    /// Put rows back in partition order the way this used to, by asking every row where it came from
    ///
    /// This is the implementation [O18](../../../docs/src/appendix/optimizations.md) was filed
    /// against, kept as the only definition of correct the new one has. It is the same discipline
    /// [F26](../../../docs/src/features/archive-routed-requests.md) applied to `split_by_shard`:
    /// a replaced implementation is worth more as an oracle than as a deletion.
    ///
    /// # Arguments
    ///
    /// * `rows` - The rows to reorder, which are read but never grouped
    /// * `order` - The partitions the query named, in the order it named them
    fn order_by_hashing(rows: &mut [Placed], order: &[u64]) {
        let ranks: HashMap<u64, usize> = order
            .iter()
            .enumerate()
            .map(|(rank, key)| (*key, rank))
            .collect();
        // one lookup per row, which is the cost the index exists to remove
        rows.sort_by_key(|row| ranks.get(&row.partition).copied().unwrap_or(usize::MAX));
    }

    /// The index covers every row exactly once, however the answer was built
    ///
    /// A client reads the rows *through* this index, so it cannot check it for itself. A gap
    /// would hide rows, an overlap would repeat them, and either would do so silently.
    #[test]
    fn groups_cover_every_row_exactly_once() {
        // an answer built from several partitions, one of which found nothing
        let mut found = GetRows::from_slots(vec![run(11, 2), run(22, 0), run(33, 3)]);
        assert!(found.is_consistent());
        assert_eq!(found.len(), 5);
        // a partition that gave nothing is an absence rather than an empty group
        assert_eq!(found.groups.len(), 2);
        // absorbing another shard's share keeps both halves describing each other
        found.absorb(GetRows::from_slots(vec![run(44, 4)]));
        assert!(found.is_consistent());
        assert_eq!(found.len(), 9);
        // as does a limit, which has to trim the index alongside the rows
        found.truncate(3);
        assert!(found.is_consistent());
        assert_eq!(found.len(), 3);
        // and so does a reorder
        found.order_by(|partition| [33u64, 11].iter().position(|key| *key == partition));
        assert!(found.is_consistent());
        assert_eq!(found.len(), 3);
    }

    /// The rows come back in the order the query named their partitions, whatever order they arrived in
    ///
    /// Each shard answers with its own partitions and the shares are merged as they land, so
    /// without this a get's answer depends on which shard happened to reply first.
    #[test]
    fn the_groups_a_get_returns_name_its_partitions_in_the_order_it_asked_for() {
        // the query named three partitions, and the shares came back in none of that order
        let order = [33u64, 11, 22];
        let mut found = GetRows::from_slots(vec![run(22, 2)]);
        found.absorb(GetRows::from_slots(vec![run(33, 1)]));
        found.absorb(GetRows::from_slots(vec![run(11, 3)]));
        found.order_by(|partition| order.iter().position(|key| *key == partition));
        // the index is in named order
        let partitions: Vec<u64> = found.groups.iter().map(|group| group.partition).collect();
        assert_eq!(partitions, vec![33, 11, 22]);
        // and so are the rows it describes, with each run still in its own order
        let placed: Vec<(u64, u64)> = found
            .rows
            .iter()
            .map(|row| (row.partition, row.offset))
            .collect();
        assert_eq!(
            placed,
            vec![(33, 0), (11, 0), (11, 1), (11, 2), (22, 0), (22, 1)]
        );
    }

    /// The reorder agrees with the one that hashed every row, which is the only oracle it has
    ///
    /// Checked over every arrival order of the shares, because the defect this would catch is
    /// one that shows up for some interleavings and not others.
    #[test]
    fn a_gathered_get_orders_its_rows_without_hashing_any_of_them() {
        // the partitions the query named, in the order it named them
        let order = [70u64, 10, 40, 20];
        // every order the four shares could arrive in
        let arrivals = [
            [0usize, 1, 2, 3],
            [3, 2, 1, 0],
            [1, 3, 0, 2],
            [2, 0, 3, 1],
            [0, 2, 1, 3],
            [3, 0, 2, 1],
        ];
        let shares = [run(10, 3), run(20, 1), run(40, 2), run(70, 4)];
        for arrival in arrivals {
            // merge the shares in this arrival order, the way a gathering shard does
            let mut found: Option<GetRows<Placed>> = None;
            for at in arrival {
                let (partition, rows) = shares[at].clone();
                let share = GetRows::from_slots(vec![(partition, rows)]);
                match &mut found {
                    Some(found) => found.absorb(share),
                    None => found = Some(share),
                }
            }
            let mut found = found.expect("four shares were merged");
            // what the index says
            found.order_by(|partition| order.iter().position(|key| *key == partition));
            // against what asking every row would have said
            let mut expected = found.rows.clone();
            order_by_hashing(&mut expected, &order);
            assert_eq!(
                found.rows, expected,
                "the grouped reorder disagreed with the hashing one it replaced, for shares \
                 arriving in {arrival:?}"
            );
            assert!(found.is_consistent());
        }
    }

    /// A limit trims the index with the rows it trims
    ///
    /// The rows and the index have to be cut in the same place. Trimming one and not the other
    /// leaves a client reading rows through an index that describes rows that are no longer there.
    #[test]
    fn a_limit_trims_the_group_index_with_the_rows_it_trims() {
        // three partitions, with the limit landing inside the second of them
        let mut found = GetRows::from_slots(vec![run(11, 2), run(22, 3), run(33, 4)]);
        found.truncate(4);
        assert!(found.is_consistent());
        // the first group survives whole, the second is clipped, the third goes entirely
        assert_eq!(
            found.groups,
            vec![
                RowGroup {
                    partition: 11,
                    len: 2
                },
                RowGroup {
                    partition: 22,
                    len: 2
                },
            ]
        );
        assert_eq!(found.len(), 4);
        // a limit no answer reaches leaves both halves alone
        let mut found = GetRows::from_slots(vec![run(11, 2)]);
        found.truncate(9);
        assert_eq!(found.groups.len(), 1);
        assert_eq!(found.len(), 2);
        // and a limit that lands exactly on a group boundary drops the groups past it, whole
        let mut found = GetRows::from_slots(vec![run(11, 2), run(22, 3)]);
        found.truncate(2);
        assert!(found.is_consistent());
        assert_eq!(found.groups.len(), 1);
    }

    /// A limit trims rows and leaves a failure alone
    #[test]
    fn a_truncate_leaves_an_error_alone() {
        // a failure has no rows to trim, so a limit cannot turn it into an empty get
        let mut failed = ResponseAction::<u64>::Error(ResponseError::new(
            ErrorCode::StorageRead,
            "could not read partition 7",
        ));
        failed.truncate(0);
        assert!(matches!(failed, ResponseAction::Error(_)));
        // while a get really is trimmed, so this test is not passing by doing nothing
        let mut rows = ResponseAction::Get(Some(GetRows::single(7, vec![1u64, 2, 3])));
        rows.truncate(2);
        assert!(matches!(rows, ResponseAction::Get(Some(ref rows)) if rows.len() == 2));
    }
}
