//! The judgement of one object's history
//!
//! A history is right when one total order of its writes and reads explains every read. In that
//! order every acknowledged write and every read appears, an unknown write appears or does not, a
//! refused write never does, and a read shows, granule by granule, the last write before it in
//! the order, or zeros. Real time binds the order: anything that completed before something else
//! was invoked comes first, except that nothing has to come before a default read, which may be
//! stale. Each write takes effect at one point in the order, so a write seen in part has no
//! order that explains it.
//!
//! Before that search, four checks that each prove a violation on their own name the read and
//! the write at fault, which the search cannot: bytes no write wrote, a refused write's bytes, an
//! acknowledged write a strong read misses, and a write shown in part. The search is bounded, as
//! C11's oracle is ([`crate::oracle::MAX_OPS_PER_KEY`]): it tries each write that can go next and
//! remembers every state it has explored.

use std::collections::{BTreeMap, HashSet};

use super::{ByteLedger, ReadId, ReadLevel, Violation, WriteId, WriteOutcome};

/// The most writes and reads one object's search is bounded to
pub const MAX_OPS_PER_OBJECT: usize = 64;

/// What a granule holds at a point in an order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Source {
    /// Nothing wrote it, so it is zeros
    Zeros,
    /// The write at this index of the object's writes
    Write(usize),
}

/// One write of the object, as the judgement sees it
struct Write<'a> {
    /// Its id in the ledger
    id: WriteId,
    /// The byte it starts at
    offset: u64,
    /// Its first granule's number
    first: u64,
    /// Its granules' checksums
    granules: &'a [u64],
    /// When it was sent
    invoke: u64,
    /// When it was acknowledged, by which it had taken effect; none if it was not
    acknowledged: Option<u64>,
    /// What it was told; a write still waiting is unknown
    outcome: WriteOutcome,
}

impl Write<'_> {
    /// Whether it wrote a granule
    ///
    /// # Arguments
    ///
    /// * `granule` - The granule's number
    fn covers(&self, granule: u64) -> bool {
        granule >= self.first && granule < self.first + self.granules.len() as u64
    }

    /// The checksum it wrote to a granule it covers
    ///
    /// # Arguments
    ///
    /// * `granule` - The granule's number
    fn checksum(&self, granule: u64) -> u64 {
        self.granules[(granule - self.first) as usize]
    }

    /// Whether it took effect before another write was sent, so every order puts it first
    ///
    /// # Arguments
    ///
    /// * `other` - The other write
    fn precedes(&self, other: &Write) -> bool {
        self.acknowledged.is_some_and(|step| step < other.invoke)
    }
}

/// One read of the object
struct Read<'a> {
    /// Its id in the ledger
    id: ReadId,
    /// The byte it starts at
    offset: u64,
    /// Its first granule's number
    first: u64,
    /// The checksums of the granules it was answered
    granules: &'a [u64],
    /// What it asked for
    level: ReadLevel,
    /// When it was sent
    invoke: u64,
    /// When it was answered
    complete: u64,
}

/// An operation in the search
#[derive(Debug, Clone, Copy)]
enum Kind {
    /// The write at this index of the object's writes
    Write(usize),
    /// The read at this index of the object's reads
    Read(usize),
}

/// Everything the search needs, built once an object
struct Search<'a> {
    /// The object's writes that may take effect: every one not refused
    writes: &'a [Write<'a>],
    /// The object's reads
    reads: &'a [Read<'a>],
    /// The checksum of a granule of zeros
    zeros: u64,
    /// Every operation in the search
    ops: Vec<Kind>,
    /// For each operation, the operations real time puts before it, as a mask
    preds: Vec<u64>,
    /// The operations every order must hold: acknowledged writes and every read
    required: u64,
    /// For each write, the slot in the state of each granule it covers
    write_slots: Vec<Vec<usize>>,
    /// For each read, the slot in the state of each granule it covers
    read_slots: Vec<Vec<usize>>,
}

impl Search<'_> {
    /// The checksum a granule holds from a source
    ///
    /// # Arguments
    ///
    /// * `source` - What wrote it
    /// * `granule` - The granule's number
    fn checksum(&self, source: Source, granule: u64) -> u64 {
        match source {
            Source::Zeros => self.zeros,
            Source::Write(index) => self.writes[index].checksum(granule),
        }
    }

    /// Whether a state shows exactly what a read was answered
    ///
    /// # Arguments
    ///
    /// * `read` - The read's index
    /// * `state` - What each granule holds
    fn explains(&self, read: usize, state: &[Source]) -> bool {
        let record = &self.reads[read];
        record
            .granules
            .iter()
            .zip(&self.read_slots[read])
            .enumerate()
            .all(|(position, (checksum, slot))| {
                self.checksum(state[*slot], record.first + position as u64) == *checksum
            })
    }

    /// Whether an operation can go next: it is not placed and everything real time puts first is
    ///
    /// # Arguments
    ///
    /// * `op` - The operation's index
    /// * `done` - What is placed
    fn ready(&self, op: usize, done: u64) -> bool {
        done & (1 << op) == 0 && self.preds[op] & !done == 0
    }

    /// Look for an order that places every required operation from here
    ///
    /// # Arguments
    ///
    /// * `done` - What is placed
    /// * `state` - What each granule holds after it
    /// * `seen` - Every point already explored
    fn find(
        &self,
        mut done: u64,
        state: &mut Vec<Source>,
        seen: &mut HashSet<(u64, Vec<Source>)>,
    ) -> bool {
        // place every read the state explains: a read changes nothing, so placing it as soon as
        // it can go loses no order, and it may let a write that must follow it go
        loop {
            let mut placed = false;
            for (op, kind) in self.ops.iter().enumerate() {
                if let Kind::Read(read) = kind {
                    if self.ready(op, done) && self.explains(*read, state) {
                        done |= 1 << op;
                        placed = true;
                    }
                }
            }
            if !placed {
                break;
            }
        }
        // every acknowledged write and every read placed is an order
        if self.required & !done == 0 {
            return true;
        }
        // a point already explored leads nowhere a second time
        if !seen.insert((done, state.clone())) {
            return false;
        }
        // try each write that can go next
        for (op, kind) in self.ops.iter().enumerate() {
            let Kind::Write(write) = kind else {
                continue;
            };
            if !self.ready(op, done) {
                continue;
            }
            // it takes effect on every granule it covers at once
            let before: Vec<Source> = self.write_slots[*write]
                .iter()
                .map(|slot| state[*slot])
                .collect();
            for slot in &self.write_slots[*write] {
                state[*slot] = Source::Write(*write);
            }
            if self.find(done | (1 << op), state, seen) {
                return true;
            }
            // and is taken back before the next is tried
            for (slot, source) in self.write_slots[*write].iter().zip(before) {
                state[*slot] = source;
            }
        }
        false
    }
}

/// Judge one object's writes and reads
///
/// # Arguments
///
/// * `ledger` - The history
/// * `object` - The object to judge
/// * `zeros` - The checksum of a granule of zeros
pub(super) fn object(ledger: &ByteLedger, object: &str, zeros: u64) -> Result<(), Violation> {
    // the object's writes, with what each was told
    let writes: Vec<Write> = ledger
        .writes
        .iter()
        .enumerate()
        .filter(|(_, record)| record.object == object)
        .map(|(index, record)| {
            let outcome = record.outcome.unwrap_or(WriteOutcome::Unknown);
            Write {
                id: WriteId(index),
                offset: record.offset,
                first: record.offset / ledger.granule,
                granules: &record.granules,
                invoke: record.invoke,
                acknowledged: match outcome {
                    WriteOutcome::Acknowledged => record.complete,
                    WriteOutcome::Refused | WriteOutcome::Unknown => None,
                },
                outcome,
            }
        })
        .collect();
    // and its reads, in invocation order
    let mut reads: Vec<Read> = ledger
        .reads
        .iter()
        .enumerate()
        .filter(|(_, record)| record.object == object)
        .map(|(index, record)| Read {
            id: ReadId(index),
            offset: record.offset,
            first: record.offset / ledger.granule,
            granules: &record.granules,
            level: record.level,
            invoke: record.invoke,
            complete: record.complete,
        })
        .collect();
    reads.sort_by_key(|read| (read.invoke, read.id));
    // the checks that name the read and the write at fault come first
    for read in &reads {
        check_read(object, ledger.granule, &writes, read, zeros)?;
    }
    // then an order, among the writes that may have taken effect
    let effective: Vec<Write> = writes
        .into_iter()
        .filter(|write| write.outcome != WriteOutcome::Refused)
        .collect();
    let count = effective.len() + reads.len();
    if count > MAX_OPS_PER_OBJECT {
        return Err(Violation::TooManyOps {
            object: object.to_string(),
            count,
        });
    }
    let search = build(&effective, &reads, zeros);
    let mut state = vec![Source::Zeros; slots(&effective, &reads).len()];
    if search.find(0, &mut state, &mut HashSet::new()) {
        return Ok(());
    }
    Err(Violation::NotLinearizable {
        object: object.to_string(),
        detail: describe(ledger.granule, &effective, &reads, zeros),
    })
}

/// What a granule of a read could have come from
///
/// # Arguments
///
/// * `writes` - The object's writes
/// * `granule` - The granule's number
/// * `checksum` - What the read was answered there
/// * `zeros` - The checksum of a granule of zeros
///
/// Returns the sources a write not refused or zeros could be, and the refused writes that match.
fn sources(
    writes: &[Write],
    granule: u64,
    checksum: u64,
    zeros: u64,
) -> (Vec<Source>, Vec<WriteId>) {
    let mut found = Vec::new();
    let mut refused = Vec::new();
    // zeros, if nothing had written it
    if checksum == zeros {
        found.push(Source::Zeros);
    }
    // and every write of these bytes to this granule
    for (index, write) in writes.iter().enumerate() {
        if !write.covers(granule) || write.checksum(granule) != checksum {
            continue;
        }
        if write.outcome == WriteOutcome::Refused {
            refused.push(write.id);
        } else {
            found.push(Source::Write(index));
        }
    }
    (found, refused)
}

/// Whether every order puts a source before a write's effect, so a granule the write covers
/// cannot show it once the write took effect
///
/// # Arguments
///
/// * `writes` - The object's writes
/// * `source` - The source
/// * `write` - The write's index
fn older(writes: &[Write], source: Source, write: usize) -> bool {
    match source {
        Source::Zeros => true,
        Source::Write(other) => other != write && writes[other].precedes(&writes[write]),
    }
}

/// The checks on one read that each prove a violation, naming the read and the write at fault
///
/// # Arguments
///
/// * `object` - The object
/// * `granule` - The bytes a granule holds
/// * `writes` - The object's writes, refused ones too
/// * `read` - The read
/// * `zeros` - The checksum of a granule of zeros
fn check_read(
    object: &str,
    granule: u64,
    writes: &[Write],
    read: &Read,
    zeros: u64,
) -> Result<(), Violation> {
    // what each granule of the read could have come from
    let mut found: BTreeMap<u64, Vec<Source>> = BTreeMap::new();
    for (position, checksum) in read.granules.iter().enumerate() {
        let number = read.first + position as u64;
        let (sources, refused) = sources(writes, number, *checksum, zeros);
        // bytes nothing may have written: a refused write's, or nobody's
        if sources.is_empty() {
            let offset = number * granule;
            return Err(match refused.first() {
                Some(write) => Violation::RefusedRead {
                    object: object.to_string(),
                    read: read.id,
                    write: *write,
                    offset,
                },
                None => Violation::Unwritten {
                    object: object.to_string(),
                    read: read.id,
                    offset,
                },
            });
        }
        found.insert(number, sources);
    }
    // a strong read shows every write acknowledged before it began, or something newer
    if read.level == ReadLevel::Strong {
        for (index, write) in writes.iter().enumerate() {
            if !write.acknowledged.is_some_and(|step| step < read.invoke) {
                continue;
            }
            for (number, sources) in &found {
                if write.covers(*number)
                    && sources.iter().all(|source| older(writes, *source, index))
                {
                    return Err(Violation::Missing {
                        object: object.to_string(),
                        read: read.id,
                        write: write.id,
                        offset: number * granule,
                    });
                }
            }
        }
    }
    // a write the read certainly shows somewhere took effect on every granule it covers
    for sources in found.values() {
        let [Source::Write(index)] = sources.as_slice() else {
            continue;
        };
        let write = &writes[*index];
        for (number, others) in &found {
            if write.covers(*number) && others.iter().all(|source| older(writes, *source, *index)) {
                return Err(Violation::Part {
                    object: object.to_string(),
                    read: read.id,
                    write: write.id,
                    offset: number * granule,
                });
            }
        }
    }
    Ok(())
}

/// Every granule an object's operations touch, each given a slot in the state
///
/// # Arguments
///
/// * `writes` - The writes in the search
/// * `reads` - The reads
fn slots(writes: &[Write], reads: &[Read]) -> BTreeMap<u64, usize> {
    // the granule numbers, in order
    let mut numbers: Vec<u64> = writes
        .iter()
        .flat_map(|write| write.first..write.first + write.granules.len() as u64)
        .chain(
            reads
                .iter()
                .flat_map(|read| read.first..read.first + read.granules.len() as u64),
        )
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    numbers
        .into_iter()
        .enumerate()
        .map(|(slot, number)| (number, slot))
        .collect()
}

/// Build the search over an object's writes that may take effect and its reads
///
/// # Arguments
///
/// * `writes` - The writes not refused
/// * `reads` - The reads
/// * `zeros` - The checksum of a granule of zeros
fn build<'a>(writes: &'a [Write<'a>], reads: &'a [Read<'a>], zeros: u64) -> Search<'a> {
    // each operation, when it was sent, when it had certainly taken effect, and whether real
    // time binds what comes before it: a default read may be stale, so nothing has to
    let mut ops = Vec::new();
    let mut times: Vec<(u64, Option<u64>, bool)> = Vec::new();
    let mut required = 0u64;
    for (index, write) in writes.iter().enumerate() {
        if write.outcome == WriteOutcome::Acknowledged {
            required |= 1 << ops.len();
        }
        ops.push(Kind::Write(index));
        times.push((write.invoke, write.acknowledged, true));
    }
    for (index, read) in reads.iter().enumerate() {
        required |= 1 << ops.len();
        ops.push(Kind::Read(index));
        times.push((
            read.invoke,
            Some(read.complete),
            read.level == ReadLevel::Strong,
        ));
    }
    // what real time puts before each operation
    let preds = times
        .iter()
        .map(|(invoke, _, bound)| {
            if !bound {
                return 0;
            }
            times
                .iter()
                .enumerate()
                .filter(|(_, (_, done_by, _))| done_by.is_some_and(|step| step < *invoke))
                .fold(0u64, |mask, (op, _)| mask | (1 << op))
        })
        .collect();
    // the slot of every granule each operation covers
    let slots = slots(writes, reads);
    let write_slots = writes
        .iter()
        .map(|write| {
            (write.first..write.first + write.granules.len() as u64)
                .map(|number| slots[&number])
                .collect()
        })
        .collect();
    let read_slots = reads
        .iter()
        .map(|read| {
            (read.first..read.first + read.granules.len() as u64)
                .map(|number| slots[&number])
                .collect()
        })
        .collect();
    Search {
        writes,
        reads,
        zeros,
        ops,
        preds,
        required,
        write_slots,
        read_slots,
    }
}

/// An object's history in invocation order, for a failure's message
///
/// # Arguments
///
/// * `granule` - The bytes a granule holds
/// * `writes` - The writes not refused
/// * `reads` - The reads
/// * `zeros` - The checksum of a granule of zeros
fn describe(granule: u64, writes: &[Write], reads: &[Read], zeros: u64) -> String {
    // every operation with the step it was sent at, so they sort into one line
    let mut lines: Vec<(u64, String)> = Vec::new();
    for write in writes {
        let end = write.offset + write.granules.len() as u64 * granule;
        let outcome = match write.outcome {
            WriteOutcome::Acknowledged => "acknowledged",
            WriteOutcome::Refused => "refused",
            WriteOutcome::Unknown => "unknown",
        };
        let complete = write
            .acknowledged
            .map_or(String::new(), |step| step.to_string());
        lines.push((
            write.invoke,
            format!(
                "write {} [{}, {end}) {outcome} {}..{complete}",
                write.id.0, write.offset, write.invoke
            ),
        ));
    }
    for read in reads {
        let end = read.offset + read.granules.len() as u64 * granule;
        let level = match read.level {
            ReadLevel::Default => "default",
            ReadLevel::Strong => "strong",
        };
        // what each granule it was answered could have come from
        let shown: Vec<String> = read
            .granules
            .iter()
            .enumerate()
            .map(|(position, checksum)| {
                let (found, _) = sources(writes, read.first + position as u64, *checksum, zeros);
                let names: Vec<String> = found
                    .iter()
                    .map(|source| match source {
                        Source::Zeros => "zeros".to_string(),
                        Source::Write(index) => format!("w{}", writes[*index].id.0),
                    })
                    .collect();
                names.join("|")
            })
            .collect();
        lines.push((
            read.invoke,
            format!(
                "read {} {level} [{}, {end}) {}..{} showing [{}]",
                read.id.0,
                read.offset,
                read.invoke,
                read.complete,
                shown.join(", ")
            ),
        ));
    }
    lines.sort();
    lines
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("; ")
}
