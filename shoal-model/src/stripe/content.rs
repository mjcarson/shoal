//! The bytes of a stripe, abstracted to which write produced them
//!
//! The model never holds real bytes. A data unit holds the identity of the write that last wrote
//! it, and a parity unit holds the data units it encodes, so two chunks agree exactly when real
//! chunks computed from the same writes would. What a code does with chunks that do not agree is
//! the one property of erasure coding this part leans on ([S8](../../../docs/src/object-storage/erasure-coding.md#the-write-hole)):
//! decoding them returns bytes nobody wrote, which is [`Unit::Garbage`] here.
//!
//! Folding a change into parity is an exclusive or, so folding the same change twice undoes it.
//! That is what makes a parity staged as a patch dangerous to replay, and the model keeps it.

use serde::{Deserialize, Serialize};

use crate::ids::OpId;
use crate::stripe::ids::Pos;
use crate::stripe::layout::{Layout, UNITS};

/// One unit of a stripe's data
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    /// Never written, or written with zeros
    Zero,
    /// Last written by this write; the object's put is operation zero
    Write(OpId),
    /// Bytes no write produced: a decode over chunks that do not agree
    Garbage,
    /// A unit an apply was cut short in, which fails its checksum
    Torn,
}

impl Unit {
    /// Whether this unit would fail its checksum
    pub fn is_torn(self) -> bool {
        self == Unit::Torn
    }
}

/// One unit of a parity chunk
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParityUnit {
    /// The data units of its unit row it encodes, one for each data chunk
    Encodes(Vec<Unit>),
    /// Parity that encodes no state of the stripe: a change folded into the wrong base
    Garbage,
    /// A unit an apply was cut short in
    Torn,
}

/// What a stripe chunk holds
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Content {
    /// A data chunk, or a copy of a replicated stripe: its units
    Data(Vec<Unit>),
    /// A parity chunk: one parity unit for each unit row
    Parity(Vec<ParityUnit>),
}

impl Content {
    /// Whether any unit of the chunk fails its checksum
    pub fn is_torn(&self) -> bool {
        match self {
            Content::Data(units) => units.iter().any(|unit| unit.is_torn()),
            Content::Parity(units) => units.contains(&ParityUnit::Torn),
        }
    }

    /// Mark some units torn, as a crash in the middle of writing them leaves them
    ///
    /// # Arguments
    ///
    /// * `units` - The units within the chunk that were being written
    pub fn tear(&mut self, units: &[usize]) {
        match self {
            Content::Data(values) => {
                for unit in units {
                    values[*unit] = Unit::Torn;
                }
            }
            Content::Parity(values) => {
                for unit in units {
                    values[*unit] = ParityUnit::Torn;
                }
            }
        }
    }
}

/// A change to one data unit, as a parity holder folds it
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Change {
    /// The data unit's index within the stripe
    pub index: u8,
    /// What it held before
    pub old: Unit,
    /// What it holds after
    pub new: Unit,
}

/// The chunk at a position for a stripe whose data units are these
///
/// # Arguments
///
/// * `layout` - The pool's layout
/// * `pos` - The position
/// * `data` - The stripe's data units, chunk after chunk
pub fn encode(layout: Layout, pos: Pos, data: &[Unit]) -> Content {
    // a replicated stripe's every chunk is the data whole
    if layout.is_replicated() {
        return Content::Data(data.to_vec());
    }
    // a data chunk is its own slice of the data
    if layout.is_data(pos) {
        let start = usize::from(pos.0) * UNITS;
        return Content::Data(data[start..start + UNITS].to_vec());
    }
    // a parity unit encodes its unit row: the same unit of every data chunk
    let rows = (0..UNITS)
        .map(|row| {
            ParityUnit::Encodes(
                (0..layout.k())
                    .map(|chunk| data[chunk * UNITS + row])
                    .collect(),
            )
        })
        .collect();
    Content::Parity(rows)
}

/// The data units a set of chunks decodes to
///
/// A replicated stripe decodes from any one copy. An erasure coded one takes each data chunk it
/// has as it is, and finds a missing data unit in its unit row's parity, which gives the right
/// answer only if the parity it uses agrees with every data unit and every other parity unit it
/// was given: otherwise the code returns bytes nobody wrote. The caller hands over at least `k`
/// chunks, none torn.
///
/// # Arguments
///
/// * `layout` - The pool's layout
/// * `chunks` - The chunks, by position
pub fn decode(layout: Layout, chunks: &[(Pos, Content)]) -> Vec<Unit> {
    // a replicated stripe: any copy is the whole state
    if layout.is_replicated() {
        return match chunks.first() {
            Some((_, Content::Data(units))) => units.clone(),
            _ => vec![Unit::Garbage; layout.data_units()],
        };
    }
    let k = layout.k();
    let mut out = vec![Unit::Garbage; layout.data_units()];
    for row in 0..UNITS {
        // the data units of this row the chunks give directly
        let mut known: Vec<Option<Unit>> = vec![None; k];
        let mut parity: Vec<&ParityUnit> = Vec::new();
        for (pos, content) in chunks {
            match content {
                Content::Data(units) if layout.is_data(*pos) => {
                    known[usize::from(pos.0)] = Some(units[row]);
                }
                Content::Parity(units) => parity.push(&units[row]),
                // a data chunk at a parity position is not a chunk the code can use
                Content::Data(_) => {}
            }
        }
        // every data unit present needs no decoding
        if known.iter().all(Option::is_some) {
            for (chunk, unit) in known.iter().enumerate() {
                out[chunk * UNITS + row] = unit.expect("checked present");
            }
            continue;
        }
        // otherwise the parity used has to agree with everything else it was given
        let agreed = agree(&known, &parity);
        for chunk in 0..k {
            out[chunk * UNITS + row] = match (known[chunk], &agreed) {
                (Some(unit), _) => unit,
                (None, Some(tuple)) => tuple[chunk],
                (None, None) => Unit::Garbage,
            };
        }
    }
    out
}

/// The tuple a row's parity agrees on with its known data units, if it does
///
/// # Arguments
///
/// * `known` - The data units given directly, by chunk
/// * `parity` - The parity units given
fn agree(known: &[Option<Unit>], parity: &[&ParityUnit]) -> Option<Vec<Unit>> {
    let mut tuple: Option<&Vec<Unit>> = None;
    for unit in parity {
        // garbage or torn parity decodes nothing
        let ParityUnit::Encodes(values) = unit else {
            return None;
        };
        // two parity units of one row that encode different states decode nothing
        if tuple.is_some_and(|seen| seen != values) {
            return None;
        }
        tuple = Some(values);
    }
    let tuple = tuple?;
    // parity that disagrees with a data unit it was decoded beside decodes nothing
    let consistent = known
        .iter()
        .zip(tuple.iter())
        .all(|(given, encoded)| given.is_none_or(|given| given == *encoded));
    consistent.then(|| tuple.clone())
}

/// Fold a change into a parity unit, as an exclusive or does
///
/// Parity that encoded the old value now encodes the new. Parity that already encoded the new
/// value goes back to the old one, since the same change twice cancels. Parity that encoded
/// anything else encodes no state at all afterwards.
///
/// # Arguments
///
/// * `unit` - The parity unit
/// * `chunk` - The data chunk within the row the change is to
/// * `old` - The old value
/// * `new` - The new value
pub fn fold(unit: &ParityUnit, chunk: usize, old: Unit, new: Unit) -> ParityUnit {
    let ParityUnit::Encodes(values) = unit else {
        return ParityUnit::Garbage;
    };
    // a change from a value to itself changes nothing
    if old == new {
        return unit.clone();
    }
    let mut values = values.clone();
    if values[chunk] == old {
        values[chunk] = new;
    } else if values[chunk] == new {
        values[chunk] = old;
    } else {
        return ParityUnit::Garbage;
    }
    ParityUnit::Encodes(values)
}

/// Fold a set of changes into a parity chunk
///
/// # Arguments
///
/// * `content` - The parity chunk
/// * `changes` - The changes, by data unit index
pub fn fold_all(content: &Content, changes: &[Change]) -> Content {
    let Content::Parity(units) = content else {
        return content.clone();
    };
    let mut units = units.clone();
    for change in changes {
        // the change lands in its unit's row, at its chunk
        let index = usize::from(change.index);
        let (chunk, row) = (index / UNITS, index % UNITS);
        units[row] = fold(&units[row], chunk, change.old, change.new);
    }
    Content::Parity(units)
}

/// The unit rows a set of changes touches
///
/// # Arguments
///
/// * `changes` - The changes
pub fn rows_of(changes: &[Change]) -> Vec<usize> {
    let mut rows: Vec<usize> = changes
        .iter()
        .map(|change| usize::from(change.index) % UNITS)
        .collect();
    rows.sort_unstable();
    rows.dedup();
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stripe of four data chunks, each unit written by its own write
    fn written() -> Vec<Unit> {
        (0..8).map(|op| Unit::Write(OpId(op + 1))).collect()
    }

    /// Any k chunks of one state decode to that state
    #[test]
    fn any_k_chunks_of_one_state_decode_to_it() {
        let layout = Layout::FourPlusTwo;
        let data = written();
        let all: Vec<(Pos, Content)> = layout
            .positions()
            .into_iter()
            .map(|pos| (pos, encode(layout, pos, &data)))
            .collect();
        // every set that leaves out two chunks
        for a in 0..6 {
            for b in a + 1..6 {
                let some: Vec<(Pos, Content)> = all
                    .iter()
                    .filter(|(pos, _)| usize::from(pos.0) != a && usize::from(pos.0) != b)
                    .cloned()
                    .collect();
                assert_eq!(decode(layout, &some), data, "without {a} and {b}");
            }
        }
    }

    /// Chunks of two states decode to bytes nobody wrote
    #[test]
    fn chunks_of_two_states_decode_to_garbage() {
        let layout = Layout::TwoPlusOne;
        let old = vec![Unit::Zero; 4];
        let mut new = old.clone();
        new[0] = Unit::Write(OpId(9));
        // data chunk 0 from the new state, parity from the old, data chunk 1 missing
        let chunks = vec![
            (Pos(0), encode(layout, Pos(0), &new)),
            (Pos(2), encode(layout, Pos(2), &old)),
        ];
        let decoded = decode(layout, &chunks);
        assert_eq!(decoded[2], Unit::Garbage);
        assert_eq!(decoded[0], Unit::Write(OpId(9)));
    }

    /// A change folded into parity at the old value gives the new state's parity
    #[test]
    fn a_change_folded_once_is_the_new_parity() {
        let layout = Layout::FourPlusTwo;
        let old = written();
        let mut new = old.clone();
        new[3] = Unit::Write(OpId(77));
        let change = Change {
            index: 3,
            old: old[3],
            new: new[3],
        };
        let parity = encode(layout, Pos(4), &old);
        assert_eq!(fold_all(&parity, &[change]), encode(layout, Pos(4), &new));
    }

    /// The same change folded twice undoes itself, which is why a patch must not be replayed
    #[test]
    fn a_change_folded_twice_undoes_itself() {
        let layout = Layout::TwoPlusOne;
        let old = vec![Unit::Zero; 4];
        let change = Change {
            index: 1,
            old: Unit::Zero,
            new: Unit::Write(OpId(5)),
        };
        let parity = encode(layout, Pos(2), &old);
        let once = fold_all(&parity, &[change]);
        assert_ne!(once, parity);
        assert_eq!(fold_all(&once, &[change]), parity);
    }

    /// A change folded into parity of another state encodes nothing
    #[test]
    fn a_change_into_the_wrong_base_is_garbage() {
        let unit = ParityUnit::Encodes(vec![Unit::Write(OpId(3)), Unit::Zero]);
        let folded = fold(&unit, 0, Unit::Write(OpId(2)), Unit::Write(OpId(4)));
        assert_eq!(folded, ParityUnit::Garbage);
    }
}
