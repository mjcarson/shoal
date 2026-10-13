//! The pool layouts the model runs
//!
//! S16 names three, each at `f = 1`: replicated three ways, 2+1 and 4+2
//! ([S16](../../../docs/src/object-storage/testing.md#the-model)). One stripe chunk sits on one
//! slice, and the slices of a stripe are on distinct devices, so a device is the failure domain.

use serde::{Deserialize, Serialize};

use crate::stripe::ids::Pos;

/// How many units a stripe chunk holds
///
/// Two is the least that tells a write of part of a chunk from a write of the whole of it, which
/// is the line between a journalled update and a whole chunk staged beside the old one
/// ([S6](../../../docs/src/object-storage/device-store.md#staging-two-cases)).
pub const UNITS: usize = 2;

/// A pool's redundancy
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Layout {
    /// Three copies: `k = 1` and two more
    Replicated3,
    /// Two data chunks and one parity chunk
    TwoPlusOne,
    /// Four data chunks and two parity chunks
    FourPlusTwo,
}

impl Layout {
    /// Every layout, in the order the search runs them
    pub const ALL: [Layout; 3] = [Layout::Replicated3, Layout::TwoPlusOne, Layout::FourPlusTwo];

    /// The data chunks a stripe holds; a replicated stripe's one chunk is its whole state
    pub fn k(self) -> usize {
        match self {
            Layout::Replicated3 => 1,
            Layout::TwoPlusOne => 2,
            Layout::FourPlusTwo => 4,
        }
    }

    /// The chunks a stripe holds beyond `k`: copies or parity
    pub fn m(self) -> usize {
        match self {
            Layout::Replicated3 => 2,
            Layout::TwoPlusOne => 1,
            Layout::FourPlusTwo => 2,
        }
    }

    /// How many more losses an acknowledged write survives; at least one ([P11](../../../docs/src/object-storage/contract.md))
    pub fn f(self) -> usize {
        1
    }

    /// The positions a stripe has
    pub fn width(self) -> usize {
        self.k() + self.m()
    }

    /// The current chunks a write must leave behind it before it is acknowledged
    pub fn ack_floor(self) -> usize {
        self.k() + self.f()
    }

    /// Whether every chunk is a whole copy of the stripe
    pub fn is_replicated(self) -> bool {
        self == Layout::Replicated3
    }

    /// Whether a position holds data, as opposed to parity
    ///
    /// Every position of a replicated stripe holds the data whole.
    ///
    /// # Arguments
    ///
    /// * `pos` - The position
    pub fn is_data(self, pos: Pos) -> bool {
        self.is_replicated() || usize::from(pos.0) < self.k()
    }

    /// The data units a stripe holds
    pub fn data_units(self) -> usize {
        self.k() * UNITS
    }

    /// Every position, in order
    pub fn positions(self) -> Vec<Pos> {
        (0..self.width()).map(|pos| Pos(pos as u8)).collect()
    }

    /// The data chunk a stripe's data unit lives in, and its unit within that chunk
    ///
    /// A stripe's bytes are laid chunk after chunk: data chunk `i` holds units `i * UNITS` up to
    /// the next. A replicated stripe's one data chunk is at position zero.
    ///
    /// # Arguments
    ///
    /// * `index` - The data unit's index within the stripe
    pub fn locate(self, index: usize) -> (Pos, usize) {
        (Pos((index / UNITS) as u8), index % UNITS)
    }

    /// A short name for tables and file names
    pub fn short(self) -> &'static str {
        match self {
            Layout::Replicated3 => "r3",
            Layout::TwoPlusOne => "2+1",
            Layout::FourPlusTwo => "4+2",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three layouts are the ones S16 names, at f = 1
    #[test]
    fn the_layouts_are_s16s() {
        let shapes: Vec<(usize, usize, usize)> = Layout::ALL
            .iter()
            .map(|layout| (layout.k(), layout.m(), layout.ack_floor()))
            .collect();
        assert_eq!(shapes, vec![(1, 2, 2), (2, 1, 3), (4, 2, 5)]);
    }

    /// A data unit lands in its chunk, chunk after chunk
    #[test]
    fn data_units_are_laid_chunk_after_chunk() {
        let layout = Layout::FourPlusTwo;
        assert_eq!(layout.locate(0), (Pos(0), 0));
        assert_eq!(layout.locate(3), (Pos(1), 1));
        assert_eq!(layout.locate(7), (Pos(3), 1));
        assert!(layout.is_data(Pos(3)));
        assert!(!layout.is_data(Pos(4)));
        assert!(Layout::Replicated3.is_data(Pos(2)));
    }
}
