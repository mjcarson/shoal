//! One parity chunk, the XOR of the data: no field arithmetic at all, and the ceiling every code is
//! read against at m = 1

use std::ops::Range;

use super::{Code, Layout, StoredShape, lost_data, xor_in, xor_into};

/// The bytes xored across every source before moving on, small enough to stay in the first level
/// cache, so the output is written to memory once and not once a source
const BLOCK: usize = 1024;

/// XOR every source into `out` in one pass over memory
///
/// # Arguments
///
/// * `out` - Where the result goes
/// * `sources` - Two or more inputs of the same length
fn xor_all(out: &mut [u8], sources: &[&[u8]]) {
    for start in (0..out.len()).step_by(BLOCK) {
        let end = (start + BLOCK).min(out.len());
        // the first two sources make the block, every further one is folded into it
        xor_into(
            &mut out[start..end],
            &sources[0][start..end],
            &sources[1][start..end],
        );
        for source in &sources[2..] {
            xor_in(&mut out[start..end], &source[start..end]);
        }
    }
}

/// Plain XOR parity
#[derive(Default)]
pub struct Xor {
    /// The layout prepared for
    layout: Option<Layout>,
    /// The size of a unit
    unit: usize,
}

impl Code for Xor {
    /// The name in every table
    fn name(&self) -> &'static str {
        "xor"
    }

    /// The data chunks are the data
    fn systematic(&self) -> bool {
        true
    }

    /// Only one parity chunk
    fn supports(&self, layout: Layout, _unit: usize) -> Result<(), String> {
        if layout.m == 1 {
            Ok(())
        } else {
            Err("one parity chunk only".to_string())
        }
    }

    /// Nothing to build
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        // refuse what it cannot do
        self.supports(layout, unit)?;
        self.layout = Some(layout);
        self.unit = unit;
        Ok(StoredShape {
            count: 1,
            len: unit,
        })
    }

    /// The parity is every data unit xored together
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        xor_all(&mut stored[0][..], data);
        Ok(())
    }

    /// A lost unit is the parity xored with every other unit
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String> {
        // the parity is chunk k
        let k = data.len();
        let missing = lost_data(k, lost);
        // nothing to do when only the parity is gone, and too much when more than one chunk is
        if missing.is_empty() {
            return Ok(true);
        }
        if lost.len() > 1 {
            return Ok(false);
        }
        self.rebuild(data, stored, missing[0])
    }

    /// The lost chunk is the xor of every other
    fn rebuild(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        target: usize,
    ) -> Result<bool, String> {
        let k = data.len();
        if target == k {
            // the parity: encode it again
            let units: Vec<&[u8]> = data.iter().map(|unit| &**unit).collect();
            return self.encode(&units, stored).map(|()| true);
        }
        // a data unit: split it from the others so it can be written while they are read
        let (before, rest) = data.split_at_mut(target);
        let (out, after) = rest.split_first_mut().expect("target is a data unit");
        // it is the parity xored with every other unit
        let mut sources: Vec<&[u8]> = vec![&stored[0][..]];
        sources.extend(before.iter().chain(after.iter()).map(|unit| &**unit));
        xor_all(out, &sources);
        Ok(true)
    }

    /// The parity changes by exactly what the data did
    fn update(
        &mut self,
        _index: usize,
        old: &[u8],
        new: &[u8],
        stored: &mut [&mut [u8]],
        range: Range<usize>,
    ) -> Result<(), String> {
        // fold old and new straight into the parity's range
        let parity = &mut stored[0][range];
        xor_in(parity, old);
        xor_in(parity, new);
        Ok(())
    }
}
