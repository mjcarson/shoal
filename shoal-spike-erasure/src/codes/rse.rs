//! `reed-solomon-erasure` 6.0.0 with `simd-accel`: the classic GF(2^8) Reed-Solomon, whose C
//! kernels are compiled for one `-march` and chosen at compile time. It has no update call; the
//! harness builds one from the public `galois_8::mul_slice_xor` and coefficients it reads out of
//! the crate by encoding unit vectors, and every table says so

use std::ops::Range;

use reed_solomon_erasure::galois_8::{self, ReedSolomon};

use super::{Code, Layout, StoredShape, lost_data, xor_into};

/// `reed-solomon-erasure` over its own Vandermonde-derived matrix
#[derive(Default)]
pub struct Rse {
    /// The codec for the prepared layout
    codec: Option<ReedSolomon>,
    /// The parity rows of the encode matrix, read out of the crate: `coefficients[r][i]`
    coefficients: Vec<Vec<u8>>,
    /// Space for an update's change
    delta: Vec<u8>,
}

impl Rse {
    /// Run a reconstruct with every chunk in `lost` marked missing
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    /// * `stored` - The parity chunks
    /// * `lost` - The chunks that cannot be read
    /// * `data_only` - Whether only lost data units are rebuilt
    fn reconstruct(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
        data_only: bool,
    ) -> Result<bool, String> {
        let codec = self.codec.as_ref().ok_or("not prepared")?;
        // every chunk in order with whether it is present; the crate writes into the missing ones
        let mut shards: Vec<(&mut [u8], bool)> = data
            .iter_mut()
            .map(|chunk| &mut **chunk)
            .chain(stored.iter_mut().map(|chunk| &mut **chunk))
            .enumerate()
            .map(|(i, chunk)| (chunk, !lost.contains(&i)))
            .collect();
        let result = if data_only {
            codec.reconstruct_data(&mut shards)
        } else {
            codec.reconstruct(&mut shards)
        };
        result.map(|()| true).map_err(|e| format!("{e:?}"))
    }
}

impl Code for Rse {
    /// The name in every table
    fn name(&self) -> &'static str {
        "reed-solomon-erasure"
    }

    /// Reed-Solomon over a systematic matrix
    fn systematic(&self) -> bool {
        true
    }

    /// Any k + m up to 256
    fn supports(&self, layout: Layout, _unit: usize) -> Result<(), String> {
        if layout.n() <= 256 {
            Ok(())
        } else {
            Err("k + m above 256".to_string())
        }
    }

    /// Build the codec and read its parity coefficients out of it
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        let codec = ReedSolomon::new(layout.k, layout.m).map_err(|e| format!("{e:?}"))?;
        // data unit i of k bytes is the unit vector e_i, so parity row r's byte i is coefficient
        // (r, i): the crate keeps its matrix private, and this is how it is read
        let units: Vec<Vec<u8>> = (0..layout.k)
            .map(|i| (0..layout.k).map(|j| u8::from(i == j)).collect())
            .collect();
        let mut coefficients = vec![vec![0u8; layout.k]; layout.m];
        codec
            .encode_sep(&units, &mut coefficients)
            .map_err(|e| format!("{e:?}"))?;
        self.coefficients = coefficients;
        self.codec = Some(codec);
        self.delta = vec![0; unit];
        Ok(StoredShape {
            count: layout.m,
            len: unit,
        })
    }

    /// Encode into the caller's buffers
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        let codec = self.codec.as_ref().ok_or("not prepared")?;
        codec.encode_sep(data, stored).map_err(|e| format!("{e:?}"))
    }

    /// Rebuild only the lost data units; the crate caches the decode matrix for a pattern itself
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String> {
        // a healthy read decodes nothing
        if lost_data(data.len(), lost).is_empty() {
            return Ok(true);
        }
        self.reconstruct(data, stored, lost, true)
    }

    /// Rebuild one chunk: the crate rebuilds every missing chunk, and only this one is missing
    fn rebuild(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        target: usize,
    ) -> Result<bool, String> {
        self.reconstruct(data, stored, &[target], false)
    }

    /// Built by the harness: each parity chunk's range gets coefficient · change
    fn update(
        &mut self,
        index: usize,
        old: &[u8],
        new: &[u8],
        stored: &mut [&mut [u8]],
        range: Range<usize>,
    ) -> Result<(), String> {
        // the change is old xor new
        let delta = &mut self.delta[..old.len()];
        xor_into(delta, old, new);
        // fold it into each parity chunk with that row's coefficient for this unit
        for (row, chunk) in self.coefficients.iter().zip(stored.iter_mut()) {
            galois_8::mul_slice_xor(row[index], delta, &mut chunk[range.clone()]);
        }
        Ok(())
    }
}
