//! `reed-solomon-simd` 3.1.0: Leopard-RS over GF(2^16), systematic, with AVX2 or SSSE3 chosen at
//! run time. It copies every shard into work space of its own and hands results back as borrows of
//! it, so every encode and decode here copies out into the caller's buffers; a node would have to

use std::ops::Range;

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use super::{Code, Layout, StoredShape, lost_data};

/// `reed-solomon-simd`, one encoder and one decoder reused through `reset`
#[derive(Default)]
pub struct RsSimd {
    /// The prepared layout
    layout: Option<Layout>,
    /// The size of a unit
    unit: usize,
    /// The encoder
    encoder: Option<ReedSolomonEncoder>,
    /// The decoder
    decoder: Option<ReedSolomonDecoder>,
}

impl Code for RsSimd {
    /// The name in every table
    fn name(&self) -> &'static str {
        "reed-solomon-simd"
    }

    /// The originals are stored as they are
    fn systematic(&self) -> bool {
        true
    }

    /// A shard's size must be even, and the counts within Leopard's bounds
    fn supports(&self, layout: Layout, unit: usize) -> Result<(), String> {
        if !unit.is_multiple_of(2) {
            return Err("a shard's size must be even".to_string());
        }
        if !ReedSolomonEncoder::supports(layout.k, layout.m) {
            return Err("k and m outside Leopard's bounds".to_string());
        }
        Ok(())
    }

    /// Build the encoder and decoder at this shape
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        self.supports(layout, unit)?;
        self.encoder =
            Some(ReedSolomonEncoder::new(layout.k, layout.m, unit).map_err(|e| format!("{e:?}"))?);
        self.decoder =
            Some(ReedSolomonDecoder::new(layout.k, layout.m, unit).map_err(|e| format!("{e:?}"))?);
        self.layout = Some(layout);
        self.unit = unit;
        Ok(StoredShape {
            count: layout.m,
            len: unit,
        })
    }

    /// Hand the originals over, encode, and copy the recovery shards out
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        let layout = self.layout.ok_or("not prepared")?;
        let encoder = self.encoder.as_mut().ok_or("not prepared")?;
        // a reset keeps the work space and forgets the last shards
        encoder
            .reset(layout.k, layout.m, self.unit)
            .map_err(|e| format!("{e:?}"))?;
        // the originals are copied into the work space
        for unit in data {
            encoder
                .add_original_shard(unit)
                .map_err(|e| format!("{e:?}"))?;
        }
        // and the recovery shards copied out of it
        let result = encoder.encode().map_err(|e| format!("{e:?}"))?;
        for (j, chunk) in stored.iter_mut().enumerate() {
            chunk.copy_from_slice(result.recovery(j).ok_or("a recovery shard is missing")?);
        }
        Ok(())
    }

    /// Hand over what survives, decode, and copy the restored originals out
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String> {
        let layout = self.layout.ok_or("not prepared")?;
        // a healthy read decodes nothing
        let missing = lost_data(layout.k, lost);
        if missing.is_empty() {
            return Ok(true);
        }
        let decoder = self.decoder.as_mut().ok_or("not prepared")?;
        decoder
            .reset(layout.k, layout.m, self.unit)
            .map_err(|e| format!("{e:?}"))?;
        // every readable original and recovery shard, by index
        for (i, unit) in data.iter().enumerate() {
            if !lost.contains(&i) {
                decoder
                    .add_original_shard(i, &**unit)
                    .map_err(|e| format!("{e:?}"))?;
            }
        }
        for (j, chunk) in stored.iter().enumerate() {
            if !lost.contains(&(layout.k + j)) {
                decoder
                    .add_recovery_shard(j, &**chunk)
                    .map_err(|e| format!("{e:?}"))?;
            }
        }
        // the restored originals are borrows of the decoder's work space
        let result = decoder.decode().map_err(|e| format!("{e:?}"))?;
        for i in missing {
            data[i].copy_from_slice(
                result
                    .restored_original(i)
                    .ok_or("an original was not restored")?,
            );
        }
        Ok(true)
    }

    /// A data unit is a decode; a recovery shard can only be had by encoding again
    fn rebuild(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        target: usize,
    ) -> Result<bool, String> {
        let layout = self.layout.ok_or("not prepared")?;
        if target < layout.k {
            return self.decode(data, stored, &[target]);
        }
        // the decoder restores originals only, so a lost recovery shard is a whole encode
        let encoder = self.encoder.as_mut().ok_or("not prepared")?;
        encoder
            .reset(layout.k, layout.m, self.unit)
            .map_err(|e| format!("{e:?}"))?;
        for unit in data.iter() {
            encoder
                .add_original_shard(&**unit)
                .map_err(|e| format!("{e:?}"))?;
        }
        let result = encoder.encode().map_err(|e| format!("{e:?}"))?;
        stored[target - layout.k].copy_from_slice(
            result
                .recovery(target - layout.k)
                .ok_or("a recovery shard is missing")?,
        );
        Ok(true)
    }

    /// None in the API
    fn update(
        &mut self,
        _index: usize,
        _old: &[u8],
        _new: &[u8],
        _stored: &mut [&mut [u8]],
        _range: Range<usize>,
    ) -> Result<(), String> {
        Err(
            "no update in the API; the FFT form exposes no coefficient to fold a change with"
                .to_string(),
        )
    }
}
