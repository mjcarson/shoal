//! `rlnc` 0.8.7: random linear network coding over GF(2^8), with run-time SIMD up to GFNI on
//! AVX-512. Two adapters:
//!
//! - [`Rlnc`] is the crate as published. Every one of the k + m chunks is a coded piece, a random
//!   combination of the data with its k coefficients in front, so a read of any range decodes and
//!   a write of any byte rewrites every chunk. The encoder pads the data with a `0x81` marker and
//!   zeros and re-cuts it, so a piece carries U + 1 bytes of data
//! - [`RlncSystematic`] is the crate bent by the harness into a systematic layout, to show what its
//!   kernels give when the data chunks are the data. The encoder cannot be handed coefficients, so
//!   parity is made by the crate's `Recoder` over pieces whose coefficient vectors are the unit
//!   vectors, and decoded by feeding the decoder those same pieces. The decoder only returns data
//!   that ends in the marker, so a constant marker piece is coded alongside the data and never
//!   stored. Neither trick is in the crate's documentation

use std::ops::Range;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rlnc::RLNCError;
use rlnc::full::{Decoder, Encoder, Recoder};

use super::{Code, Layout, StoredShape, lost_data};

/// The seed every coefficient this harness asks for is drawn from
const COEFFICIENT_SEED: u64 = 0x005e_ed0f_c0ef;

/// The boundary marker the crate pads with (`src/full/consts.rs:5`)
const MARKER: u8 = 0x81;

/// Feed a decoder pieces until it has decoded, ignoring pieces that add nothing
///
/// # Arguments
///
/// * `decoder` - The decoder
/// * `piece` - One full coded piece
fn feed(decoder: &mut Decoder, piece: &[u8]) -> Result<(), String> {
    // a piece that adds nothing is the expected result of a dependent set, not an error
    match decoder.decode(piece) {
        Ok(()) | Err(RLNCError::PieceNotUseful) | Err(RLNCError::ReceivedAllPieces) => Ok(()),
        Err(e) => Err(format!("{e:?}")),
    }
}

/// The crate as published: every chunk a random combination of the data
pub struct Rlnc {
    /// The prepared layout
    layout: Option<Layout>,
    /// The size of a unit
    unit: usize,
    /// Where coefficients come from, seeded so a run can be repeated
    rng: ChaCha8Rng,
}

impl Default for Rlnc {
    /// An unprepared adapter with the seeded generator
    fn default() -> Self {
        Rlnc {
            layout: None,
            unit: 0,
            rng: ChaCha8Rng::seed_from_u64(COEFFICIENT_SEED),
        }
    }
}

impl Code for Rlnc {
    /// The name in every table
    fn name(&self) -> &'static str {
        "rlnc"
    }

    /// Start the coefficients again from the seed
    fn reseed(&mut self) {
        self.rng = ChaCha8Rng::seed_from_u64(COEFFICIENT_SEED);
    }

    /// Every chunk is coded
    fn systematic(&self) -> bool {
        false
    }

    /// A rebuild recodes a new piece; it is not the lost one's bytes
    fn rebuild_restores(&self, _target: usize) -> bool {
        false
    }

    /// Anything
    fn supports(&self, _layout: Layout, _unit: usize) -> Result<(), String> {
        Ok(())
    }

    /// k + m pieces, each k coefficients and U + 1 bytes of padded data
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        self.layout = Some(layout);
        self.unit = unit;
        // k·U bytes and the marker, cut into k pieces, is U + 1 bytes a piece
        Ok(StoredShape {
            count: layout.n(),
            len: layout.k + unit + 1,
        })
    }

    /// Concatenate the data, hand it to an encoder, and draw k + m coded pieces
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        let layout = self.layout.ok_or("not prepared")?;
        // the encoder owns its data, so the units are copied into one buffer
        let mut whole = Vec::with_capacity(layout.k * (self.unit + 1));
        for unit in data {
            whole.extend_from_slice(unit);
        }
        let encoder = Encoder::new(whole, layout.k).map_err(|e| format!("{e:?}"))?;
        // every stored chunk is a fresh random combination
        for piece in stored.iter_mut() {
            encoder
                .code_with_buf(&mut self.rng, piece)
                .map_err(|e| format!("{e:?}"))?;
        }
        Ok(())
    }

    /// Feed readable pieces until k are independent, then copy the whole of the data out
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String> {
        let layout = self.layout.ok_or("not prepared")?;
        let mut decoder = Decoder::new(self.unit + 1, layout.k).map_err(|e| format!("{e:?}"))?;
        // every chunk is a stored piece; stop as soon as the decoder has enough
        for (j, piece) in stored.iter().enumerate() {
            if decoder.is_already_decoded() {
                break;
            }
            if !lost.contains(&j) {
                feed(&mut decoder, piece)?;
            }
        }
        // a dependent set is counted, not an error
        if !decoder.is_already_decoded() {
            return Ok(false);
        }
        // the decoder hands back the whole of the data, which is copied into the units
        let whole = decoder.get_decoded_data().map_err(|e| format!("{e:?}"))?;
        for (unit, bytes) in data.iter_mut().zip(whole.chunks_exact(self.unit)) {
            unit.copy_from_slice(bytes);
        }
        Ok(true)
    }

    /// Recode a new piece from k readable ones, with no decode
    fn rebuild(
        &mut self,
        _data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        target: usize,
    ) -> Result<bool, String> {
        let layout = self.layout.ok_or("not prepared")?;
        let piece_len = layout.k + self.unit + 1;
        // the recoder owns its pieces too, so k of them are copied into one buffer
        let mut pieces = Vec::with_capacity(layout.k * piece_len);
        for (j, piece) in stored.iter().enumerate() {
            if j != target && pieces.len() < layout.k * piece_len {
                pieces.extend_from_slice(piece);
            }
        }
        // `Recoder::new` reaches undefined behaviour on a buffer shorter than one piece
        // (`src/full/recoder.rs:83,97`); this one is always k whole pieces
        let mut recoder =
            Recoder::new(pieces, piece_len, layout.k).map_err(|e| format!("{e:?}"))?;
        recoder
            .recode_with_buf(&mut self.rng, stored[target])
            .map_err(|e| format!("{e:?}"))?;
        Ok(true)
    }

    /// None: every piece mixes every data byte
    fn update(
        &mut self,
        _index: usize,
        _old: &[u8],
        _new: &[u8],
        _stored: &mut [&mut [u8]],
        _range: Range<usize>,
    ) -> Result<(), String> {
        Err("no update in the API; every piece mixes every data byte, so a write rewrites k + m chunks".to_string())
    }
}

/// The crate bent into a systematic layout by the harness
pub struct RlncSystematic {
    /// The prepared layout
    layout: Option<Layout>,
    /// The size of a unit
    unit: usize,
    /// Where coefficients come from, seeded so a run can be repeated
    rng: ChaCha8Rng,
    /// The constant last piece, the marker and zeros, coded with the data and never stored
    marker: Vec<u8>,
    /// Space for one piece fed to the decoder
    scratch: Vec<u8>,
}

impl Default for RlncSystematic {
    /// An unprepared adapter with the seeded generator
    fn default() -> Self {
        RlncSystematic {
            layout: None,
            unit: 0,
            rng: ChaCha8Rng::seed_from_u64(COEFFICIENT_SEED),
            marker: Vec::new(),
            scratch: Vec::new(),
        }
    }
}

impl RlncSystematic {
    /// The length of a full piece: k + 1 coefficients and a unit
    fn piece_len(&self, layout: Layout) -> usize {
        layout.k + 1 + self.unit
    }

    /// Write piece `i`'s unit coefficient vector and its bytes into `out`
    ///
    /// # Arguments
    ///
    /// * `out` - A buffer one piece long
    /// * `i` - The piece's position among the k + 1
    /// * `bytes` - Its unit of data
    fn unit_piece(out: &mut [u8], i: usize, k: usize, bytes: &[u8]) {
        // the coefficient vector is e_i over k + 1 positions
        out[..=k].fill(0);
        out[i] = 1;
        out[k + 1..].copy_from_slice(bytes);
    }

    /// A recoder over the k data pieces and the marker piece
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    fn recoder(&self, data: &[&[u8]]) -> Result<Recoder, String> {
        let layout = self.layout.ok_or("not prepared")?;
        let k = layout.k;
        let piece_len = self.piece_len(layout);
        // the recoder owns its pieces, so all k + 1 are written into one buffer
        let mut pieces = vec![0u8; (k + 1) * piece_len];
        for (i, piece) in pieces.chunks_exact_mut(piece_len).enumerate() {
            let bytes = if i < k { data[i] } else { &self.marker[..] };
            Self::unit_piece(piece, i, k, bytes);
        }
        Recoder::new(pieces, piece_len, k + 1).map_err(|e| format!("{e:?}"))
    }
}

impl Code for RlncSystematic {
    /// The name in every table
    fn name(&self) -> &'static str {
        "rlnc, systematic"
    }

    /// Start the coefficients again from the seed
    fn reseed(&mut self) {
        self.rng = ChaCha8Rng::seed_from_u64(COEFFICIENT_SEED);
    }

    /// The data chunks are the data
    fn systematic(&self) -> bool {
        true
    }

    /// A data unit is decoded back; a parity piece is recoded afresh
    fn rebuild_restores(&self, target: usize) -> bool {
        self.layout.is_some_and(|layout| target < layout.k)
    }

    /// Anything
    fn supports(&self, _layout: Layout, _unit: usize) -> Result<(), String> {
        Ok(())
    }

    /// m parity pieces of k + 1 coefficients and a unit
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        self.layout = Some(layout);
        self.unit = unit;
        // the marker piece is the marker and then zeros, which is what the decoder trims
        self.marker = vec![0; unit];
        self.marker[0] = MARKER;
        self.scratch = vec![0; self.piece_len(layout)];
        Ok(StoredShape {
            count: layout.m,
            len: self.piece_len(layout),
        })
    }

    /// Recode m random combinations of the data and the marker piece
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        let mut recoder = self.recoder(data)?;
        for piece in stored.iter_mut() {
            recoder
                .recode_with_buf(&mut self.rng, piece)
                .map_err(|e| format!("{e:?}"))?;
        }
        Ok(())
    }

    /// Feed the marker piece, every readable data unit as a unit piece, and the parity
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String> {
        let layout = self.layout.ok_or("not prepared")?;
        let k = layout.k;
        // a healthy read decodes nothing
        let missing = lost_data(k, lost);
        if missing.is_empty() {
            return Ok(true);
        }
        let mut decoder = Decoder::new(self.unit, k + 1).map_err(|e| format!("{e:?}"))?;
        // the marker piece is always known
        Self::unit_piece(&mut self.scratch, k, k, &self.marker);
        feed(&mut decoder, &self.scratch)?;
        // each readable data unit goes in with its unit vector, which costs a copy
        for (i, unit) in data.iter().enumerate() {
            if !lost.contains(&i) {
                Self::unit_piece(&mut self.scratch, i, k, unit);
                feed(&mut decoder, &self.scratch)?;
            }
        }
        // then the parity, until the decoder has enough
        for (j, piece) in stored.iter().enumerate() {
            if decoder.is_already_decoded() {
                break;
            }
            if !lost.contains(&(k + j)) {
                feed(&mut decoder, piece)?;
            }
        }
        // a dependent set is counted, not an error
        if !decoder.is_already_decoded() {
            return Ok(false);
        }
        // the whole of the data comes back; copy the lost units out of it
        let whole = decoder.get_decoded_data().map_err(|e| format!("{e:?}"))?;
        for i in missing {
            data[i].copy_from_slice(&whole[i * self.unit..(i + 1) * self.unit]);
        }
        Ok(true)
    }

    /// A data unit is a decode; a parity piece is one new recoded piece
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
        let units: Vec<&[u8]> = data.iter().map(|unit| &**unit).collect();
        let mut recoder = self.recoder(&units)?;
        recoder
            .recode_with_buf(&mut self.rng, stored[target - layout.k])
            .map_err(|e| format!("{e:?}"))?;
        Ok(true)
    }

    /// None in the API: the field arithmetic a change would be folded with is private
    fn update(
        &mut self,
        _index: usize,
        _old: &[u8],
        _new: &[u8],
        _stored: &mut [&mut [u8]],
        _range: Range<usize>,
    ) -> Result<(), String> {
        Err("no update in the API; its GF(2^8) kernels are in a private module".to_string())
    }
}
