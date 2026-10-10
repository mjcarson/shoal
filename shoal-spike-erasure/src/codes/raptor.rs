//! `raptorq` 2.0.1: RFC 6330 RaptorQ, a fountain code that is systematic. A symbol is at most
//! 65535 bytes, so a unit above [`SYMBOL_MAX`] is cut into columns of that size, each column of a
//! unit row a source block of its own: all of a row's columns lose the same chunks, so this is the
//! layout a node would have to use, and every table says it was used

use std::ops::Range;

use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, PayloadId, SourceBlockDecoder,
    SourceBlockEncoder, SourceBlockEncodingPlan,
};

use super::{Code, Layout, StoredShape, lost_data};

/// The largest symbol used: the largest power of two a `u16` holds
pub const SYMBOL_MAX: usize = 32 * 1024;

/// The alignment a symbol is declared with
const ALIGNMENT: u8 = 8;

/// RaptorQ, with an encoding plan reused for every block of k symbols
#[derive(Default)]
pub struct Raptor {
    /// The prepared layout
    layout: Option<Layout>,
    /// The symbol size
    symbol: usize,
    /// The columns of a unit
    columns: usize,
    /// The transmission information for one column of k symbols
    oti: Option<ObjectTransmissionInformation>,
    /// The encoding plan for k source symbols
    plan: Option<SourceBlockEncodingPlan>,
    /// The encoding symbol id each repair symbol carries
    repair_ids: Vec<u32>,
    /// One column of the k data units, contiguous, which is how the encoder takes it
    column: Vec<u8>,
}

impl Raptor {
    /// Fill the contiguous column buffer from column `c` of every data unit
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    /// * `c` - The column
    fn gather(&mut self, data: &[&[u8]], c: usize) {
        let span = c * self.symbol..(c + 1) * self.symbol;
        for (i, unit) in data.iter().enumerate() {
            self.column[i * self.symbol..(i + 1) * self.symbol]
                .copy_from_slice(&unit[span.clone()]);
        }
    }

    /// Encode one column and write its repair symbols into the stored chunks
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    /// * `stored` - The stored chunks
    /// * `c` - The column
    /// * `only` - One repair symbol to produce, or every one
    fn encode_column(
        &mut self,
        data: &[&[u8]],
        stored: &mut [&mut [u8]],
        c: usize,
        only: Option<usize>,
    ) -> Result<(), String> {
        // the encoder takes the column contiguously
        self.gather(data, c);
        let oti = self.oti.as_ref().ok_or("not prepared")?;
        let plan = self.plan.as_ref().ok_or("not prepared")?;
        let encoder = SourceBlockEncoder::with_encoding_plan(0, oti, &self.column, plan);
        let span = c * self.symbol..(c + 1) * self.symbol;
        // repair symbols are produced from the first, or the one asked for
        let (start, count) = match only {
            Some(j) => (j as u32, 1),
            None => (0, stored.len() as u32),
        };
        for (offset, packet) in encoder.repair_packets(start, count).iter().enumerate() {
            let j = start as usize + offset;
            stored[j][span.clone()].copy_from_slice(packet.data());
        }
        Ok(())
    }
}

impl Code for Raptor {
    /// The name in every table
    fn name(&self) -> &'static str {
        "raptorq"
    }

    /// Source symbols are the data as written
    fn systematic(&self) -> bool {
        true
    }

    /// A unit is a whole number of symbols
    fn supports(&self, layout: Layout, unit: usize) -> Result<(), String> {
        let symbol = unit.min(SYMBOL_MAX);
        if !unit.is_multiple_of(symbol) || !symbol.is_multiple_of(ALIGNMENT as usize) {
            return Err(format!(
                "a unit of {unit} is not a whole number of {symbol} byte symbols"
            ));
        }
        if layout.k > 56_403 {
            return Err("more source symbols than a block holds".to_string());
        }
        Ok(())
    }

    /// Build the plan for k source symbols and learn the repair symbols' ids
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        self.supports(layout, unit)?;
        self.symbol = unit.min(SYMBOL_MAX);
        self.columns = unit / self.symbol;
        let oti = ObjectTransmissionInformation::new(
            (layout.k * self.symbol) as u64,
            self.symbol as u16,
            1,
            1,
            ALIGNMENT,
        );
        let plan = SourceBlockEncodingPlan::generate(layout.k as u16);
        // the ids are a function of k; read them from one encode rather than restating the RFC
        self.column = vec![0; layout.k * self.symbol];
        let encoder = SourceBlockEncoder::with_encoding_plan(0, &oti, &self.column, &plan);
        self.repair_ids = encoder
            .repair_packets(0, layout.m as u32)
            .iter()
            .map(|packet| packet.payload_id().encoding_symbol_id())
            .collect();
        self.oti = Some(oti);
        self.plan = Some(plan);
        self.layout = Some(layout);
        Ok(StoredShape {
            count: layout.m,
            len: unit,
        })
    }

    /// Encode each column with the plan
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        for c in 0..self.columns {
            self.encode_column(data, stored, c, None)?;
        }
        Ok(())
    }

    /// Decode each column from every readable symbol; the decoder returns the whole column
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
        let oti = self.oti.as_ref().ok_or("not prepared")?;
        for c in 0..self.columns {
            let span = c * self.symbol..(c + 1) * self.symbol;
            // every readable symbol of this column, as the packet the decoder takes
            let mut packets = Vec::with_capacity(layout.n());
            for (i, unit) in data.iter().enumerate() {
                if !lost.contains(&i) {
                    packets.push(EncodingPacket::new(
                        PayloadId::new(0, i as u32),
                        unit[span.clone()].to_vec(),
                    ));
                }
            }
            for (j, chunk) in stored.iter().enumerate() {
                if !lost.contains(&(layout.k + j)) {
                    packets.push(EncodingPacket::new(
                        PayloadId::new(0, self.repair_ids[j]),
                        chunk[span.clone()].to_vec(),
                    ));
                }
            }
            // a singular system is a set this code cannot decode from
            let mut decoder = SourceBlockDecoder::new(0, oti, (layout.k * self.symbol) as u64);
            let Some(block) = decoder.decode(packets) else {
                return Ok(false);
            };
            // copy the lost units' columns out of the whole
            for &i in &missing {
                data[i][span.clone()]
                    .copy_from_slice(&block[i * self.symbol..(i + 1) * self.symbol]);
            }
        }
        Ok(true)
    }

    /// A data unit is a decode; a repair symbol is an encode of that symbol alone
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
        // every data unit is readable, so encode just the one repair symbol of each column
        let units: Vec<&[u8]> = data.iter().map(|unit| &**unit).collect();
        for c in 0..self.columns {
            self.encode_column(&units, stored, c, Some(target - layout.k))?;
        }
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
            "no update in the API; a repair symbol depends on the block's intermediate symbols"
                .to_string(),
        )
    }
}
