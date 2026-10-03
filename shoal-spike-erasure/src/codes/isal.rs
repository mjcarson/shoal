//! `isa-l` 0.2.0: bindings to Intel's ISA-L 2.29.0, which the sys crate builds from the source it
//! bundles. The crate binds no update call; the C library exports one, so the harness declares it
//! itself and every table says the update is the library's and not the crate's

use std::ops::Range;
use std::os::raw::{c_int, c_uchar};

use super::{Code, Layout, StoredShape, lost_data, split_row, xor_into};

unsafe extern "C" {
    /// ISA-L's incremental encode: `coding[r] ^= g[r][vec_i] · data` for every parity row
    /// (`isa-l/include/erasure_code.h:131` in libisal-sys 0.1.2)
    fn ec_encode_data_update(
        len: c_int,
        k: c_int,
        rows: c_int,
        vec_i: c_int,
        g_tbls: *mut c_uchar,
        data: *mut c_uchar,
        coding: *mut *mut c_uchar,
    );
}

/// The decode tables last built, kept for as long as the same chunks are lost
struct CachedTables {
    /// The chunks lost
    lost: Vec<usize>,
    /// The chunks rebuilt
    rebuild: Vec<usize>,
    /// The chunks read, k of them
    sources: Vec<usize>,
    /// The expanded tables
    tables: Vec<u8>,
}

/// ISA-L over its Cauchy matrix
#[derive(Default)]
pub struct Isal {
    /// The prepared layout
    layout: Option<Layout>,
    /// The encode matrix, (k + m) rows of k
    matrix: Vec<u8>,
    /// The expanded encode tables for the parity rows
    tables: Vec<u8>,
    /// The last decode tables
    decode: Option<CachedTables>,
    /// Space for an update's change
    delta: Vec<u8>,
}

impl Isal {
    /// Build the tables that rebuild `rebuild` with `lost` unreadable, unless they are already built
    ///
    /// # Arguments
    ///
    /// * `lost` - The chunks that cannot be read
    /// * `rebuild` - The chunks to rebuild
    fn tables_for(&mut self, lost: &[usize], rebuild: &[usize]) -> Result<(), String> {
        // reuse the tables while the pattern is the same
        if let Some(cached) = &self.decode
            && cached.lost == lost
            && cached.rebuild == rebuild
        {
            return Ok(());
        }
        let layout = self.layout.ok_or("not prepared")?;
        let k = layout.k;
        // the first k readable chunks are the sources
        let sources: Vec<usize> = (0..layout.n())
            .filter(|i| !lost.contains(i))
            .take(k)
            .collect();
        if sources.len() < k {
            return Err("fewer than k chunks readable".to_string());
        }
        // invert the sources' rows of the encode matrix
        let rows: Vec<u8> = sources
            .iter()
            .flat_map(|&i| self.matrix[i * k..(i + 1) * k].iter().copied())
            .collect();
        let inverse = isa_l::gf_invert_matrix(rows).ok_or("singular decode matrix")?;
        // a lost data unit is its row of the inverse; a lost parity chunk is its encode row times it
        let mut coefficients = Vec::with_capacity(rebuild.len() * k);
        for &target in rebuild {
            if target < k {
                coefficients.extend_from_slice(&inverse[target * k..(target + 1) * k]);
            } else {
                for column in 0..k {
                    let mut sum = 0u8;
                    for j in 0..k {
                        sum ^= isa_l::gf_mul(self.matrix[target * k + j], inverse[j * k + column]);
                    }
                    coefficients.push(sum);
                }
            }
        }
        let tables = isa_l::ec_init_tables_owned(k, rebuild.len(), coefficients);
        self.decode = Some(CachedTables {
            lost: lost.to_vec(),
            rebuild: rebuild.to_vec(),
            sources,
            tables,
        });
        Ok(())
    }

    /// Rebuild `rebuild` into `outputs` from the survivors
    ///
    /// # Arguments
    ///
    /// * `survivors` - Every readable chunk, by number
    /// * `outputs` - One buffer for each chunk rebuilt
    /// * `lost` - The chunks that cannot be read
    /// * `rebuild` - The chunks to rebuild
    fn recover(
        &mut self,
        survivors: &[(usize, &[u8])],
        outputs: &mut [&mut [u8]],
        lost: &[usize],
        rebuild: &[usize],
    ) -> Result<bool, String> {
        // the tables are built once for a pattern
        self.tables_for(lost, rebuild)?;
        let cached = self.decode.as_ref().expect("built above");
        // read exactly the chunks the tables were built from, in their order
        let sources: Vec<&[u8]> = cached
            .sources
            .iter()
            .map(|i| {
                survivors
                    .iter()
                    .find(|(n, _)| n == i)
                    .expect("a source survives")
                    .1
            })
            .collect();
        let len = outputs[0].len();
        let k = sources.len();
        isa_l::ec_encode_data(len, k, rebuild.len(), &cached.tables, &sources, outputs);
        Ok(true)
    }
}

impl Code for Isal {
    /// The name in every table
    fn name(&self) -> &'static str {
        "isa-l"
    }

    /// Reed-Solomon over a systematic matrix
    fn systematic(&self) -> bool {
        true
    }

    /// Any k + m up to the field's 255; a length is a C int
    fn supports(&self, layout: Layout, unit: usize) -> Result<(), String> {
        if layout.n() > 255 {
            return Err("k + m above 255".to_string());
        }
        if unit > i32::MAX as usize {
            return Err("a unit longer than a C int".to_string());
        }
        Ok(())
    }

    /// Build the Cauchy matrix and expand its parity rows
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        // gf_gen_cauchy1_matrix takes the total rows, data rows included
        self.matrix = isa_l::gf_gen_cauchy1_matrix(layout.k, layout.n());
        self.tables =
            isa_l::ec_init_tables_owned(layout.k, layout.m, &self.matrix[layout.k * layout.k..]);
        self.layout = Some(layout);
        self.decode = None;
        self.delta = vec![0; unit];
        Ok(StoredShape {
            count: layout.m,
            len: unit,
        })
    }

    /// Encode into the caller's buffers
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        let layout = self.layout.ok_or("not prepared")?;
        let len = data[0].len();
        isa_l::ec_encode_data(len, layout.k, layout.m, &self.tables, data, stored);
        Ok(())
    }

    /// Rebuild only the lost data units, with tables kept for the pattern
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String> {
        // a healthy read decodes nothing
        let missing = lost_data(data.len(), lost);
        if missing.is_empty() {
            return Ok(true);
        }
        // read the survivors and write the lost units
        let (survivors, mut outputs) = split_row(data, stored, lost, true);
        self.recover(&survivors, &mut outputs, lost, &missing)
    }

    /// Rebuild exactly one chunk, data or parity, from k others
    fn rebuild(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        target: usize,
    ) -> Result<bool, String> {
        let k = data.len();
        if target < k {
            // a data unit is a decode of one lost unit
            return self.decode(data, stored, &[target]);
        }
        // a parity chunk: every data unit and the other parity chunks are readable
        let (before, rest) = stored.split_at_mut(target - k);
        let (out, after) = rest.split_first_mut().expect("target is a stored chunk");
        let mut survivors: Vec<(usize, &[u8])> = data
            .iter()
            .enumerate()
            .map(|(i, unit)| (i, &**unit))
            .collect();
        survivors.extend(
            before
                .iter()
                .enumerate()
                .map(|(j, chunk)| (k + j, &**chunk)),
        );
        survivors.extend(
            after
                .iter()
                .enumerate()
                .map(|(j, chunk)| (target + 1 + j, &**chunk)),
        );
        self.recover(&survivors, &mut [&mut **out], &[target], &[target])
    }

    /// The change folded into every parity chunk by the C library's own update
    fn update(
        &mut self,
        index: usize,
        old: &[u8],
        new: &[u8],
        stored: &mut [&mut [u8]],
        range: Range<usize>,
    ) -> Result<(), String> {
        let layout = self.layout.ok_or("not prepared")?;
        // the change is old xor new
        let len = old.len();
        xor_into(&mut self.delta[..len], old, new);
        // a pointer to each parity chunk's range
        let mut coding: Vec<*mut u8> = stored
            .iter_mut()
            .map(|chunk| chunk[range.clone()].as_mut_ptr())
            .collect();
        // SAFETY: the tables are 32·k·m bytes from ec_init_tables, the change is `len` bytes, and
        // every coding pointer has `len` writable bytes behind it that nothing else borrows
        unsafe {
            ec_encode_data_update(
                len as c_int,
                layout.k as c_int,
                layout.m as c_int,
                index as c_int,
                self.tables.as_mut_ptr(),
                self.delta.as_mut_ptr(),
                coding.as_mut_ptr(),
            );
        }
        Ok(())
    }
}
