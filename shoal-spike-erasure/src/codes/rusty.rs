//! `rusty_erasure` 0.4.1: a Rust port of ISA-L's erasure code, with ISA-L's Cauchy matrix and an
//! update call

use std::ops::Range;

use rusty_erasure::{Coder, DecodePlan, Matrix};

use super::{Code, Layout, StoredShape, lost_data, split_row, xor_into};

/// The decode plan last used, kept for as long as the same chunks are lost
struct CachedPlan {
    /// The chunks lost
    lost: Vec<usize>,
    /// The chunks rebuilt
    rebuild: Vec<usize>,
    /// The plan for them
    plan: DecodePlan,
}

/// The kernel sets `rusty_erasure` can be forced to, by the name `kernels_named` takes, with the
/// name each is listed under
pub const KERNEL_SETS: &[(&str, &str)] = &[
    ("scalar", "rusty_erasure [scalar]"),
    ("ssse3", "rusty_erasure [ssse3]"),
    ("avx2", "rusty_erasure [avx2]"),
    ("gfni", "rusty_erasure [gfni]"),
];

/// `rusty_erasure` over a Cauchy matrix
#[derive(Default)]
pub struct Rusty {
    /// The kernel set it is forced to, by name, and the name it is listed under; none is the best
    /// set for the cpu, which is what a node would use
    forced: Option<(&'static str, &'static str)>,
    /// The coder for the prepared layout
    coder: Option<Coder>,
    /// The last decode plan
    plan: Option<CachedPlan>,
    /// Space for an update's change
    delta: Vec<u8>,
}

impl Rusty {
    /// `rusty_erasure` forced to one kernel set, if this cpu has it
    ///
    /// # Arguments
    ///
    /// * `set` - A kernel set and the name it is listed under, from [`KERNEL_SETS`]
    pub fn forced(set: (&'static str, &'static str)) -> Option<Self> {
        // a set the cpu lacks is not offered at all
        rusty_erasure::kernels_named(set.0)?;
        Some(Rusty {
            forced: Some(set),
            ..Rusty::default()
        })
    }

    /// The coder's kernel set, for the facts table
    pub fn kernels(&self) -> Option<&'static str> {
        self.coder.as_ref().map(|coder| coder.kernels().name)
    }

    /// Make sure the cached plan rebuilds `rebuild` with `lost` unreadable
    ///
    /// # Arguments
    ///
    /// * `lost` - The chunks that cannot be read
    /// * `rebuild` - The chunks to rebuild
    fn plan_for(&mut self, lost: &[usize], rebuild: &[usize]) -> Result<(), String> {
        // reuse the plan while the pattern is the same, as a degraded read of many stripes would
        if let Some(cached) = &self.plan
            && cached.lost == lost
            && cached.rebuild == rebuild
        {
            return Ok(());
        }
        // otherwise derive one: invert the survivors' rows once
        let coder = self.coder.as_ref().ok_or("not prepared")?;
        let n = coder.k() + coder.p();
        let present: Vec<bool> = (0..n).map(|i| !lost.contains(&i)).collect();
        let plan = coder
            .decode_plan(&present, rebuild)
            .map_err(|e| format!("{e:?}"))?;
        self.plan = Some(CachedPlan {
            lost: lost.to_vec(),
            rebuild: rebuild.to_vec(),
            plan,
        });
        Ok(())
    }

    /// Rebuild `rebuild` into `outputs` from the survivors with `lost` unreadable
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
        // the plan is derived once for a pattern
        self.plan_for(lost, rebuild)?;
        let coder = self.coder.as_ref().ok_or("not prepared")?;
        let n = coder.k() + coder.p();
        // the shards in chunk order, with nothing where a chunk is lost
        let mut shards: Vec<Option<&[u8]>> = vec![None; n];
        for &(i, bytes) in survivors {
            shards[i] = Some(bytes);
        }
        let plan = &self.plan.as_ref().expect("planned above").plan;
        coder
            .recover_with(plan, &shards, outputs)
            .map_err(|e| format!("{e:?}"))?;
        Ok(true)
    }
}

impl Code for Rusty {
    /// The name in every table
    fn name(&self) -> &'static str {
        self.forced.map_or("rusty_erasure", |(_, name)| name)
    }

    /// Reed-Solomon over a systematic matrix
    fn systematic(&self) -> bool {
        true
    }

    /// Any k + m up to the field's 255
    fn supports(&self, layout: Layout, _unit: usize) -> Result<(), String> {
        if layout.n() <= 255 {
            Ok(())
        } else {
            Err("k + m above 255".to_string())
        }
    }

    /// Build ISA-L's Cauchy matrix and the best kernel set for this cpu
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String> {
        // the Cauchy construction inverts at every k and m, unlike the Vandermonde one
        let matrix = Matrix::cauchy(layout.k, layout.m).map_err(|e| format!("{e:?}"))?;
        // the best set for this cpu, unless one is forced
        let kernels = match self.forced {
            Some((set, _)) => {
                rusty_erasure::kernels_named(set).ok_or("this cpu lacks the kernel set")?
            }
            None => rusty_erasure::best_kernels(),
        };
        self.coder = Some(Coder::with_kernels(matrix, kernels).map_err(|e| format!("{e:?}"))?);
        self.plan = None;
        self.delta = vec![0; unit];
        Ok(StoredShape {
            count: layout.m,
            len: unit,
        })
    }

    /// Encode straight into the caller's buffers
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String> {
        let coder = self.coder.as_ref().ok_or("not prepared")?;
        coder.encode(data, stored).map_err(|e| format!("{e:?}"))
    }

    /// Rebuild only the lost data units, with a plan kept for the pattern
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

    /// The change folded into every parity chunk in one pass
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
        // each parity chunk's range is folded into
        let mut parity: Vec<&mut [u8]> = stored
            .iter_mut()
            .map(|chunk| &mut chunk[range.clone()])
            .collect();
        let coder = self.coder.as_ref().ok_or("not prepared")?;
        coder
            .update(index, delta, &mut parity)
            .map_err(|e| format!("{e:?}"))
    }
}

/// Parity bytes from `reed-solomon-erasure`'s own matrix, through `rusty_erasure`, for the check
/// that the two agree byte for byte
///
/// # Arguments
///
/// * `layout` - The layout
/// * `data` - The k data units
/// * `parity` - Where the parity goes
pub fn encode_with_rse_matrix(
    layout: Layout,
    data: &[&[u8]],
    parity: &mut [&mut [u8]],
) -> Result<(), String> {
    // the compat constructor rebuilds that crate's V · V_top⁻¹
    let matrix = rusty_erasure::compat::reed_solomon_erasure_matrix(layout.k, layout.m)
        .map_err(|e| format!("{e:?}"))?;
    let coder = rusty_erasure::coder(matrix).map_err(|e| format!("{e:?}"))?;
    coder.encode(data, parity).map_err(|e| format!("{e:?}"))
}
