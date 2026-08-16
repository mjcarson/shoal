//! How wide a row is, when a workload's rows are not all the same width
//!
//! Every workload before [F17](../../../../docs/src/features/workload-grid.md) built rows of one
//! fixed width, because a width that varied would have made every percentile a mixture of two
//! things and neither recoverable. That is still the right shape for a workload isolating a path.
//!
//! It is the wrong shape for the question the grid asks. Real callers do not store one width, and
//! a store whose cost is flat from 64 bytes to 8 KiB behaves differently under a mixture than one
//! whose cost is flat at each width but has a cliff between them. So a mixture is a **declared,
//! named distribution** here rather than a range: `mixed_small` is always the same four widths in
//! the same proportion, drawn the same way from the same seed, so two captures of it measure the
//! same thing.
//!
//! # Width is a function of the row index, not of draw order
//!
//! [`RowProfile::width`] takes the row's index and derives the width from it directly, rather than
//! pulling from a [`Seeded`](super::seed::Seeded) stream in sequence. A workload seeds its rows in
//! one phase and reads them back in another, often in a different order and sometimes in a
//! different process, and a sequence-drawn width would only agree between the two by accident.
//! Deriving from the index means row 12,345 is the same width everywhere, always.

use crate::workloads::harness::seed::Seeded;

/// The widths `mixed_small` draws from - the sub-kilobyte mixture
///
/// Four powers of two under a kilobyte, uniformly chosen, so the mean is 240 bytes and no single
/// width dominates. This is the shape of a store holding small records of a few different kinds.
pub const MIXED_SMALL: [u64; 4] = [64, 128, 256, 512];

/// The widths `mixed_mid` draws from - the kilobyte to eight kilobyte mixture
pub const MIXED_MID: [u64; 4] = [1024, 2048, 4096, 8192];

/// The widths `mixed_large` draws from - the half megabyte to megabyte mixture
///
/// Three points rather than four, because the range spans one doubling rather than three and a
/// fourth would sit on top of its neighbour.
pub const MIXED_LARGE: [u64; 3] = [512 * 1024, 768 * 1024, 1024 * 1024];

/// How wide the rows a workload builds are
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowProfile {
    /// Every row is exactly this many bytes wide
    Fixed(u64),
    /// Each row's width is drawn from a named set, uniformly, by its index
    Mixed {
        /// What this distribution is called, in identifiers and in the artifact
        name: &'static str,
        /// The widths it draws from
        widths: &'static [u64],
    },
}

impl RowProfile {
    /// The `mixed_small` distribution
    pub const fn small() -> Self {
        RowProfile::Mixed {
            name: "mixed_small",
            widths: &MIXED_SMALL,
        }
    }

    /// The `mixed_mid` distribution
    pub const fn mid() -> Self {
        RowProfile::Mixed {
            name: "mixed_mid",
            widths: &MIXED_MID,
        }
    }

    /// The `mixed_large` distribution
    pub const fn large() -> Self {
        RowProfile::Mixed {
            name: "mixed_large",
            widths: &MIXED_LARGE,
        }
    }

    /// The segment this profile contributes to a workload identifier
    ///
    /// A fixed width names its width, so `macro/grid/unsorted/r50/1024` reads as what it is. A
    /// mixture names itself, since the set of widths cannot fit in a path segment.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_bench::workloads::harness::rows::RowProfile;
    ///
    /// assert_eq!(RowProfile::Fixed(1024).segment(), "1024");
    /// assert_eq!(RowProfile::small().segment(), "mixed_small");
    /// ```
    pub fn segment(&self) -> String {
        match self {
            RowProfile::Fixed(width) => width.to_string(),
            RowProfile::Mixed { name, .. } => (*name).to_string(),
        }
    }

    /// What this profile is called in the artifact, when it is a mixture
    ///
    /// `None` for a fixed width, which is what leaves `row_profile` off the artifact entirely and
    /// keeps every existing workload's bytes unchanged.
    pub fn artifact_name(&self) -> Option<String> {
        match self {
            RowProfile::Fixed(_) => None,
            RowProfile::Mixed { name, .. } => Some((*name).to_string()),
        }
    }

    /// The mean width of a row under this profile
    ///
    /// This is what goes in `ScaleFacts::row_bytes`, and what every byte budget and every
    /// bytes-per-second figure is computed from. For a mixture it is a mean and not a measurement:
    /// the artifact says so by carrying [`RowProfile::artifact_name`] beside it.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_bench::workloads::harness::rows::RowProfile;
    ///
    /// assert_eq!(RowProfile::Fixed(512).mean(), 512);
    /// // (64 + 128 + 256 + 512) / 4
    /// assert_eq!(RowProfile::small().mean(), 240);
    /// ```
    pub fn mean(&self) -> u64 {
        match self {
            RowProfile::Fixed(width) => *width,
            RowProfile::Mixed { widths, .. } => {
                // uniform over the set, so the mean is the plain average of it
                let total: u64 = widths.iter().sum();
                total / widths.len() as u64
            }
        }
    }

    /// The widest row this profile can produce
    ///
    /// What a frame budget and an in-flight gate have to be sized against: a mixture that averages
    /// 240 bytes still has to survive its 512 byte rows, and a gate sized on the mean would let a
    /// run of wide rows past it.
    pub fn widest(&self) -> u64 {
        match self {
            RowProfile::Fixed(width) => *width,
            RowProfile::Mixed { widths, .. } => widths.iter().copied().max().unwrap_or(0),
        }
    }

    /// Every width this profile can produce, in ascending order
    ///
    /// What a caller building payloads up front needs: a payload of the wrong width would make the
    /// row a different size than the arm reports, so one set has to exist per width the profile can
    /// draw.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_bench::workloads::harness::rows::RowProfile;
    ///
    /// assert_eq!(RowProfile::Fixed(1024).widths(), vec![1024]);
    /// assert_eq!(RowProfile::small().widths(), vec![64, 128, 256, 512]);
    /// ```
    pub fn widths(&self) -> Vec<u64> {
        match self {
            RowProfile::Fixed(width) => vec![*width],
            RowProfile::Mixed { widths, .. } => {
                // sorted, because the caller keys a map on these and a deterministic order is what
                // keeps two runs of one arm building the same payloads
                let mut declared = widths.to_vec();
                declared.sort_unstable();
                declared
            }
        }
    }

    /// How wide the row at an index is
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed the whole run derives from
    /// * `index` - Which row's width is wanted
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_bench::workloads::harness::rows::RowProfile;
    ///
    /// // the same index gives the same width however many times it is asked
    /// let profile = RowProfile::small();
    /// assert_eq!(profile.width(42, 7), profile.width(42, 7));
    /// // and a fixed profile ignores both
    /// assert_eq!(RowProfile::Fixed(256).width(42, 7), 256);
    /// ```
    pub fn width(&self, seed: u64, index: u64) -> u64 {
        match self {
            RowProfile::Fixed(width) => *width,
            RowProfile::Mixed { name, widths } => {
                // one generator per row, addressed by the index, so the answer depends on the
                // index and on nothing else that happened during the run
                let mut seeded = Seeded::at(Seeded::stream(seed, name).next_u64(), index);
                widths[seeded.below(widths.len() as u64) as usize]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MIXED_LARGE, MIXED_MID, MIXED_SMALL, RowProfile};

    /// A row's width depends on its index and not on when it was asked for
    ///
    /// The property the seed and measure phases both rely on. Without it a workload would write a
    /// 512 byte row and read back expecting a 64 byte one.
    #[test]
    fn a_width_is_addressed_by_index() {
        let profile = RowProfile::mid();
        // asked in order, then backwards, then scattered - the same answers every time
        let forward: Vec<u64> = (0..64).map(|index| profile.width(42, index)).collect();
        let backward: Vec<u64> = (0..64)
            .rev()
            .map(|index| profile.width(42, index))
            .collect();
        assert_eq!(forward, backward.into_iter().rev().collect::<Vec<u64>>());
    }

    /// Every width a mixture produces is one it declared
    #[test]
    fn a_mixture_only_produces_declared_widths() {
        for (profile, declared) in [
            (RowProfile::small(), &MIXED_SMALL[..]),
            (RowProfile::mid(), &MIXED_MID[..]),
            (RowProfile::large(), &MIXED_LARGE[..]),
        ] {
            for index in 0..4_096 {
                let width = profile.width(7, index);
                assert!(declared.contains(&width), "{width} is not in {declared:?}");
            }
        }
    }

    /// A mixture covers its whole set rather than collapsing onto one of them
    ///
    /// A `Seeded` stream keyed on the index could in principle correlate with the index; this is
    /// what would notice.
    #[test]
    fn a_mixture_covers_its_whole_set() {
        let profile = RowProfile::small();
        let mut seen = [0usize; MIXED_SMALL.len()];
        for index in 0..8_192 {
            let width = profile.width(7, index);
            let slot = MIXED_SMALL.iter().position(|w| *w == width).expect("declared");
            seen[slot] += 1;
        }
        // 8,192 rows over four widths averages 2,048, so nothing should be far off it
        assert!(seen.iter().all(|count| *count > 1_700), "{seen:?}");
    }

    /// The seed reaches the widths, so two seeds do not build identical rows
    #[test]
    fn two_seeds_lay_the_widths_out_differently() {
        let profile = RowProfile::large();
        let left: Vec<u64> = (0..256).map(|index| profile.width(1, index)).collect();
        let right: Vec<u64> = (0..256).map(|index| profile.width(2, index)).collect();
        assert_ne!(left, right);
    }

    /// A mixture's mean is the mean of what it actually produces
    ///
    /// `ScaleFacts::row_bytes` carries this number and every byte budget is computed from it, so a
    /// mean that did not describe the draw would silently mis-size every wide arm of the grid.
    #[test]
    fn the_declared_mean_is_the_observed_mean() {
        for profile in [RowProfile::small(), RowProfile::mid(), RowProfile::large()] {
            let rows = 16_384u64;
            let total: u64 = (0..rows).map(|index| profile.width(11, index)).sum();
            let observed = total / rows;
            let declared = profile.mean();
            // within two percent of the declared mean over sixteen thousand rows
            let drift = observed.abs_diff(declared) * 100 / declared;
            assert!(drift <= 2, "{observed} against a declared {declared}");
        }
    }

    /// A fixed profile is not a mixture, and says so everywhere it is asked
    #[test]
    fn a_fixed_profile_names_no_distribution() {
        let profile = RowProfile::Fixed(8192);
        assert_eq!(profile.artifact_name(), None);
        assert_eq!(profile.mean(), 8192);
        assert_eq!(profile.widest(), 8192);
        assert_eq!(profile.segment(), "8192");
    }
}
