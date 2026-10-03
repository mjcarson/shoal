//! Correctness before speed: every loss pattern, the chance a random code fails, updates against a
//! fresh encode, digests that say whether output is stable, and which candidates agree byte for
//! byte

use crate::buffers::{SplitMix64, Stripe, fnv1a};
use crate::codes::{self, Code, Layout, rusty};
use crate::record::{
    CheckRecord, CompatResult, DigestResult, PatternResult, RandomResult, UpdateResult,
};

/// The unit every check runs at
const UNIT: usize = 4096;

/// The unit the random-failure trials run at: the coefficients decide a failure, not the bytes
const TRIAL_UNIT: usize = 64;

/// A byte a lost chunk is filled with, so that a code reading it gives wrong bytes
const SCRIBBLE: u8 = 0xee;

/// The layouts every loss pattern is tried at
///
/// # Arguments
///
/// * `quick` - Whether this is the quick pass
fn pattern_layouts(quick: bool) -> Vec<Layout> {
    if quick {
        return vec![Layout::new(2, 1), Layout::new(4, 2)];
    }
    // every k from 2 to 6 at every m from 1 to 3, then the two wide layouts X4 names
    let mut out = Vec::new();
    for k in 2..=6 {
        for m in 1..=3 {
            out.push(Layout::new(k, m));
        }
    }
    out.push(Layout::new(8, 3));
    out.push(Layout::new(10, 4));
    out
}

/// The layouts X4 measures speed at, which the digests and compat checks also use
pub fn x4_layouts() -> Vec<Layout> {
    vec![
        Layout::new(2, 1),
        Layout::new(4, 2),
        Layout::new(6, 3),
        Layout::new(8, 3),
        Layout::new(10, 4),
    ]
}

/// Every set of between 1 and m chunks out of n, smallest first
///
/// # Arguments
///
/// * `layout` - The layout
fn loss_patterns(layout: Layout) -> Vec<Vec<usize>> {
    let n = layout.n();
    // each mask with between one and m bits set is a pattern
    let mut out: Vec<Vec<usize>> = (1u32..(1 << n))
        .filter(|mask| (1..=layout.m as u32).contains(&mask.count_ones()))
        .map(|mask| (0..n).filter(|i| mask & (1 << i) != 0).collect())
        .collect();
    out.sort_by_key(Vec::len);
    out
}

/// Fill a lost chunk's buffer with the scribble byte
///
/// # Arguments
///
/// * `stripe` - The stripe
/// * `chunk` - The chunk number
/// * `systematic` - Whether data units are chunks
fn scribble(stripe: &mut Stripe, chunk: usize, systematic: bool) {
    let k = stripe.data.len();
    if systematic && chunk < k {
        stripe.data[chunk].fill(SCRIBBLE);
    } else if systematic {
        stripe.stored[chunk - k].fill(SCRIBBLE);
    } else {
        stripe.stored[chunk].fill(SCRIBBLE);
    }
}

/// Put a stripe back to a snapshot
///
/// # Arguments
///
/// * `stripe` - The stripe
/// * `pristine` - The snapshot
fn restore(stripe: &mut Stripe, pristine: &(Vec<Vec<u8>>, Vec<Vec<u8>>)) {
    for (buf, bytes) in stripe.data.iter_mut().zip(&pristine.0) {
        buf.copy_from_slice(bytes);
    }
    for (buf, bytes) in stripe.stored.iter_mut().zip(&pristine.1) {
        buf.copy_from_slice(bytes);
    }
}

/// Whether every data unit a decode should have written holds the original bytes
///
/// # Arguments
///
/// * `stripe` - The stripe after the decode
/// * `pristine` - The snapshot before it
/// * `lost` - The lost chunks
/// * `systematic` - Whether data units are chunks
fn data_restored(
    stripe: &Stripe,
    pristine: &(Vec<Vec<u8>>, Vec<Vec<u8>>),
    lost: &[usize],
    systematic: bool,
) -> bool {
    let k = stripe.data.len();
    // a systematic code writes the lost data units; one that is not writes every unit
    (0..k)
        .filter(|i| !systematic || lost.contains(i))
        .all(|i| stripe.data[i][..] == pristine.0[i][..])
}

/// Try every loss pattern of one layout, and rebuild every single chunk
///
/// # Arguments
///
/// * `code` - The candidate
/// * `layout` - The layout
fn patterns(code: &mut dyn Code, layout: Layout) -> PatternResult {
    let mut result = PatternResult {
        code: code.name().to_string(),
        layout: layout.to_string(),
        ..PatternResult::default()
    };
    let systematic = code.systematic();
    // encode a seeded stripe and keep it
    let shape = match code.prepare(layout, UNIT) {
        Ok(shape) => shape,
        Err(e) => {
            result.errors.push(e);
            return result;
        }
    };
    let mut stripe = Stripe::new(
        layout.k,
        UNIT,
        shape.count,
        shape.len,
        0xc4ec + layout.n() as u64,
    );
    {
        let (data, mut stored) = stripe.encode_refs();
        if let Err(e) = code.encode(&data, &mut stored) {
            result.errors.push(e);
            return result;
        }
    }
    let pristine = stripe.snapshot();
    // every pattern of one to m lost chunks
    for lost in loss_patterns(layout) {
        result.patterns += 1;
        let at_m = lost.len() == layout.m;
        result.patterns_at_m += u64::from(at_m);
        // a lost chunk holds garbage, and so does every data unit a non-systematic code writes
        for &chunk in &lost {
            scribble(&mut stripe, chunk, systematic);
        }
        if !systematic {
            stripe.data.iter_mut().for_each(|unit| unit.fill(SCRIBBLE));
        }
        let outcome = {
            let (mut data, mut stored) = stripe.refs_mut();
            code.decode(&mut data, &mut stored, &lost)
        };
        match outcome {
            Ok(true) if data_restored(&stripe, &pristine, &lost, systematic) => result.decoded += 1,
            Ok(true) => result.wrong += 1,
            Ok(false) => {
                result.undecodable += 1;
                result.undecodable_at_m += u64::from(at_m);
            }
            Err(e) if result.errors.len() < 4 => result.errors.push(format!("{lost:?}: {e}")),
            Err(_) => {}
        }
        restore(&mut stripe, &pristine);
    }
    // every single chunk rebuilt from the others
    for target in 0..layout.n() {
        result.rebuilds += 1;
        scribble(&mut stripe, target, systematic);
        let outcome = {
            let (mut data, mut stored) = stripe.refs_mut();
            code.rebuild(&mut data, &mut stored, target)
        };
        match outcome {
            Ok(true) if code.rebuild_restores(target) => {
                // the chunk is its old bytes again
                let k = layout.k;
                let (now, then) = if systematic && target < k {
                    (&stripe.data[target][..], &pristine.0[target][..])
                } else if systematic {
                    (&stripe.stored[target - k][..], &pristine.1[target - k][..])
                } else {
                    (&stripe.stored[target][..], &pristine.1[target][..])
                };
                result.rebuilt_wrong += u64::from(now != then);
            }
            Ok(true) => {
                // a recoded chunk is checked by decoding with it: lose m other chunks
                let lost: Vec<usize> = (0..layout.n())
                    .filter(|&i| i != target)
                    .take(layout.m)
                    .collect();
                let snapshot = stripe.snapshot();
                for &chunk in &lost {
                    scribble(&mut stripe, chunk, systematic);
                }
                if !systematic {
                    stripe.data.iter_mut().for_each(|unit| unit.fill(SCRIBBLE));
                }
                let decoded = {
                    let (mut data, mut stored) = stripe.refs_mut();
                    code.decode(&mut data, &mut stored, &lost)
                };
                match decoded {
                    Ok(true) if data_restored(&stripe, &pristine, &lost, systematic) => {}
                    Ok(true) => result.rebuilt_wrong += 1,
                    Ok(false) => result.rebuilt_undecodable += 1,
                    Err(e) => result.errors.push(format!("decode after recode: {e}")),
                }
                restore(&mut stripe, &snapshot);
            }
            Ok(false) => result.rebuilt_undecodable += 1,
            Err(e) if result.errors.len() < 4 => {
                result.errors.push(format!("rebuild {target}: {e}"))
            }
            Err(_) => {}
        }
        restore(&mut stripe, &pristine);
    }
    result
}

/// The chance a random set of k chunks fails to decode, for a code with random coefficients
///
/// # Arguments
///
/// * `code` - The candidate
/// * `layout` - The layout
/// * `trials` - How many trials
fn random_failures(code: &mut dyn Code, layout: Layout, trials: u64) -> RandomResult {
    let mut result = RandomResult {
        code: code.name().to_string(),
        layout: layout.to_string(),
        trials,
        ..RandomResult::default()
    };
    let systematic = code.systematic();
    let Ok(shape) = code.prepare(layout, TRIAL_UNIT) else {
        return result;
    };
    let mut stripe = Stripe::new(layout.k, TRIAL_UNIT, shape.count, shape.len, 0x7a1);
    let mut picker = SplitMix64::new(0x7a1a1);
    for _ in 0..trials {
        // every trial is a fresh encode, so fresh coefficients
        {
            let (data, mut stored) = stripe.encode_refs();
            code.encode(&data, &mut stored)
                .expect("an encode the patterns already passed");
        }
        let pristine = stripe.snapshot();
        // lose m chunks chosen at random: a partial shuffle of the chunk numbers
        let mut order: Vec<usize> = (0..layout.n()).collect();
        for i in 0..layout.m {
            let j = i + picker.below(layout.n() - i);
            order.swap(i, j);
        }
        let lost = &order[..layout.m];
        for &chunk in lost {
            scribble(&mut stripe, chunk, systematic);
        }
        if !systematic {
            stripe.data.iter_mut().for_each(|unit| unit.fill(SCRIBBLE));
        }
        let outcome = {
            let (mut data, mut stored) = stripe.refs_mut();
            code.decode(&mut data, &mut stored, lost)
        };
        match outcome {
            Ok(true) if data_restored(&stripe, &pristine, lost, systematic) => {}
            Ok(true) => result.wrong += 1,
            Ok(false) | Err(_) => result.failures += 1,
        }
        restore(&mut stripe, &pristine);
    }
    result
}

/// Apply random partial updates and compare the parity with a fresh encode
///
/// # Arguments
///
/// * `code` - The candidate
/// * `layout` - The layout
/// * `updates` - How many updates
fn updates(code: &mut dyn Code, layout: Layout, updates: u64) -> UpdateResult {
    let mut result = UpdateResult {
        code: code.name().to_string(),
        layout: layout.to_string(),
        ..UpdateResult::default()
    };
    // a code that is not systematic has no data chunk to update
    if !code.systematic() {
        result.note = Some("not systematic: a write of any byte rewrites every chunk".to_string());
        return result;
    }
    let Ok(shape) = code.prepare(layout, UNIT) else {
        return result;
    };
    let mut stripe = Stripe::new(layout.k, UNIT, shape.count, shape.len, 0x0bda7e);
    {
        let (data, mut stored) = stripe.encode_refs();
        code.encode(&data, &mut stored)
            .expect("an encode the patterns already passed");
    }
    let mut picker = SplitMix64::new(0x0bda7e5);
    for _ in 0..updates {
        // a random range of a random data unit
        let index = picker.below(layout.k);
        let start = picker.below(UNIT);
        let end = start + 1 + picker.below(UNIT - start);
        let mut new = vec![0u8; end - start];
        picker.fill(&mut new);
        let old = stripe.data[index][start..end].to_vec();
        // fold it into the parity, then write it into the data
        let outcome = {
            let (_, mut stored) = stripe.refs_mut();
            code.update(index, &old, &new, &mut stored, start..end)
        };
        if let Err(note) = outcome {
            result.note = Some(note);
            return result;
        }
        stripe.data[index][start..end].copy_from_slice(&new);
        result.updates += 1;
    }
    // the parity a fresh encode of the data as it now is gives
    let updated = stripe.snapshot().1;
    {
        let (data, mut stored) = stripe.encode_refs();
        code.encode(&data, &mut stored)
            .expect("an encode the patterns already passed");
    }
    result.equal = Some(stripe.snapshot().1 == updated);
    result
}

/// Digest a candidate's stored chunks for fixed inputs, twice
///
/// # Arguments
///
/// * `code` - The candidate
/// * `layout` - The layout
fn digest(code: &mut dyn Code, layout: Layout) -> Option<DigestResult> {
    let shape = code.prepare(layout, UNIT).ok()?;
    // the same seeded data every run, on every host
    let mut stripe = Stripe::new(layout.k, UNIT, shape.count, shape.len, 0xd16e57);
    let mut once = || {
        code.reseed();
        let (data, mut stored) = stripe.encode_refs();
        code.encode(&data, &mut stored).ok()?;
        Some(fnv1a(stored.iter().map(|chunk| &**chunk)))
    };
    let first = once()?;
    let second = once()?;
    Some(DigestResult {
        code: code.name().to_string(),
        layout: layout.to_string(),
        digest: format!("{first:016x}"),
        repeatable: first == second,
    })
}

/// One way of encoding a row's data units into its parity
type EncodeFn<'a> = &'a mut dyn FnMut(&[&[u8]], &mut [&mut [u8]]) -> Result<(), String>;

/// Whether two encodes of the same data give the same parity
///
/// # Arguments
///
/// * `layout` - The layout
/// * `left` - One encode, by name
/// * `right` - The other
fn compat(layout: Layout, left: (&str, EncodeFn<'_>), right: (&str, EncodeFn<'_>)) -> CompatResult {
    // the same data through both, into two sets of parity buffers
    let mut a = Stripe::new(layout.k, UNIT, layout.m, UNIT, 0xc0de);
    let mut b = Stripe::new(layout.k, UNIT, layout.m, UNIT, 0xc0de);
    let ok_a = {
        let (data, mut stored) = a.encode_refs();
        (left.1)(&data, &mut stored).is_ok()
    };
    let ok_b = {
        let (data, mut stored) = b.encode_refs();
        (right.1)(&data, &mut stored).is_ok()
    };
    CompatResult {
        left: left.0.to_string(),
        right: right.0.to_string(),
        layout: layout.to_string(),
        equal: ok_a && ok_b && a.snapshot().1 == b.snapshot().1,
    }
}

/// Every check, printing a line as each candidate finishes
///
/// # Arguments
///
/// * `quick` - Whether this is the quick pass
pub fn run(quick: bool) -> CheckRecord {
    let mut record = CheckRecord::default();
    let trials = if quick { 2_000 } else { 100_000 };
    for mut code in codes::all() {
        // every loss pattern at every small layout the candidate supports
        for layout in pattern_layouts(quick) {
            if code.supports(layout, UNIT).is_err() {
                continue;
            }
            let result = patterns(code.as_mut(), layout);
            eprintln!(
                "check {:<22} {:>5} patterns {:>5} decoded {:>5} undecodable {:>3} wrong {:>3} rebuilt wrong {:>3} errors {}",
                result.code,
                result.layout,
                result.patterns,
                result.decoded,
                result.undecodable,
                result.wrong,
                result.rebuilt_wrong,
                result.errors.len()
            );
            record.patterns.push(result);
        }
        // random coefficients get the failure fraction measured, not assumed
        if code.name().starts_with("rlnc") {
            for layout in [Layout::new(4, 2), Layout::new(6, 3), Layout::new(10, 4)] {
                let result = random_failures(code.as_mut(), layout, trials);
                eprintln!(
                    "random {:<22} {:>5} trials {:>7} failures {:>5} wrong {}",
                    result.code, result.layout, result.trials, result.failures, result.wrong
                );
                record.random.push(result);
            }
        }
        // partial updates against a fresh encode, at the layouts the candidate supports
        for layout in [Layout::new(2, 1), Layout::new(4, 2), Layout::new(10, 4)] {
            if code.supports(layout, UNIT).is_err() {
                continue;
            }
            let result = updates(code.as_mut(), layout, if quick { 50 } else { 500 });
            eprintln!(
                "update {:<22} {:>5} updates {:>4} equal {:?} {}",
                result.code,
                result.layout,
                result.updates,
                result.equal,
                result.note.as_deref().unwrap_or("")
            );
            record.updates.push(result);
        }
        // digests of fixed inputs at every speed layout
        for layout in x4_layouts() {
            if code.supports(layout, UNIT).is_err() {
                continue;
            }
            if let Some(result) = digest(code.as_mut(), layout) {
                record.digests.push(result);
            }
        }
    }
    // which candidates' parity is the same bytes
    for layout in x4_layouts() {
        // ISA-L's Cauchy matrix, through the C library and through the port
        let mut isal = codes::isal::Isal::default();
        let mut port = codes::rusty::Rusty::default();
        isal.prepare(layout, UNIT)
            .expect("isa-l supports every x4 layout");
        port.prepare(layout, UNIT)
            .expect("rusty_erasure supports every x4 layout");
        record.compat.push(compat(
            layout,
            ("isa-l", &mut |d: &[&[u8]], p: &mut [&mut [u8]]| {
                isal.encode(d, p)
            }),
            ("rusty_erasure", &mut |d: &[&[u8]], p: &mut [&mut [u8]]| {
                port.encode(d, p)
            }),
        ));
        // reed-solomon-erasure's own matrix, through that crate and through the port's compat one
        let mut rse = codes::rse::Rse::default();
        rse.prepare(layout, UNIT)
            .expect("reed-solomon-erasure supports every x4 layout");
        record.compat.push(compat(
            layout,
            (
                "reed-solomon-erasure",
                &mut |d: &[&[u8]], p: &mut [&mut [u8]]| rse.encode(d, p),
            ),
            (
                "rusty_erasure, compat matrix",
                &mut |d: &[&[u8]], p: &mut [&mut [u8]]| rusty::encode_with_rse_matrix(layout, d, p),
            ),
        ));
    }
    for result in &record.compat {
        eprintln!(
            "compat {} = {} at {}: {}",
            result.left, result.right, result.layout, result.equal
        );
    }
    record
}
