//! Correctness and stability: published check values, digests of seeded inputs that `report`
//! compares across hosts and builds, the same bytes at every alignment, the incremental interface
//! fed in pieces against one call, and a whole's checksum made from its parts'

use blake3::hazmat::{self, HasherExt};

use crate::buffers::{AlignedBuf, SplitMix64, fnv1a};
use crate::record::{
    AlignResult, CheckRecord, CombineResult, DigestResult, ExtendResult, StreamResult,
    VectorResult,
};
use crate::sums::{self, CrcMath, Digest, Splits, Sum};

/// The units every digest and speed cell is taken at
pub const UNITS: [usize; 5] = [4096, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024];

/// A unit's size as a table writes it
///
/// # Arguments
///
/// * `unit` - The size in bytes
pub fn unit_label(unit: usize) -> String {
    if unit >= 1 << 20 {
        format!("{} MiB", unit >> 20)
    } else {
        format!("{} KiB", unit >> 10)
    }
}

/// A published check value
struct Vector {
    /// The candidate it is for
    sum: &'static str,
    /// The input, described
    input: &'static str,
    /// The input's bytes
    bytes: Vec<u8>,
    /// The expected output, in hex as its source writes it
    expected: &'static str,
    /// Where the value is published
    source: &'static str,
}

/// The input BLAKE3's test vectors and twox-hash's XXH3 tests both use: the bytes 0 to 250
/// repeating
///
/// # Arguments
///
/// * `len` - How many bytes
fn pattern251(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// The published check values, each from the definition's own catalogue, its reference test
/// vectors, or an independent implementation's tests
fn vectors() -> Vec<Vector> {
    // the CRC catalogue's check input
    let check = b"123456789".to_vec();
    // the CRC-32C and CRC-64/NVME rows are the same for both crates that implement each
    let mut out = Vec::new();
    for sum in ["crc32c", "crc-fast crc32c"] {
        out.push(Vector {
            sum,
            input: "\"123456789\"",
            bytes: check.clone(),
            expected: "e3069283",
            source: "CRC-32/ISCSI check, RevEng catalogue (crc-catalog `algorithm.rs:2270`)",
        });
        out.push(Vector {
            sum,
            input: "\"Hello world!\"",
            bytes: b"Hello world!".to_vec(),
            expected: "7b98e751",
            source: "crc32c's own doc test (`lib.rs:8-12`)",
        });
    }
    for sum in ["crc-fast crc64nvme", "crc64fast-nvme"] {
        out.push(Vector {
            sum,
            input: "\"123456789\"",
            bytes: check.clone(),
            expected: "ae8b14860a799888",
            source: "CRC-64/NVME check, RevEng catalogue (crc-catalog `algorithm.rs:2500`)",
        });
        out.push(Vector {
            sum,
            input: "4,096 zero bytes",
            bytes: vec![0; 4096],
            expected: "6482d367eb22b64e",
            source: "crc64fast-nvme's tests (`lib.rs:202`)",
        });
    }
    out.push(Vector {
        sum: "crc32fast",
        input: "\"123456789\"",
        bytes: check,
        expected: "cbf43926",
        source: "CRC-32/ISO-HDLC check, RevEng catalogue",
    });
    // XXH3 at seed zero, from twox-hash, a second implementation checked against the C reference
    for (sum, len, expected) in [
        ("xxh3-64", 0, "2d06800538d394c2"),
        ("xxh3-64", 1024, "e5d78bafa45b2aa5"),
        ("xxh3-64", 10240, "bcd63266df6e2244"),
        ("xxh3-128", 0, "99aa06d3014798d86001c324468d497f"),
        ("xxh3-128", 1024, "d0ac1f7b93bf57b9e5d78bafa45b2aa5"),
        ("xxh3-128", 10240, "4f6375cca7ece1e1bcd63266df6e2244"),
    ] {
        out.push(Vector {
            sum,
            input: match len {
                0 => "empty",
                1024 => "1,024 bytes of the 251 pattern",
                _ => "10,240 bytes of the 251 pattern",
            },
            bytes: pattern251(len),
            expected,
            source: "twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`)",
        });
    }
    // BLAKE3, from the reference repository's test vectors at the crate's tag
    for (len, expected) in [
        (0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"),
        (1, "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"),
        (1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"),
        (1025, "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"),
        (102400, "bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085"),
    ] {
        out.push(Vector {
            sum: "blake3",
            input: match len {
                0 => "empty",
                1 => "1 byte of the 251 pattern",
                1024 => "1,024 bytes of the 251 pattern",
                1025 => "1,025 bytes of the 251 pattern",
                _ => "102,400 bytes of the 251 pattern",
            },
            bytes: pattern251(len),
            expected,
            source: "BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate",
        });
    }
    out
}

/// Check every candidate that has a published value against it
fn check_vectors() -> Vec<VectorResult> {
    let sums = sums::all();
    let mut out = Vec::new();
    for vector in vectors() {
        // the candidate the value is for
        let Some(sum) = sums.iter().find(|sum| sum.name() == vector.sum) else {
            continue;
        };
        let got = sum.one_shot(&vector.bytes).hex(sum.integer());
        out.push(VectorResult {
            sum: vector.sum.to_string(),
            input: vector.input.to_string(),
            expected: vector.expected.to_string(),
            got,
            source: vector.source.to_string(),
        });
    }
    out
}

/// Digests of seeded inputs: every length from 0 to 257, each unit, and a chunk of 1 MiB plus 13
///
/// Every length gets its own seed, so a candidate whose short-input path differs between builds
/// shows it. The unit digests are kept one by one, since M13's frozen vectors start from them.
///
/// # Arguments
///
/// * `sum` - The candidate
fn digests(sum: &dyn Sum) -> Vec<DigestResult> {
    let mut out = Vec::new();
    let integer = sum.integer();
    // every short length, folded into one FNV-1a so a run's record stays small
    let shorts: Vec<Digest> = (0..=257)
        .map(|len| sum.one_shot(&AlignedBuf::seeded(len, 0x5100 + len as u64)))
        .collect();
    let folded = fnv1a(shorts.iter().map(Digest::bytes));
    out.push(DigestResult {
        sum: sum.name().to_string(),
        set: "lengths 0-257".to_string(),
        digest: format!("{folded:016x}"),
    });
    // each unit size, kept as the candidate's own output
    for unit in UNITS {
        let bytes = AlignedBuf::seeded(unit, 0x0417 + unit as u64);
        out.push(DigestResult {
            sum: sum.name().to_string(),
            set: format!("unit {}", unit_label(unit)),
            digest: sum.one_shot(&bytes).hex(integer),
        });
    }
    // and an odd length past every unit, which ends in a partial block
    let odd = AlignedBuf::seeded((1 << 20) + 13, 0x0dd);
    out.push(DigestResult {
        sum: sum.name().to_string(),
        set: "1 MiB + 13".to_string(),
        digest: sum.one_shot(&odd).hex(integer),
    });
    out
}

/// The same 64 KiB + 7 bytes copied to every start offset in a cache line, and the output at each
/// compared with the output at offset zero
///
/// # Arguments
///
/// * `sum` - The candidate
fn alignment(sum: &dyn Sum) -> AlignResult {
    // the bytes, and a buffer with room to slide them along a cache line
    let len = 64 * 1024 + 7;
    let source = AlignedBuf::seeded(len, 0xa11);
    let mut slide = AlignedBuf::new(len + 64);
    let reference = sum.one_shot(&source);
    let mut equal = 0;
    for offset in 0..64 {
        // copy to this offset and checksum from there
        slide[offset..offset + len].copy_from_slice(&source);
        if sum.one_shot(&slide[offset..offset + len]) == reference {
            equal += 1;
        }
    }
    AlignResult {
        sum: sum.name().to_string(),
        offsets: 64,
        equal,
    }
}

/// The incremental interface fed in pieces, against one call and against the same interface fed
/// the whole in one piece
///
/// # Arguments
///
/// * `sum` - The candidate
fn streams(sum: &dyn Sum) -> Vec<StreamResult> {
    let mut out = Vec::new();
    // a candidate with no incremental interface says so once
    let probe = AlignedBuf::seeded(64, 1);
    if sum.stream(&probe, &Splits::Every(64)).is_none() {
        out.push(StreamResult {
            sum: sum.name().to_string(),
            api: sum.stream_api().to_string(),
            splits: "none".to_string(),
            ..StreamResult::default()
        });
        return out;
    }
    // inputs of several lengths, the longest past every unit
    let lens = [0, 1, 15, 16, 17, 63, 64, 65, 255, 1000, 4096, 65_543, (1 << 20) + 13];
    let inputs: Vec<AlignedBuf> = lens
        .iter()
        .map(|&len| AlignedBuf::seeded(len, 0x57e0 + len as u64))
        .collect();
    // fixed piece sizes, from one byte to more than any input
    let mut cuts: Vec<(String, Vec<Splits>)> = [1usize, 7, 64, 4096, 65_536]
        .into_iter()
        .map(|len| (format!("pieces of {len} B"), vec![Splits::Every(len)]))
        .collect();
    // and a hundred seeded random cuttings, each with pieces of 0 to 9,999 bytes
    let mut rng = SplitMix64::new(0x5917);
    let random = (0..100)
        .map(|_| Splits::At((0..256).map(|_| rng.below(10_000)).collect()))
        .collect();
    cuts.push(("100 random cuttings".to_string(), random));
    for (name, splits) in cuts {
        let mut result = StreamResult {
            sum: sum.name().to_string(),
            api: sum.stream_api().to_string(),
            splits: name,
            ..StreamResult::default()
        };
        for input in &inputs {
            // the two references: one call, and the interface fed the whole at once
            let one_shot = sum.one_shot(input);
            let whole = sum.stream(input, &Splits::Every(usize::MAX));
            for split in &splits {
                let streamed = sum.stream(input, split);
                result.tried += 1;
                if streamed == Some(one_shot) {
                    result.equal_one_shot += 1;
                }
                if streamed == whole {
                    result.equal_whole += 1;
                }
            }
        }
        out.push(result);
    }
    out
}

/// A chunk's checksum made from its units' checksums with the crate's combine, against one call
/// over the chunk
///
/// # Arguments
///
/// * `sum` - The candidate
fn combines(sum: &dyn Sum) -> Vec<CombineResult> {
    let mut out = Vec::new();
    // a candidate with no combine is left out here and says so on its stream row
    let probe = sum.one_shot(b"x");
    if sum.combine(probe, probe, 1).is_none() {
        return out;
    }
    // a chunk of sixteen units at each unit size, folded left to right
    let mut units = CombineResult {
        sum: sum.name().to_string(),
        how: "16 units into a chunk, every unit size".to_string(),
        ..CombineResult::default()
    };
    for unit in UNITS {
        let chunk = AlignedBuf::seeded(16 * unit, 0xc4 + unit as u64);
        let mut acc = sum.one_shot(&chunk[..unit]);
        for part in chunk[unit..].chunks(unit) {
            acc = sum
                .combine(acc, sum.one_shot(part), part.len() as u64)
                .expect("it combined once");
        }
        units.tried += 1;
        if acc == sum.one_shot(&chunk) {
            units.equal += 1;
        }
    }
    out.push(units);
    // a thousand seeded cuts of 1 MiB + 13 into two parts at any byte, the empty part included
    let whole = AlignedBuf::seeded((1 << 20) + 13, 0xc5);
    let reference = sum.one_shot(&whole);
    let mut rng = SplitMix64::new(0xc6);
    let mut cuts = CombineResult {
        sum: sum.name().to_string(),
        how: "1,000 random cuts in two, any length".to_string(),
        ..CombineResult::default()
    };
    for _ in 0..1000 {
        let at = rng.below(whole.len() + 1);
        let (a, b) = whole.split_at(at);
        let combined = sum
            .combine(sum.one_shot(a), sum.one_shot(b), b.len() as u64)
            .expect("it combined once");
        cuts.tried += 1;
        if combined == reference {
            cuts.equal += 1;
        }
    }
    out.push(cuts);
    out
}

/// BLAKE3's nearest thing to a combine: a chunk's root hash merged from its units' chaining
/// values through the crate's `hazmat` module, which needs each unit a power of two of 1 KiB at
/// an offset that is a multiple of its length
fn blake3_subtrees() -> CombineResult {
    let mut result = CombineResult {
        sum: "blake3".to_string(),
        how: "16 units into a chunk through hazmat subtrees, every unit size".to_string(),
        ..CombineResult::default()
    };
    for unit in UNITS {
        // each unit's chaining value, computed knowing its offset in the chunk
        let chunk = AlignedBuf::seeded(16 * unit, 0xb3 + unit as u64);
        let mut level: Vec<hazmat::ChainingValue> = chunk
            .chunks(unit)
            .enumerate()
            .map(|(i, part)| {
                let mut hasher = blake3::Hasher::new();
                hasher.set_input_offset((i * unit) as u64);
                hasher.update(part);
                hasher.finalize_non_root()
            })
            .collect();
        // merged pairwise up the tree, the last merge being the root
        while level.len() > 2 {
            level = level
                .chunks(2)
                .map(|pair| hazmat::merge_subtrees_non_root(&pair[0], &pair[1], hazmat::Mode::Hash))
                .collect();
        }
        let root = hazmat::merge_subtrees_root(&level[0], &level[1], hazmat::Mode::Hash);
        result.tried += 1;
        if root == blake3::hash(&chunk) {
            result.equal += 1;
        }
    }
    result
}

/// A unit's checksum carried to its place: the checksum of the bytes followed by the place's
/// identity, made from the checksum of the bytes alone and the identity, without the bytes
///
/// # Arguments
///
/// * `sum` - The candidate
fn extends(sum: &dyn Sum) -> Vec<ExtendResult> {
    let mut out = Vec::new();
    // units of every size, each followed by a 32-byte identity
    let mut rng = SplitMix64::new(0xe7);
    let inputs: Vec<(AlignedBuf, [u8; 32])> = UNITS
        .iter()
        .map(|&unit| {
            let mut identity = [0; 32];
            rng.fill(&mut identity);
            (AlignedBuf::seeded(unit, 0xe8 + unit as u64), identity)
        })
        .collect();
    // through a resume from a finished value, where the crate has one
    if sum.append(sum.one_shot(b"x"), b"y").is_some() {
        let mut result = ExtendResult {
            sum: sum.name().to_string(),
            how: sum.append_api().to_string(),
            ..ExtendResult::default()
        };
        for (bytes, identity) in &inputs {
            let joined = [&bytes[..], &identity[..]].concat();
            let extended = sum.append(sum.one_shot(bytes), identity);
            result.tried += 1;
            if extended == Some(sum.one_shot(&joined)) {
                result.equal += 1;
            }
        }
        out.push(result);
    }
    // and through a combine with the identity's own checksum, where the crate has one
    let probe = sum.one_shot(b"x");
    if sum.combine(probe, probe, 1).is_some() {
        let mut result = ExtendResult {
            sum: sum.name().to_string(),
            how: sum.combine_api().to_string(),
            ..ExtendResult::default()
        };
        for (bytes, identity) in &inputs {
            let joined = [&bytes[..], &identity[..]].concat();
            let extended = sum.combine(sum.one_shot(bytes), sum.one_shot(identity), 32);
            result.tried += 1;
            if extended == Some(sum.one_shot(&joined)) {
                result.equal += 1;
            }
        }
        out.push(result);
    }
    out
}

/// Whether two crates that implement one definition give the same bytes, by combining one's
/// output with the other's combine: CRC-64/NVME from `crc64fast-nvme` through `crc-fast`'s combine
fn cross_combine() -> CombineResult {
    // the two halves checksummed by the crate that has no combine
    let nvme = sums::Crc64FastNvme;
    let fast = sums::CrcFast::crc64nvme();
    let whole = AlignedBuf::seeded((1 << 20) + 13, 0xc7);
    let mut rng = SplitMix64::new(0xc8);
    let mut result = CombineResult {
        sum: "crc64fast-nvme".to_string(),
        how: "100 random cuts, combined by crc-fast's checksum_combine".to_string(),
        ..CombineResult::default()
    };
    for _ in 0..100 {
        let at = rng.below(whole.len() + 1);
        let (a, b) = whole.split_at(at);
        let combined = fast
            .combine(nvme.one_shot(a), nvme.one_shot(b), b.len() as u64)
            .expect("crc-fast combines");
        result.tried += 1;
        if combined == nvme.one_shot(&whole) {
            result.equal += 1;
        }
    }
    result
}

/// The harness's own combine, zlib's method, held to one call over the whole for both CRCs a
/// crate was measured for, at any cut and with the multiplier made once for a fixed unit
fn harness_combines() -> Vec<CombineResult> {
    let mut out = Vec::new();
    let cases: [(&str, CrcMath, Box<dyn Sum>); 2] = [
        ("crc32c", CrcMath::crc32c(), Box::new(sums::Crc32c)),
        ("crc64nvme", CrcMath::crc64nvme(), Box::new(sums::Crc64FastNvme)),
    ];
    for (name, math, sum) in cases {
        // any cut of 1 MiB + 13 in two
        let whole = AlignedBuf::seeded((1 << 20) + 13, 0xc9);
        let reference = sum.one_shot(&whole).as_u64();
        let mut rng = SplitMix64::new(0xca);
        let mut cuts = CombineResult {
            sum: format!("harness {name}"),
            how: "1,000 random cuts in two, any length".to_string(),
            ..CombineResult::default()
        };
        for _ in 0..1000 {
            let at = rng.below(whole.len() + 1);
            let (a, b) = whole.split_at(at);
            let combined = math.combine(
                sum.one_shot(a).as_u64(),
                sum.one_shot(b).as_u64(),
                b.len() as u64,
            );
            cuts.tried += 1;
            if combined == reference {
                cuts.equal += 1;
            }
        }
        out.push(cuts);
        // sixteen units into a chunk with one multiplier a unit size
        let mut units = CombineResult {
            sum: format!("harness {name}"),
            how: "16 units into a chunk, the multiplier made once a unit size".to_string(),
            ..CombineResult::default()
        };
        for unit in UNITS {
            let chunk = AlignedBuf::seeded(16 * unit, 0xcb + unit as u64);
            let op = math.operator(unit as u64);
            let mut acc = sum.one_shot(&chunk[..unit]).as_u64();
            for part in chunk[unit..].chunks(unit) {
                acc = math.combine_op(acc, sum.one_shot(part).as_u64(), op);
            }
            units.tried += 1;
            if acc == sum.one_shot(&chunk).as_u64() {
                units.equal += 1;
            }
        }
        out.push(units);
    }
    out
}

/// Run every check
///
/// # Arguments
///
/// * `quick` - Whether this is the quick pass, which skips nothing: every check is fast
pub fn run(quick: bool) -> CheckRecord {
    let _ = quick;
    let mut record = CheckRecord {
        vectors: check_vectors(),
        ..CheckRecord::default()
    };
    for sum in sums::all() {
        eprintln!("check {}", sum.name());
        record.digests.extend(digests(sum.as_ref()));
        record.alignment.push(alignment(sum.as_ref()));
        record.streams.extend(streams(sum.as_ref()));
        record.combines.extend(combines(sum.as_ref()));
        record.extends.extend(extends(sum.as_ref()));
    }
    // the two checks that are not one candidate's alone
    record.combines.push(blake3_subtrees());
    record.combines.push(cross_combine());
    record.combines.extend(harness_combines());
    record
}
