//! X2: the placement candidates of S5, simulated over generated pool maps
//!
//! [Q19](../../../docs/src/object-storage/contract.md) asks which placement function the object
//! store uses, how many placement groups a tablet holds, and how large the pool map is; X2
//! (`docs/src/object-storage/spikes.md#x2-placement-simulation`) answers it by simulation. The
//! candidates place every placement group of a consumer on the slices of a pool, over shapes
//! from the lab's three hosts to fifty hosts of twenty-four devices. Each is read for how evenly
//! it fills devices, how much a change to the map moves, whether it keeps the domain rule, what
//! exceptions it needs, and what a lookup costs. `fanout` sizes the map's frame beside the
//! tablet map's.
//!
//! - `shoal-spike placement [--threads N] [--per-tablet 1,4,16,64] [--only <shape>]` prints the
//!   simulation, which is seeded and the same on every host
//! - `shoal-spike placement lookups [--core N] [--label <build>]` times a lookup on one pinned
//!   core, the only figure that depends on the host
//!
//! Like every spike's, this code is thrown away. A pool map type, a placement function and its
//! constants are M14's to write, from the decision this records.

pub mod candidates;
pub mod measure;
pub mod poolmap;
pub mod score;
pub mod shape;
pub mod table;
pub mod timing;

/// The value after a flag, if the flag is given
///
/// # Arguments
///
/// * `args` - The arguments after the subcommand
/// * `flag` - The flag
fn value_of(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|at| args.get(at + 1))
        .cloned()
}

/// Run `shoal-spike placement` with the arguments after the subcommand
///
/// # Arguments
///
/// * `args` - The arguments after `placement`
pub fn main(args: &[String]) {
    // the lookups stand alone and are the only host-dependent table
    if args.first().map(String::as_str) == Some("lookups") {
        let core = value_of(args, "--core").map(|core| core.parse().expect("--core takes a cpu number"));
        let label = value_of(args, "--label").unwrap_or_else(|| "unlabelled".to_string());
        print!("{}", timing::lookups(core, &label));
        return;
    }
    // the simulation, across every thread unless told otherwise
    let threads = value_of(args, "--threads")
        .map(|threads| threads.parse().expect("--threads takes a count"))
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    let per_tablet = value_of(args, "--per-tablet")
        .map(|list| {
            list.split(',')
                .map(|count| count.parse().expect("--per-tablet takes counts"))
                .collect()
        })
        .unwrap_or_else(|| vec![1, 4, 16, 64]);
    let options = measure::Options {
        per_tablet,
        threads,
        only: value_of(args, "--only"),
    };
    print!("{}", measure::simulate(&options));
}
