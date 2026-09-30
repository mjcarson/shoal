//! Naming the cpu a host has, so a node program is built for it
//!
//! A node program built for the machine it was compiled on dies of SIGILL on an older host at
//! its first instruction that host lacks, which is how every deployment before
//! [F63](../../docs/src/features/shoaladm.md) was told to build for the oldest host by hand.
//! This module asks a host what it has, over one read-only ssh round trip, and turns the answer
//! into the `-C target-cpu=<name>` its program is built with: a name from a short table where
//! the part is known, and the x86-64 level its flags reach where it is not. The name is then
//! judged against what the local `rustc` knows, so a part newer than the toolchain never becomes
//! a flag rustc refuses.
//!
//! An operator who knows better writes `target_cpu` on the node, its group or the deployment,
//! and the probe is not consulted.

use color_eyre::eyre::bail;
use std::collections::BTreeSet;
use std::process::Command;

use crate::deploy::remote::Host;

/// What a host said about its cpu
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CpuFacts {
    /// The machine architecture, as `uname -m` prints it (`x86_64`, `aarch64`)
    pub arch: String,
    /// The vendor string (`AuthenticAMD`, `GenuineIntel`)
    pub vendor: String,
    /// The cpu family
    pub family: u32,
    /// The model within the family
    pub model: u32,
    /// The marketing name, for messages
    pub name: String,
    /// The feature flags, as `/proc/cpuinfo` lists them
    pub flags: BTreeSet<String>,
}

/// The script a probe runs on a host
///
/// The architecture and the first processor block of `/proc/cpuinfo` as `key=value` lines. It
/// only reads: nothing is created and no sudo is asked for.
#[must_use]
pub fn script() -> String {
    // the key is the first field, with the padding before its colon, so `model` is told from
    // `model name` by what follows it
    "echo arch=$(uname -m); \
     awk -F': *' '/^processor/ && seen++ {exit} \
       $1 ~ /^vendor_id/ {print \"vendor=\" $2} \
       $1 ~ /^cpu family/ {print \"family=\" $2} \
       $1 ~ /^model[ \t]*$/ {print \"model=\" $2} \
       $1 ~ /^model name/ {print \"name=\" $2} \
       $1 ~ /^flags/ {print \"flags=\" $2} \
       $1 ~ /^Features/ {print \"flags=\" $2}' /proc/cpuinfo"
        .to_string()
}

/// Read what a probe's script printed
///
/// # Arguments
///
/// * `output` - What the script printed
#[must_use]
pub fn parse(output: &str) -> CpuFacts {
    let mut facts = CpuFacts::default();
    // every line is a key and a value, and anything else is ignored
    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "arch" => facts.arch = value.to_string(),
            "vendor" => facts.vendor = value.to_string(),
            "family" => facts.family = value.parse().unwrap_or(0),
            "model" => facts.model = value.parse().unwrap_or(0),
            "name" => facts.name = value.to_string(),
            "flags" => facts.flags = value.split_whitespace().map(str::to_string).collect(),
            _ => (),
        }
    }
    facts
}

/// Probe a host over ssh, blocking until it answers
///
/// # Arguments
///
/// * `target` - What ssh is given
///
/// # Errors
///
/// When ssh cannot reach the host, or the script fails there.
pub fn probe(target: &str) -> color_eyre::Result<CpuFacts> {
    // one round trip, in batch mode so a host that would prompt is refused rather than hanging
    let host = Host {
        target: target.to_string(),
    };
    let output = host.run(&script())?;
    Ok(parse(&output))
}

/// The architecture this program was built for, as `uname -m` would print it
#[must_use]
pub fn local_arch() -> &'static str {
    std::env::consts::ARCH
}

/// The x86-64 level a set of flags reaches, as rustc names it
///
/// The levels are the psABI's: v2 is Nehalem's baseline, v3 Haswell's, v4 the AVX-512
/// foundation set. A part that has a level's flags runs a build for it whatever its name.
///
/// # Arguments
///
/// * `flags` - The feature flags, as `/proc/cpuinfo` lists them
#[must_use]
pub fn level(flags: &BTreeSet<String>) -> &'static str {
    let has = |wanted: &[&str]| wanted.iter().all(|flag| flags.contains(*flag));
    if has(&["avx512f", "avx512bw", "avx512cd", "avx512dq", "avx512vl"]) {
        "x86-64-v4"
    } else if has(&["avx", "avx2", "bmi1", "bmi2", "fma", "f16c", "movbe", "abm"]) {
        "x86-64-v3"
    } else if has(&["sse4_2", "ssse3", "popcnt", "cx16"]) {
        "x86-64-v2"
    } else {
        "x86-64"
    }
}

/// The rustc name for a host's cpu, before it is judged against the toolchain
///
/// # Arguments
///
/// * `facts` - What the host said
#[must_use]
pub fn name(facts: &CpuFacts) -> String {
    // anything but x86-64 gets the generic build for its architecture; the build machine has
    // to be that architecture too, which `check_arch` decides
    if facts.arch != "x86_64" {
        return "generic".to_string();
    }
    let named = match (facts.vendor.as_str(), facts.family) {
        // Zen 1 and Zen+ are models below 0x30; Zen 2 the rest of the family
        ("AuthenticAMD", 23) => Some(if facts.model < 0x30 { "znver1" } else { "znver2" }),
        // Zen 3 and Zen 4 share a family, and only Zen 4 has AVX-512
        ("AuthenticAMD", 25) => Some(if facts.flags.contains("avx512f") { "znver4" } else { "znver3" }),
        ("AuthenticAMD", 26) => Some("znver5"),
        // the Intel parts a server or a workstation is likely to be, by model
        ("GenuineIntel", 6) => match facts.model {
            0x3c | 0x3f | 0x45 | 0x46 => Some("haswell"),
            0x3d | 0x47 | 0x4f | 0x56 => Some("broadwell"),
            0x4e | 0x5e | 0x8e | 0x9e | 0xa5 | 0xa6 => Some("skylake"),
            // Skylake-SP without VNNI, Cascade Lake with it
            0x55 => Some(if facts.flags.contains("avx512_vnni") { "cascadelake" } else { "skylake-avx512" }),
            0x6a | 0x6c => Some("icelake-server"),
            0x7d | 0x7e => Some("icelake-client"),
            0x8c | 0x8d => Some("tigerlake"),
            0xa7 => Some("rocketlake"),
            0x97 | 0x9a | 0xbe => Some("alderlake"),
            0xb7 | 0xba | 0xbf => Some("raptorlake"),
            0x8f => Some("sapphirerapids"),
            0xcf => Some("emeraldrapids"),
            0xad | 0xae => Some("graniterapids"),
            _ => None,
        },
        _ => None,
    };
    named.map_or_else(|| level(&facts.flags).to_string(), str::to_string)
}

/// Every cpu name the local rustc accepts
///
/// # Errors
///
/// When rustc cannot be run.
pub fn known_names() -> color_eyre::Result<BTreeSet<String>> {
    // rustc prints them one per line, indented, the first being `native` with a note
    let output = Command::new(rustc()).args(["--print", "target-cpus"]).output()?;
    if !output.status.success() {
        bail!(
            "rustc --print target-cpus failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    // the names are the indented lines' first words; the heading is not indented, and `native`
    // is the one name that means another cpu on every machine
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.starts_with(char::is_whitespace))
        .filter_map(|line| line.split_whitespace().next())
        .filter(|word| *word != "native")
        .map(str::to_string)
        .collect())
}

/// The rustc to ask, which is cargo's when this program runs under it
fn rustc() -> String {
    std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string())
}

/// Judge a name against the toolchain, falling back to the level a host's flags reach
///
/// # Arguments
///
/// * `wanted` - The name chosen for the host
/// * `facts` - What the host said, for the fallback
/// * `known` - Every name the toolchain accepts
#[must_use]
pub fn judge(wanted: &str, facts: &CpuFacts, known: &BTreeSet<String>) -> String {
    // a name the toolchain knows is used as it is
    if known.contains(wanted) {
        return wanted.to_string();
    }
    // otherwise the level, which every toolchain since 1.66 knows; `generic` last of all
    let fallback = level(&facts.flags);
    if known.contains(fallback) {
        fallback.to_string()
    } else {
        "generic".to_string()
    }
}

/// Refuse a host whose architecture is not the build machine's
///
/// A build for another architecture is a cross-compilation, which needs a target installed and
/// a linker for it, and is not something this program does.
///
/// # Arguments
///
/// * `node` - The node, for the refusal
/// * `facts` - What the host said
/// * `local` - The build machine's architecture, as `uname -m` prints it
///
/// # Errors
///
/// When the two differ.
pub fn check_arch(node: &str, facts: &CpuFacts, local: &str) -> color_eyre::Result<()> {
    if facts.arch != local {
        bail!(
            "{node} is {} and this machine is {local}; a node program is built on a machine of \
             the host's architecture, and cross-compiling is not supported",
            facts.arch
        );
    }
    Ok(())
}

/// Decide the cpu a node's program is built for
///
/// The inventory's `target_cpu` if it sets one, otherwise the probe's name judged against the
/// toolchain.
///
/// # Arguments
///
/// * `node` - The node, for messages
/// * `override_name` - What the inventory says, if anything
/// * `facts` - What the host said
/// * `known` - Every name the toolchain accepts
///
/// # Errors
///
/// When the host is another architecture, or the inventory names a cpu the toolchain does not.
pub fn decide(
    node: &str,
    override_name: Option<&str>,
    facts: &CpuFacts,
    known: &BTreeSet<String>,
) -> color_eyre::Result<String> {
    check_arch(node, facts, local_arch())?;
    // the operator's word is final, once rustc agrees it is a cpu
    if let Some(name) = override_name {
        if !known.contains(name) {
            bail!(
                "{node}'s target_cpu {name:?} is not a cpu this rustc knows; `rustc --print target-cpus` lists them"
            );
        }
        return Ok(name.to_string());
    }
    Ok(judge(&name(facts), facts, known))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A facts value with a vendor, a family, a model and some flags
    fn facts(vendor: &str, family: u32, model: u32, flags: &[&str]) -> CpuFacts {
        CpuFacts {
            arch: "x86_64".to_string(),
            vendor: vendor.to_string(),
            family,
            model,
            name: String::new(),
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
        }
    }

    /// The v3 flag set, which every Zen and every Haswell or later has
    const V3: &[&str] = &[
        "sse4_2", "ssse3", "popcnt", "cx16", "avx", "avx2", "bmi1", "bmi2", "fma", "f16c", "movbe", "abm",
    ];

    /// The AVX-512 foundation on top of v3
    fn v4() -> Vec<&'static str> {
        let mut flags = V3.to_vec();
        flags.extend(["avx512f", "avx512bw", "avx512cd", "avx512dq", "avx512vl"]);
        flags
    }

    /// The two lab parts and their neighbours are named by family and model
    #[test]
    fn amd_parts_are_named_by_family() {
        // the V1756B on hyperion and titan: family 23 model 0x11
        assert_eq!(name(&facts("AuthenticAMD", 23, 0x11, V3)), "znver1");
        // Rome and Matisse
        assert_eq!(name(&facts("AuthenticAMD", 23, 0x31, V3)), "znver2");
        assert_eq!(name(&facts("AuthenticAMD", 23, 0x71, V3)), "znver2");
        // Vermeer has no AVX-512, the 7945HX on europa (family 25 model 97) does
        assert_eq!(name(&facts("AuthenticAMD", 25, 0x21, V3)), "znver3");
        assert_eq!(name(&facts("AuthenticAMD", 25, 97, &v4())), "znver4");
        assert_eq!(name(&facts("AuthenticAMD", 26, 0x44, &v4())), "znver5");
    }

    /// Intel parts in the table are named, and one outside it gets its level
    #[test]
    fn intel_parts_are_named_or_levelled() {
        assert_eq!(name(&facts("GenuineIntel", 6, 0x55, &v4())), "skylake-avx512");
        let mut cascade = v4();
        cascade.push("avx512_vnni");
        assert_eq!(name(&facts("GenuineIntel", 6, 0x55, &cascade)), "cascadelake");
        assert_eq!(name(&facts("GenuineIntel", 6, 0x8f, &v4())), "sapphirerapids");
        assert_eq!(name(&facts("GenuineIntel", 6, 0x9e, V3)), "skylake");
        // a model nobody listed falls back to what its flags reach
        assert_eq!(name(&facts("GenuineIntel", 6, 0xff, &v4())), "x86-64-v4");
        assert_eq!(name(&facts("GenuineIntel", 6, 0xff, V3)), "x86-64-v3");
        assert_eq!(name(&facts("GenuineIntel", 6, 0xff, &["sse4_2", "ssse3", "popcnt", "cx16"])), "x86-64-v2");
        assert_eq!(name(&facts("GenuineIntel", 6, 0xff, &["sse2"])), "x86-64");
        // an unknown vendor too
        assert_eq!(name(&facts("CentaurHauls", 7, 0x3b, V3)), "x86-64-v3");
    }

    /// A name the toolchain does not know is demoted to the level, and an override is refused
    /// unless it is known
    #[test]
    fn names_are_judged_against_the_toolchain() {
        let known: BTreeSet<String> = ["znver1", "znver4", "x86-64-v3", "x86-64-v4", "generic"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        let zen5 = facts("AuthenticAMD", 26, 0x44, &v4());
        // an old toolchain without znver5 builds for the level Zen 5 reaches
        assert_eq!(judge("znver5", &zen5, &known), "x86-64-v4");
        assert_eq!(judge("znver4", &zen5, &known), "znver4");
        // nothing known at all is the generic build
        assert_eq!(judge("znver5", &zen5, &BTreeSet::new()), "generic");
        // the operator's override wins, once rustc knows it
        assert_eq!(decide("a", Some("znver1"), &zen5, &known).unwrap(), "znver1");
        assert!(decide("a", Some("znver9"), &zen5, &known).is_err());
        assert_eq!(decide("a", None, &zen5, &known).unwrap(), "x86-64-v4");
    }

    /// A host of another architecture is refused by name
    #[test]
    fn another_architecture_is_refused() {
        let mut arm = facts("", 0, 0, &[]);
        arm.arch = "aarch64".to_string();
        assert_eq!(name(&arm), "generic");
        let error = check_arch("pi", &arm, "x86_64").unwrap_err().to_string();
        assert!(error.contains("pi is aarch64"), "{error}");
        assert!(check_arch("pi", &arm, "aarch64").is_ok());
    }

    /// The script runs on this machine and its output reads back as this machine's cpu
    #[test]
    fn the_script_reads_this_machine() {
        let output = Command::new("sh").arg("-c").arg(script()).output().expect("sh");
        let facts = parse(&String::from_utf8_lossy(&output.stdout));
        assert_eq!(facts.arch, local_arch(), "{facts:?}");
        if facts.arch == "x86_64" {
            assert!(!facts.vendor.is_empty(), "{facts:?}");
            assert!(facts.family > 0, "{facts:?}");
            assert!(facts.flags.contains("sse2"), "{facts:?}");
        }
        // and what rustc knows includes the level every fallback reaches
        let known = known_names().expect("rustc");
        assert!(known.contains("x86-64-v3"), "{known:?}");
        assert!(!known.contains("native"), "{known:?}");
        // the parser reads a block as the awk prints it
        let parsed = parse("arch=x86_64\nvendor=AuthenticAMD\nfamily=25\nmodel=97\nname=AMD Ryzen 9 7945HX\nflags=fpu avx512f\n");
        assert_eq!(parsed.family, 25);
        assert_eq!(parsed.model, 97);
        assert_eq!(parsed.name, "AMD Ryzen 9 7945HX");
        assert!(parsed.flags.contains("avx512f"));
    }
}
