//! Records what this binary was built with, so every table it prints says which build it came from

use std::process::Command;

/// Hand the build's target cpu, any target features named beside it and the compiler's version to
/// the binary
fn main() {
    // cargo passes every rustflag joined by the unit separator
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    // a flag comes either joined (`-Ctarget-cpu=x`) or as two words (`-C`, `target-cpu=x`)
    let words: Vec<&str> = flags
        .split('\x1f')
        .map(|flag| flag.trim_start_matches("-C"))
        .collect();
    // the last target-cpu named wins, as it does for rustc
    let cpu = words
        .iter()
        .filter_map(|flag| flag.strip_prefix("target-cpu="))
        .next_back()
        .unwrap_or("default")
        .to_string();
    // every target-feature list named, joined, so a build of `x86-64-v3,+aes` says so
    let features = words
        .iter()
        .filter_map(|flag| flag.strip_prefix("target-feature="))
        .collect::<Vec<_>>()
        .join(",");
    // ask the compiler cargo is using for its version
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let version = Command::new(rustc)
        .arg("-V")
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=SPIKE_TARGET_CPU={cpu}");
    println!("cargo:rustc-env=SPIKE_TARGET_FEATURES={features}");
    println!("cargo:rustc-env=SPIKE_RUSTC={version}");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
}
