//! Records what this binary was built with, so every table it prints says which build it came from

use std::process::Command;

/// Hand the build's target cpu, the C kernels' `-march` and the compiler's version to the binary
fn main() {
    // cargo passes every rustflag joined by the unit separator
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    // the flag comes either joined (`-Ctarget-cpu=x`) or as two words (`-C`, `target-cpu=x`)
    let cpu = flags
        .split('\x1f')
        .filter_map(|flag| flag.trim_start_matches("-C").strip_prefix("target-cpu="))
        .next_back()
        .unwrap_or("default")
        .to_string();
    // the C kernels of reed-solomon-erasure take their -march from this, not from rustflags
    let rse_arch =
        std::env::var("RUST_REED_SOLOMON_ERASURE_ARCH").unwrap_or_else(|_| "haswell".to_string());
    // ask the compiler cargo is using for its version
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let version = Command::new(rustc)
        .arg("-V")
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=SPIKE_TARGET_CPU={cpu}");
    println!("cargo:rustc-env=SPIKE_RSE_ARCH={rse_arch}");
    println!("cargo:rustc-env=SPIKE_RUSTC={version}");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    println!("cargo:rerun-if-env-changed=RUST_REED_SOLOMON_ERASURE_ARCH");
}
