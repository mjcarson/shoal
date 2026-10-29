//! Names the build a table came from
//!
//! X13 runs one binary built for `znver1` and, on europa, one built for its own cpu, and every
//! table it prints says which. Cargo hands a build script the flags the crate is compiled with;
//! this passes them to the crate as `SPIKE_RUSTFLAGS`.

/// Pass the crate's compiler flags through to it
fn main() {
    // the flags as cargo encodes them, separated by 0x1f, written as spaces
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS")
        .unwrap_or_default()
        .replace('\u{1f}', " ");
    println!("cargo:rustc-env=SPIKE_RUSTFLAGS={flags}");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
}
