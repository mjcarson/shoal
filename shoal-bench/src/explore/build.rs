//! Builds the explorer to WebAssembly
//!
//! # Why this shells out
//!
//! The bundle is a second compilation of `shoal-top` for a second target, and cargo is the only
//! thing that can produce it. Two details are load bearing and neither is obvious.
//!
//! **The working directory.** Cargo discovers `.cargo/config.toml` by walking up from the current
//! directory, not from `--manifest-path`, so the build is spawned with its working directory inside
//! `shoal-top/` where that crate's own config sits.
//!
//! **`RUSTFLAGS`.** The workspace config sets `-Ctarget-cpu=native`, which reaches wasm32, where
//! rustc substitutes the host CPU name and LLVM rejects it - about two hundred warning lines per
//! crate, which is fifty thousand across an eframe build. `shoal-top/.cargo/config.toml` overrides
//! it for that triple, but a config file loses to a `RUSTFLAGS` **environment variable**, and this
//! repository's shell exports one. So the variable is set explicitly on the child, which is the
//! only source cargo checks ahead of it. It is set on the child alone and never exported: a capture
//! taken afterwards must still record the `RUSTFLAGS` it was really built with, and
//! `fingerprint.rs` hashes that into every artifact's provenance.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

/// The target the explorer is built for
const TARGET: &str = "wasm32-unknown-unknown";

/// What the wasm build is compiled with instead of the workspace's native CPU flag
///
/// Not an empty list. Cargo picks the first of three mutually exclusive sources - `RUSTFLAGS`, then
/// the matching `target.<triple>` config entries joined, then `build.rustflags` - and an **empty**
/// join does not count as a match, so an empty override silently falls through to the native flag.
const WASM_RUSTFLAGS: &str = "-C target-cpu=generic";

/// Builds the bundle and returns the directory holding it
///
/// # Arguments
///
/// * `root` - The repository to build in
/// * `out` - Where the bundle should be written
pub fn wasm(root: &Path, out: &Path) -> Result<()> {
    // the tool that turns a wasm artifact into something a browser can import
    let wanted = locked_version(root)?;
    preflight(&wanted)?;
    let crate_dir = root.join("shoal-top");
    if !crate_dir.is_dir() {
        bail!("{} is not there", crate_dir.display());
    }
    println!("building the explorer for {TARGET}");
    // spawned from inside the crate, so its own cargo config is the one that is found
    let status = Command::new("cargo")
        .current_dir(&crate_dir)
        .env("RUSTFLAGS", WASM_RUSTFLAGS)
        .args([
            "build",
            "--release",
            "--target",
            TARGET,
            "--lib",
            "--features",
            "web",
        ])
        .status()
        .context("running cargo for the wasm build")?;
    if !status.success() {
        bail!("the wasm build failed");
    }
    // where cargo put it, which is the workspace target directory rather than the crate's
    let artifact = root
        .join("target")
        .join(TARGET)
        .join("release")
        .join("shoal_top.wasm");
    if !artifact.is_file() {
        bail!("the wasm build produced no {}", artifact.display());
    }
    // generate the glue beside it
    let status = Command::new("wasm-bindgen")
        .args(["--target", "web", "--no-typescript", "--out-dir"])
        .arg(out)
        .arg(&artifact)
        .status()
        .context("running wasm-bindgen")?;
    if !status.success() {
        bail!("wasm-bindgen failed");
    }
    // the page that loads the glue, copied rather than generated so it can be edited by hand
    let page = crate_dir.join("index.html");
    std::fs::copy(&page, out.join("index.html"))
        .with_context(|| format!("copying {}", page.display()))?;
    Ok(())
}

/// Refuses to build when `wasm-bindgen` is missing or is not the version the bundle needs
///
/// The CLI and the crate must be the **same** version, not merely compatible ones. A mismatch is
/// not caught at build time: it produces a bundle that fails in the browser with
/// `import object field '__wbindgen_placeholder__' is not a Function`, hours later and nowhere near
/// the cause.
///
/// # Arguments
///
/// * `wanted` - The version the lockfile resolved
fn preflight(wanted: &str) -> Result<()> {
    // ask the tool what it is, which also tells us whether it is there at all
    let found = Command::new("wasm-bindgen").arg("--version").output();
    let installed = match found {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .last()
            .unwrap_or_default()
            .to_string(),
        // no such program, or one that could not say what it was
        _ => {
            bail!(
                "wasm-bindgen is not installed, and the wasm bundle cannot be generated without \
                 it.\n\nInstall exactly the version the lockfile resolved:\n\
                 \x20   cargo install wasm-bindgen-cli --version {wanted}"
            );
        }
    };
    if installed != wanted {
        bail!(
            "wasm-bindgen {installed} is installed, but this bundle needs {wanted}. The two must \
             match exactly - a near miss produces a bundle that fails in the browser rather than \
             at build time.\n\n\x20   cargo install wasm-bindgen-cli --version {wanted}"
        );
    }
    Ok(())
}

/// The `wasm-bindgen` version the lockfile resolved
///
/// Read rather than hardcoded. A hardcoded version goes stale the first time anything in the tree
/// bumps its `wasm-bindgen`, and goes stale silently.
///
/// # Arguments
///
/// * `root` - The repository whose lockfile to read
fn locked_version(root: &Path) -> Result<String> {
    let path: PathBuf = root.join("Cargo.lock");
    let lock = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    // a deliberately small parse: find the package block, take the version line that follows. the
    // alternative is a toml dependency, which this crate's manifest is explicit about not adding
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "name = \"wasm-bindgen\"" {
            continue;
        }
        for following in lines.by_ref().take(3) {
            let trimmed = following.trim();
            if let Some(version) = trimmed.strip_prefix("version = \"") {
                return Ok(version.trim_end_matches('"').to_string());
            }
        }
    }
    bail!("no wasm-bindgen version in {}", path.display())
}
