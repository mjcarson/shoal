//! Checking over the hotpath profile a run wrote
//!
//! The profile is written by the run itself: `hotpath` prints it as the last line of stdout, and
//! the runner captures that line straight into the artifact. There is nothing to fold, so all
//! this does is confirm that what landed is a profile rather than whatever the run printed last
//! before it failed - which is exactly what a profiling feature that was not forwarded to the
//! crate carrying the instrumentation looks like.

use std::path::Path;

use anyhow::{Result, bail};

use crate::model::hotpath::HotpathProfile;

/// Checks a hotpath artifact and describes what it holds
///
/// # Arguments
///
/// * `path` - The artifact to check
pub fn check(path: &Path) -> Result<String> {
    let profile: HotpathProfile = crate::store::read_json(path)?;
    // an empty profile is the signature of the instrumentation feature not reaching the crate
    // that carries the measured scopes, which is a bug that has happened here before
    if profile.output.is_empty() {
        bail!(
            "{} attributed time to no scopes at all - check that the `hotpath` feature is \
             forwarded to shoal-core",
            path.display()
        );
    }
    Ok(format!(
        "{} scopes over {}",
        profile.output.len(),
        crate::fmt::duration_ns(profile.total_elapsed as f64)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A profile with scopes in it passes, and says how many
    #[test]
    fn a_profile_with_scopes_passes() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("profile.json");
        std::fs::write(
            &path,
            r#"{"hotpath_profiling_mode":"timing","total_elapsed":12070266532,
                "caller_name":"tmdb::main",
                "output":{"a::b":{"calls":1,"avg":1,"p50":1,"p90":1,"p95":1,"p99":1,"total":1,
                "percent_total":0}}}"#,
        )
        .expect("writing a profile");
        let described = check(&path).expect("it checks out");
        assert!(described.contains("1 scopes"), "{described}");
    }

    /// A profile that measured nothing is refused, and says what usually causes it
    #[test]
    fn an_empty_profile_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("profile.json");
        std::fs::write(
            &path,
            r#"{"hotpath_profiling_mode":"timing","total_elapsed":1,"caller_name":"m","output":{}}"#,
        )
        .expect("writing a profile");
        let err = check(&path).expect_err("an empty profile is not a profile");
        assert!(format!("{err}").contains("forwarded"), "{err}");
    }

    /// Something that is not a profile at all is refused
    #[test]
    fn a_non_profile_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("profile.json");
        std::fs::write(&path, "inserting 99999 rows...\n").expect("writing junk");
        assert!(check(&path).is_err());
    }
}
