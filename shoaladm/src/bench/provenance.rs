//! Where a capture's code came from, and what time it is
//!
//! A capture of code that is in no commit cannot be found again, so a dirty project or shoal is
//! refused unless `--allow-dirty` was given, and recorded as dirty when it was. A shoal from a
//! registry has no commit and is recorded by its version, which is not an error: the bench runs
//! against any project.

use color_eyre::eyre::bail;
use shoal_loadgen::results::CodeFacts;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Run git in a directory and return what it printed, if it ran
///
/// # Arguments
///
/// * `dir` - The directory
/// * `args` - Its arguments
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    // a directory outside any checkout has no answer, which is not an error
    let output = Command::new("git").arg("-C").arg(dir).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// The commit a directory is checked out at, and whether it has changes
///
/// # Arguments
///
/// * `dir` - A directory in the checkout
#[must_use]
pub fn code_facts(dir: &Path) -> CodeFacts {
    // the commit, and any tracked or untracked change at all
    let commit = git(dir, &["rev-parse", "HEAD"]);
    let dirty = git(dir, &["status", "--porcelain", "--untracked-files=no"])
        .is_some_and(|status| !status.is_empty());
    CodeFacts {
        commit,
        dirty,
        version: None,
    }
}

/// Where the shoal a project builds against comes from: a checkout's directory, or a version
///
/// # Arguments
///
/// * `project` - The project's directory
fn shoal_source(project: &Path) -> Option<(Option<PathBuf>, String)> {
    // cargo's own answer, so a path, a git and a registry dependency are all read the same way
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .current_dir(project)
        .output()
        .ok()?;
    let metadata: shoal::serde_json::Value = shoal::serde_json::from_slice(&output.stdout).ok()?;
    let package = metadata["packages"]
        .as_array()?
        .iter()
        .find(|package| package["name"] == "shoal")?;
    let version = package["version"].as_str().unwrap_or_default().to_string();
    // a package with no source is a path dependency, which is a checkout
    let dir = package["source"]
        .is_null()
        .then(|| package["manifest_path"].as_str().map(PathBuf::from))
        .flatten()
        .and_then(|manifest| manifest.parent().map(Path::to_path_buf));
    Some((dir, version))
}

/// The facts of the shoal a project builds against
///
/// # Arguments
///
/// * `project` - The project's directory
#[must_use]
pub fn shoal_facts(project: &Path) -> CodeFacts {
    // a checkout by its commit, anything else by its version
    match shoal_source(project) {
        Some((Some(dir), version)) => CodeFacts {
            version: Some(version),
            ..code_facts(&dir)
        },
        Some((None, version)) => CodeFacts {
            commit: None,
            dirty: false,
            version: Some(version),
        },
        None => CodeFacts::default(),
    }
}

/// Refuse code with uncommitted changes unless it was allowed
///
/// # Arguments
///
/// * `project` - The project's facts
/// * `shoal` - Shoal's facts
/// * `allow` - Whether `--allow-dirty` was given
///
/// # Errors
///
/// When either is dirty and it was not allowed.
pub fn refuse_dirty(project: &CodeFacts, shoal: &CodeFacts, allow: bool) -> color_eyre::Result<()> {
    // a capture of bytes in no commit cannot be found again
    if allow {
        return Ok(());
    }
    let dirty: Vec<&str> = [("the project", project), ("shoal", shoal)]
        .into_iter()
        .filter(|(_, facts)| facts.dirty)
        .map(|(name, _)| name)
        .collect();
    if !dirty.is_empty() {
        bail!(
            "{} has uncommitted changes, so this capture could never be found in history again; \
             commit first, or pass --allow-dirty and it is recorded as dirty",
            dirty.join(" and ")
        );
    }
    Ok(())
}

/// `rustc -V`, if rustc runs
#[must_use]
pub fn rustc() -> Option<String> {
    let output = Command::new("rustc").arg("-V").output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// The civil date of a day count since 1970, by Howard Hinnant's algorithm
///
/// # Arguments
///
/// * `days` - Days since 1970-01-01
fn civil(days: i64) -> (i64, u32, u32) {
    // shift the epoch to 0000-03-01, so a leap day ends a year
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// A time as RFC 3339, in UTC, to the second
///
/// # Arguments
///
/// * `time` - The time
#[must_use]
pub fn rfc3339(time: SystemTime) -> String {
    // seconds since the epoch, split into the day and the time in it
    let secs = time.duration_since(UNIX_EPOCH).map_or(0, |since| since.as_secs()) as i64;
    let (year, month, day) = civil(secs.div_euclid(86_400));
    let of_day = secs.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        of_day / 3600,
        (of_day % 3600) / 60,
        of_day % 60
    )
}

/// The label a capture is given when none was asked for: when, and at which commit
///
/// # Arguments
///
/// * `time` - When it was taken
/// * `project` - The project's facts
#[must_use]
pub fn default_label(time: SystemTime, project: &CodeFacts) -> String {
    // compact enough to type, sortable by time
    let stamp: String = rfc3339(time)
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();
    let commit = project
        .commit
        .as_deref()
        .map_or("nocommit".to_string(), |commit| commit.chars().take(8).collect());
    let dirty = if project.dirty { "-dirty" } else { "" };
    format!("{stamp}-{commit}{dirty}")
}

#[cfg(test)]
mod tests {
    use super::{default_label, refuse_dirty, rfc3339};
    use shoal_loadgen::results::CodeFacts;
    use std::time::{Duration, UNIX_EPOCH};

    /// A time is written in UTC, leap days included
    #[test]
    fn times_are_written_in_utc() {
        assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        // 2024-02-29 12:34:56
        assert_eq!(rfc3339(UNIX_EPOCH + Duration::from_secs(1_709_210_096)), "2024-02-29T12:34:56Z");
        let label = default_label(
            UNIX_EPOCH + Duration::from_secs(1_709_210_096),
            &CodeFacts {
                commit: Some("0123456789abcdef".to_string()),
                dirty: true,
                version: None,
            },
        );
        assert_eq!(label, "20240229T123456Z-01234567-dirty");
    }

    /// Dirty code is refused unless allowed
    #[test]
    fn dirty_code_is_refused_unless_allowed() {
        let clean = CodeFacts::default();
        let dirty = CodeFacts {
            dirty: true,
            ..CodeFacts::default()
        };
        assert!(refuse_dirty(&clean, &clean, false).is_ok());
        assert!(refuse_dirty(&dirty, &clean, false).unwrap_err().to_string().contains("the project"));
        assert!(refuse_dirty(&clean, &dirty, true).is_ok());
    }
}
