//! Storage faults for a directory a test names: a torn write, a full disk and a lost device
//!
//! The failure model object storage adds to the tablets' ([P7](../../../docs/src/object-storage/contract.md#the-contract))
//! names three faults of a device that its node outlives: a write that tears, a device that
//! fills, and a device that stops answering. A clause no test can violate is not checked, so a
//! test has to be able to cause each of them, for one directory and nothing beside it
//! ([F70](../../../docs/src/features/storage-faults.md)).
//!
//! The faults are decided here and applied in glommio, whose files ask an [`IoHook`] before
//! every open, read, write, sync and metadata operation once one is set. Every byte of a
//! table's intent log, an archive, the shard's WAL, a checkpoint, a snapshot and the control
//! log goes through a glommio file, so they are all reached, handles opened before the fault
//! was armed included. The few small files a start writes through `std::fs` - the storage
//! marker, its lock and the hosting table - ask [`guard`] themselves.
//!
//! Like the crash points, this is off unless armed and behind no feature: until a fault is
//! armed the hook is not even set, and once one has been, an operation outside every armed
//! directory costs one relaxed load and a short scan of the table.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use glommio::io::{IoHook, IoOp, IoVerdict};

/// What happens to the operations under a directory
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The write that takes the bytes written under the directory past this many is torn: its
    /// start reaches the device and is synced, and the process ends as a crash would
    Torn {
        /// How many bytes are written whole before the tear
        after_bytes: u64,
    },
    /// The directory holds this many more bytes than it did when the fault was armed, and a
    /// write that would grow its files past that fails with `ENOSPC`
    Full {
        /// How many bytes the files may grow by
        budget: u64,
    },
    /// Every operation under the directory fails with `EIO`, as a device that stopped answering
    Lost,
}

/// One armed fault and what it has counted
#[derive(Debug)]
struct Armed {
    /// The directory, and everything under it
    dir: PathBuf,
    /// The fault
    fault: Fault,
    /// The bytes written under the directory since it was armed, for a tear
    written: u64,
    /// The bytes the directory's files have grown by since it was armed, for a full disk
    grown: u64,
    /// The size each file has reached, seeded from its size on disk the first time it is written
    sizes: HashMap<PathBuf, u64>,
    /// Whether the tear has happened
    torn: bool,
}

/// Whether any fault has been armed, which is all an operation looks at otherwise
static ARMED: AtomicBool = AtomicBool::new(false);

/// Every armed fault, one a directory
static TABLE: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

/// Whether a tear ends the process, which every caller but this module's own tests wants
static EXIT_ON_TEAR: AtomicBool = AtomicBool::new(true);

/// The hook glommio asks, which decides by the table
struct Faults;

/// The one hook this process sets, on the first fault armed
static HOOK: Faults = Faults;

/// Take the table, whatever a panicking holder left
fn table() -> MutexGuard<'static, Vec<Armed>> {
    TABLE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The armed fault whose directory holds a path, the deepest when two do
///
/// # Arguments
///
/// * `table` - The armed faults
/// * `path` - The path
fn covering<'a>(table: &'a mut [Armed], path: &Path) -> Option<&'a mut Armed> {
    // by component, so `/a/b` never covers `/a/bc`
    table
        .iter_mut()
        .filter(|armed| path.starts_with(&armed.dir))
        .max_by_key(|armed| armed.dir.components().count())
}

/// The error a lost device answers everything with
fn lost() -> io::Error {
    io::Error::from_raw_os_error(libc::EIO)
}

impl IoHook for Faults {
    /// Decide an operation by the fault armed over its path, if any is
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file or directory
    /// * `op` - What the operation is about to do
    fn check(&self, path: &Path, op: IoOp) -> IoVerdict {
        // nothing armed is nothing to decide
        if !ARMED.load(Ordering::Relaxed) {
            return IoVerdict::Proceed;
        }
        let mut table = table();
        let Some(armed) = covering(&mut table, path) else {
            return IoVerdict::Proceed;
        };
        match (armed.fault, op) {
            // a lost device answers nothing
            (Fault::Lost, _) => IoVerdict::Fail(lost()),
            // a full disk refuses a write that grows a file past the budget, and only that
            (Fault::Full { budget }, IoOp::Write { pos, len }) => {
                let end = pos.saturating_add(len as u64);
                let size = *armed
                    .sizes
                    .entry(path.to_path_buf())
                    .or_insert_with(|| std::fs::metadata(path).map_or(0, |meta| meta.len()));
                let growth = end.saturating_sub(size);
                if armed.grown.saturating_add(growth) > budget {
                    return IoVerdict::Fail(io::Error::from_raw_os_error(libc::ENOSPC));
                }
                armed.grown += growth;
                armed.sizes.insert(path.to_path_buf(), size.max(end));
                IoVerdict::Proceed
            }
            // a tear cuts the one write that crosses its mark, and nothing after it
            (Fault::Torn { after_bytes }, IoOp::Write { len, .. }) if !armed.torn => {
                let after = armed.written.saturating_add(len as u64);
                if after > after_bytes {
                    armed.torn = true;
                    let keep = after_bytes.saturating_sub(armed.written);
                    return IoVerdict::Tear(usize::try_from(keep).unwrap_or(usize::MAX));
                }
                armed.written = after;
                IoVerdict::Proceed
            }
            // anything else under a full or torn directory goes ahead
            _ => IoVerdict::Proceed,
        }
    }

    /// End the process once a torn write's start is on the device
    ///
    /// # Arguments
    ///
    /// * `path` - The file the write was torn in
    /// * `written` - How many of its bytes reached the device
    fn torn(&self, path: &Path, written: usize) {
        tracing::error!(
            msg = "dying at an armed torn write",
            path = %path.display(),
            written
        );
        // exits with 137, the way a kill and a crash point do, with nothing flushed
        if EXIT_ON_TEAR.load(Ordering::Relaxed) {
            std::process::exit(137);
        }
    }
}

/// Arm a fault for a directory and everything under it, replacing one armed there before
///
/// # Arguments
///
/// * `dir` - The directory
/// * `fault` - The fault
pub fn arm(dir: &Path, fault: Fault) {
    // the hook is set on the first fault, and never before, so a process that arms nothing
    // pays nothing for it
    glommio::io::set_io_hook(&HOOK);
    let mut table = table();
    table.retain(|armed| armed.dir != dir);
    table.push(Armed {
        dir: dir.to_path_buf(),
        fault,
        written: 0,
        grown: 0,
        sizes: HashMap::new(),
        torn: false,
    });
    ARMED.store(true, Ordering::Relaxed);
    tracing::warn!(msg = "armed a storage fault", dir = %dir.display(), ?fault);
}

/// Lift the fault armed for a directory, if there is one
///
/// # Arguments
///
/// * `dir` - The directory
pub fn clear(dir: &Path) {
    let mut table = table();
    table.retain(|armed| armed.dir != dir);
    ARMED.store(!table.is_empty(), Ordering::Relaxed);
}

/// Read a fault as a test spells it: `torn <bytes>`, `full <bytes>` or `lost`
///
/// # Arguments
///
/// * `spec` - The fault as spelled
///
/// # Errors
///
/// Refuses a fault this module does not know, and a byte count that is not a number.
pub fn parse(spec: &str) -> Result<Fault, String> {
    // the kind, then its byte count where it takes one
    let mut words = spec.split_whitespace();
    let bytes = |word: Option<&str>| -> Result<u64, String> {
        word.ok_or_else(|| format!("{spec:?} names no byte count"))?
            .parse()
            .map_err(|error| format!("{spec:?} has a bad byte count: {error}"))
    };
    match words.next() {
        Some("torn") => Ok(Fault::Torn {
            after_bytes: bytes(words.next())?,
        }),
        Some("full") => Ok(Fault::Full {
            budget: bytes(words.next())?,
        }),
        Some("lost") => Ok(Fault::Lost),
        _ => Err(format!(
            "{spec:?} is not a fault; a fault is torn <bytes>, full <bytes> or lost"
        )),
    }
}

/// Refuse an operation through `std::fs` on a path under a lost directory
///
/// The storage marker, its lock and the hosting table are written without glommio, so they ask
/// here. A full or torn directory lets them through: they are a few hundred bytes, written whole.
///
/// # Arguments
///
/// * `path` - The path about to be opened, read or written
///
/// # Errors
///
/// `EIO` when a lost fault is armed over the path.
pub fn guard(path: &Path) -> io::Result<()> {
    // nothing armed is nothing to refuse
    if !ARMED.load(Ordering::Relaxed) {
        return Ok(());
    }
    match covering(&mut table(), path).map(|armed| armed.fault) {
        Some(Fault::Lost) => Err(lost()),
        _ => Ok(()),
    }
}

/// The free bytes a full fault armed over a path leaves, if one is
///
/// A full fault's directory reports what is left of its budget as its free bytes, so a node
/// sees its disk fill the way a real one would and sheds before it fails, if its reserve says to.
///
/// # Arguments
///
/// * `path` - The path free bytes are asked of
#[must_use]
pub fn free_bytes_under(path: &Path) -> Option<u64> {
    // nothing armed is the filesystem's own answer
    if !ARMED.load(Ordering::Relaxed) {
        return None;
    }
    match covering(&mut table(), path) {
        Some(Armed {
            fault: Fault::Full { budget },
            grown,
            ..
        }) => Some(budget.saturating_sub(*grown)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glommio::io::{BufferedFile, DmaFile};
    use glommio::LocalExecutor;

    /// A fresh directory
    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("a temp dir")
    }

    /// A fault acts under its directory, by component, and nowhere beside it
    #[test]
    fn a_fault_matches_its_directory_and_nothing_beside_it() {
        let root = dir();
        let armed = root.path().join("a");
        arm(&armed, Fault::Lost);
        assert!(guard(&armed.join("deep").join("file")).is_err());
        assert!(guard(&armed).is_err());
        assert!(guard(&root.path().join("ab")).is_ok());
        assert!(guard(root.path()).is_ok());
        // the deepest fault wins where two cover a path
        arm(&armed.join("deep"), Fault::Full { budget: 7 });
        assert!(guard(&armed.join("deep").join("file")).is_ok());
        assert_eq!(free_bytes_under(&armed.join("deep").join("file")), Some(7));
        clear(&armed.join("deep"));
        clear(&armed);
        assert!(guard(&armed.join("deep")).is_ok());
        // and a fault is read as a test spells it
        assert_eq!(parse("torn 4096"), Ok(Fault::Torn { after_bytes: 4096 }));
        assert_eq!(parse("full 1"), Ok(Fault::Full { budget: 1 }));
        assert_eq!(parse("lost"), Ok(Fault::Lost));
        assert!(parse("full").is_err() && parse("melt").is_err());
    }

    /// A full directory counts what its files grow by, not every byte written, and refuses the
    /// write that would pass its budget
    #[test]
    fn full_counts_growth_since_arming() {
        let root = dir();
        let path = root.path().join("file");
        std::fs::write(&path, vec![0u8; 100]).expect("a file");
        arm(root.path(), Fault::Full { budget: 50 });
        LocalExecutor::default().run(async {
            let file = glommio::io::OpenOptions::new()
                .read(true)
                .write(true)
                .buffered_open(&path)
                .await
                .expect("the file opens");
            // rewriting what is there grows nothing
            file.write_at(vec![1u8; 100], 0).await.expect("a rewrite");
            // forty bytes past the end grow the file by forty, which fits
            file.write_at(vec![2u8; 40], 100)
                .await
                .expect("growth within the budget");
            assert_eq!(free_bytes_under(&path), Some(10));
            // and twenty more do not
            let refused = file
                .write_at(vec![3u8; 20], 140)
                .await
                .expect_err("past the budget");
            assert!(format!("{refused}").contains("No space left"), "{refused}");
            file.close().await.expect("the file closes");
        });
        clear(root.path());
    }

    /// A lost directory fails every operation, on a handle opened before it was lost too
    #[test]
    fn lost_fails_every_operation() {
        let root = dir();
        let path = root.path().join("file");
        LocalExecutor::default().run(async {
            let file = BufferedFile::create(&path).await.expect("the file is made");
            file.write_at(vec![1u8; 8], 0)
                .await
                .expect("a write before the loss");
            arm(root.path(), Fault::Lost);
            assert!(file.write_at(vec![2u8; 8], 8).await.is_err(), "a write");
            assert!(file.read_at(0, 8).await.is_err(), "a read");
            assert!(file.fdatasync().await.is_err(), "a sync");
            assert!(BufferedFile::open(&path).await.is_err(), "an open");
            assert!(glommio::io::remove(&path).await.is_err(), "a remove");
            clear(root.path());
            file.close()
                .await
                .expect("the file closes once the device is back");
        });
    }

    /// A torn write keeps its start, synced, fails, and tears nothing after it
    #[test]
    fn torn_cuts_one_write_once() {
        // a tear here fails the write rather than ending the test binary
        EXIT_ON_TEAR.store(false, Ordering::Relaxed);
        let root = dir();
        let path = root.path().join("file");
        LocalExecutor::default().run(async {
            arm(root.path(), Fault::Torn { after_bytes: 30 });
            let file = BufferedFile::create(&path).await.expect("the file is made");
            // twenty bytes whole, then a write of twenty torn after ten
            file.write_at(vec![1u8; 20], 0)
                .await
                .expect("a whole write");
            assert!(
                file.write_at(vec![2u8; 20], 20).await.is_err(),
                "the torn write"
            );
            let on_disk = std::fs::read(&path).expect("the file reads");
            assert_eq!(on_disk.len(), 30);
            assert!(on_disk[20..].iter().all(|byte| *byte == 2));
            // and the next write is whole again
            file.write_at(vec![3u8; 5], 30)
                .await
                .expect("a write after the tear");
            file.close().await.expect("the file closes");
            // a direct write is torn on a block boundary
            let direct = root.path().join("direct");
            let file = DmaFile::create(&direct).await.expect("the file is made");
            let align = file.alignment() as usize;
            arm(
                root.path(),
                Fault::Torn {
                    after_bytes: (align * 2 + 3) as u64,
                },
            );
            let mut buf = file.alloc_dma_buffer(align * 4);
            buf.as_bytes_mut().fill(9);
            assert!(
                file.write_at(buf, 0).await.is_err(),
                "the torn direct write"
            );
            let kept = std::fs::read(&direct).expect("the file reads").len();
            assert_eq!(
                kept,
                align * 2,
                "the tear did not keep whole blocks of {align}"
            );
            file.close().await.expect("the file closes");
        });
        clear(root.path());
    }
}
