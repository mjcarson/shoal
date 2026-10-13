//! The shared flush held to the writes a real device saw ([F85](../../docs/src/features/devices-and-pools.md#the-shared-flush))
//!
//! A slice whose device applies with `apply_sync: shared` makes an apply durable with a sync of
//! one small file of its own, begun after the apply's writes completed, rather than a sync of the
//! chunk it wrote. That is only correct if the filesystem answers a sync of an unchanged file with
//! a flush of the device's cache, and if a chunk's writes need nothing but the cache flushed: no
//! extent to convert, no size to log. S6 asked for this to be shown on a device before it was the
//! default, so this records every write and flush a slice makes through `dm-log-writes`, and
//! after the run replays the log to the state a crash would leave at the last flush before each
//! acknowledged apply. Every apply acknowledged by then must be in its chunk, at its label, with
//! its bytes. It does this on XFS and on ext4, the two filesystems the store accepts.
//!
//! It also counts the applies that the flush before theirs did not yet hold, so a replay that
//! could not tell one flush from another fails rather than passes.
//!
//! It needs `sudo` without a password, `losetup`, `dmsetup`, `mkfs.xfs`, `mkfs.ext4` and the
//! `dm-log-writes` module, and is ignored by default; without them it says so by name and passes.
//! Run it with `cargo test -p shoal --test shared_flush -- --ignored --nocapture`.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::Arc;

use shoal::glommio::LocalExecutorBuilder;
use shoal::server::devices::{DeviceState, Facts};
use shoal::server::objects::store::chunk::{ChunkHeader, ChunkId, BLOCK};
use shoal::server::objects::store::{Slice, SliceOptions, StageUnits, StageWhole};
use shoal::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
use shoal::shared::objects::Label;
use shoal::shared::poolmap::PlacementGroup;
use shoal::shared::rendezvous::mix64;

/// The data device's bytes: XFS needs 300 MiB at least
const DATA_BYTES: u64 = 512 << 20;
/// The log device's bytes, sparse, room for every write the run makes
const LOG_BYTES: u64 = 4 << 30;
/// A unit's bytes
const UNIT: u32 = 4096;
/// A chunk's units
const UNITS: u32 = 64;
/// A chunk's bytes
const CHUNK: u64 = UNIT as u64 * UNITS as u64;
/// The writers at once, each with chunks of its own
const WRITERS: u64 = 8;
/// The chunks each writer applies over
const CHUNKS_EACH: u64 = 2;
/// The applies each writer makes
const APPLIES_EACH: u64 = 12;

/// The log's magic, `dm-log-writes`' own
const LOG_MAGIC: u64 = 0x6a73_6677_7368_72;
/// An entry that flushed the device's cache
const WRITE_FLUSH: u64 = 1 << 0;
/// A write the device made durable itself
const WRITE_FUA: u64 = 1 << 1;
/// A discard
const WRITE_DISCARD: u64 = 1 << 2;
/// A mark the test logged
const WRITE_MARK: u64 = 1 << 3;

/// Run a command under `sudo`, returning what it printed if it succeeded
///
/// # Arguments
///
/// * `args` - The command and its arguments
fn sudo(args: &[&str]) -> Option<String> {
    // never prompt: a test that would wait on a password is one that cannot run here
    let output = Command::new("sudo").arg("-n").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Why this machine cannot run the test, if it cannot
fn missing() -> Option<String> {
    // passwordless sudo, the tools, and the target
    if sudo(&["true"]).is_none() {
        return Some("sudo needs a password here".to_string());
    }
    for tool in ["losetup", "dmsetup", "mkfs.xfs", "mkfs.ext4"] {
        if sudo(&["which", tool]).is_none() {
            return Some(format!("{tool} is not installed"));
        }
    }
    if sudo(&["modprobe", "dm-log-writes"]).is_none() {
        return Some("the dm-log-writes module cannot be loaded".to_string());
    }
    None
}

/// A `dm-log-writes` target over two loop devices, a filesystem on it, mounted for the run
///
/// Torn down on drop whatever state the test left it in.
struct Logged {
    /// The device-mapper name
    name: String,
    /// The data loop device
    data_loop: String,
    /// The log loop device
    log_loop: String,
    /// The file the log is kept in, which outlives the target
    log: PathBuf,
    /// The file behind the data device
    data: PathBuf,
    /// Where the filesystem is mounted
    mount: PathBuf,
    /// Whether it is still mounted and mapped
    up: bool,
}

/// A file of a size, sparse
///
/// # Arguments
///
/// * `path` - Where
/// * `bytes` - How large
fn sparse(path: &Path, bytes: u64) {
    std::fs::File::create(path)
        .and_then(|file| file.set_len(bytes))
        .expect("a sparse file");
}

impl Logged {
    /// Make the target and a filesystem on it
    ///
    /// # Arguments
    ///
    /// * `dir` - Where the files and the mount point go
    /// * `filesystem` - `xfs` or `ext4`
    fn make(dir: &Path, filesystem: &str) -> Self {
        // the two files, the data device's first written by nothing
        let data = dir.join(format!("{filesystem}-data.img"));
        let log = dir.join(format!("{filesystem}-log.img"));
        sparse(&data, DATA_BYTES);
        sparse(&log, LOG_BYTES);
        let data_loop = sudo(&["losetup", "--find", "--show", &data.to_string_lossy()]).expect("a data loop");
        let log_loop = sudo(&["losetup", "--find", "--show", &log.to_string_lossy()]).expect("a log loop");
        let name = format!("shoal-flush-{}-{filesystem}", std::process::id());
        let table = format!("0 {} log-writes {data_loop} {log_loop}", DATA_BYTES / 512);
        sudo(&["dmsetup", "create", &name, "--table", &table]).expect("the log-writes target");
        let mapped = format!("/dev/mapper/{name}");
        // no discards, so the replay starts from zeros and writes alone
        match filesystem {
            "xfs" => sudo(&["mkfs.xfs", "-q", "-K", &mapped]).expect("mkfs.xfs"),
            _ => sudo(&["mkfs.ext4", "-q", "-E", "nodiscard", &mapped]).expect("mkfs.ext4"),
        };
        let mount = dir.join(format!("{filesystem}-mount"));
        std::fs::create_dir_all(&mount).expect("a mount point");
        sudo(&["mount", &mapped, &mount.to_string_lossy()]).expect("mounted");
        // the mount belongs to whoever runs the test, so it can write there
        // SAFETY: getuid and getgid read the calling process's ids and cannot fail
        let owner = format!("{}:{}", unsafe { libc::getuid() }, unsafe { libc::getgid() });
        sudo(&["chown", &owner, &mount.to_string_lossy()]).expect("chowned");
        Logged {
            name,
            data_loop,
            log_loop,
            log,
            data,
            mount,
            up: true,
        }
    }

    /// Log a mark, after everything that completed before it
    ///
    /// # Arguments
    ///
    /// * `mark` - Its name
    fn mark(&self, mark: &str) {
        sudo(&["dmsetup", "message", &self.name, "0", "mark", mark]).expect("a mark is logged");
    }

    /// Unmount and remove the target, leaving the log file whole
    fn down(&mut self) {
        if !self.up {
            return;
        }
        self.up = false;
        // unmounted once the executor's ring has let go of its files: io_uring tears a ring down
        // after its executor is gone, and the filesystem is busy until it has
        let unmounted = (0..100).any(|_| {
            sudo(&["umount", &self.mount.to_string_lossy()]).is_some() || {
                std::thread::sleep(std::time::Duration::from_millis(100));
                false
            }
        });
        assert!(unmounted, "{} never unmounted", self.mount.display());
        // removed once udev lets go of it after the unmount, which is when the target writes the
        // last of its log; forced only if it never does
        let removed = (0..20).any(|_| {
            sudo(&["dmsetup", "remove", "--retry", &self.name]).is_some() || {
                std::thread::sleep(std::time::Duration::from_millis(100));
                false
            }
        });
        if !removed {
            let _ = sudo(&["dmsetup", "remove", "--force", &self.name]);
        }
        let _ = sudo(&["losetup", "-d", &self.data_loop]);
        let _ = sudo(&["losetup", "-d", &self.log_loop]);
    }
}

impl Drop for Logged {
    /// Everything down, and the files gone
    fn drop(&mut self) {
        let _ = sudo(&["umount", "-l", &self.mount.to_string_lossy()]);
        self.down();
        let _ = std::fs::remove_file(&self.log);
        let _ = std::fs::remove_file(&self.data);
    }
}

/// One entry of the log
#[derive(Debug, Clone)]
struct Entry {
    /// The first sector it wrote
    sector: u64,
    /// The sectors it wrote
    sectors: u64,
    /// What kind of entry it is
    flags: u64,
    /// The mark's name, for a mark
    mark: Option<String>,
    /// Where its data starts in the log file, for a write
    data_at: u64,
}

/// Read every entry of a log
///
/// # Arguments
///
/// * `log` - The log file
fn read_log(log: &Path) -> (u64, Vec<Entry>) {
    let mut file = std::fs::File::open(log).expect("the log");
    // the super block: magic, version, entries, sector size
    let mut head = [0u8; 28];
    file.read_exact(&mut head).expect("the log's super block");
    let word = |at: usize| u64::from_le_bytes(head[at..at + 8].try_into().expect("eight bytes"));
    assert_eq!(word(0), LOG_MAGIC, "not a dm-log-writes log");
    let count = word(16);
    let sector_size = u64::from(u32::from_le_bytes(head[24..28].try_into().expect("four bytes")));
    let mut entries = Vec::new();
    let mut at = sector_size;
    for _ in 0..count {
        let mut block = vec![0u8; sector_size as usize];
        file.seek(SeekFrom::Start(at)).expect("seek");
        file.read_exact(&mut block).expect("an entry");
        let field = |offset: usize| u64::from_le_bytes(block[offset..offset + 8].try_into().expect("eight bytes"));
        let (sector, sectors, flags, data_len) = (field(0), field(8), field(16), field(24));
        let mark = (flags & WRITE_MARK != 0)
            .then(|| String::from_utf8_lossy(&block[32..32 + data_len as usize]).into_owned());
        let data_at = at + sector_size;
        // a write's data follows its entry; nothing else carries any
        let carries = flags & (WRITE_MARK | WRITE_DISCARD) == 0 && sectors > 0;
        at = data_at + if carries { sectors * sector_size } else { 0 };
        entries.push(Entry {
            sector,
            sectors: if carries { sectors } else { 0 },
            flags,
            mark,
            data_at,
        });
    }
    (sector_size, entries)
}

/// Lay entries' writes over an image
///
/// # Arguments
///
/// * `log` - The log file
/// * `image` - The image
/// * `sector_size` - The log's sector
/// * `entries` - The entries to lay
fn lay(log: &Path, image: &Path, sector_size: u64, entries: &[&Entry]) {
    let mut log = std::fs::File::open(log).expect("the log");
    let mut image = std::fs::OpenOptions::new().write(true).open(image).expect("the image");
    for entry in entries {
        if entry.sectors == 0 {
            continue;
        }
        let mut data = vec![0u8; (entry.sectors * sector_size) as usize];
        log.seek(SeekFrom::Start(entry.data_at)).expect("seek");
        log.read_exact(&mut data).expect("a write's data");
        image.seek(SeekFrom::Start(entry.sector * sector_size)).expect("seek");
        image.write_all(&data).expect("laid");
    }
}

/// The chunk an apply lands in
///
/// # Arguments
///
/// * `object` - Its object
fn chunk(object: u64) -> ChunkId {
    ChunkId {
        consumer: 9,
        pg: PlacementGroup { tablet: 2, sub: 0 },
        object: u128::from(object),
        stripe: 0,
        position: 0,
    }
}

/// Seeded bytes
///
/// # Arguments
///
/// * `seed` - The seed
/// * `len` - How many
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    (0..len.div_ceil(8) as u64)
        .flat_map(|index| mix64(index ^ seed.rotate_left(29)).to_le_bytes())
        .take(len)
        .collect()
}

/// What each chunk holds at each label it was acknowledged at, and the marks in the order the
/// acknowledgements came
type Acked = (BTreeMap<(u64, u64), Vec<u8>>, Vec<(String, u64, u64)>);

/// Drive a slice on the logged device: whole chunks, then applies over them from every writer
/// at once, each acknowledgement marked in the log
///
/// # Arguments
///
/// * `logged` - The target
/// * `filesystem` - What it is
fn drive(logged: &Logged, filesystem: &str) -> Acked {
    let root = logged.mount.join("device");
    std::fs::create_dir_all(root.join("slice-0")).expect("the slice's directory");
    let name = logged.name.clone();
    let filesystem = filesystem.to_string();
    LocalExecutorBuilder::default()
        .spawn(move || async move {
            let device = Arc::new(DeviceState::new(
                DeviceId::mint(),
                DeviceSpec::new(&root, "ssd").apply_sync(ApplySync::Shared).journal(16 << 20).free_files(2),
                Facts {
                    filesystem,
                    ..Facts::default()
                },
                0,
                ApplySync::Shared,
                vec![(SliceId::mint(), root.join("slice-0"))],
                Vec::new(),
            ));
            let spec = device.spec.clone();
            let slice: Rc<Slice> = Slice::open(
                device.clone(),
                device.slices[0].clone(),
                SliceOptions {
                    segment_bytes: spec.segment_bytes(),
                    free_files: spec.free_files_kept(),
                    chunk_lengths: vec![CHUNK],
                    budget: None,
                    keep_previous: true,
                },
                None,
            )
            .await
            .expect("the slice opens");
            let mark = |mark: &str| {
                sudo(&["dmsetup", "message", &name, "0", "mark", mark]).expect("a mark is logged");
            };
            let mut contents: BTreeMap<(u64, u64), Vec<u8>> = BTreeMap::new();
            let mut marks = Vec::new();
            // every chunk whole first, each acknowledged
            for object in 0..WRITERS * CHUNKS_EACH {
                let data = bytes(object, CHUNK as usize);
                let label = Label { sequence: 1, tag: 0 };
                slice
                    .stage_whole(StageWhole {
                        id: chunk(object),
                        label,
                        chunk_bytes: CHUNK,
                        unit_bytes: UNIT,
                        length: CHUNK,
                        bytes: data.clone(),
                    })
                    .await
                    .expect("staged whole");
                slice.apply(chunk(object), label).await.expect("applied whole");
                let name = format!("ack-{object}-1");
                mark(&name);
                contents.insert((object, 1), data);
                marks.push((name, object, 1));
            }
            // then every writer at once, a unit at a time over its own chunks
            let base = contents.clone();
            let writers = (0..WRITERS).map(|writer| {
                let slice = slice.clone();
                let base = base.clone();
                let mark = &mark;
                async move {
                    let mut contents = Vec::new();
                    let mut now: BTreeMap<u64, (u64, Vec<u8>)> = (0..CHUNKS_EACH)
                        .map(|index| {
                            let object = writer * CHUNKS_EACH + index;
                            (object, (1, base[&(object, 1)].clone()))
                        })
                        .collect();
                    for apply in 0..APPLIES_EACH {
                        let object = writer * CHUNKS_EACH + apply % CHUNKS_EACH;
                        let (sequence, mut data) = now[&object].clone();
                        let unit = ((apply * 13 + writer) % u64::from(UNITS)) as u32;
                        let written = bytes(object << 32 | apply, UNIT as usize);
                        let next = sequence + 1;
                        slice
                            .stage_units(StageUnits {
                                id: chunk(object),
                                makes: Label { sequence: next, tag: 0 },
                                expects: Label { sequence, tag: 0 },
                                also: Vec::new(),
                                length: CHUNK,
                                units: vec![(unit, written.clone())],
                            })
                            .await
                            .expect("staged");
                        slice.apply(chunk(object), Label { sequence: next, tag: 0 }).await.expect("applied");
                        // acknowledged: marked in the log after everything that completed before it
                        let name = format!("ack-{object}-{next}");
                        mark(&name);
                        let at = (unit * UNIT) as usize;
                        data[at..at + UNIT as usize].copy_from_slice(&written);
                        now.insert(object, (next, data.clone()));
                        contents.push(((object, next), data, name));
                    }
                    contents
                }
            });
            for found in futures::future::join_all(writers).await {
                for (key, data, name) in found {
                    contents.insert(key, data);
                    marks.push((name, key.0, key.1));
                }
            }
            assert!(slice.flushes().0 > 0, "the shared flush never ran");
            slice.close().await;
            (contents, marks)
        })
        .expect("an executor")
        .join()
        .expect("the run")
}

/// Each chunk's label in a state of the device, and whether its bytes are what that label made
///
/// # Arguments
///
/// * `image` - The state's image
/// * `filesystem` - What it holds
/// * `dir` - Where it is mounted
/// * `contents` - What each chunk holds at each label
fn chunk_labels(image: &Path, filesystem: &str, dir: &Path, contents: &BTreeMap<(u64, u64), Vec<u8>>) -> BTreeMap<u64, u64> {
    let image_loop = sudo(&["losetup", "--find", "--show", &image.to_string_lossy()]).expect("a state's loop");
    std::fs::create_dir_all(dir).expect("a mount point");
    // mounted read and write, so the filesystem replays its own journal as a restart would
    let options = if filesystem == "xfs" { "nouuid" } else { "defaults" };
    let mounted = sudo(&["mount", "-o", options, &image_loop, &dir.to_string_lossy()]).is_some();
    let mut labels = BTreeMap::new();
    if mounted {
        let chunks = dir.join("device").join("slice-0").join("chunks");
        for object in 0..WRITERS * CHUNKS_EACH {
            let path = chunks.join(chunk(object).relative());
            let Ok(file) = std::fs::read(&path) else {
                continue;
            };
            let Ok(header) = ChunkHeader::decode(&file[..BLOCK.min(file.len())]) else {
                continue;
            };
            let label = header.label.sequence;
            // a label whose bytes are not the ones it made is no label at all
            let made = contents.get(&(object, label));
            if made.is_some_and(|made| file.len() >= BLOCK + made.len() && &file[BLOCK..BLOCK + made.len()] == made.as_slice()) {
                labels.insert(object, label);
            }
        }
        let _ = sudo(&["umount", &dir.to_string_lossy()]);
    }
    let _ = sudo(&["losetup", "-d", &image_loop]);
    assert!(mounted, "the state at a flush did not mount");
    labels
}

/// Every apply the shared flush acknowledged is in its chunk at the last flush before the
/// acknowledgement, on XFS and on ext4
#[test]
#[ignore]
fn shared_flush_covers_every_completed_apply() {
    if let Some(why) = missing() {
        eprintln!("skipped: {why}");
        return;
    }
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("a directory");
    for filesystem in ["xfs", "ext4"] {
        let mut logged = Logged::make(dir.path(), filesystem);
        logged.mark("mkfs");
        let (contents, marks) = drive(&logged, filesystem);
        // a closed slice holds nothing open on its device, or the device could not be unmounted
        // once its node stopped
        let open: Vec<PathBuf> = std::fs::read_dir("/proc/self/fd")
            .expect("this process's files")
            .flatten()
            .filter_map(|fd| std::fs::read_link(fd.path()).ok())
            .filter(|target| target.starts_with(&logged.mount))
            .collect();
        assert!(open.is_empty(), "{filesystem}: a closed slice still holds {open:?}");
        logged.down();
        // the log, and where each mark and flush sits in it
        let (sector_size, entries) = read_log(&logged.log);
        let at_mark: BTreeMap<String, usize> = entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| entry.mark.clone().map(|mark| (mark, index)))
            .collect();
        let flushes: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.flags & WRITE_FLUSH != 0)
            .map(|(index, _)| index)
            .collect();
        // each acknowledgement's state: everything before the last flush before it, and every
        // write the device made durable itself between that flush and it
        let mut states: BTreeMap<(usize, usize), Vec<(u64, u64)>> = BTreeMap::new();
        for (name, object, label) in &marks {
            let mark = at_mark[name];
            let flush = flushes.iter().copied().filter(|flush| *flush < mark).max().expect("a flush before every acknowledgement");
            let fua = entries[flush..mark]
                .iter()
                .rposition(|entry| entry.flags & WRITE_FUA != 0)
                .map_or(flush, |offset| flush + offset);
            states.entry((flush, fua)).or_default().push((*object, *label));
        }
        // each state built over a copy of the last, newest flush last, and judged
        let image = dir.path().join(format!("{filesystem}-state.img"));
        sparse(&image, DATA_BYTES);
        let mut laid = 0usize;
        let mut earlier: Option<BTreeMap<u64, u64>> = None;
        let mut held_back = 0usize;
        for ((flush, fua), acked) in &states {
            // the writes up to the flush, then the durable ones after it, on a copy
            let up_to: Vec<&Entry> = entries[laid..*flush].iter().collect();
            lay(&logged.log, &image, sector_size, &up_to);
            laid = *flush;
            let copy = dir.path().join(format!("{filesystem}-copy.img"));
            assert!(Command::new("cp").arg("--sparse=always").arg(&image).arg(&copy).status().expect("cp").success());
            let after: Vec<&Entry> = entries[*flush..=*fua].iter().filter(|entry| entry.flags & WRITE_FUA != 0).collect();
            lay(&logged.log, &copy, sector_size, &after);
            let labels = chunk_labels(&copy, filesystem, &dir.path().join(format!("{filesystem}-state")), &contents);
            let _ = std::fs::remove_file(&copy);
            // every apply acknowledged by now is there, at its label or a later one
            for (object, label) in acked {
                let found = labels.get(object).copied().unwrap_or(0);
                assert!(
                    found >= *label,
                    "{filesystem}: chunk {object} was acknowledged at label {label} and a crash at the flush before reads {found}"
                );
                // and the state one judged flush earlier did not hold it, for some apply
                if earlier.as_ref().is_some_and(|earlier| earlier.get(object).copied().unwrap_or(0) < *label) {
                    held_back += 1;
                }
            }
            earlier = Some(labels);
        }
        eprintln!(
            "{filesystem}: {} acknowledgements over {} states from {} entries and {} flushes; {held_back} were not yet durable a state earlier",
            marks.len(),
            states.len(),
            entries.len(),
            flushes.len()
        );
        // a replay that cannot tell one flush from the next would find every apply everywhere
        assert!(held_back > 0, "{filesystem}: no state lacked an apply the next one held, so the replay proved nothing");
    }
}
