//! The fixture's storage faults, held to what a real device does ([F70](../../docs/src/features/storage-faults.md))
//!
//! A full disk and a lost device are armed in process by `shoal::server::faults`, through a hook
//! every glommio file asks. That is only worth testing against if it answers as the kernel does,
//! so this puts a loop device behind a device-mapper target, fails it for real - a filesystem
//! that fills, and a table swapped for `error` - and holds the same operations under the fault
//! armed in process to the same answers.
//!
//! The comparison is of operations and not of a server's reaction. Which of a server's files
//! meets a full disk first is an accident of timing, and each reaction is the subject of a test
//! of its own (`device_faults_do_what_they_say` in the fixture).
//!
//! It needs `sudo` without a password and `losetup`, `dmsetup` and `mkfs.ext4`, and is ignored
//! by default; without them it says so by name and passes. Run it with
//! `cargo test -p shoal --test kernel_faults -- --ignored --nocapture`.

use shoal::glommio::io::{DmaFile, OpenOptions};
use shoal::glommio::LocalExecutor;
use shoal::server::faults::{self, Fault};
use std::path::{Path, PathBuf};
use std::process::Command;

mod utils;

/// How large each device is
const DEVICE_BYTES: u64 = 64 * 1024 * 1024;

/// How large each write that fills a device is
const CHUNK: usize = 1024 * 1024;

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

/// A loop device behind a device-mapper target, with ext4 on it, mounted for this test
///
/// Torn down on drop whatever state the test left it in.
struct Device {
    /// The device-mapper name
    name: String,
    /// The loop device
    loop_device: String,
    /// The file behind it
    backing: PathBuf,
    /// Where the filesystem is mounted
    mount: PathBuf,
    /// The device's size in sectors, for its tables
    sectors: u64,
}

impl Device {
    /// Make a device, or say why this machine cannot
    ///
    /// # Arguments
    ///
    /// * `dir` - Where the backing file and the mount point go
    /// * `tag` - What distinguishes this device's names from another's
    fn make(dir: &Path, tag: &str) -> Result<Self, String> {
        // passwordless sudo and the tools, or the test is skipped by name
        if sudo(&["true"]).is_none() {
            return Err("sudo needs a password here".to_string());
        }
        for tool in ["losetup", "dmsetup", "mkfs.ext4"] {
            if sudo(&["which", tool]).is_none() {
                return Err(format!("{tool} is not installed"));
            }
        }
        // the backing file, sized and empty
        let backing = dir.join(format!("{tag}.img"));
        std::fs::File::create(&backing)
            .and_then(|file| file.set_len(DEVICE_BYTES))
            .map_err(|error| format!("the backing file: {error}"))?;
        let loop_device = sudo(&["losetup", "--find", "--show", &backing.to_string_lossy()])
            .ok_or("losetup refused the backing file")?;
        let sectors = DEVICE_BYTES / 512;
        let device = Device {
            name: format!("shoal-fault-{}-{tag}", std::process::id()),
            loop_device,
            backing,
            mount: dir.join(format!("{tag}-mount")),
            sectors,
        };
        // a linear table over the loop device, which a lost device swaps for `error`
        let table = format!("0 {sectors} linear {} 0", device.loop_device);
        sudo(&["dmsetup", "create", &device.name, "--table", &table])
            .ok_or("dmsetup refused the table")?;
        let mapped = format!("/dev/mapper/{}", device.name);
        sudo(&["mkfs.ext4", "-q", "-m", "0", &mapped]).ok_or("mkfs.ext4 failed")?;
        std::fs::create_dir_all(&device.mount).map_err(|error| error.to_string())?;
        sudo(&["mount", &mapped, &device.mount.to_string_lossy()]).ok_or("mount failed")?;
        // the mount belongs to whoever runs the test, so it can write there
        // SAFETY: getuid and getgid read the calling process's ids and cannot fail
        let owner = format!("{}:{}", unsafe { libc::getuid() }, unsafe {
            libc::getgid()
        });
        sudo(&["chown", &owner, &device.mount.to_string_lossy()]).ok_or("chown failed")?;
        Ok(device)
    }

    /// Swap the device's table for one that fails every I/O, as a device that stopped answering
    fn lose(&self) {
        let table = format!("0 {} error", self.sectors);
        assert!(sudo(&["dmsetup", "suspend", "--nolockfs", &self.name]).is_some());
        assert!(sudo(&["dmsetup", "load", &self.name, "--table", &table]).is_some());
        assert!(sudo(&["dmsetup", "resume", &self.name]).is_some());
    }
}

impl Drop for Device {
    /// Unmount, remove the target and the loop device, and delete the backing file
    fn drop(&mut self) {
        let _ = sudo(&["umount", "-l", &self.mount.to_string_lossy()]);
        let _ = sudo(&["dmsetup", "remove", "--force", &self.name]);
        let _ = sudo(&["losetup", "-d", &self.loop_device]);
        let _ = std::fs::remove_file(&self.backing);
    }
}

/// The OS error a glommio operation failed with, or none for a success
///
/// # Arguments
///
/// * `result` - What the operation returned
fn errno<T>(result: shoal::glommio::Result<T, ()>) -> Option<i32> {
    // the os error itself, which the conversion to an io error does not keep
    result.err().map(|error| match error {
        shoal::glommio::GlommioError::IoError(source)
        | shoal::glommio::GlommioError::EnhancedIoError { source, .. } => {
            source.raw_os_error().unwrap_or(-1)
        }
        _ => -1,
    })
}

/// Fill a directory with direct writes until one fails, then rewrite the first block
///
/// Returns how many writes landed, the error the failing one met, and what a rewrite of space
/// the file already holds met, which a full disk allows.
///
/// # Arguments
///
/// * `dir` - Where the file goes
/// * `before` - Run once the file exists and before it is filled
fn fill(dir: &Path, before: impl FnOnce() + 'static) -> (u64, Option<i32>, Option<i32>) {
    let path = dir.join("fill");
    LocalExecutor::default().run(async move {
        let file = DmaFile::create(&path).await.expect("the file is made");
        before();
        // chunk after chunk, until the disk says no
        let mut landed = 0u64;
        let failed = loop {
            let mut buf = file.alloc_dma_buffer(CHUNK);
            buf.as_bytes_mut().fill(7);
            match errno(file.write_at(buf, landed * CHUNK as u64).await) {
                None if landed * (CHUNK as u64) < DEVICE_BYTES * 2 => landed += 1,
                outcome => break outcome,
            }
        };
        // a rewrite of the first block takes no new space
        let mut buf = file.alloc_dma_buffer(4096);
        buf.as_bytes_mut().fill(8);
        let rewrite = errno(file.write_at(buf, 0).await);
        let _ = file.close().await;
        (landed, failed, rewrite)
    })
}

/// Write and sync a block, lose the device, and try a write, a sync and a read of it
///
/// Returns the error each of the three met.
///
/// # Arguments
///
/// * `dir` - Where the file goes
/// * `lose` - What makes the device answer nothing
fn after_loss(dir: &Path, lose: impl FnOnce() + 'static) -> [Option<i32>; 3] {
    let path = dir.join("lost");
    LocalExecutor::default().run(async move {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await
            .expect("the file is made");
        let mut buf = file.alloc_dma_buffer(4096);
        buf.as_bytes_mut().fill(1);
        assert_eq!(
            errno(file.write_at(buf, 0).await),
            None,
            "a write before the loss"
        );
        assert_eq!(
            errno(file.fdatasync().await),
            None,
            "a sync before the loss"
        );
        lose();
        // a write, a sync and a direct read, each of which has to reach the device
        let mut buf = file.alloc_dma_buffer(4096);
        buf.as_bytes_mut().fill(2);
        let write = errno(file.write_at(buf, 4096).await);
        let sync = errno(file.fdatasync().await);
        let read = errno(file.read_at_aligned(0, 4096).await);
        let _ = file.close().await;
        [write, sync, read]
    })
}

/// A full disk and a lost device answer the same operations the same way armed in process as
/// they do on a real device
#[test]
#[ignore]
fn device_faults_on_a_real_device_match() {
    let dir = utils::test_dir();
    // a full disk: a filesystem of 64 MiB, filled a mebibyte at a time
    let device = match Device::make(dir.path(), "full") {
        Ok(device) => device,
        Err(why) => {
            eprintln!("skipping device_faults_on_a_real_device_match: {why}");
            return;
        }
    };
    let (landed, kernel_full, kernel_rewrite) = fill(&device.mount, || {});
    drop(device);
    // the same budget the device had, armed in process
    let armed = utils::test_dir();
    let root = armed.path().to_path_buf();
    let budget = landed * CHUNK as u64;
    let arm_root = root.clone();
    let (armed_landed, armed_full, armed_rewrite) = fill(&root, move || {
        faults::arm(&arm_root, Fault::Full { budget })
    });
    faults::clear(&root);
    eprintln!(
        "full: a device took {landed} MiB and refused with {kernel_full:?}, a rewrite {kernel_rewrite:?}; \
         in process {armed_landed} MiB, {armed_full:?}, a rewrite {armed_rewrite:?}"
    );
    assert_eq!(
        kernel_full,
        Some(libc::ENOSPC),
        "a full device refused otherwise"
    );
    assert_eq!(
        armed_full, kernel_full,
        "a full disk armed in process refused otherwise"
    );
    assert_eq!(
        armed_landed, landed,
        "a full disk armed in process took another amount"
    );
    assert_eq!(
        armed_rewrite, kernel_rewrite,
        "a rewrite on a full disk was answered otherwise"
    );
    // a lost device: the table swapped for `error` under an open file
    let device = Device::make(dir.path(), "lost").expect("the second device");
    let name = device.name.clone();
    let sectors = device.sectors;
    let kernel_lost = after_loss(&device.mount, move || {
        // the same swap `Device::lose` makes, by name, since the device outlives this closure
        let table = format!("0 {sectors} error");
        assert!(sudo(&["dmsetup", "suspend", "--nolockfs", &name]).is_some());
        assert!(sudo(&["dmsetup", "load", &name, "--table", &table]).is_some());
        assert!(sudo(&["dmsetup", "resume", &name]).is_some());
    });
    drop(device);
    let armed = utils::test_dir();
    let root = armed.path().to_path_buf();
    let arm_root = root.clone();
    let armed_lost = after_loss(&root, move || faults::arm(&arm_root, Fault::Lost));
    faults::clear(&root);
    eprintln!("lost (write, sync, read): a device {kernel_lost:?}, in process {armed_lost:?}");
    assert!(
        kernel_lost.iter().all(Option::is_some),
        "a lost device answered something: {kernel_lost:?}"
    );
    assert_eq!(
        armed_lost, kernel_lost,
        "a lost device armed in process answered otherwise"
    );
}
