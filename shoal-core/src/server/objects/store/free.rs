//! A slice's pool of files written ahead, which whole chunks are written into
//!
//! A new file's `fdatasync` commits its allocation as well as its data, which on the lab's 970 EVO
//! cost 7.2 ms against 3.2 ms to overwrite the same bytes, so a whole chunk is written into a file
//! the slice wrote ahead with zeros and renamed into place
//! ([X6](../../../../../docs/src/object-storage/device-store-ssd.md#1-a-whole-chunk)). The pool is
//! `free/<file bytes>/<number>`, one pool for each chunk length the slice serves.
//!
//! # Invariants
//!
//! **A file enters the pool whole**: it is made under a temporary name, written to its length with
//! zeros, synced, and only then renamed in, so a crash leaves a whole file or a temporary one, which
//! the next open removes.
//!
//! **Refilling never breaches the device's reserve**: a stage takes no new space, since the pool is
//! written ahead, and the reserve is what gates writing the pool ahead. A file a recycled chunk
//! left holds old bytes, which never verify under another chunk's identity (P15).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::{open, sync_dir, zero_fill, StoreError};

/// The files of one chunk length
#[derive(Debug, Default)]
struct Lengths {
    /// Files ready to take, by number
    available: BTreeSet<u64>,
    /// The number the next file made takes
    next: u64,
}

/// A slice's pool of files written ahead
#[derive(Debug)]
pub struct FreePool {
    /// The pool's directory, `<slice>/free`
    dir: PathBuf,
    /// The files kept ready for each length
    target: u32,
    /// Each length's files
    lengths: RefCell<BTreeMap<u64, Lengths>>,
}

impl FreePool {
    /// Open a slice's pool, removing what a crash left half made
    ///
    /// # Arguments
    ///
    /// * `dir` - The pool's directory, made if it is not there
    /// * `target` - The files to keep ready for each length
    /// * `held` - Files a staged whole chunk holds, by length and number, which are not free
    ///
    /// # Errors
    ///
    /// A directory that cannot be read.
    pub fn open(dir: &Path, target: u32, held: &BTreeSet<(u64, u64)>) -> Result<Self, StoreError> {
        std::fs::create_dir_all(dir).map_err(|error| super::failed("making the free pool", error))?;
        let mut lengths: BTreeMap<u64, Lengths> = BTreeMap::new();
        for entry in std::fs::read_dir(dir).map_err(|error| super::failed("listing the free pool", error))? {
            let entry = entry.map_err(|error| super::failed("listing the free pool", error))?;
            let Some(length) = entry.file_name().to_str().and_then(|name| name.parse::<u64>().ok()) else {
                continue;
            };
            let pool = lengths.entry(length).or_default();
            for file in std::fs::read_dir(entry.path()).map_err(|error| super::failed("listing the free pool", error))? {
                let file = file.map_err(|error| super::failed("listing the free pool", error))?;
                let name = file.file_name().to_string_lossy().into_owned();
                // a file being made when a crash came is not a file of the pool
                if name.ends_with(".tmp") {
                    let _ = std::fs::remove_file(file.path());
                    continue;
                }
                let Ok(number) = name.parse::<u64>() else {
                    continue;
                };
                pool.next = pool.next.max(number + 1);
                // a whole file, unless a staged chunk holds it
                let whole = file.metadata().is_ok_and(|meta| meta.len() == length);
                if whole && !held.contains(&(length, number)) {
                    pool.available.insert(number);
                }
            }
        }
        Ok(FreePool {
            dir: dir.to_path_buf(),
            target,
            lengths: RefCell::new(lengths),
        })
    }

    /// Where a file of the pool is
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    /// * `number` - Its number
    #[must_use]
    pub fn path(&self, length: u64, number: u64) -> PathBuf {
        self.dir.join(length.to_string()).join(format!("{number:010}"))
    }

    /// Take a file of a length, if one is ready
    ///
    /// # Arguments
    ///
    /// * `length` - The length
    pub fn take(&self, length: u64) -> Option<u64> {
        let mut lengths = self.lengths.borrow_mut();
        let pool = lengths.entry(length).or_default();
        let number = pool.available.iter().next().copied()?;
        pool.available.remove(&number);
        Some(number)
    }

    /// Give a file back, its staged chunk discarded
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    /// * `number` - Its number
    pub fn give_back(&self, length: u64, number: u64) {
        self.lengths
            .borrow_mut()
            .entry(length)
            .or_default()
            .available
            .insert(number);
    }

    /// Keep this length ready from now on
    ///
    /// # Arguments
    ///
    /// * `length` - The length
    pub fn serve(&self, length: u64) {
        self.lengths.borrow_mut().entry(length).or_default();
    }

    /// The lengths short of their target, and by how many files
    #[must_use]
    pub fn wanted(&self) -> Vec<(u64, u32)> {
        self.lengths
            .borrow()
            .iter()
            .filter_map(|(length, pool)| {
                let have = u32::try_from(pool.available.len()).unwrap_or(u32::MAX);
                (have < self.target).then(|| (*length, self.target - have))
            })
            .collect()
    }

    /// The files ready, across every length
    #[must_use]
    pub fn available(&self) -> usize {
        self.lengths
            .borrow()
            .values()
            .map(|pool| pool.available.len())
            .sum()
    }

    /// Make one file of a length and put it in the pool
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    ///
    /// # Errors
    ///
    /// A file the device failed to make.
    pub async fn make_one(&self, length: u64) -> Result<u64, StoreError> {
        // made whole, then given to whoever takes next
        let number = self.make_held(length).await?;
        self.give_back(length, number);
        Ok(number)
    }

    /// Make one file of a length for the caller alone, never offered to the pool
    ///
    /// A stage that found its pool empty writes into the file it made, so no other stage can take
    /// it between the two; the file is the pool's again once its chunk is discarded.
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    ///
    /// # Errors
    ///
    /// A file the device failed to make.
    pub async fn make_held(&self, length: u64) -> Result<u64, StoreError> {
        // its number first, so two at once never take one name
        let number = {
            let mut lengths = self.lengths.borrow_mut();
            let pool = lengths.entry(length).or_default();
            let number = pool.next;
            pool.next += 1;
            number
        };
        let dir = self.dir.join(length.to_string());
        if !dir.exists() {
            std::fs::create_dir_all(&dir).map_err(|error| super::failed("making the free pool", error))?;
            sync_dir(&self.dir).await?;
        }
        let staged = dir.join(format!("{number:010}.tmp"));
        let file = open(&staged, true).await?;
        zero_fill(&file, length).await?;
        super::close(file).await;
        glommio::io::rename(&staged, self.path(length, number))
            .await
            .map_err(|error| super::failed("renaming a file into the free pool", error))?;
        sync_dir(&dir).await?;
        Ok(number)
    }
}
