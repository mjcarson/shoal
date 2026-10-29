//! A slice's pool of whole files a whole chunk is written into
//!
//! ~~A new file's `fdatasync` commits its allocation as well as its data, which on the lab's 970 EVO
//! cost 7.2 ms against 3.2 ms to overwrite the same bytes, so a whole chunk is written into a file
//! the slice wrote ahead with zeros and renamed into place
//! ([X6](../../../../../docs/src/object-storage/device-store-ssd.md#1-a-whole-chunk)).~~ Since
//! [F96](../../../../../docs/src/features/chunk-file-classes.md) nothing is written ahead. A whole
//! chunk takes a file of the class of units it holds that a removed chunk gave back, or a fresh one
//! made for it: the refill that wrote a file of zeros for every chunk taken cost each chunk its
//! file's length again, and [X15](../../../../../docs/src/object-storage/short-chunk-files.md) found
//! a fresh file synced by itself faster than a pool written ahead and its refill, at half the bytes.
//! The pool is `free/<file bytes>/<number>`, one pool for each file length, its fresh files made
//! there as `<number>.tmp`.
//!
//! # Invariants
//!
//! **A file enters the pool whole**: a removed chunk's file, renamed in, or a fresh file once every
//! byte of it is written and synced, renamed from its temporary name. A crash leaves a whole file
//! or a temporary one, which the next open removes, so no file of the pool has an extent its
//! writes left unwritten, and an overwrite of it is made durable by the shared flush.
//!
//! **A fresh file takes its space at the stage**, refused above the device's reserve before any
//! commit depends on it. A file a recycled chunk left holds old bytes, which never verify under
//! another chunk's identity (P15).
//!
//! **The pool keeps at most its target of each length**: a file given back past it is removed.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use super::{sync_dir, StoreError};

/// The files of one chunk length
#[derive(Debug, Default)]
struct Lengths {
    /// Files ready to take, by number
    available: BTreeSet<u64>,
    /// The number the next file made takes
    next: u64,
}

/// A slice's pool of files removed chunks gave back, where fresh files are made (F96)
#[derive(Debug)]
pub struct FreePool {
    /// The pool's directory, `<slice>/free`
    dir: PathBuf,
    /// The files kept of each length, at most
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
    /// * `target` - The files to keep of each length, at most
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

    /// Give a file back, its staged chunk discarded, or remove it once the pool holds its target
    /// of that length (F96)
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    /// * `number` - Its number
    pub fn give_back(&self, length: u64, number: u64) {
        let kept = {
            let mut lengths = self.lengths.borrow_mut();
            let pool = lengths.entry(length).or_default();
            // kept while the pool is short of its target
            let short = u32::try_from(pool.available.len()).unwrap_or(u32::MAX) < self.target;
            if short {
                pool.available.insert(number);
            }
            short
        };
        // past it, the file goes: nothing is kept ready that the device could use otherwise
        if !kept {
            let _ = std::fs::remove_file(self.path(length, number));
        }
    }

    /// A number for the file a removed chunk of this length gives back, while its pool is short of
    /// its target (M20)
    ///
    /// The caller renames the file in at [`FreePool::path`] and makes it durable, then gives it
    /// back; past the target it is unlinked instead, so the pool never grows past what it keeps
    /// ready. A file a removed chunk leaves holds that chunk's bytes, which never verify under
    /// another chunk's identity (P15).
    ///
    /// # Arguments
    ///
    /// * `length` - The file's length
    pub fn recycle(&self, length: u64) -> Option<u64> {
        let mut lengths = self.lengths.borrow_mut();
        // a file of any length, a class's as well as a whole chunk's, while its pool is short (F96)
        let pool = lengths.entry(length).or_default();
        if u32::try_from(pool.available.len()).unwrap_or(u32::MAX) >= self.target {
            return None;
        }
        let number = pool.next;
        pool.next += 1;
        Some(number)
    }

    /// How many files the pool holds ready, every length's together
    #[must_use]
    pub fn ready(&self) -> u64 {
        self.lengths
            .borrow()
            .values()
            .map(|pool| pool.available.len() as u64)
            .sum()
    }

    /// Keep this length's files from now on, the ones removed chunks give back
    ///
    /// # Arguments
    ///
    /// * `length` - The length
    pub fn serve(&self, length: u64) {
        self.lengths.borrow_mut().entry(length).or_default();
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

    /// A number and a temporary path for a fresh file of a length, made for one chunk alone
    ///
    /// The caller creates the file there, allocates and writes all of it, syncs it, and then either
    /// renames it over its chunk or adopts it into the pool ([`FreePool::adopt`]); a temporary file
    /// a crash leaves is removed at the next open (F96).
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    ///
    /// # Errors
    ///
    /// A length's directory the device could not make.
    pub async fn fresh(&self, length: u64) -> Result<(u64, PathBuf), StoreError> {
        // its number first, so two at once never take one name
        let number = {
            let mut lengths = self.lengths.borrow_mut();
            let pool = lengths.entry(length).or_default();
            let number = pool.next;
            pool.next += 1;
            number
        };
        // the length's directory, made durable in the pool's the first time
        let dir = self.dir.join(length.to_string());
        if !dir.exists() {
            std::fs::create_dir_all(&dir).map_err(|error| super::failed("making the free pool", error))?;
            sync_dir(&self.dir).await?;
        }
        Ok((number, self.temporary(length, number)))
    }

    /// Where a fresh file is made, before it is whole
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    /// * `number` - Its number
    #[must_use]
    pub fn temporary(&self, length: u64, number: u64) -> PathBuf {
        self.dir.join(length.to_string()).join(format!("{number:010}.tmp"))
    }

    /// Take a fresh file into the pool once every byte of it is written and synced, held by the
    /// caller rather than offered: a whole chunk staged in it, named by its record (F96)
    ///
    /// # Arguments
    ///
    /// * `length` - Its length
    /// * `number` - Its number
    ///
    /// # Errors
    ///
    /// A rename or a directory sync the device failed.
    pub async fn adopt(&self, length: u64, number: u64) -> Result<(), StoreError> {
        glommio::io::rename(self.temporary(length, number), self.path(length, number))
            .await
            .map_err(|error| super::failed("adopting a fresh file into the free pool", error))?;
        sync_dir(&self.dir.join(length.to_string())).await
    }
}
