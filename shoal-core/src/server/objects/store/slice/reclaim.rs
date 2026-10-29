//! A slice's seals, and its removal of what a committed fact frees (M20)
//!
//! A reclaimer frees two things a slice cannot judge alone: a retired object's chunks, and a
//! placement group the slice no longer holds. Labels cannot fence either - a put registered again
//! under an id writes at the put's label, and a group's chunks on a slice a move comes back to are
//! at the labels they had - so the reclaimer seals what it frees first, reads the committed fact
//! while it is sealed, and only then asks for the removal under its seal
//! ([S10](../../../../../../docs/src/object-storage/recovery.md#reclamation)). A sealed slice
//! defers every request for what is sealed until the seal ends: a reclamation under it, an unseal,
//! or its lapse on the slice's own clock. A stripe a truncate's floor hides is freed by a
//! discard through the sequence its tombstone committed past, which needs no seal: no write staged
//! before the tombstone can name a label at or below it again (X1's `Reclaim`).
//!
//! # Invariants
//!
//! **Nothing under a seal is removed but under that seal, while it is live.** A reclamation whose
//! seal lapsed or was taken by another is refused `NotSealed` and removes nothing, so a reclaimer's
//! word that arrives late never removes what was written after its read.
//!
//! **A sealed request waits; it is never refused for the seal.** A put or a copy that meets a seal
//! runs once the seal ends, and finds what the reclamation left.
//!
//! **A removed chunk's file goes back to its pool while the pool is short**, renamed whole so its
//! extents stay written, as the shared flush needs; past the pool's target it is unlinked. Nothing
//! here punches a hole.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use tracing::{event, Level};

use super::super::chunk::ChunkId;
use super::super::record::RecordKind;
use super::super::{close, failed, sync_dir, StoreError};
use super::{ChunkState, Slice};
use crate::server::devices::take_from;
use crate::shared::poolmap::PlacementGroup;
use crate::shared::protocol::peer::chunk::SealScope;

/// How long a seal holds on its slice's own clock when no reclamation or unseal ends it
///
/// A timer only ever ends a refusal here: past it the deferred requests run, and a reclamation
/// under the seal is refused, so a reclaimer that died leaves nothing waiting for long.
pub const SEAL_LAPSE: Duration = Duration::from_secs(30);

/// A seal a reclaimer set on part of a slice
pub(in crate::server::objects::store) struct Seal {
    /// Its id, which a reclamation under it names
    id: u64,
    /// When it lapses
    until: Instant,
    /// The requests waiting for it to end
    waiters: Vec<oneshot::Sender<()>>,
}

impl Slice {
    /// Seal what a scope covers under an id: every request for it waits until the seal ends
    ///
    /// A later seal of the same scope takes the earlier one's place, and its waiters wait for it.
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covers
    /// * `id` - The seal's id
    pub fn seal(&self, scope: SealScope, id: u64) {
        self.seal_for(scope, id, SEAL_LAPSE);
    }

    /// Seal what a scope covers under an id until a lapse other than [`SEAL_LAPSE`], for a test
    /// that watches one lapse
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covers
    /// * `id` - The seal's id
    /// * `lapse` - How long it holds when nothing ends it
    pub fn seal_for(&self, scope: SealScope, id: u64, lapse: Duration) {
        let until = Instant::now() + lapse;
        let mut seals = self.seals.borrow_mut();
        match seals.get_mut(&scope) {
            Some(seal) => {
                seal.id = id;
                seal.until = until;
            }
            None => {
                seals.insert(
                    scope,
                    Seal {
                        id,
                        until,
                        waiters: Vec::new(),
                    },
                );
            }
        }
    }

    /// End a seal without removing anything, if it is the one named
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covered
    /// * `id` - The seal's id
    pub fn unseal(&self, scope: SealScope, id: u64) -> bool {
        let ours = self.seals.borrow().get(&scope).is_some_and(|seal| seal.id == id);
        if ours {
            self.end_seal(&scope);
        }
        ours
    }

    /// End a seal and let every request waiting for it run
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covered
    fn end_seal(&self, scope: &SealScope) {
        let ended = self.seals.borrow_mut().remove(scope);
        if let Some(seal) = ended {
            for waiter in seal.waiters {
                let _ = waiter.send(());
            }
        }
    }

    /// The live seal over a chunk, if one covers it, ending any that lapsed on the way
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk
    fn seal_over(&self, chunk: &ChunkId) -> Option<(SealScope, Instant)> {
        // the chunk's object in its group, then its whole group
        let scopes = [
            SealScope::Object {
                consumer: chunk.consumer,
                pg: chunk.pg,
                object: chunk.object,
            },
            SealScope::Group {
                consumer: chunk.consumer,
                pg: chunk.pg,
            },
        ];
        for scope in scopes {
            let until = self.seals.borrow().get(&scope).map(|seal| seal.until);
            match until {
                Some(until) if until <= Instant::now() => self.end_seal(&scope),
                Some(until) => return Some((scope, until)),
                None => {}
            }
        }
        None
    }

    /// Refuse a write of a chunk whose object or group a reclamation removed from this slice within
    /// a seal's lapse: a piece in flight when the removal ran, or one that waited on its seal, would
    /// otherwise bring back what nothing refers to (item 253)
    ///
    /// One look at an empty table, which is every slice nearly always.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk a write would make
    ///
    /// # Errors
    ///
    /// `Removed` while such a removal is recent.
    pub fn refuse_removed(&self, chunk: &ChunkId) -> Result<(), StoreError> {
        if self.removed.borrow().is_empty() {
            return Ok(());
        }
        // the removals past their window are forgotten on the way
        let now = Instant::now();
        self.removed.borrow_mut().retain(|_, until| *until > now);
        let scopes = [
            SealScope::Object {
                consumer: chunk.consumer,
                pg: chunk.pg,
                object: chunk.object,
            },
            SealScope::Group {
                consumer: chunk.consumer,
                pg: chunk.pg,
            },
        ];
        if scopes.iter().any(|scope| self.removed.borrow().contains_key(scope)) {
            return Err(StoreError::Removed);
        }
        Ok(())
    }

    /// Wait until no live seal covers a chunk
    ///
    /// What every request naming a chunk waits on before it runs; a slice with no seal answers at
    /// once, which is every slice nearly always.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk
    pub async fn until_unsealed(&self, chunk: &ChunkId) {
        // nothing sealed is one look at an empty table
        if self.seals.borrow().is_empty() {
            return;
        }
        while let Some((scope, until)) = self.seal_over(chunk) {
            let (tx, rx) = oneshot::channel();
            if let Some(seal) = self.seals.borrow_mut().get_mut(&scope) {
                seal.waiters.push(tx);
            }
            // woken when the seal ends, or at its lapse, which the next look ends
            let wait = until.saturating_duration_since(Instant::now());
            let _ = glommio::timer::timeout(wait, async { Ok(rx.await) }).await;
        }
    }

    /// Remove everything a scope covers, under the seal named, and end the seal
    ///
    /// Answers how many chunks it removed. Every chunk's records and kept state go with it, and a
    /// reclaimed object's or group's chunks set aside for a checksum go too: nothing will rebuild
    /// them.
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covers
    /// * `id` - The seal it is removed under
    ///
    /// # Errors
    ///
    /// `NotSealed` when the seal lapsed or another took its place, a failed device, or a file
    /// that could not be removed.
    pub async fn reclaim(&self, scope: SealScope, id: u64) -> Result<u64, StoreError> {
        self.serving()?;
        // only under its own seal, while it is live
        let live = self
            .seals
            .borrow()
            .get(&scope)
            .is_some_and(|seal| seal.id == id && seal.until > Instant::now());
        if !live {
            return Err(StoreError::NotSealed);
        }
        let removed = self.remove_scope(&scope).await;
        // a write of what it removed is refused for a seal's lapse, so a piece that was on its
        // way, or waited on the seal, does not bring it back (item 253)
        if removed.is_ok() {
            self.removed.borrow_mut().insert(scope, Instant::now() + SEAL_LAPSE);
        }
        // the seal ends either way, and what waited on it runs and finds what is left
        self.end_seal(&scope);
        if let Ok(count) = &removed {
            event!(Level::DEBUG, msg = "a slice reclaimed what a seal covered", scope = ?scope, chunks = count);
        }
        removed
    }

    /// Remove a chunk if the label it is at is at or below a sequence, and its records at or below
    /// it: a stripe a reclamation's tombstone committed past (M20)
    ///
    /// Answers whether it removed the chunk.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `through` - The sequence the tombstone committed past
    ///
    /// # Errors
    ///
    /// A failed device, or a file that could not be removed.
    pub async fn discard_through(&self, id: ChunkId, through: u64) -> Result<bool, StoreError> {
        self.serving()?;
        let removed = self.remove_chunk(&id, Some(through)).await?;
        if removed {
            self.remove_empty(&id).await?;
        }
        Ok(removed)
    }

    /// Remove every chunk a scope covers
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covers
    async fn remove_scope(&self, scope: &SealScope) -> Result<u64, StoreError> {
        // the chunks on the device, then any staged or set aside that have no file
        let mut ids: Vec<ChunkId> = self.chunks_in(scope)?;
        ids.extend(self.staged.borrow().keys().filter(|id| scope.covers(id)).copied());
        ids.extend(self.quarantine.borrow().keys().filter(|id| scope.covers(id)).copied());
        ids.sort_unstable();
        ids.dedup();
        let mut removed = 0;
        for id in &ids {
            if self.remove_chunk(id, None).await? {
                removed += 1;
            }
            glommio::yield_if_needed().await;
        }
        // the directories that held them, emptied
        if let Some(first) = ids.first() {
            self.remove_empty(first).await?;
        }
        if let SealScope::Group { .. } = scope {
            for id in &ids {
                self.remove_empty(id).await?;
            }
            // and the group's own directory if nothing is left in it, so no sweep lists it again;
            // nothing of the group is written while it is sealed
            let group_dir = self
                .chunk_path(&scope.as_chunk())
                .parent()
                .and_then(Path::parent)
                .map(Path::to_path_buf);
            if let Some(group_dir) = group_dir {
                let empty = std::fs::read_dir(&group_dir).is_ok_and(|mut entries| entries.next().is_none());
                if empty {
                    std::fs::remove_dir(&group_dir).map_err(|error| failed("removing an emptied group's directory", error))?;
                    if let Some(up) = group_dir.parent() {
                        sync_dir(up).await?;
                    }
                }
            }
        }
        Ok(removed)
    }

    /// Every chunk file a scope covers, by its path: an object's directory in its group, or every
    /// object's of the group
    ///
    /// # Arguments
    ///
    /// * `scope` - What it covers
    fn chunks_in(&self, scope: &SealScope) -> Result<Vec<ChunkId>, StoreError> {
        let probe = scope.as_chunk();
        // the group's directory, two above a chunk's file
        let group_dir = self
            .chunk_path(&probe)
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .ok_or_else(|| StoreError::Invalid("a chunk's path has no group directory".to_string()))?;
        let objects: Vec<(u128, PathBuf)> = match scope {
            SealScope::Object { object, .. } => vec![(*object, group_dir.join(format!("{object:032x}")))],
            SealScope::Group { .. } => {
                if !group_dir.exists() {
                    return Ok(Vec::new());
                }
                std::fs::read_dir(&group_dir)
                    .map_err(|error| failed("listing a placement group", error))?
                    .filter_map(Result::ok)
                    .filter_map(|entry| {
                        let name = entry.file_name();
                        let object = u128::from_str_radix(name.to_str()?, 16).ok()?;
                        Some((object, entry.path()))
                    })
                    .collect()
            }
        };
        let mut ids = Vec::new();
        for (object, dir) in objects {
            if !dir.exists() {
                continue;
            }
            for file in std::fs::read_dir(&dir).map_err(|error| failed("listing an object's chunks", error))? {
                let file = file.map_err(|error| failed("listing an object's chunks", error))?;
                let name = file.file_name();
                // a chunk's file is its stripe and its position
                let Some((stripe, position)) = name.to_str().and_then(|name| name.split_once('.')) else {
                    continue;
                };
                let (Ok(stripe), Ok(position)) = (u64::from_str_radix(stripe, 16), position.parse::<u8>()) else {
                    continue;
                };
                ids.push(ChunkId {
                    object,
                    stripe,
                    position,
                    ..probe
                });
            }
        }
        Ok(ids)
    }

    /// Remove one chunk under its write lock: its file, its records and its kept state, every one
    /// for a reclamation, and only at or below a sequence for a discard through it
    ///
    /// Answers whether its file was removed.
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `through` - The sequence a discard is bounded by, none for a reclamation
    async fn remove_chunk(&self, id: &ChunkId, through: Option<u64>) -> Result<bool, StoreError> {
        let lock = self.lock_of(id);
        let _write = lock.write().await.map_err(|error| failed("locking a chunk", error))?;
        // its records first, so no apply makes it again
        self.drop_records(id, through);
        let path = self.chunk_path(id);
        let removed = match self.chunk_state(id).await? {
            ChunkState::At(header, file) => {
                close(file).await;
                if through.is_none_or(|through| header.label.sequence <= through) {
                    self.drop_previous(id);
                    // and what its executor's cache held of it (F102)
                    self.uncache(id);
                    // the file's own length, its class's, which its pool is kept by (F96)
                    let bytes = std::fs::metadata(&path).map_or(header.file_bytes(), |meta| meta.len());
                    self.free_file(&path, bytes).await?;
                    take_from(&self.state.chunks, 1);
                    take_from(&self.state.chunk_bytes, bytes);
                    true
                } else {
                    false
                }
            }
            // a header that does not read is a reclaimed object's all the same
            ChunkState::Torn if through.is_none() => {
                self.drop_previous(id);
                self.uncache(id);
                let bytes = std::fs::metadata(&path).map_or(0, |meta| meta.len());
                std::fs::remove_file(&path).map_err(|error| failed("removing a reclaimed chunk", error))?;
                if let Some(dir) = path.parent() {
                    sync_dir(dir).await?;
                }
                take_from(&self.state.chunks, 1);
                take_from(&self.state.chunk_bytes, bytes);
                true
            }
            ChunkState::Torn | ChunkState::Missing => false,
        };
        // the evidence a reclaimed chunk left goes with it: no replacement will ever verify there
        if through.is_none() {
            let gone = self.quarantine.borrow_mut().remove(id).unwrap_or_default();
            for set_aside in &gone {
                let _ = std::fs::remove_file(&set_aside.path);
            }
            if let Some(dir) = gone.first().and_then(|first| first.path.parent()) {
                sync_dir(dir).await?;
            }
            take_from(&self.state.quarantined, gone.len() as u64);
        }
        Ok(removed)
    }

    /// Drop a chunk's records: every one, or those making a label at or below a sequence
    ///
    /// # Arguments
    ///
    /// * `id` - The chunk
    /// * `through` - The sequence, none for every record
    fn drop_records(&self, id: &ChunkId, through: Option<u64>) {
        let gone = {
            let mut staged = self.staged.borrow_mut();
            let Some(list) = staged.get_mut(id) else {
                return;
            };
            let (dropped, kept): (Vec<_>, Vec<_>) = std::mem::take(list)
                .into_iter()
                .partition(|record| through.is_none_or(|through| record.record.makes.sequence <= through));
            if kept.is_empty() {
                staged.remove(id);
            } else {
                *list = kept;
            }
            dropped
        };
        for record in gone {
            self.journal.release(record.location);
            self.state.staged_bytes.fetch_sub(record.bytes(), Ordering::Relaxed);
            if let RecordKind::Whole { file_bytes, file } = record.record.kind {
                self.free.give_back(file_bytes, file);
            }
        }
    }

    /// Give a removed chunk's file back to its pool while the pool is short, unlink it otherwise
    ///
    /// # Arguments
    ///
    /// * `path` - The chunk's file
    /// * `file_bytes` - Its length
    async fn free_file(&self, path: &Path, file_bytes: u64) -> Result<(), StoreError> {
        match self.free.recycle(file_bytes) {
            Some(number) => {
                // renamed in whole, so its extents stay written, then given back once durable
                let target = self.free.path(file_bytes, number);
                if let Some(dir) = target.parent() {
                    std::fs::create_dir_all(dir).map_err(|error| failed("making a pool's directory", error))?;
                }
                glommio::io::rename(path, &target)
                    .await
                    .map_err(|error| failed("giving a reclaimed chunk's file to its pool", error))?;
                for dir in [path.parent(), target.parent()].into_iter().flatten() {
                    sync_dir(dir).await?;
                }
                self.free.give_back(file_bytes, number);
            }
            None => {
                std::fs::remove_file(path).map_err(|error| failed("removing a reclaimed chunk", error))?;
                if let Some(dir) = path.parent() {
                    sync_dir(dir).await?;
                }
            }
        }
        Ok(())
    }

    /// Remove a chunk's object directory, then its group's, each only once it is empty
    ///
    /// # Arguments
    ///
    /// * `id` - A chunk that was in them
    async fn remove_empty(&self, id: &ChunkId) -> Result<(), StoreError> {
        let path = self.chunk_path(id);
        let chunks = self.state.path.join("chunks");
        let mut dir = path.parent().map(Path::to_path_buf);
        // the object's directory, then the group's, then the consumer's, never the root
        for _ in 0..3 {
            let Some(here) = dir.clone() else {
                break;
            };
            if here == chunks || !here.exists() {
                break;
            }
            let empty = std::fs::read_dir(&here).map_or(false, |mut entries| entries.next().is_none());
            if !empty {
                break;
            }
            std::fs::remove_dir(&here).map_err(|error| failed("removing an emptied directory", error))?;
            if let Some(up) = here.parent() {
                sync_dir(up).await?;
            }
            dir = here.parent().map(Path::to_path_buf);
        }
        Ok(())
    }

    /// Every placement group this slice has a directory of, by its consumer
    ///
    /// What a sweep asks a slice, to find a group's chunks on a slice the group no longer places on
    /// or names left: the late pieces a release's window did not catch (item 253).
    ///
    /// # Errors
    ///
    /// A `chunks/` directory that does not list.
    pub fn groups(&self) -> Result<Vec<(u64, PlacementGroup)>, StoreError> {
        let chunks = self.state.path.join("chunks");
        let mut found = Vec::new();
        // the consumers' directories, each `<consumer>` in hex
        let consumers = match std::fs::read_dir(&chunks) {
            Ok(consumers) => consumers,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(found),
            Err(error) => return Err(failed("listing a slice's consumers", error)),
        };
        for consumer in consumers.filter_map(Result::ok) {
            let Some(id) = consumer.file_name().to_str().and_then(|name| u64::from_str_radix(name, 16).ok()) else {
                continue;
            };
            let Ok(groups) = std::fs::read_dir(consumer.path()) else {
                continue;
            };
            // each group's directory, `<tablet>-<sub>` in hex; one left empty is listed too, and a
            // removal of its group takes it
            for group in groups.filter_map(Result::ok) {
                let name = group.file_name();
                let Some((tablet, sub)) = name.to_str().and_then(|name| name.split_once('-')) else {
                    continue;
                };
                let (Ok(tablet), Ok(sub)) = (u16::from_str_radix(tablet, 16), u16::from_str_radix(sub, 16)) else {
                    continue;
                };
                found.push((id, PlacementGroup { tablet, sub }));
            }
        }
        Ok(found)
    }
}
