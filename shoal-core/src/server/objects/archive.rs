//! A bucket's objects as a backup holds them: an archive for each `ObjectMeta` group
//! ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md))
//!
//! A backup of a bucket is a backup of its objects, never of its rows: a row restored without its
//! bytes is older than every chunk the moment one changes, which P10 refuses, and it strands a
//! striped object at once. So each `ObjectMeta` group's leader writes every live object it leads,
//! its entry whole and its bytes read at one state, to `<path>/<op>/<Bucket>/<group>.objects`, with
//! a JSON manifest of kind `objects` beside it, and a restore puts every object again through the
//! destination's coordinator, into the pool the destination binds the bucket to.
//!
//! ```text
//!  header   b"SHOALOBJ" | format u16 | reserved (6 B) | bucket id u64 | group u64 | op (16 B)
//!  record   entry length u32 | entry (JSON) | object length u64 | the object's bytes | CRC-64/NVME of them u64
//!  trailer  b"SHOALEND" | records u64 | CRC-64/NVME of every byte before the trailer u64
//! ```
//!
//! Every integer is little endian. The entry is the object's `ObjectEntry` as JSON, kept whole
//! except its inline bytes, which are the record's bytes; nothing else a cluster minted - no
//! placement state, pool map, device, slice, label or consumer id - is in it, and the entry's own
//! object id is the source's, which a restore does not keep.
//!
//! # Invariants
//!
//! **A record is in the archive whole or not at all.** A record a reader restarted on is cut back
//! to where it began before the next is written, and the file's checksum folds only records that
//! ended, so a crash leaves a `.tmp` and never a file that names a torn record.
//!
//! **The archive's I/O and its checksums never run on a shard's loop**: every file operation and
//! every checksum of a piece is done on the executor's blocking pool.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::RangeParts;
use crate::shared::checksum::{Chain, Crc64};
use crate::shared::identity::{BucketId, ClusterId, GroupId, NodeId};
use crate::shared::objects::ObjectEntry;

/// The bytes an archive starts with
pub const ARCHIVE_MAGIC: &[u8; 8] = b"SHOALOBJ";

/// The bytes an archive's trailer starts with
pub const ARCHIVE_END: &[u8; 8] = b"SHOALEND";

/// The archive format this build writes and reads
pub const ARCHIVE_FORMAT: u16 = 1;

/// What an archive's name ends with
pub const ARCHIVE_SUFFIX: &str = ".objects";

/// What its manifest's name ends with, beside it
pub const ARCHIVE_MANIFEST_SUFFIX: &str = ".objects.json";

/// The manifest's kind, which a table file's manifest never carries
pub const OBJECTS_KIND: &str = "objects";

/// How long an archive's header is
const HEADER_LEN: usize = 48;

/// How long its trailer is
const TRAILER_LEN: usize = 24;

/// The longest entry a record carries, past which an archive is not one this build reads
const MAX_ENTRY_BYTES: u32 = 1 << 20;

/// What a manifest beside an archive says of it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectsManifest {
    /// What the file is: `objects`, which a table file's manifest is not
    pub kind: String,
    /// The archive format
    pub format: u16,
    /// The backup operation
    pub op: Uuid,
    /// The cluster the archive was written in
    pub cluster: ClusterId,
    /// The node that wrote it
    pub origin: NodeId,
    /// The bucket, by its name
    pub bucket: String,
    /// The bucket's identity, the hash of its name
    pub bucket_id: BucketId,
    /// The `ObjectMeta` group whose objects it holds
    pub group: GroupId,
    /// The tablets that group served, which a restore judges the bucket's archives by: every
    /// tablet in exactly one archive, so a group whose archive failed is a gap refused by name;
    /// none in an archive written before they were named (item 315)
    #[serde(default)]
    pub tablets: Vec<u16>,
    /// The layout of the object rows its entries were written from
    pub rows_layout: u64,
    /// How many objects it holds
    pub records: u64,
    /// How many bytes of objects
    pub bytes: u64,
    /// How many bytes the file takes
    pub total: u64,
    /// The CRC-64/NVME of every byte of the file before its trailer
    pub checksum: u64,
    /// When it was written, in milliseconds since the Unix epoch
    pub created_ms: u64,
}

/// What a writer had written when it finished
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveTotals {
    /// Objects
    pub records: u64,
    /// Bytes of objects
    pub bytes: u64,
    /// Bytes of file
    pub total: u64,
    /// The CRC-64/NVME of every byte before the trailer
    pub checksum: u64,
}

/// One object as an archive holds it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedObject {
    /// Its entry, its inline bytes taken out
    pub entry: ObjectEntry,
    /// Its bytes
    pub bytes: Vec<u8>,
}

/// The record a writer is in the middle of
struct OpenRecord {
    /// The checksum of every byte of the record so far
    part: Crc64,
    /// How many bytes of the record so far
    len: u64,
    /// The checksum of the object's bytes so far
    bytes: Crc64,
    /// How many bytes of the object so far
    object_len: u64,
    /// How many bytes the record said the object has
    declared: u64,
}

/// An archive being written, a record at a time
pub struct ArchiveWriter {
    /// The file, which every operation takes to the blocking pool and gives back
    file: Option<std::fs::File>,
    /// Where it is written while it is written
    tmp: PathBuf,
    /// Where it lands when it is finished
    path: PathBuf,
    /// The checksum of every record that ended, and the header
    chain: Chain,
    /// Where the record being written began
    record_start: u64,
    /// The record being written, if one is
    record: Option<OpenRecord>,
    /// How many records ended
    records: u64,
    /// How many bytes of objects they hold
    bytes: u64,
}

/// Run a piece of file work on the executor's blocking pool
///
/// # Arguments
///
/// * `work` - The work
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    glommio::executor().spawn_blocking(work).await
}

impl ArchiveWriter {
    /// Create an archive at a path, its header written
    ///
    /// # Arguments
    ///
    /// * `path` - Where the archive lands
    /// * `bucket` - The bucket whose objects it holds
    /// * `group` - The group
    /// * `op` - The backup operation
    ///
    /// # Errors
    ///
    /// The directory or the file cannot be made.
    pub async fn create(
        path: &Path,
        bucket: BucketId,
        group: GroupId,
        op: Uuid,
    ) -> Result<Self, String> {
        // the header, in full
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(ARCHIVE_MAGIC);
        header.extend_from_slice(&ARCHIVE_FORMAT.to_le_bytes());
        header.extend_from_slice(&[0u8; 6]);
        header.extend_from_slice(&bucket.0.to_le_bytes());
        header.extend_from_slice(&group.0.to_le_bytes());
        header.extend_from_slice(op.as_bytes());
        // written under a temporary name until it is finished, its directory made first
        let tmp = path.with_extension("objects.tmp");
        let target = tmp.clone();
        let written = header.clone();
        let file = blocking(move || -> std::io::Result<std::fs::File> {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::File::create(&target)?;
            file.write_all(&written)?;
            Ok(file)
        })
        .await
        .map_err(|error| format!("creating {}: {error}", tmp.display()))?;
        // the file's checksum begins with the header
        let mut chain = Chain::new(1 << 20);
        chain.push_bytes(&header);
        Ok(ArchiveWriter {
            file: Some(file),
            tmp,
            path: path.to_path_buf(),
            chain,
            record_start: HEADER_LEN as u64,
            record: None,
            records: 0,
            bytes: 0,
        })
    }

    /// Write bytes to the file and fold them into the record being written
    ///
    /// # Arguments
    ///
    /// * `bytes` - What to write, as the parts they were read in
    /// * `object` - Whether they are the object's own bytes
    async fn write(&mut self, bytes: RangeParts, object: bool) -> Result<(), String> {
        // the file and the open record go to the blocking pool and come back
        let mut file = self.file.take().ok_or("the archive's file was lost")?;
        let Some(record) = self.record.take() else {
            self.file = Some(file);
            return Err("no record was begun".to_string());
        };
        // the write and both checksums of the piece, off the loop, a part at a time and a hole from
        // a block of zeros (F109)
        let (file, record, written) = blocking(move || {
            let mut record = record;
            let mut written = Ok(());
            for slice in bytes.slices() {
                written = file.write_all(slice);
                if written.is_err() {
                    break;
                }
                record.part.update(slice);
                record.len += slice.len() as u64;
                if object {
                    record.bytes.update(slice);
                    record.object_len += slice.len() as u64;
                }
            }
            (file, record, written)
        })
        .await;
        // both kept whatever the write came to
        self.file = Some(file);
        self.record = Some(record);
        written.map_err(|error| format!("writing {}: {error}", self.tmp.display()))
    }

    /// Begin a record: its entry and the length of the object to follow
    ///
    /// # Arguments
    ///
    /// * `entry` - The object's entry
    /// * `length` - How many bytes the object has
    ///
    /// # Errors
    ///
    /// A record is already open, the entry does not encode, or the file cannot be written.
    pub async fn begin(&mut self, entry: &ObjectEntry, length: u64) -> Result<(), String> {
        // one record at a time
        if self.record.is_some() {
            return Err("a record is already open".to_string());
        }
        // the entry whole but for its inline bytes, which are the record's own
        let mut kept = entry.clone();
        kept.inline = None;
        let json = serde_json::to_vec(&kept).map_err(|error| format!("encoding an entry: {error}"))?;
        let entry_len = u32::try_from(json.len()).map_err(|_| "an entry too long to archive".to_string())?;
        // the head: the entry's length, the entry, and the object's length
        let mut head = Vec::with_capacity(json.len() + 12);
        head.extend_from_slice(&entry_len.to_le_bytes());
        head.extend_from_slice(&json);
        head.extend_from_slice(&length.to_le_bytes());
        // the record open, its checksums empty, then its head written into it
        self.record = Some(OpenRecord {
            part: Crc64::new(),
            len: 0,
            bytes: Crc64::new(),
            object_len: 0,
            declared: length,
        });
        self.write(head.into(), false).await
    }

    /// Write a piece of the open record's object
    ///
    /// # Arguments
    ///
    /// * `bytes` - The piece, in a buffer or as the parts a read gathered it in (F109)
    ///
    /// # Errors
    ///
    /// No record is open, or the file cannot be written.
    pub async fn push(&mut self, bytes: impl Into<RangeParts>) -> Result<(), String> {
        self.write(bytes.into(), true).await
    }

    /// End the open record: its object's checksum, and the record folded into the file's
    ///
    /// # Errors
    ///
    /// No record is open, its object is not the length it said, or the file cannot be written.
    pub async fn end(&mut self) -> Result<(), String> {
        // the open record, whose object has to be the length its head declared
        let Some(record) = self.record.as_ref() else {
            return Err("no record is open".to_string());
        };
        if record.object_len != record.declared {
            return Err(format!(
                "a record said its object has {} bytes and was given {}",
                record.declared, record.object_len
            ));
        }
        // the object's checksum ends the record
        let crc = record.bytes.finish();
        self.write(crc.to_le_bytes().to_vec().into(), false).await?;
        // the record folded into the file's checksum and counted, and the next begins past it
        let record = self.record.take().ok_or("no record is open")?;
        self.chain.push(record.part.finish(), record.len);
        self.record_start += record.len;
        self.records += 1;
        self.bytes += record.object_len;
        Ok(())
    }

    /// Cut the open record back to where it began, as though it was never begun
    ///
    /// # Errors
    ///
    /// The file cannot be cut.
    pub async fn abort(&mut self) -> Result<(), String> {
        // nothing open, nothing to cut
        if self.record.take().is_none() {
            return Ok(());
        }
        // the file cut back to where the record began, and the next write placed there
        let mut file = self.file.take().ok_or("the archive's file was lost")?;
        let start = self.record_start;
        let (file, cut) = blocking(move || {
            let cut = file.set_len(start).and_then(|()| file.seek(SeekFrom::Start(start)).map(|_| ()));
            (file, cut)
        })
        .await;
        self.file = Some(file);
        cut.map_err(|error| format!("cutting {} back: {error}", self.tmp.display()))
    }

    /// Write the trailer, sync the file and put it where it lands
    ///
    /// # Errors
    ///
    /// A record is still open, or the file cannot be written, synced or renamed.
    pub async fn finish(mut self) -> Result<ArchiveTotals, String> {
        // every record ended or cut back
        if self.record.is_some() {
            return Err("a record is still open".to_string());
        }
        // the trailer: the end's magic, the count and the checksum of everything before it
        let checksum = self.chain.crc();
        let mut trailer = Vec::with_capacity(TRAILER_LEN);
        trailer.extend_from_slice(ARCHIVE_END);
        trailer.extend_from_slice(&self.records.to_le_bytes());
        trailer.extend_from_slice(&checksum.to_le_bytes());
        // written and synced, then renamed to where it lands and its directory synced
        let mut file = self.file.take().ok_or("the archive's file was lost")?;
        let (tmp, path) = (self.tmp.clone(), self.path.clone());
        blocking(move || -> std::io::Result<()> {
            file.write_all(&trailer)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(&tmp, &path)?;
            if let Some(parent) = path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })
        .await
        .map_err(|error| format!("finishing {}: {error}", self.path.display()))?;
        Ok(ArchiveTotals {
            records: self.records,
            bytes: self.bytes,
            total: self.record_start + TRAILER_LEN as u64,
            checksum,
        })
    }
}

/// Write an archive's manifest beside it, synced
///
/// # Arguments
///
/// * `archive` - The archive
/// * `manifest` - What it holds
///
/// # Errors
///
/// The manifest cannot be written.
pub async fn write_manifest(archive: &Path, manifest: &ObjectsManifest) -> Result<(), String> {
    // the manifest as JSON, beside the archive
    let json = serde_json::to_vec_pretty(manifest).map_err(|error| format!("encoding a manifest: {error}"))?;
    let path = manifest_path(archive);
    // written under a temporary name, synced, renamed, and its directory synced
    blocking(move || -> std::io::Result<()> {
        let tmp = path.with_extension("json.tmp");
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&json)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("writing the manifest of {}: {error}", archive.display()))
}

/// The path of an archive's manifest
///
/// # Arguments
///
/// * `archive` - The archive
#[must_use]
pub fn manifest_path(archive: &Path) -> PathBuf {
    // the archive's own name with `.json` after it
    let mut name = archive.as_os_str().to_owned();
    name.push(".json");
    PathBuf::from(name)
}

/// Read an archive's manifest
///
/// # Arguments
///
/// * `path` - The manifest
///
/// # Errors
///
/// It does not read, or it is not of an archive.
pub fn read_manifest(path: &Path) -> Result<ObjectsManifest, String> {
    // the file, read and decoded
    let bytes = std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    let manifest: ObjectsManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{} is not an objects manifest: {error}", path.display()))?;
    // and of an archive of objects, not a table's
    if manifest.kind != OBJECTS_KIND {
        return Err(format!("{} is a manifest of kind {}, not objects", path.display(), manifest.kind));
    }
    Ok(manifest)
}

/// An archive being read, a record at a time, on the blocking pool
pub struct ArchiveReader {
    /// The file
    file: Option<std::fs::File>,
    /// Where it is
    path: PathBuf,
    /// How many records are left
    left: u64,
    /// Where its records end: its trailer's offset
    end: u64,
}

impl ArchiveReader {
    /// Open an archive, its header and its trailer judged against its manifest, and every record
    /// read once and checked before any is handed out
    ///
    /// # Arguments
    ///
    /// * `path` - The archive
    /// * `manifest` - What its manifest says it holds
    ///
    /// # Errors
    ///
    /// The archive is not what its manifest says, byte for byte.
    pub async fn open(path: &Path, manifest: &ObjectsManifest) -> Result<Self, String> {
        // a format this build reads
        if manifest.format != ARCHIVE_FORMAT {
            return Err(format!(
                "{} is archive format {}, and this build reads {ARCHIVE_FORMAT}",
                path.display(),
                manifest.format
            ));
        }
        // the whole file verified off the loop, and handed back at its first record
        let target = path.to_path_buf();
        let expected = manifest.clone();
        let file = blocking(move || verify(&target, &expected)).await?;
        Ok(ArchiveReader {
            file: Some(file),
            path: path.to_path_buf(),
            left: manifest.records,
            // the file is the length its manifest says, which verify held
            end: manifest.total - TRAILER_LEN as u64,
        })
    }

    /// The next object, or none past the last
    ///
    /// # Errors
    ///
    /// The file cannot be read, a record runs past the archive's records, or an object fails its
    /// checksum.
    pub async fn next(&mut self) -> Result<Option<ArchivedObject>, String> {
        // every record handed out already
        if self.left == 0 {
            return Ok(None);
        }
        // the record read off the loop, the file handed back whatever came of it
        let mut file = self.file.take().ok_or("the archive's file was lost")?;
        let end = self.end;
        let (file, record) = blocking(move || {
            let record = read_record(&mut file, end);
            (file, record)
        })
        .await;
        self.file = Some(file);
        // its object held to its own checksum before it is handed out
        let (_, entry, bytes, crc) = record.map_err(|error| format!("reading {}: {error}", self.path.display()))?;
        if crate::shared::checksum::crc64(&bytes) != crc {
            return Err(format!("a record of {} fails its checksum", self.path.display()));
        }
        self.left -= 1;
        Ok(Some(ArchivedObject { entry, bytes }))
    }

    /// Skip records already put, as a restore resumed past them does
    ///
    /// # Arguments
    ///
    /// * `count` - How many
    ///
    /// # Errors
    ///
    /// The file cannot be read.
    pub async fn skip(&mut self, count: u64) -> Result<(), String> {
        // each read and dropped, up to the last record
        for _ in 0..count {
            if self.next().await?.is_none() {
                break;
            }
        }
        Ok(())
    }
}

/// Read one record at the file's position, refusing one that runs past the archive's records
///
/// A record's lengths are read from the file, so each is judged against the bytes left before
/// the trailer before anything is allocated for it: one flipped bit in an object's length asked
/// for the length it names, which panicked the blocking pool's one thread past `isize::MAX` and
/// aborted the node in the allocator below it
/// ([Resolved #264](../../../../docs/src/appendix/resolved/archive-record-length-bound.md)).
///
/// # Arguments
///
/// * `file` - The archive, at a record
/// * `end` - Where its records end: its trailer's offset
fn read_record(file: &mut std::fs::File, end: u64) -> std::io::Result<(Vec<u8>, ObjectEntry, Vec<u8>, u64)> {
    // a length that runs past the records is refused by name
    let past = |what: &str| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{what} runs past the archive's records"),
        )
    };
    // the entry's length, under the bound and within what is left beside the object's length
    let mut word = [0u8; 4];
    file.read_exact(&mut word)?;
    let entry_len = u32::from_le_bytes(word);
    if entry_len > MAX_ENTRY_BYTES {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "an entry past the bound"));
    }
    if u64::from(entry_len) + 8 > end.saturating_sub(file.stream_position()?) {
        return Err(past("an entry"));
    }
    // the entry itself
    let mut json = vec![0u8; entry_len as usize];
    file.read_exact(&mut json)?;
    let entry: ObjectEntry = serde_json::from_slice(&json)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    // the object's length, within what is left beside its checksum
    let mut long = [0u8; 8];
    file.read_exact(&mut long)?;
    let length = u64::from_le_bytes(long);
    let left = end.saturating_sub(file.stream_position()?);
    if length.checked_add(8).is_none_or(|needed| needed > left) {
        return Err(past("an object"));
    }
    // the object's bytes, then its checksum
    let mut bytes = vec![0u8; usize::try_from(length).map_err(|_| past("an object"))?];
    file.read_exact(&mut bytes)?;
    file.read_exact(&mut long)?;
    Ok((json, entry, bytes, u64::from_le_bytes(long)))
}

/// Read a whole archive once: its header, every record's checksum and its trailer against its
/// manifest, then hand the file back at its first record
///
/// # Arguments
///
/// * `path` - The archive
/// * `manifest` - What its manifest says
fn verify(path: &Path, manifest: &ObjectsManifest) -> Result<std::fs::File, String> {
    let fail = |why: String| format!("{} does not verify against its manifest: {why}", path.display());
    // the file, the length its manifest says and long enough for a header and a trailer
    let mut file = std::fs::File::open(path).map_err(|error| fail(error.to_string()))?;
    let length = file.metadata().map_err(|error| fail(error.to_string()))?.len();
    if length != manifest.total || length < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Err(fail(format!("it is {length} bytes and its manifest says {}", manifest.total)));
    }
    // the header names the bucket and the group its manifest does
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header).map_err(|error| fail(error.to_string()))?;
    if &header[..8] != ARCHIVE_MAGIC {
        return Err(fail("it is not an archive".to_string()));
    }
    let bucket = u64::from_le_bytes(header[16..24].try_into().unwrap_or_default());
    let group = u64::from_le_bytes(header[24..32].try_into().unwrap_or_default());
    if bucket != manifest.bucket_id.0 || group != manifest.group.0 {
        return Err(fail("its header names another bucket or group".to_string()));
    }
    let mut chain = Chain::new(1 << 20);
    chain.push_bytes(&header);
    // every record, its own checksum and its share of the file's
    let mut records = 0u64;
    let mut position = HEADER_LEN as u64;
    let end = length - TRAILER_LEN as u64;
    while position < end {
        let (json, entry, bytes, crc) = read_record(&mut file, end).map_err(|error| fail(error.to_string()))?;
        if crate::shared::checksum::crc64(&bytes) != crc {
            return Err(fail(format!("the record of {} fails its checksum", entry.path)));
        }
        // the record's bytes as written, folded into the file's checksum
        let mut part = Crc64::new();
        part.update(&u32::try_from(json.len()).unwrap_or(u32::MAX).to_le_bytes());
        part.update(&json);
        part.update(&(bytes.len() as u64).to_le_bytes());
        part.update(&bytes);
        part.update(&crc.to_le_bytes());
        let len = 4 + json.len() as u64 + 8 + bytes.len() as u64 + 8;
        chain.push(part.finish(), len);
        position += len;
        records += 1;
    }
    // the trailer counts them and checksums the rest
    let mut trailer = [0u8; TRAILER_LEN];
    file.read_exact(&mut trailer).map_err(|error| fail(error.to_string()))?;
    let counted = u64::from_le_bytes(trailer[8..16].try_into().unwrap_or_default());
    let checksum = u64::from_le_bytes(trailer[16..24].try_into().unwrap_or_default());
    if &trailer[..8] != ARCHIVE_END || counted != records || records != manifest.records {
        return Err(fail(format!("it holds {records} records and says {counted}, its manifest {}", manifest.records)));
    }
    if checksum != chain.crc() || checksum != manifest.checksum {
        return Err(fail("its checksum is not the one it was written with".to_string()));
    }
    // handed back at its first record
    file.seek(SeekFrom::Start(HEADER_LEN as u64)).map_err(|error| fail(error.to_string()))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::objects::ObjectEntry;

    /// An entry at a path, of a length
    ///
    /// # Arguments
    ///
    /// * `path` - Its path
    /// * `size` - Its length
    fn entry(path: &str, size: u64) -> ObjectEntry {
        ObjectEntry {
            path: path.to_string(),
            object: 7,
            size,
            ..ObjectEntry::default()
        }
    }

    /// An archive round trips every record that ended, never one a reader cut back, and a byte
    /// changed anywhere fails it by name
    #[test]
    fn an_archive_holds_every_record_that_ended() {
        let dir = std::env::temp_dir().join(format!("shoal-archive-{}", Uuid::new_v4()));
        let path = dir.join("Posters").join("g.objects");
        let executor = glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .make()
            .expect("an executor");
        let path_run = path.clone();
        executor.run(async move {
            let bucket = BucketId::of("Posters");
            let group = GroupId(9);
            let op = Uuid::new_v4();
            let mut writer = ArchiveWriter::create(&path_run, bucket, group, op).await.expect("created");
            // a record whole, a record cut back, and another whole
            writer.begin(&entry("a", 6), 6).await.expect("begun");
            writer.push(b"abc".to_vec()).await.expect("pushed");
            writer.push(b"def".to_vec()).await.expect("pushed");
            writer.end().await.expect("ended");
            writer.begin(&entry("torn", 3), 3).await.expect("begun");
            writer.push(b"xy".to_vec()).await.expect("pushed");
            writer.abort().await.expect("cut back");
            writer.begin(&entry("b", 2), 2).await.expect("begun");
            writer.push(b"gh".to_vec()).await.expect("pushed");
            writer.end().await.expect("ended");
            let totals = writer.finish().await.expect("finished");
            assert_eq!((totals.records, totals.bytes), (2, 8));
            let manifest = ObjectsManifest {
                kind: OBJECTS_KIND.to_string(),
                format: ARCHIVE_FORMAT,
                op,
                cluster: ClusterId::mint(),
                origin: NodeId::mint(),
                bucket: "Posters".to_string(),
                bucket_id: bucket,
                group,
                tablets: vec![0, 3, 6],
                rows_layout: 0,
                records: totals.records,
                bytes: totals.bytes,
                total: totals.total,
                checksum: totals.checksum,
                created_ms: 0,
            };
            write_manifest(&path_run, &manifest).await.expect("manifest");
            assert_eq!(read_manifest(&manifest_path(&path_run)).expect("reads"), manifest);
            // what was read back is what ended, in order
            let mut reader = ArchiveReader::open(&path_run, &manifest).await.expect("verifies");
            let first = reader.next().await.expect("reads").expect("a record");
            assert_eq!((first.entry.path.as_str(), first.bytes.as_slice()), ("a", &b"abcdef"[..]));
            let second = reader.next().await.expect("reads").expect("a record");
            assert_eq!((second.entry.path.as_str(), second.bytes.as_slice()), ("b", &b"gh"[..]));
            assert!(reader.next().await.expect("reads").is_none());
            // a byte changed in an object fails the archive
            let mut bytes = std::fs::read(&path_run).expect("reads");
            let at = bytes.windows(6).position(|window| window == b"abcdef").expect("the object");
            bytes[at] ^= 1;
            std::fs::write(&path_run, &bytes).expect("writes");
            let refused = ArchiveReader::open(&path_run, &manifest).await.err().expect("refused");
            assert!(refused.contains("fails its checksum"), "{refused}");
        });
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A record whose object length a flipped bit corrupted is refused by name, never allocated
    /// as the length it names, and so is one whose length runs past the records (item 264)
    #[test]
    fn a_corrupt_record_length_is_refused_before_it_is_allocated() {
        let dir = std::env::temp_dir().join(format!("shoal-archive-{}", Uuid::new_v4()));
        let path = dir.join("Posters").join("g.objects");
        let executor = glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .make()
            .expect("an executor");
        let path_run = path.clone();
        let manifest = executor.run(async move {
            // one record of four bytes, and its manifest
            let bucket = BucketId::of("Posters");
            let group = GroupId(9);
            let op = Uuid::new_v4();
            let mut writer = ArchiveWriter::create(&path_run, bucket, group, op).await.expect("created");
            writer.begin(&entry("a", 4), 4).await.expect("begun");
            writer.push(b"abcd".to_vec()).await.expect("pushed");
            writer.end().await.expect("ended");
            let totals = writer.finish().await.expect("finished");
            ObjectsManifest {
                kind: OBJECTS_KIND.to_string(),
                format: ARCHIVE_FORMAT,
                op,
                cluster: ClusterId::mint(),
                origin: NodeId::mint(),
                bucket: "Posters".to_string(),
                bucket_id: bucket,
                group,
                tablets: vec![0, 3, 6],
                rows_layout: 0,
                records: totals.records,
                bytes: totals.bytes,
                total: totals.total,
                checksum: totals.checksum,
                created_ms: 0,
            }
        });
        // the object's length sits just before its bytes
        let whole = std::fs::read(&path).expect("reads");
        let at = whole.windows(4).position(|window| window == b"abcd").expect("the object") - 8;
        assert_eq!(u64::from_le_bytes(whole[at..at + 8].try_into().expect("eight bytes")), 4);
        for length in [4u64 | 1 << 63, whole.len() as u64] {
            // the length changed in place, so the file is still the size its manifest says
            let mut bytes = whole.clone();
            bytes[at..at + 8].copy_from_slice(&length.to_le_bytes());
            std::fs::write(&path, &bytes).expect("writes");
            let refused = verify(&path, &manifest).err().expect("refused");
            assert!(refused.contains("does not verify against its manifest"), "{refused}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
