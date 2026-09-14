//! A tablet group's snapshot: a stable cut of its rows at a boundary, as one file
//!
//! A follower behind the purge point cannot be fed from the log, so its leader sends it the
//! rows instead ([F43](../../../../docs/src/features/node-recovery.md)). The rows of a
//! persistent group are its table's archives, and the cut is the compactor's: between two of
//! its segment jobs the archives are exactly the state after every frame it has merged, so a
//! file it writes there is a cut at one index of the group's log and nothing else. A volatile
//! group's rows are its ephemeral table's resident partitions, cut by the shard loop, which
//! is the one place they are all visible at once.
//!
//! ```text
//! version 1:
//! [magic 8 B "SHOALSNP"][version u8][table u64][group u64][boundary index u64][records u64]
//! version 2 (F48, written once the cluster has activated wire version 5):
//! [magic 8 B "SHOALSNP"][version u8][table u64][group u64][boundary index u64][records u64]
//! [cluster 16 B][schema id u64][created ms u64]
//! [record]*  : [key u64][len u32][rkyv partition bytes]     - the archive's own record shape
//! [trailer]  : [len u32][retries postcard: Vec<(RequestId, Remembered)>]
//! ```
//!
//! The manifest ([`SnapshotManifest`]) travels beside the file rather than inside it: in the
//! begin RPC on the sender's side and in the pending marker on the receiver's, so a file is
//! never read to find out what it is. The checksum is a [`FileHasher`] fold over every byte of
//! the file in fixed blocks, computed as the file is written and as it is assembled, so a
//! receiver verifies a stream without reading it back and however the bytes were chunked.
//!
//! **The manifest is the one body whose encoding differs between wire versions 4 and 5**
//! ([F48](../../../../docs/src/features/rolling-compatibility.md)): at 5 it carries the
//! cluster, the node that cut it and when, which a backup file needs to identify itself; at 4
//! it is the record every build before F48 reads ([`ManifestV4`]). A begin RPC is encoded at
//! the version its link negotiated and decoded at the version its frame names, so a build at
//! 5 feeds a build at 4 and the other way about, and the receiver of a v4 manifest fills the
//! three fields with what it can - its own cluster, and the sender it heard from.
//!
//! **Absence is total.** A snapshot names the tablets it covers, and every partition of those
//! tablets not in the file is removed on install. That is what lets a delete travel: a row
//! deleted before the boundary is simply not there.

use std::io;
use std::path::{Path, PathBuf};

use glommio::io::{BufferedFile, Directory};
use openraft::type_config::alias::StoredMembershipOf;
use serde::{Deserialize, Serialize};

use super::types::{DataConfig, Remembered};
use crate::server::wal::{Vote, WalLogId};
use crate::shared::identity::{ClusterId, GroupId, NodeId, TableId};
use crate::shared::protocol::peer::RequestId;

/// The first eight bytes of every snapshot file
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"SHOALSNP";

/// The oldest file format this build reads and writes
pub const SNAPSHOT_VERSION: u8 = 1;

/// The file format that identifies its cluster, written once wire version 5 is activated
///
/// The header grows by the cluster, the schema id and the cut's time, which is what a backup
/// file is judged by before its bytes are trusted
/// ([F48](../../../../docs/src/features/rolling-compatibility.md)). Written only past the
/// activation, so a member that rolled back before it never meets a file it cannot read.
pub const SNAPSHOT_VERSION_2: u8 = 2;

/// How many bytes the version 1 header takes
pub const SNAPSHOT_HEADER_LEN: usize = 8 + 1 + 8 + 8 + 8 + 8;

/// How many bytes the version 2 header takes: the version 1 header and its three new fields
pub const SNAPSHOT_HEADER_LEN_V2: usize = SNAPSHOT_HEADER_LEN + 16 + 8 + 8;

/// The wire version from which the version 2 file header is written
pub const SNAPSHOT_V2_FROM_WIRE: u8 = 5;

/// The directory under a shard's WAL directory that built snapshots are written to
pub const SNAPSHOTS_DIR: &str = "snapshots";

/// The directory under a shard's WAL directory that received snapshots are assembled in
pub const INSTALL_DIR: &str = "install";

/// How many bytes are buffered before a write or a read reaches the file
const BUFFER_BYTES: usize = 1024 * 1024;

/// How many bytes one block of the file checksum covers
const HASH_BLOCK_BYTES: usize = 64 * 1024;

/// A checksum over a file's bytes that does not depend on how they were fed in
///
/// `GxHasher`'s streaming `write` compresses each call on its own, so two feeds of the same
/// bytes chunked differently hash differently; a sender writing a megabyte at a time and a
/// receiver assembling whatever the lane delivered would never agree. This folds the file in
/// fixed blocks at fixed offsets instead: each full block is hashed with the fold so far as its
/// seed, and the tail block the same way at the end.
#[derive(Debug, Clone)]
pub struct FileHasher {
    /// The block being filled
    block: Vec<u8>,
    /// The fold over every block before it
    acc: u64,
}

impl Default for FileHasher {
    /// A hasher over nothing
    fn default() -> Self {
        FileHasher {
            block: Vec::with_capacity(HASH_BLOCK_BYTES),
            acc: 0,
        }
    }
}

impl FileHasher {
    /// Feed bytes, in file order
    ///
    /// # Arguments
    ///
    /// * `bytes` - The next bytes of the file
    pub fn write(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let room = HASH_BLOCK_BYTES - self.block.len();
            let take = room.min(bytes.len());
            self.block.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.block.len() == HASH_BLOCK_BYTES {
                self.fold();
            }
        }
    }

    /// Fold the block into the accumulator and empty it
    fn fold(&mut self) {
        // the fold so far seeds the block's hash, so blocks are ordered
        // `i64` from `u64` is a reinterpretation, which is all a seed needs
        #[allow(clippy::cast_possible_wrap)]
        let seed = self.acc as i64;
        self.acc = gxhash::gxhash64(&self.block, seed);
        self.block.clear();
    }

    /// The checksum over everything fed so far
    #[must_use]
    pub fn finish(&self) -> u64 {
        // the tail block, if there is one, folded the same way without emptying it
        if self.block.is_empty() {
            return self.acc;
        }
        #[allow(clippy::cast_possible_wrap)]
        let seed = self.acc as i64;
        gxhash::gxhash64(&self.block, seed)
    }
}

/// What a snapshot is: everything a receiver needs to judge one before a byte of it arrives
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    /// The group
    pub group: GroupId,
    /// The table it serves
    pub table: TableId,
    /// The structural fingerprint of the schema the sender serves
    pub schema_id: u64,
    /// The last log id whose effect the file holds
    pub boundary: WalLogId,
    /// The membership as of the boundary
    pub membership: StoredMembershipOf<DataConfig>,
    /// The tablets the file covers: every partition of them not in the file is absent
    pub tablets: Vec<u16>,
    /// How many records the file holds
    pub records: u64,
    /// How many bytes the file takes
    pub total: u64,
    /// What every byte of the file hashes to
    pub checksum: u64,
    /// How many remembered requests the trailer holds
    pub retries: u32,
    /// The newest time-ordered identity the sender had forgotten, in milliseconds since the
    /// epoch, so the receiver refuses what the sender would
    /// ([F45](../../../../docs/src/features/replica-migration.md))
    #[serde(default)]
    pub expired_before: u64,
    /// The cluster the snapshot was cut in ([F48](../../../../docs/src/features/rolling-compatibility.md))
    ///
    /// The nil id from a manifest that crossed a wire version 4 link or was read from a marker
    /// written before F48; the receiver treats such a manifest as its own cluster's.
    #[serde(default)]
    pub cluster: ClusterId,
    /// The node that cut it
    #[serde(default)]
    pub origin: NodeId,
    /// When it was cut, in milliseconds since the epoch; zero when not recorded
    #[serde(default)]
    pub created_ms: u64,
}

impl SnapshotManifest {
    /// Stamp where and when this snapshot was cut
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster it was cut in
    /// * `origin` - The node that cut it
    #[must_use]
    pub fn stamped(mut self, cluster: ClusterId, origin: NodeId) -> Self {
        self.cluster = cluster;
        self.origin = origin;
        self.created_ms = now_ms();
        self
    }

    /// Fill what a manifest from a version 4 link could not carry
    ///
    /// # Arguments
    ///
    /// * `cluster` - The receiver's cluster, which a v4 manifest is taken to belong to
    /// * `origin` - The sender the receiver heard the manifest from
    #[must_use]
    pub fn filled(mut self, cluster: ClusterId, origin: NodeId) -> Self {
        if self.cluster == ClusterId::default() {
            self.cluster = cluster;
        }
        if self.origin == NodeId::default() {
            self.origin = origin;
        }
        self
    }

    /// The manifest as a build at wire version 4 reads it
    #[must_use]
    pub fn to_v4(&self) -> ManifestV4 {
        ManifestV4 {
            group: self.group,
            table: self.table,
            schema_id: self.schema_id,
            boundary: self.boundary.clone(),
            membership: self.membership.clone(),
            tablets: self.tablets.clone(),
            records: self.records,
            total: self.total,
            checksum: self.checksum,
            retries: self.retries,
            expired_before: self.expired_before,
        }
    }

    /// The version 2 file header for this snapshot
    #[must_use]
    pub fn header_v2(&self) -> SnapshotHeader {
        SnapshotHeader {
            table: self.table,
            group: self.group,
            boundary: self.boundary.index,
            records: self.records,
            version: SNAPSHOT_VERSION_2,
            cluster: self.cluster,
            schema_id: self.schema_id,
            created_ms: self.created_ms,
        }
    }
}

/// The manifest as every build before F48 reads it, which a wire version 4 link carries
///
/// The field order is the wire: a v5 manifest is this with three fields after it, and a
/// postcard sequence has no way to say a field is missing, so the shape is a type of its own
/// rather than a default ([F48](../../../../docs/src/features/rolling-compatibility.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestV4 {
    /// The group
    pub group: GroupId,
    /// The table it serves
    pub table: TableId,
    /// The structural fingerprint of the schema the sender serves
    pub schema_id: u64,
    /// The last log id whose effect the file holds
    pub boundary: WalLogId,
    /// The membership as of the boundary
    pub membership: StoredMembershipOf<DataConfig>,
    /// The tablets the file covers
    pub tablets: Vec<u16>,
    /// How many records the file holds
    pub records: u64,
    /// How many bytes the file takes
    pub total: u64,
    /// What every byte of the file hashes to
    pub checksum: u64,
    /// How many remembered requests the trailer holds
    pub retries: u32,
    /// The newest time-ordered identity the sender had forgotten
    pub expired_before: u64,
}

impl ManifestV4 {
    /// The manifest this build works with, with the three newer fields unset
    #[must_use]
    pub fn into_manifest(self) -> SnapshotManifest {
        SnapshotManifest {
            group: self.group,
            table: self.table,
            schema_id: self.schema_id,
            boundary: self.boundary,
            membership: self.membership,
            tablets: self.tablets,
            records: self.records,
            total: self.total,
            checksum: self.checksum,
            retries: self.retries,
            expired_before: self.expired_before,
            cluster: ClusterId::default(),
            origin: NodeId::default(),
            created_ms: 0,
        }
    }
}

/// Where a cut is made, and which file format it is written in
///
/// Decided on the shard loop from the map it holds: the cluster and the node are its own, and
/// the file format is version 2 once the cluster has activated wire version 5 and version 1
/// before, so a member that could still roll back never meets a file it cannot read
/// ([F48](../../../../docs/src/features/rolling-compatibility.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotProvenance {
    /// The cluster the cut is made in
    pub cluster: ClusterId,
    /// The node making it
    pub origin: NodeId,
    /// The file format to write, 1 or 2
    pub file_version: u8,
}

impl SnapshotProvenance {
    /// What a node cuts under, given the wire version its cluster has activated
    ///
    /// # Arguments
    ///
    /// * `cluster` - The cluster
    /// * `origin` - This node
    /// * `activated_wire` - The wire version the cluster has activated
    #[must_use]
    pub fn at(cluster: ClusterId, origin: NodeId, activated_wire: u8) -> Self {
        SnapshotProvenance {
            cluster,
            origin,
            file_version: if activated_wire >= SNAPSHOT_V2_FROM_WIRE {
                SNAPSHOT_VERSION_2
            } else {
                SNAPSHOT_VERSION
            },
        }
    }

    /// The header a cut under this provenance writes
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `group` - The group
    /// * `boundary` - The boundary's index
    /// * `records` - How many records follow
    /// * `schema_id` - The schema's fingerprint, which a version 2 header carries
    #[must_use]
    pub fn header(&self, table: TableId, group: GroupId, boundary: u64, records: u64, schema_id: u64) -> SnapshotHeader {
        let mut header = SnapshotHeader::v1(table, group, boundary, records);
        if self.file_version == SNAPSHOT_VERSION_2 {
            header.version = SNAPSHOT_VERSION_2;
            header.cluster = self.cluster;
            header.schema_id = schema_id;
            header.created_ms = now_ms();
        }
        header
    }

    /// Stamp a manifest with this provenance, its time being the header's
    ///
    /// # Arguments
    ///
    /// * `manifest` - The manifest of the cut
    /// * `header` - The header the cut was written under
    #[must_use]
    pub fn stamp(&self, manifest: SnapshotManifest, header: &SnapshotHeader) -> SnapshotManifest {
        let mut manifest = manifest.stamped(self.cluster, self.origin);
        if header.created_ms > 0 {
            manifest.created_ms = header.created_ms;
        }
        manifest
    }
}

/// Milliseconds since the epoch, or zero on a clock before it
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// The header of a snapshot file
///
/// A version 1 header carries the first four fields; a version 2 header carries all of them
/// ([F48](../../../../docs/src/features/rolling-compatibility.md)). The version decides how
/// many bytes are written and read, and a build that meets a version it does not read refuses
/// it by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// The table
    pub table: TableId,
    /// The group
    pub group: GroupId,
    /// The boundary's index
    pub boundary: u64,
    /// How many records follow
    pub records: u64,
    /// Which format the file is in: 1 or 2
    pub version: u8,
    /// The cluster the file was cut in; the nil id in a version 1 file
    pub cluster: ClusterId,
    /// The structural fingerprint of the schema; zero in a version 1 file
    pub schema_id: u64,
    /// When it was cut, in milliseconds since the epoch; zero in a version 1 file
    pub created_ms: u64,
}

impl SnapshotHeader {
    /// A version 1 header
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `group` - The group
    /// * `boundary` - The boundary's index
    /// * `records` - How many records follow
    #[must_use]
    pub fn v1(table: TableId, group: GroupId, boundary: u64, records: u64) -> Self {
        SnapshotHeader {
            table,
            group,
            boundary,
            records,
            version: SNAPSHOT_VERSION,
            cluster: ClusterId::default(),
            schema_id: 0,
            created_ms: 0,
        }
    }

    /// How many bytes this header takes on disk, by its version
    #[must_use]
    pub const fn len(&self) -> usize {
        if self.version == SNAPSHOT_VERSION_2 {
            SNAPSHOT_HEADER_LEN_V2
        } else {
            SNAPSHOT_HEADER_LEN
        }
    }

    /// Whether this header has no bytes, which none does
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Write this header, at its version's length
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut raw = vec![0u8; self.len()];
        raw[..8].copy_from_slice(SNAPSHOT_MAGIC);
        raw[8] = self.version;
        raw[9..17].copy_from_slice(&self.table.0.to_le_bytes());
        raw[17..25].copy_from_slice(&self.group.0.to_le_bytes());
        raw[25..33].copy_from_slice(&self.boundary.to_le_bytes());
        raw[33..41].copy_from_slice(&self.records.to_le_bytes());
        // the version 2 fields, after the version 1 header
        if self.version == SNAPSHOT_VERSION_2 {
            raw[41..57].copy_from_slice(self.cluster.0.as_bytes());
            raw[57..65].copy_from_slice(&self.schema_id.to_le_bytes());
            raw[65..73].copy_from_slice(&self.created_ms.to_le_bytes());
        }
        raw
    }

    /// The length a file's header has, from its first bytes, refusing a version this build does not read
    ///
    /// # Arguments
    ///
    /// * `raw` - At least the version 1 header's bytes
    pub fn len_of(raw: &[u8]) -> io::Result<usize> {
        // the magic and the version first, so a foreign file is refused by name
        if raw.len() < SNAPSHOT_HEADER_LEN || &raw[..8] != SNAPSHOT_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a snapshot file: the magic is wrong"));
        }
        match raw[8] {
            SNAPSHOT_VERSION => Ok(SNAPSHOT_HEADER_LEN),
            SNAPSHOT_VERSION_2 => Ok(SNAPSHOT_HEADER_LEN_V2),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot format {other} is not one this build reads ({SNAPSHOT_VERSION} or {SNAPSHOT_VERSION_2})"),
            )),
        }
    }

    /// Read a header of either version, refusing anything that is not one this build reads
    ///
    /// # Arguments
    ///
    /// * `raw` - The header bytes, as many as [`SnapshotHeader::len_of`] said
    pub fn decode(raw: &[u8]) -> io::Result<Self> {
        let len = Self::len_of(raw)?;
        if raw.len() < len {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the snapshot header is cut short"));
        }
        let word = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().expect("eight bytes"));
        let mut header = SnapshotHeader::v1(TableId(word(9)), GroupId(word(17)), word(25), word(33));
        header.version = raw[8];
        // the version 2 fields, when the file has them
        if header.version == SNAPSHOT_VERSION_2 {
            header.cluster = ClusterId(uuid::Uuid::from_bytes(raw[41..57].try_into().expect("sixteen bytes")));
            header.schema_id = word(57);
            header.created_ms = word(65);
        }
        Ok(header)
    }
}

/// The name a built snapshot file is given
///
/// # Arguments
///
/// * `group` - The group
/// * `boundary` - The boundary's index
#[must_use]
pub fn snapshot_name(group: GroupId, boundary: u64) -> String {
    format!("{group}-{boundary}.snap")
}

/// Sync a directory, so a file created or removed in it is durable
///
/// # Arguments
///
/// * `dir` - The directory
pub async fn sync_dir(dir: &Path) -> io::Result<()> {
    let directory = Directory::open(dir).await.map_err(to_io)?;
    directory.sync().await.map_err(to_io)?;
    directory.close().await.map_err(to_io)?;
    Ok(())
}

/// Turn a glommio error into an io error
///
/// # Arguments
///
/// * `error` - The glommio error
fn to_io<T>(error: glommio::GlommioError<T>) -> io::Error {
    match error {
        glommio::GlommioError::IoError(error) => error,
        other => io::Error::other(other.to_string()),
    }
}

/// Writes one snapshot file, record by record, hashing as it goes
pub struct SnapshotWriter {
    /// The file
    file: BufferedFile,
    /// Bytes not yet written
    buffer: Vec<u8>,
    /// Where the next byte lands in the file
    pos: u64,
    /// The running checksum over every byte
    hasher: FileHasher,
    /// How many records were written
    records: u64,
    /// How many the header promised
    promised: u64,
}

impl SnapshotWriter {
    /// Create the file and write its header
    ///
    /// # Arguments
    ///
    /// * `path` - Where the file goes
    /// * `header` - What it is
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be created.
    pub async fn create(path: &Path, header: SnapshotHeader) -> io::Result<Self> {
        let file = BufferedFile::create(path).await.map_err(to_io)?;
        let mut writer = SnapshotWriter {
            file,
            buffer: Vec::with_capacity(BUFFER_BYTES),
            pos: 0,
            hasher: FileHasher::default(),
            records: 0,
            promised: header.records,
        };
        writer.put(&header.encode()).await?;
        Ok(writer)
    }

    /// Append bytes, flushing the buffer when it is full
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    async fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.hasher.write(bytes);
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() >= BUFFER_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    /// Write the buffer to the file
    async fn flush(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::replace(&mut self.buffer, Vec::with_capacity(BUFFER_BYTES));
        let len = bytes.len() as u64;
        self.file.write_at(bytes, self.pos).await.map_err(to_io)?;
        self.pos += len;
        Ok(())
    }

    /// Write one partition's record
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    /// * `bytes` - The partition's archived bytes
    ///
    /// # Errors
    ///
    /// Fails if more records are written than the header promised, or on a write error.
    pub async fn record(&mut self, key: u64, bytes: &[u8]) -> io::Result<()> {
        if self.records >= self.promised {
            return Err(io::Error::other(format!(
                "the snapshot header promised {} records and a {}th was written",
                self.promised,
                self.records + 1
            )));
        }
        // truncation cannot happen: a partition's archived bytes are bounded by a u32 length
        #[allow(clippy::cast_possible_truncation)]
        let len = bytes.len() as u32;
        self.put(&key.to_le_bytes()).await?;
        self.put(&len.to_le_bytes()).await?;
        self.put(bytes).await?;
        self.records += 1;
        Ok(())
    }

    /// Write the trailer, make the file durable and close it
    ///
    /// # Arguments
    ///
    /// * `retries` - The remembered requests as of the boundary, oldest first
    ///
    /// # Errors
    ///
    /// Fails if fewer records were written than the header promised, or on a write error.
    ///
    /// Returns how many bytes the file takes and what they hash to.
    pub async fn finish(mut self, retries: &[(RequestId, Remembered)]) -> io::Result<(u64, u64)> {
        if self.records != self.promised {
            return Err(io::Error::other(format!(
                "the snapshot header promised {} records and {} were written",
                self.promised, self.records
            )));
        }
        let trailer = postcard::to_allocvec(retries).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        // truncation cannot happen: the retry table is bounded at a few thousand entries
        #[allow(clippy::cast_possible_truncation)]
        let len = trailer.len() as u32;
        self.put(&len.to_le_bytes()).await?;
        self.put(&trailer).await?;
        self.flush().await?;
        self.file.fdatasync().await.map_err(to_io)?;
        self.file.close().await.map_err(to_io)?;
        Ok((self.pos, self.hasher.finish()))
    }
}

/// Reads one snapshot file, record by record
pub struct SnapshotReader {
    /// The file
    file: BufferedFile,
    /// How many bytes it holds
    size: u64,
    /// The bytes read ahead
    buffer: Vec<u8>,
    /// Where the buffer starts in the file
    buffer_at: u64,
    /// Where the next byte to hand out lies in the file
    pos: u64,
    /// What the header says
    header: SnapshotHeader,
    /// How many records were handed out
    read: u64,
}

impl SnapshotReader {
    /// Open a file and read its header
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be opened or its header is not a snapshot's.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let file = BufferedFile::open(path).await.map_err(to_io)?;
        let size = file.file_size().await.map_err(to_io)?;
        let mut reader = SnapshotReader {
            file,
            size,
            buffer: Vec::new(),
            buffer_at: 0,
            pos: 0,
            header: SnapshotHeader::v1(TableId(0), GroupId(0), 0, 0),
            read: 0,
        };
        // the version 1 header first, which says how long the header is, then the rest
        let mut raw = reader.take(SNAPSHOT_HEADER_LEN).await?;
        let len = SnapshotHeader::len_of(&raw)?;
        if len > SNAPSHOT_HEADER_LEN {
            raw.extend(reader.take(len - SNAPSHOT_HEADER_LEN).await?);
        }
        reader.header = SnapshotHeader::decode(&raw)?;
        Ok(reader)
    }

    /// What the header says
    #[must_use]
    pub fn header(&self) -> SnapshotHeader {
        self.header
    }

    /// Take the next bytes, reading ahead a buffer at a time
    ///
    /// # Arguments
    ///
    /// * `len` - How many bytes
    async fn take(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            // refill when the buffer is behind the position
            let offset = self.pos.saturating_sub(self.buffer_at);
            if self.buffer.is_empty() || offset >= self.buffer.len() as u64 {
                if self.pos >= self.size {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("the snapshot file ends at {} bytes, before a record was whole", self.size),
                    ));
                }
                let want = BUFFER_BYTES.min((self.size - self.pos) as usize);
                let read = self.file.read_at(self.pos, want).await.map_err(to_io)?;
                self.buffer = read.to_vec();
                self.buffer_at = self.pos;
                continue;
            }
            // `usize` from `u64` is lossless on every target this runs on
            let offset = offset as usize;
            let take = (len - out.len()).min(self.buffer.len() - offset);
            out.extend_from_slice(&self.buffer[offset..offset + take]);
            self.pos += take as u64;
        }
        Ok(out)
    }

    /// The next record, or none once every promised record was read
    ///
    /// # Errors
    ///
    /// Fails if the file ends before the record is whole.
    pub async fn next_record(&mut self) -> io::Result<Option<(u64, Vec<u8>)>> {
        if self.read >= self.header.records {
            return Ok(None);
        }
        let key = self.take(8).await?;
        let key = u64::from_le_bytes(key.as_slice().try_into().expect("eight bytes"));
        let len = self.take(4).await?;
        let len = u32::from_le_bytes(len.as_slice().try_into().expect("four bytes"));
        let bytes = self.take(len as usize).await?;
        self.read += 1;
        Ok(Some((key, bytes)))
    }

    /// The trailer's remembered requests, once every record was read
    ///
    /// # Errors
    ///
    /// Fails if records remain, the file ends early, or the trailer does not decode.
    pub async fn trailer(&mut self) -> io::Result<Vec<(RequestId, Remembered)>> {
        if self.read < self.header.records {
            return Err(io::Error::other("the trailer was asked for before every record was read"));
        }
        let len = self.take(4).await?;
        let len = u32::from_le_bytes(len.as_slice().try_into().expect("four bytes"));
        let bytes = self.take(len as usize).await?;
        postcard::from_bytes(&bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Close the file
    pub async fn close(self) -> io::Result<()> {
        self.file.close().await.map_err(to_io)
    }
}

/// Check a file against its manifest: its header, its length and its checksum
///
/// # Arguments
///
/// * `path` - The file
/// * `manifest` - What it claims to be
///
/// # Errors
///
/// Says what did not match.
pub async fn verify(path: &Path, manifest: &SnapshotManifest) -> io::Result<()> {
    let file = BufferedFile::open(path).await.map_err(to_io)?;
    let size = file.file_size().await.map_err(to_io)?;
    if size != manifest.total {
        let _ = file.close().await;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the snapshot file is {size} bytes and the manifest says {}", manifest.total),
        ));
    }
    // every byte through the hasher, a buffer at a time
    let mut hasher = FileHasher::default();
    let mut pos = 0u64;
    let mut header = None;
    while pos < size {
        let want = BUFFER_BYTES.min((size - pos) as usize);
        let read = file.read_at(pos, want).await.map_err(to_io)?;
        if pos == 0 {
            if read.len() < SNAPSHOT_HEADER_LEN {
                let _ = file.close().await;
                return Err(io::Error::new(io::ErrorKind::InvalidData, "the snapshot file is shorter than a header"));
            }
            let len = SnapshotHeader::len_of(&read)?;
            if read.len() < len {
                let _ = file.close().await;
                return Err(io::Error::new(io::ErrorKind::InvalidData, "the snapshot file is shorter than its header"));
            }
            header = Some(SnapshotHeader::decode(&read[..len])?);
        }
        hasher.write(&read);
        pos += read.len() as u64;
    }
    file.close().await.map_err(to_io)?;
    let checksum = hasher.finish();
    if checksum != manifest.checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the snapshot file hashes to {checksum:016x} and the manifest says {:016x}", manifest.checksum),
        ));
    }
    // and the header has to name what the manifest names
    let Some(header) = header else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "the snapshot file is empty"));
    };
    if header.group != manifest.group || header.table != manifest.table || header.boundary != manifest.boundary.index {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "the snapshot file is group {} of table {} at {} and the manifest says group {} of table {} at {}",
                header.group, header.table, header.boundary, manifest.group, manifest.table, manifest.boundary.index
            ),
        ));
    }
    // a version 2 file names its cluster and schema, which have to be the manifest's when the
    // manifest names them ([F48](../../../../docs/src/features/rolling-compatibility.md))
    if header.version == SNAPSHOT_VERSION_2 {
        if manifest.cluster != ClusterId::default() && header.cluster != manifest.cluster {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the snapshot file was cut in cluster {} and the manifest says {}", header.cluster, manifest.cluster),
            ));
        }
        if header.schema_id != manifest.schema_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the snapshot file was cut from schema {:#018x} and the manifest says {:#018x}", header.schema_id, manifest.schema_id),
            ));
        }
    }
    Ok(())
}

/// A snapshot the shard loop built and holds for transfers
///
/// Held behind an `Rc`: a transfer in flight clones it, and the loop deletes the file only
/// once no transfer holds it and a newer cut has replaced it.
#[derive(Debug)]
pub struct BuiltSnapshot {
    /// Where the file is
    pub path: PathBuf,
    /// What it is
    pub manifest: SnapshotManifest,
}

/// What rides the replication lane under `ReplicateKind::Snapshot`
///
/// Control on the replication lane, bytes on the bulk lane
/// ([C2](../../../../docs/src/distributed/transport.md)): the begin says what is coming and
/// learns where to start, the end says the bytes are all sent and waits for the install.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotRpc {
    /// A stream is about to start
    Begin {
        /// The sender's vote, which the receiver judges as it judges an append
        vote: Vote,
        /// The stream, so its chunks are told from another's
        stream: [u8; 16],
        /// What is coming
        manifest: SnapshotManifest,
        /// The repair operation this stream serves, if it is one
        ///
        /// A repair stream replaces a quarantined copy that is live and applied past the
        /// boundary, so it is judged against the receiver's checkpoint rather than its applied
        /// position and installed by restarting the group from that checkpoint
        /// ([F44](../../../../docs/src/features/repair.md)).
        repair: Option<uuid::Uuid>,
    },
    /// Every byte of a stream was sent
    End {
        /// The stream
        stream: [u8; 16],
        /// How many bytes
        total: u64,
        /// What they hash to
        checksum: u64,
    },
}

/// The begin RPC as a wire version 4 link carries it: the manifest in its version 4 shape
///
/// The enum's variants and their order are `SnapshotRpc`'s, so an `End` encodes the same at
/// either version ([F48](../../../../docs/src/features/rolling-compatibility.md)).
#[derive(Debug, Clone, Serialize, Deserialize)]
enum SnapshotRpcV4 {
    /// A stream is about to start
    Begin {
        /// The sender's vote
        vote: Vote,
        /// The stream
        stream: [u8; 16],
        /// What is coming, as a version 4 build reads it
        manifest: ManifestV4,
        /// The repair operation this stream serves, if it is one
        repair: Option<uuid::Uuid>,
    },
    /// Every byte of a stream was sent
    End {
        /// The stream
        stream: [u8; 16],
        /// How many bytes
        total: u64,
        /// What they hash to
        checksum: u64,
    },
}

impl SnapshotRpc {
    /// Encode this RPC for a link at a wire version
    ///
    /// Below [`SNAPSHOT_V2_FROM_WIRE`] the manifest goes in its version 4 shape; at it or above
    /// in its own ([F48](../../../../docs/src/features/rolling-compatibility.md)).
    ///
    /// # Arguments
    ///
    /// * `version` - The wire version the frame will name
    pub fn encode_at(&self, version: u8) -> Result<Vec<u8>, postcard::Error> {
        if version >= SNAPSHOT_V2_FROM_WIRE {
            return postcard::to_allocvec(self);
        }
        // the version 4 shape, field for field
        let older = match self {
            SnapshotRpc::Begin { vote, stream, manifest, repair } => SnapshotRpcV4::Begin {
                vote: vote.clone(),
                stream: *stream,
                manifest: manifest.to_v4(),
                repair: *repair,
            },
            SnapshotRpc::End { stream, total, checksum } => SnapshotRpcV4::End {
                stream: *stream,
                total: *total,
                checksum: *checksum,
            },
        };
        postcard::to_allocvec(&older)
    }

    /// Decode an RPC a frame at a wire version carried
    ///
    /// # Arguments
    ///
    /// * `bytes` - The frame's payload
    /// * `version` - The wire version the frame named
    pub fn decode_at(bytes: &[u8], version: u8) -> Result<Self, postcard::Error> {
        if version >= SNAPSHOT_V2_FROM_WIRE {
            return postcard::from_bytes(bytes);
        }
        // the version 4 shape, lifted into this build's with its new fields unset
        Ok(match postcard::from_bytes::<SnapshotRpcV4>(bytes)? {
            SnapshotRpcV4::Begin { vote, stream, manifest, repair } => SnapshotRpc::Begin {
                vote,
                stream,
                manifest: manifest.into_manifest(),
                repair,
            },
            SnapshotRpcV4::End { stream, total, checksum } => SnapshotRpc::End { stream, total, checksum },
        })
    }
}

/// What the receiver answers a snapshot RPC with
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotAnswer {
    /// Send the bytes from this offset: zero for a new stream, the prefix held for a resumed one
    Resume {
        /// The first byte wanted
        from: u64,
    },
    /// The snapshot is installed, durably, and this is the receiver's vote
    Installed {
        /// The receiver's vote
        vote: Vote,
    },
    /// The snapshot is refused, and this is why
    Refused(String),
    /// A repair stream's boundary is not past the receiver's checkpoint; cut again past this
    Behind {
        /// The receiver's checkpoint
        checkpoint: u64,
    },
}

/// What a begin frame's manifest carries on the bulk lane: where its chunks go
///
/// The bulk lane lands on whichever shard the kernel chose; the route is what lets that
/// shard hand the bytes to the one that hosts the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkRoute {
    /// The group
    pub group: GroupId,
    /// The shard on the receiving node that hosts it
    pub target_shard: u16,
}

#[cfg(test)]
mod tests {
    use openraft::vote::RaftLeaderId as _;
    use openraft::{AsyncRuntime as _, LogId, StoredMembership};

    use super::{
        verify, ManifestV4, SnapshotHeader, SnapshotManifest, SnapshotReader, SnapshotRpc, SnapshotWriter,
        SNAPSHOT_HEADER_LEN, SNAPSHOT_HEADER_LEN_V2, SNAPSHOT_MAGIC, SNAPSHOT_VERSION_2,
    };
    use crate::server::wal::Vote;
    use crate::server::control::runtime::GlommioRuntime;
    use crate::server::replication::{CommandResult, Remembered, ResultKind};
    use crate::server::wal::LeaderId;
    use crate::shared::identity::{ClusterId, GroupId, NodeId, ShardAddr, TableId};
    use crate::shared::protocol::peer::RequestId;

    /// A snapshot file round trips its records and its trailer, verifies against its manifest,
    /// and is refused torn, foreign, or under the wrong manifest
    #[test]
    fn a_snapshot_file_round_trips_and_a_torn_or_foreign_one_is_refused() {
        let mut runtime = GlommioRuntime::new(1);
        runtime.block_on(async {
            let dir = tempfile::tempdir().expect("failed to build a temp dir");
            let path = dir.path().join("g-7.snap");
            let header = SnapshotHeader::v1(TableId::of("Note"), GroupId(0xabc), 7, 3);
            let retries = vec![(
                RequestId {
                    bundle: [3u8; 16],
                    index: 4,
                },
                Remembered {
                    digest: 99,
                    result: CommandResult {
                        kind: ResultKind::Insert,
                        ok: true,
                    },
                    applied: 5,
                },
            )];
            // three records of different sizes, one larger than the read buffer
            let records: Vec<(u64, Vec<u8>)> = vec![(1, vec![1u8; 10]), (2, vec![2u8; 3 * 1024 * 1024]), (3, Vec::new())];
            let mut writer = SnapshotWriter::create(&path, header).await.expect("failed to create");
            for (key, bytes) in &records {
                writer.record(*key, bytes).await.expect("failed to write a record");
            }
            let (total, checksum) = writer.finish(&retries).await.expect("failed to finish");
            assert_eq!(total, std::fs::metadata(&path).expect("the file").len());
            let manifest = SnapshotManifest {
                group: header.group,
                table: header.table,
                schema_id: 1,
                boundary: LogId::new(LeaderId::new(1, ShardAddr::from(1)), 7),
                membership: StoredMembership::default(),
                tablets: vec![1, 2],
                records: 3,
                total,
                checksum,
                retries: 1,
                expired_before: 0,
                cluster: ClusterId::default(),
                origin: NodeId::default(),
                created_ms: 0,
            };
            verify(&path, &manifest).await.expect("the file does not verify");
            // the checksum is the same however the bytes are chunked
            let bytes = std::fs::read(&path).expect("the file");
            for chunk in [1usize, 7, 4096, 65_536, 100_000, bytes.len()] {
                let mut hasher = super::FileHasher::default();
                for piece in bytes.chunks(chunk) {
                    hasher.write(piece);
                }
                assert_eq!(hasher.finish(), checksum, "chunked by {chunk}");
            }
            // the records and the trailer read back
            let mut reader = SnapshotReader::open(&path).await.expect("failed to open");
            assert_eq!(reader.header(), header);
            let mut read = Vec::new();
            while let Some(record) = reader.next_record().await.expect("failed to read a record") {
                read.push(record);
            }
            assert_eq!(read, records);
            assert_eq!(reader.trailer().await.expect("failed to read the trailer"), retries);
            reader.close().await.expect("failed to close");
            // a wrong boundary in the manifest is refused, as is a wrong checksum
            let mut wrong = manifest.clone();
            wrong.boundary = LogId::new(LeaderId::new(1, ShardAddr::from(1)), 8);
            assert!(verify(&path, &wrong).await.is_err());
            let mut wrong = manifest.clone();
            wrong.checksum ^= 1;
            assert!(verify(&path, &wrong).await.is_err());
            // a torn file is refused by its length, and a reader stops at the tear
            let torn = dir.path().join("torn.snap");
            let mut bytes = std::fs::read(&path).expect("the file");
            bytes.truncate(bytes.len() - 5);
            std::fs::write(&torn, &bytes).expect("the torn file");
            assert!(verify(&torn, &manifest).await.is_err());
            let mut reader = SnapshotReader::open(&torn).await.expect("the torn header is whole");
            let mut count = 0;
            let outcome = loop {
                match reader.next_record().await {
                    Ok(Some(_)) => count += 1,
                    Ok(None) => break reader.trailer().await.map(|_| ()),
                    Err(error) => break Err(error),
                }
            };
            assert!(outcome.is_err(), "a torn file read whole after {count} records");
            // a foreign file is refused at its magic, and a wrong version by name
            let foreign = dir.path().join("foreign.snap");
            let mut raw = header.encode();
            raw[..8].copy_from_slice(b"NOTASNAP");
            std::fs::write(&foreign, raw).expect("the foreign file");
            assert!(SnapshotReader::open(&foreign).await.is_err());
            let mut raw = header.encode();
            raw[8] = 9;
            let error = SnapshotHeader::decode(&raw).expect_err("a wrong version was accepted");
            assert!(error.to_string().contains("format 9"), "{error}");
            assert_eq!(&header.encode()[..8], SNAPSHOT_MAGIC);
            assert_eq!(header.encode().len(), SNAPSHOT_HEADER_LEN);
            // a writer that promises more records than it writes, or fewer, is refused
            let short = dir.path().join("short.snap");
            let writer = SnapshotWriter::create(&short, header).await.expect("failed to create");
            assert!(writer.finish(&[]).await.is_err());
        });
    }

    /// A version 5 manifest round trips through its version 4 shape with the new fields
    /// defaulted, a pending marker from before F48 still loads, and a version 2 file header
    /// identifies its cluster ([F48](../../../../docs/src/features/rolling-compatibility.md))
    /// A file cut past the activation names its cluster in its own header, the manifest
    /// stamped from it names the origin too, and the backup manifest written beside it
    /// rebuilds a manifest the file verifies against; a file cut below the activation is a
    /// version 1 file that names nothing, and a manifest for another cluster does not verify it
    #[test]
    fn a_backup_file_identifies_its_cluster() {
        use super::{SnapshotProvenance, SNAPSHOT_V2_FROM_WIRE};
        use crate::server::control::backup::BackupManifest;
        let mut runtime = GlommioRuntime::new(1);
        runtime.block_on(async {
            let dir = tempfile::tempdir().expect("failed to build a temp dir");
            let cluster = ClusterId::mint();
            let origin = NodeId::mint();
            let table = TableId::of("Note");
            let group = GroupId(0xabc);
            let records: Vec<(u64, Vec<u8>)> = vec![(1, vec![1u8; 10]), (2, vec![2u8; 20])];
            // one file at each side of the activation, from the same records
            let mut files = Vec::new();
            for wire in [SNAPSHOT_V2_FROM_WIRE - 1, SNAPSHOT_V2_FROM_WIRE] {
                let provenance = SnapshotProvenance::at(cluster, origin, wire);
                let header = provenance.header(table, group, 7, records.len() as u64, 0x1234);
                let path = dir.path().join(format!("{group}-{wire}.snap"));
                let mut writer = SnapshotWriter::create(&path, header).await.expect("failed to create");
                for (key, bytes) in &records {
                    writer.record(*key, bytes).await.expect("failed to write a record");
                }
                let (total, checksum) = writer.finish(&[]).await.expect("failed to finish");
                let manifest = provenance.stamp(
                    SnapshotManifest {
                        group,
                        table,
                        schema_id: 0x1234,
                        boundary: LogId::new(LeaderId::new(3, ShardAddr::new(origin, 0)), 7),
                        membership: StoredMembership::default(),
                        tablets: vec![1, 2],
                        records: records.len() as u64,
                        total,
                        checksum,
                        retries: 0,
                        expired_before: 0,
                        cluster: ClusterId::default(),
                        origin: NodeId::default(),
                        created_ms: 0,
                    },
                    &header,
                );
                files.push((wire, path, header, manifest));
            }
            let (_, below, header_below, manifest_below) = &files[0];
            let (_, past, header_past, manifest_past) = &files[1];
            // the header past the activation names the cluster and the schema; the one below does not
            assert_eq!(header_past.version, SNAPSHOT_VERSION_2);
            assert_eq!(header_past.cluster, cluster);
            assert_eq!(header_past.schema_id, 0x1234);
            assert!(header_past.created_ms > 0);
            assert_eq!(header_below.version, super::SNAPSHOT_VERSION);
            assert_eq!(header_below.cluster, ClusterId::default());
            assert_eq!(std::fs::metadata(past).expect("the file").len(), std::fs::metadata(below).expect("the file").len() + (SNAPSHOT_HEADER_LEN_V2 - SNAPSHOT_HEADER_LEN) as u64);
            // and the file itself says so when opened, with no manifest in hand
            let reader = SnapshotReader::open(past).await.expect("the file opens");
            assert_eq!(reader.header().cluster, cluster);
            assert_eq!(reader.header().created_ms, header_past.created_ms);
            reader.close().await.expect("close");
            // both manifests are stamped with the cluster and the origin
            for manifest in [manifest_below, manifest_past] {
                assert_eq!(manifest.cluster, cluster);
                assert_eq!(manifest.origin, origin);
                assert!(manifest.created_ms > 0);
            }
            assert_eq!(manifest_past.created_ms, header_past.created_ms, "the manifest's time is the header's");
            // the backup manifest beside a file rebuilds one the file verifies against, and
            // names what a restore judges before a byte is trusted
            let op = uuid::Uuid::new_v4();
            let beside = BackupManifest::of(op, "Note", manifest_past);
            assert_eq!(beside.cluster, cluster);
            assert_eq!(beside.origin, origin);
            assert_eq!(beside.schema_id, 0x1234);
            assert_eq!(beside.boundary, 7);
            assert_eq!(beside.term, 3);
            let json = serde_json::to_vec(&beside).expect("a backup manifest is json");
            let loaded: BackupManifest = serde_json::from_slice(&json).expect("a backup manifest loads");
            assert_eq!(loaded, beside);
            verify(past, &loaded.to_snapshot()).await.expect("the file verifies against the manifest beside it");
            verify(below, &BackupManifest::of(op, "Note", manifest_below).to_snapshot()).await.expect("the version 1 file verifies too");
            // a manifest naming another cluster does not verify a file cut past the activation
            let mut foreign = loaded.clone();
            foreign.cluster = ClusterId::mint();
            assert!(verify(past, &foreign.to_snapshot()).await.is_err(), "a file was verified under another cluster's manifest");
        });
    }

    #[test]
    fn a_v4_manifest_round_trips_with_defaults() {
        let cluster = ClusterId::mint();
        let origin = NodeId::mint();
        let manifest = SnapshotManifest {
            group: GroupId(0xabc),
            table: TableId::of("Note"),
            schema_id: 0x1234,
            boundary: LogId::new(LeaderId::new(1, ShardAddr::from(1)), 7),
            membership: StoredMembership::default(),
            tablets: vec![1, 2],
            records: 3,
            total: 99,
            checksum: 0xfeed,
            retries: 1,
            expired_before: 5,
            cluster: ClusterId::default(),
            origin: NodeId::default(),
            created_ms: 0,
        }
        .stamped(cluster, origin);
        assert!(manifest.created_ms > 0);
        // v5 -> v4 -> v5: the three fields are lost on the way and defaulted back
        let v4 = manifest.to_v4();
        let bytes = postcard::to_allocvec(&v4).expect("a v4 manifest encodes");
        let back: ManifestV4 = postcard::from_bytes(&bytes).expect("a v4 manifest decodes");
        assert_eq!(back, v4);
        let restored = back.into_manifest();
        assert_eq!(restored.cluster, ClusterId::default());
        assert_eq!(restored.origin, NodeId::default());
        assert_eq!(restored.created_ms, 0);
        assert_eq!(restored.to_v4(), v4);
        // the receiver fills what a v4 link could not carry, and leaves a v5 manifest alone
        let filled = restored.filled(cluster, origin);
        assert_eq!(filled.cluster, cluster);
        assert_eq!(filled.origin, origin);
        let other = manifest.clone().filled(ClusterId::mint(), NodeId::mint());
        assert_eq!(other.cluster, cluster);
        assert_eq!(other.origin, origin);
        // the v5 shape round trips whole
        let bytes = postcard::to_allocvec(&manifest).expect("a v5 manifest encodes");
        let back: SnapshotManifest = postcard::from_bytes(&bytes).expect("a v5 manifest decodes");
        assert_eq!(back, manifest);
        // a pending marker written before F48 - the v4 shape as json - still loads with defaults
        let json = serde_json::to_vec(&v4).expect("a v4 manifest as json");
        let loaded: SnapshotManifest = serde_json::from_slice(&json).expect("an older marker loads");
        assert_eq!(loaded.cluster, ClusterId::default());
        assert_eq!(loaded.to_v4(), v4);
        // a version 2 header carries the cluster, the schema and the time, and round trips
        let header = manifest.header_v2();
        assert_eq!(header.version, SNAPSHOT_VERSION_2);
        let raw = header.encode();
        assert_eq!(raw.len(), SNAPSHOT_HEADER_LEN_V2);
        assert_eq!(SnapshotHeader::len_of(&raw).expect("a v2 header's length"), SNAPSHOT_HEADER_LEN_V2);
        let decoded = SnapshotHeader::decode(&raw).expect("a v2 header decodes");
        assert_eq!(decoded, header);
        assert_eq!(decoded.cluster, cluster);
        assert_eq!(decoded.schema_id, 0x1234);
        // a version 1 header is shorter and names no cluster
        let v1 = SnapshotHeader::v1(header.table, header.group, header.boundary, header.records);
        let raw = v1.encode();
        assert_eq!(raw.len(), SNAPSHOT_HEADER_LEN);
        assert_eq!(SnapshotHeader::decode(&raw).expect("a v1 header decodes"), v1);
        // a v2 header cut short is refused, not read as a v1
        assert!(SnapshotHeader::decode(&header.encode()[..SNAPSHOT_HEADER_LEN]).is_err());
        // a begin encoded for a v4 link is the v4 shape and decodes back with defaults; one
        // encoded at v5 carries everything; an end is the same bytes at either
        let begin = SnapshotRpc::Begin {
            vote: Vote::new(1, ShardAddr::from(1)),
            stream: [7u8; 16],
            manifest: manifest.clone(),
            repair: None,
        };
        let at_4 = begin.encode_at(4).expect("encodes at 4");
        let at_5 = begin.encode_at(5).expect("encodes at 5");
        assert!(at_5.len() > at_4.len());
        match SnapshotRpc::decode_at(&at_4, 4).expect("decodes at 4") {
            SnapshotRpc::Begin { manifest: read, .. } => {
                assert_eq!(read.cluster, ClusterId::default());
                assert_eq!(read.to_v4(), v4);
            }
            SnapshotRpc::End { .. } => panic!("a begin decoded as an end"),
        }
        match SnapshotRpc::decode_at(&at_5, 5).expect("decodes at 5") {
            SnapshotRpc::Begin { manifest: read, .. } => assert_eq!(read, manifest),
            SnapshotRpc::End { .. } => panic!("a begin decoded as an end"),
        }
        // a v5 payload read as v4 does not decode as the wrong thing silently: the repair
        // field lands in the cluster's bytes, so the decode fails or the begin differs
        if let Ok(SnapshotRpc::Begin { manifest: read, .. }) = SnapshotRpc::decode_at(&at_5, 4) {
            assert_ne!(read, manifest);
        }
        let end = SnapshotRpc::End { stream: [1u8; 16], total: 5, checksum: 6 };
        assert_eq!(end.encode_at(4).expect("an end at 4"), end.encode_at(5).expect("an end at 5"));
    }
}
