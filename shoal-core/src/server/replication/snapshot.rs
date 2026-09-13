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
//! [magic 8 B "SHOALSNP"][version u8][table u64][group u64][boundary index u64][records u64]
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
use crate::shared::identity::{GroupId, TableId};
use crate::shared::protocol::peer::RequestId;

/// The first eight bytes of every snapshot file
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"SHOALSNP";

/// The file format this build writes and reads
pub const SNAPSHOT_VERSION: u8 = 1;

/// How many bytes the header takes
pub const SNAPSHOT_HEADER_LEN: usize = 8 + 1 + 8 + 8 + 8 + 8;

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
}

/// The header of a snapshot file
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
}

impl SnapshotHeader {
    /// Write this header
    #[must_use]
    pub fn encode(&self) -> [u8; SNAPSHOT_HEADER_LEN] {
        let mut raw = [0u8; SNAPSHOT_HEADER_LEN];
        raw[..8].copy_from_slice(SNAPSHOT_MAGIC);
        raw[8] = SNAPSHOT_VERSION;
        raw[9..17].copy_from_slice(&self.table.0.to_le_bytes());
        raw[17..25].copy_from_slice(&self.group.0.to_le_bytes());
        raw[25..33].copy_from_slice(&self.boundary.to_le_bytes());
        raw[33..41].copy_from_slice(&self.records.to_le_bytes());
        raw
    }

    /// Read a header, refusing anything that is not one this build writes
    ///
    /// # Arguments
    ///
    /// * `raw` - The header bytes
    pub fn decode(raw: &[u8; SNAPSHOT_HEADER_LEN]) -> io::Result<Self> {
        // the magic and the version first, so a foreign file is refused by name
        if &raw[..8] != SNAPSHOT_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a snapshot file: the magic is wrong"));
        }
        if raw[8] != SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot format {} is not one this build reads ({SNAPSHOT_VERSION})", raw[8]),
            ));
        }
        let word = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().expect("eight bytes"));
        Ok(SnapshotHeader {
            table: TableId(word(9)),
            group: GroupId(word(17)),
            boundary: word(25),
            records: word(33),
        })
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
            header: SnapshotHeader {
                table: TableId(0),
                group: GroupId(0),
                boundary: 0,
                records: 0,
            },
            read: 0,
        };
        let raw = reader.take(SNAPSHOT_HEADER_LEN).await?;
        let raw: [u8; SNAPSHOT_HEADER_LEN] = raw.as_slice().try_into().expect("the header's length");
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
            let raw: [u8; SNAPSHOT_HEADER_LEN] = read[..SNAPSHOT_HEADER_LEN].try_into().expect("the header's length");
            header = Some(SnapshotHeader::decode(&raw)?);
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

    use super::{verify, SnapshotHeader, SnapshotManifest, SnapshotReader, SnapshotWriter, SNAPSHOT_HEADER_LEN, SNAPSHOT_MAGIC};
    use crate::server::control::runtime::GlommioRuntime;
    use crate::server::replication::{CommandResult, Remembered, ResultKind};
    use crate::server::wal::LeaderId;
    use crate::shared::identity::{GroupId, ShardAddr, TableId};
    use crate::shared::protocol::peer::RequestId;

    /// A snapshot file round trips its records and its trailer, verifies against its manifest,
    /// and is refused torn, foreign, or under the wrong manifest
    #[test]
    fn a_snapshot_file_round_trips_and_a_torn_or_foreign_one_is_refused() {
        let mut runtime = GlommioRuntime::new(1);
        runtime.block_on(async {
            let dir = tempfile::tempdir().expect("failed to build a temp dir");
            let path = dir.path().join("g-7.snap");
            let header = SnapshotHeader {
                table: TableId::of("Note"),
                group: GroupId(0xabc),
                boundary: 7,
                records: 3,
            };
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
}
