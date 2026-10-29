//! The manifest of a paged archive map: which runs make it up, and what it counts
//!
//! The manifest is the map's commit point. It is written whole to a temp file, renamed over the
//! last one and the directory synced, as the whole map was before it was paged; a run is part of
//! the map only once a durable manifest names it, and a run file no manifest names is one a crash
//! left, removed when the map is next opened
//! ([F76](../../../../../../../docs/src/features/paged-archive-map.md)).
//!
//! ```text
//! [checksum u64, over everything after it][magic u64][rkyv Manifest]
//! ```

use futures::AsyncWriteExt;
use glommio::io::{DmaStreamWriterBuilder, OpenOptions};
use glommio::GlommioError;
use gxhash::GxHasher;
use rkyv::{Archive, Deserialize, Serialize};
use std::hash::Hasher;
use std::path::Path;
use tracing::{event, instrument, Level};
use uuid::Uuid;

use crate::server::errors::ShoalError;
use crate::server::ServerError;

/// The magic a manifest begins with, after its checksum
///
/// A map from before F76 is a checksum and a whole serialized index, so its checksum still
/// matches and its next eight bytes are not this: it is refused by name rather than misread.
const MANIFEST_MAGIC: u64 = u64::from_le_bytes(*b"SHOALMAP");

/// What a map counts as it changes, which a manifest carries so an open need not count it again
///
/// Every figure is what the map named when the manifest was written; the intent log replayed over
/// it moves them as it moved them the first time.
#[derive(Debug, Clone, Default, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct MapState {
    /// Every archive the shard knows of
    pub all_archives: Vec<Uuid>,
    /// The archived bytes of every partition of each tablet
    pub tablet_bytes: Vec<u64>,
    /// How many archived partitions each tablet holds
    pub tablet_partitions: Vec<u64>,
    /// How many of them are chains
    pub tablet_chained: Vec<u64>,
    /// The bytes of live records each archive holds, their prefixes included
    pub archive_bytes: Vec<(Uuid, u64)>,
}

/// A map's manifest as it is written
#[derive(Debug, Clone, Default, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// The runs that make up the map, by id, newest first
    pub runs: Vec<u64>,
    /// The id the next run is written under
    pub next_run: u64,
    /// What the map counts
    pub state: MapState,
}

/// Whether a failed open means the file was not there
///
/// # Arguments
///
/// * `error` - The error an open failed with
fn is_not_found(error: &GlommioError<()>) -> bool {
    // glommio reports the same errno in two shapes, so both are unwrapped
    match error {
        GlommioError::IoError(source) | GlommioError::EnhancedIoError { source, .. } => {
            source.kind() == std::io::ErrorKind::NotFound
        }
        _ => false,
    }
}

impl Manifest {
    /// Read a map's manifest, or none if the map was never committed
    ///
    /// # Arguments
    ///
    /// * `path` - The manifest's file
    #[instrument(name = "Manifest::load", err(Debug))]
    pub async fn load(path: &Path) -> Result<Option<Self>, ServerError> {
        // a map never committed has no manifest yet
        let file = match OpenOptions::new().read(true).dma_open(path).await {
            Ok(file) => file,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        // the whole file, which is small: its counters are fixed in size
        let size = file.file_size().await?;
        // truncation cannot happen: a manifest is bounded by its counters
        #[allow(clippy::cast_possible_truncation)]
        let read = file.read_at(0, size as usize).await?;
        file.close().await?;
        // an empty file is a map whose first save never finished, which held nothing
        if read.is_empty() {
            return Ok(None);
        }
        if read.len() < 16 {
            return Err(ServerError::Shoal(ShoalError::TruncatedIntentLog));
        }
        // the checksum over everything after it
        let expected = u64::from_le_bytes(read[..8].try_into()?);
        let mut hasher = GxHasher::default();
        hasher.write(&read[8..]);
        let found = hasher.finish();
        if expected != found {
            return Err(ServerError::Shoal(ShoalError::MapCorruption {
                found,
                expected,
            }));
        }
        // a whole map from before the map was paged is refused by name
        if u64::from_le_bytes(read[8..16].try_into()?) != MANIFEST_MAGIC {
            event!(Level::ERROR, msg = "an archive map was written before F76 paged the map and cannot be read", path = %path.display());
            return Err(ServerError::GlommioGeneric(format!(
                "the archive map at {} was written before F76 paged the map; this build cannot read it",
                path.display()
            )));
        }
        // copied to an aligned buffer, since rkyv reads it in place
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(read.len() - 16);
        aligned.extend_from_slice(&read[16..]);
        let archived = rkyv::access::<ArchivedManifest, rkyv::rancor::Error>(&aligned)?;
        Ok(Some(rkyv::deserialize::<Manifest, rkyv::rancor::Error>(
            archived,
        )?))
    }

    /// Write this manifest over the last one, durably
    ///
    /// # Arguments
    ///
    /// * `path` - The manifest's file
    /// * `temp_path` - Where it is written before it is renamed over the last
    #[instrument(name = "Manifest::save", skip_all, err(Debug))]
    pub async fn save(&self, path: &Path, temp_path: &Path) -> Result<(), ServerError> {
        // the magic and the manifest, and the checksum over both
        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(self)?;
        let mut body = Vec::with_capacity(8 + archived.len());
        body.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        body.extend_from_slice(&archived);
        let mut hasher = GxHasher::default();
        hasher.write(&body);
        // a temp file here is only ever a save that never reached its rename, which holds
        // nothing the committed manifest needs
        // ([Resolved #135](../../../../../../../docs/src/appendix/resolved/leftover-temp-map.md))
        match std::fs::remove_file(temp_path) {
            Ok(()) => event!(
                Level::WARN,
                msg = "removed a temp map a save that did not finish left behind",
                path = %temp_path.display(),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(ServerError::IO(error)),
        }
        // nobody else writes this shard's map, so a file here now is a second writer
        let temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .truncate(true)
            .dma_open(temp_path)
            .await?;
        let mut writer = DmaStreamWriterBuilder::new(temp).build();
        writer.write_all(&hasher.finish().to_le_bytes()).await?;
        writer.write_all(&body).await?;
        writer.sync().await?;
        writer.close().await?;
        // renamed over the last, and the directory synced so the rename and every run it names
        // are durable
        glommio::io::rename(temp_path, path).await?;
        if let Some(parent) = path.parent() {
            let dir = glommio::io::Directory::open(parent).await?;
            dir.sync().await?;
            dir.close().await?;
        }
        Ok(())
    }
}
