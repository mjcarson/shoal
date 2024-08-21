//! The file system storage module for shoal

use std::{collections::BTreeMap, marker::PhantomData, path::PathBuf};

use futures::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use rkyv::{Archive, Deserialize};
use rkyv_with::DeserializeWith;

use super::{Intents, ShoalStorage};
use crate::{
    server::Conf,
    shared::traits::{Archivable, ShoalTable},
    storage::ArchivedIntents,
    tables::partitions::Partition,
};

/// Store shoal data in an existing filesytem for persistence
pub struct FileSystem<T: std::fmt::Debug + Archive> {
    /// The name of the shard we are storing data for
    shard_name: String,
    /// The intent log to write too
    intent_log: DmaStreamWriter,
    /// The data we are storing
    store_data: PhantomData<T>,
}

impl<T: ShoalTable + Archivable> ShoalStorage<T> for FileSystem<T> {
    /// Create a new instance of this storage engine
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - The Shoal config
    async fn new(shard_name: &str, conf: &Conf) -> Self {
        // build the path to this shards intent log
        let path = conf
            .storage
            .fs
            .path
            .join(format!("Intents/{shard_name}-active"));
        // open this file
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await
            .unwrap();
        // wrap our file in a stream writer
        let intent_log = DmaStreamWriterBuilder::new(file).build();
        // build our file system storage module
        FileSystem {
            shard_name: shard_name.to_owned(),
            intent_log,
            store_data: PhantomData,
        }
    }

    /// Write this new row to our storage
    ///
    /// # Arguments
    ///
    /// * `insert` - The row to write
    async fn insert(&mut self, insert: &Intents<T>) {
        // archive our queries
        let archived = rkyv::to_bytes::<_, 1024>(insert).unwrap();
        // get the size of the data to write
        let size = archived.len().to_le_bytes();
        // write our size
        self.intent_log.write_all(&size).await.unwrap();
        // write our data
        self.intent_log
            .write_all(archived.as_slice())
            .await
            .unwrap();
        // flush our data
        self.flush().await;
    }

    /// Flush all currently pending writes to storage
    async fn flush(&mut self) {
        self.intent_log.flush().await.unwrap();
    }

    /// Read an intent log from storage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the intent log to read in
    async fn read_intents(path: &PathBuf, partitions: &mut BTreeMap<T::Sort, Partition<T>>) {
        // open the target intent log
        let file = DmaFile::open(path).await.unwrap();
        println!("{:?} -> {}", path, file.file_size().await.unwrap());
        // don't read a file with 0 bytes
        if file.file_size().await.unwrap() == 0 {
            // bail out since this shards intent log is empty
            return;
        }
        println!("READ INTENTS?");
        // track the amount of data we have currently read
        let mut pos = 0;
        loop {
            // try to read the size of the next entry in this intent log
            let read = file.read_at(pos, 8).await.unwrap();
            // check if we read any data
            let size = usize::from_le_bytes(read[..8].try_into().unwrap());
            // if size is 0 then stop reading entries
            println!("size -> {size}");
            if size == 0 {
                break;
            }
            // increment our pos 8
            pos += 8;
            // try to read the next entry
            let read = file.read_at(pos, size).await.unwrap();
            // try to deserialize this row from our intent log
            let intent = unsafe { rkyv::archived_root::<Intents<T>>(&read[..]) };
            // add this intent to our btreemap
            match intent {
                ArchivedIntents::Insert(archived) => {
                    // deserialize this row
                    let row = <T as Archivable>::deserialize(archived);
                    // get our sort  for this row
                    let key = row.get_sort().clone();
                    println!("INSERTING {row:?} at {key:?}");
                    // get this rows partition
                    let entry = partitions.entry(key).or_default();
                    // insert this row
                    entry.insert(row);
                }
            }
            pos += size as u64;
        }
    }
}
