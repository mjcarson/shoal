//! A node's devices and their slices: claimed, reported and refused by name (S4, F85)
//!
//! A device is claimed the way a storage root is, with three outcomes and no fourth: an empty
//! directory is a new device, a marked one is held to this node, and files with no marker are
//! somebody's data ([S4](../../docs/src/object-storage/pools-and-devices.md#a-device-has-slices)).
//! Every node here is standalone and in process, its devices directories under the test's own.

use shoal::server::errors::ShoalError;
use shoal::server::ServerError;
use shoal::shared::devices::{DeviceHealth, DeviceSpec};
use shoal::ShoalPool;

mod utils;

/// The schema every node here serves: one table, and no bucket
mod plain {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::PersistentUnsortedTable;
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A note, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Notes")]
    pub struct Note {
        /// Its key
        #[shoal(partition)]
        pub id: u64,
        /// What it says
        #[shoal(update)]
        pub text: String,
    }

    /// The database every node here serves
    #[db]
    pub struct Notes {
        /// The notes
        pub notes: PersistentUnsortedTable<Note, FileSystem>,
    }
}

/// How long a node has to answer
const READY: std::time::Duration = std::time::Duration::from_secs(30);

/// Start a standalone node over a test directory with some devices
///
/// # Arguments
///
/// * `dir` - The node's storage
/// * `devices` - Its devices
fn node(
    dir: &tempfile::TempDir,
    devices: &[DeviceSpec],
) -> Result<ShoalPool<plain::Notes>, ServerError> {
    let conf = utils::with_devices(utils::build_config(dir), devices);
    let mut pool = ShoalPool::<plain::Notes>::start(conf)?;
    pool.ready(READY)?;
    Ok(pool)
}

/// A device directory emptied and restarted comes up under a new id with new slices, holding
/// nothing, and the old ids are never served (S4)
#[test]
fn empty_directory_is_a_new_device() -> Result<(), ServerError> {
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let device = utils::test_dir();
    let spec = DeviceSpec::new(device.path(), "ssd").slices(2);
    // the first start claims it new, under ids of its own
    let pool = node(&dir, &[spec.clone()])?;
    let first = pool.devices();
    assert_eq!(first.len(), 1);
    let old = first[0].id;
    let old_slices: Vec<_> = first[0].slices.iter().map(|slice| slice.id).collect();
    assert_eq!(old_slices.len(), 2);
    assert_eq!(first[0].health, DeviceHealth::Serving);
    pool.exit()?;
    // a replaced disk mounted at the old path: the directory empty
    for entry in std::fs::read_dir(device.path())? {
        let path = entry?.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
    }
    let pool = node(&dir, &[spec])?;
    let devices = pool.devices();
    // the new device, with new slices holding nothing
    let new = devices
        .iter()
        .find(|report| report.health == DeviceHealth::Serving)
        .expect("the new device serves");
    assert_ne!(new.id, old);
    assert_eq!(new.slices.len(), 2);
    for slice in &new.slices {
        assert!(!old_slices.contains(&slice.id), "an old slice id came back: {slice:?}");
        assert_eq!(slice.chunks, 0);
    }
    // and the old id is failed, never served
    let gone = devices
        .iter()
        .find(|report| report.id == old)
        .expect("the old id is still reported");
    assert!(
        matches!(&gone.health, DeviceHealth::Failed { reason } if reason.contains("found empty")),
        "{gone:?}"
    );
    // the pool map holds both, the old one failed
    let map = pool.pool_map()?;
    assert!(map.devices[&old].failed.is_some());
    assert!(map.devices[&new.id].failed.is_none());
    // and no request naming an old slice is served
    let store = pool.object_store().expect("the node has object executors");
    for slice in &old_slices {
        let label = shoal::shared::objects::Label::default();
        let id = shoal::server::objects::store::ChunkId {
            consumer: 1,
            pg: shoal::shared::poolmap::PlacementGroup::default(),
            object: 1,
            stripe: 0,
            position: 0,
        };
        let refused = futures::executor::block_on(store.read(*slice, id, 0, 1, label));
        assert_eq!(
            refused,
            Err(shoal::server::objects::store::StoreError::UnknownSlice(*slice))
        );
    }
    pool.exit()?;
    Ok(())
}

/// A non-empty directory with no marker stops the node by name (S4)
#[test]
fn unmarked_directory_with_files_is_refused() -> Result<(), ServerError> {
    let _facts = utils::device_facts(&[]);
    let dir = utils::test_dir();
    let device = utils::test_dir();
    std::fs::write(device.path().join("photos.tar"), b"somebody's data")?;
    let refused = match node(&dir, &[DeviceSpec::new(device.path(), "ssd")]) {
        Ok(pool) => {
            pool.exit()?;
            panic!("a directory of somebody's files was claimed as a device");
        }
        Err(error) => error,
    };
    match &refused {
        ServerError::Shoal(ShoalError::DeviceRefused { path, why }) => {
            assert_eq!(path, device.path());
            assert!(why.contains("files and no marker") && why.contains("photos.tar"), "{why}");
        }
        other => panic!("refused, but not by name: {other:?}"),
    }
    // and nothing was written into it
    let entries: Vec<_> = std::fs::read_dir(device.path())?.collect();
    assert_eq!(entries.len(), 1);
    Ok(())
}

/// A device the kernel calls rotational under a class that says otherwise starts, keeps its class,
/// and is named in a warning (S4)
#[test]
fn mislabelled_device_is_warned_about_and_not_overruled() -> Result<(), ServerError> {
    let dir = utils::test_dir();
    let device = utils::test_dir();
    let _facts = utils::device_facts(&[device.path().to_path_buf()]);
    let pool = node(&dir, &[DeviceSpec::new(device.path(), "ssd")])?;
    let devices = pool.devices();
    assert_eq!(devices.len(), 1);
    // it serves, under the class its operator wrote
    assert_eq!(devices[0].health, DeviceHealth::Serving);
    assert_eq!(devices[0].class, "ssd");
    assert!(devices[0].rotational);
    assert!(
        devices[0]
            .warnings
            .iter()
            .any(|warning| warning.contains("rotational") && warning.contains("keeps its class")),
        "{:?}",
        devices[0].warnings
    );
    // and the pool map draws it as an ssd
    let map = pool.pool_map()?;
    let era = map.devices[&devices[0].id].current().expect("on the map");
    assert_eq!(era.class, "ssd");
    pool.exit()?;
    Ok(())
}
