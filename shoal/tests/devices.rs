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
    // a disk has one slice, so a rotational run cuts the device into one (F91)
    let slices = if utils::rotational_run() { 1 } else { 2 };
    let spec = DeviceSpec::new(device.path(), "ssd").slices(slices);
    // the first start claims it new, under ids of its own
    let pool = node(&dir, &[spec.clone()])?;
    let first = pool.devices();
    assert_eq!(first.len(), 1);
    let old = first[0].id;
    let old_slices: Vec<_> = first[0].slices.iter().map(|slice| slice.id).collect();
    assert_eq!(old_slices.len(), usize::from(slices));
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
    assert_eq!(new.slices.len(), usize::from(slices));
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
    let journal = utils::test_dir();
    // a rotational device keeps a disk's rules whatever its class says: a journal on an SSD and
    // its cache writing through (F91)
    let _facts = utils::disk_facts(&[device.path().to_path_buf()], Some("write through"));
    let pool = node(&dir, &[DeviceSpec::new(device.path(), "ssd").journal(journal.path())])?;
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

/// Whether a start was refused, naming a device and something about it
///
/// # Arguments
///
/// * `started` - What the start answered
/// * `named` - What the refusal says
fn refused_naming(started: Result<ShoalPool<plain::Notes>, ServerError>, named: &str) {
    match started {
        Err(ServerError::Shoal(ShoalError::DeviceRefused { why, .. })) => {
            assert!(why.contains(named), "refused, but not for {named:?}: {why}");
        }
        Err(other) => panic!("refused otherwise than by a device's name: {other}"),
        Ok(pool) => {
            let _ = pool.exit();
            panic!("a start that breaks a disk's rule was not refused ({named:?})");
        }
    }
}

/// S6's `rotational_device_with_its_cache_on_is_refused`. A device the kernel calls rotational
/// whose disk's write cache writes back is not started, unless its configuration says the cache is
/// on, and then it is named in a warning; one whose cache writes through starts with no such
/// warning (X7, F91)
#[test]
fn rotational_device_with_its_cache_on_is_refused() -> Result<(), ServerError> {
    let dir = utils::test_dir();
    let device = utils::test_dir();
    let journal = utils::test_dir();
    let spec = DeviceSpec::new(device.path(), "hdd").journal(journal.path());
    // its cache writing back refuses the node, by the device's name and the cache's
    {
        let facts = utils::disk_facts(&[device.path().to_path_buf()], Some("write back"));
        refused_naming(node(&dir, std::slice::from_ref(&spec)), "write cache is write back");
        drop(facts);
        // as does a kernel that says nothing of it
        let _facts = utils::disk_facts(&[device.path().to_path_buf()], None);
        refused_naming(node(&dir, std::slice::from_ref(&spec)), "write cache is unknown");
    }
    // the configuration's word that the cache is on starts it, named in a warning
    {
        let _facts = utils::disk_facts(&[device.path().to_path_buf()], Some("write back"));
        let on = spec.clone().write_cache(shoal::shared::devices::WriteCache::On);
        let pool = node(&dir, &[on])?;
        let devices = pool.devices();
        assert_eq!(devices[0].health, DeviceHealth::Serving);
        assert!(
            devices[0].warnings.iter().any(|warning| warning.contains("write cache is write back")),
            "{:?}",
            devices[0].warnings
        );
        pool.exit()?;
    }
    // and a cache writing through starts it with no word of the cache
    let _facts = utils::disk_facts(&[device.path().to_path_buf()], Some("write through"));
    let pool = node(&dir, std::slice::from_ref(&spec))?;
    let devices = pool.devices();
    assert_eq!(devices[0].health, DeviceHealth::Serving);
    assert!(devices[0].rotational);
    assert_eq!(devices[0].write_cache.as_deref(), Some("write through"));
    assert!(
        devices[0].warnings.iter().all(|warning| !warning.contains("write cache")),
        "{:?}",
        devices[0].warnings
    );
    pool.exit()?;
    Ok(())
}

/// S6's `rotational_device_needs_a_journal_device`. A rotational device with no journal named, one
/// on a filesystem other than XFS, one cut into slices, or one whose journal is on a disk, missing
/// or inside it, is refused at start by name, and nothing is created; a good journal holds the
/// slice's segments, none under the device; and devices that share a journal are one `device`
/// failure domain (X7, S5, F91)
#[test]
fn rotational_device_needs_a_journal_device() -> Result<(), ServerError> {
    use shoal::shared::pools::{FailureDomain, Media, PoolSpec, Redundancy};
    let dir = utils::test_dir();
    let device = utils::test_dir();
    let journal = utils::test_dir();
    let disk = [device.path().to_path_buf()];
    let spec = DeviceSpec::new(device.path(), "hdd").journal(journal.path());
    {
        let _facts = utils::disk_facts(&disk, Some("write through"));
        // no journal
        refused_naming(node(&dir, &[DeviceSpec::new(device.path(), "hdd")]), "names no journal");
        // more than one slice
        refused_naming(node(&dir, &[spec.clone().slices(2)]), "a disk has one slice");
        // a journal that is not there, which is never created
        let missing = journal.path().join("not-mounted");
        refused_naming(
            node(&dir, &[DeviceSpec::new(device.path(), "hdd").journal(&missing)]),
            "never created",
        );
        assert!(!missing.exists(), "the claim created a journal's directory");
        // a journal inside the device
        refused_naming(
            node(&dir, &[DeviceSpec::new(device.path(), "hdd").journal(device.path().join("journal"))]),
            "in or around the device",
        );
    }
    // a journal on a disk the kernel calls rotational
    {
        let _facts = utils::disk_facts(&[device.path().to_path_buf(), journal.path().to_path_buf()], Some("write through"));
        refused_naming(node(&dir, std::slice::from_ref(&spec)), "rotational");
    }
    // a disk on ext4
    {
        let _facts = utils::disk_facts(&disk, Some("write through"));
        shoal::server::devices::probe::set_override(Some(shoal::server::devices::probe::ProbeOverride {
            filesystem: Some("ext4".to_string()),
            rotational: disk.to_vec(),
            shared_filesystem: true,
            write_cache: Some("write through".to_string()),
            every_device_rotational: false,
        }));
        refused_naming(node(&dir, std::slice::from_ref(&spec)), "XFS alone");
    }
    // a good journal holds the slice's segments, in a directory of the device's and the slice's
    let _facts = utils::disk_facts(&disk, Some("write through"));
    let pool = node(&dir, std::slice::from_ref(&spec))?;
    let devices = pool.devices();
    assert_eq!(devices[0].health, DeviceHealth::Serving);
    assert_eq!(devices[0].journal.as_deref(), Some(journal.path().to_string_lossy().as_ref()));
    let under = journal.path().join(devices[0].id.to_string()).join("slice-0");
    assert!(under.join("segment-0").exists(), "no segment under {}", under.display());
    assert!(under.join("shoal-slice-journal.json").exists());
    assert!(journal.path().join("shoal-journal.json").exists());
    assert!(!device.path().join("slice-0").join("journal").exists(), "a disk's journal on the disk");
    pool.exit()?;
    drop(_facts);
    // two disks on one journal are one device domain; on two journals, two
    let second = utils::test_dir();
    let other_journal = utils::test_dir();
    let both = [device.path().to_path_buf(), second.path().to_path_buf()];
    let pool_spec = PoolSpec::new("hdd", Redundancy::Replicas(2))
        .failure_domain(FailureDomain::Device)
        .media(Media::Rotational)
        .accept_rebuild_exposure();
    let with_pool = |devices: &[DeviceSpec]| -> Result<ShoalPool<plain::Notes>, ServerError> {
        let mut conf = utils::with_devices(utils::build_config(&dir), devices);
        conf.storage = conf.storage.clone().pool("bulk", pool_spec.clone());
        let mut pool = ShoalPool::<plain::Notes>::start(conf)?;
        pool.ready(READY)?;
        Ok(pool)
    };
    let _facts = utils::disk_facts(&both, Some("write through"));
    let shared = with_pool(&[spec.clone(), DeviceSpec::new(second.path(), "hdd").journal(journal.path())])?;
    let status = shared.pool_status()?;
    assert!(
        status[0].short.iter().any(|why| why.contains("2 distinct devices and its devices span 1")),
        "{status:?}"
    );
    assert!(status[0].warnings.iter().any(|why| why.contains("accepts the exposure")), "{status:?}");
    shared.exit()?;
    // a disk moved to a journal of its own holds no chunk, so its journal moves with it
    let apart = with_pool(&[spec, DeviceSpec::new(second.path(), "hdd").journal(other_journal.path())])?;
    let status = apart.pool_status()?;
    assert!(status[0].ready(), "{status:?}");
    apart.exit()?;
    Ok(())
}
