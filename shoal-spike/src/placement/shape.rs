//! The pool maps X2 simulates: hosts, devices with their slices, pools, and the changes made to them
//!
//! A shape is what S4 says a pool map holds, stripped to what placement reads: for each device
//! its host, class, size, weight and slices, and a **seat**, the key rendezvous scores it by. The
//! seat is minted with the device and is not its identity; a replacement may take over the old
//! device's seat, which is one of the two ways X2 replaces a device.

use super::score::{mix64, SplitMix};

/// Slices a device can have at most, which is what makes a slice id `uid * MAX_SLICES + index`
pub const MAX_SLICES: u32 = 16;

/// GiB in the lab's 970 EVO 500GB, titan's and hyperion's one device
const EVO_970: u64 = 466;

/// GiB in europa's Optane 900P 280GB
const OPTANE_900P: u64 = 261;

/// GiB in europa's 990 PRO 1TB
const PRO_990: u64 = 931;

/// GiB in an 8 TB device
const TB8: u64 = 7452;

/// GiB in a 4 TB device
const TB4: u64 = 3726;

/// GiB in a 16 TB device
const TB16: u64 = 14_901;

/// GiB in a 3.84 TB SSD
const TB3_84: u64 = 3576;

/// What no two chunks of a stripe may share
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Domain {
    /// No two chunks on one host
    Host,
    /// No two chunks on one device, which every pool also requires
    Device,
}

impl Domain {
    /// The domain as a pool definition spells it
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Domain::Host => "host",
            Domain::Device => "device",
        }
    }
}

/// A pool's redundancy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// Whole copies, where a chunk's position does not matter
    Replicas(usize),
    /// k data chunks and m parity, where chunk `i` lives at position `i`
    Erasure(usize, usize),
}

impl Layout {
    /// How many slices a placement group's answer names
    #[must_use]
    pub fn width(self) -> usize {
        match self {
            Layout::Replicas(r) => r,
            Layout::Erasure(k, m) => k + m,
        }
    }

    /// Whether a chunk that changes position is a different chunk
    #[must_use]
    pub fn positional(self) -> bool {
        matches!(self, Layout::Erasure(_, _))
    }

    /// The layout as the tables print it
    #[must_use]
    pub fn label(self) -> String {
        match self {
            Layout::Replicas(r) => format!("r{r}"),
            Layout::Erasure(k, m) => format!("{k}+{m}"),
        }
    }
}

/// A storage pool, as much of S4's as placement reads
#[derive(Debug, Clone)]
pub struct Pool {
    /// The pool's name
    pub name: &'static str,
    /// The class of device it is made of
    pub class: &'static str,
    /// Its redundancy
    pub layout: Layout,
    /// What no two of its chunks may share
    pub domain: Domain,
}

impl Pool {
    /// The pool as the tables print it, such as `4+2/host`
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}/{}", self.layout.label(), self.domain.name())
    }
}

/// One host, a failure domain
#[derive(Debug, Clone)]
pub struct Host {
    /// Its name
    pub name: String,
    /// The key rendezvous scores it by, where a candidate draws a host first
    pub seat: u64,
}

/// One device
#[derive(Debug, Clone)]
pub struct Device {
    /// Its identity in the simulation, never reused
    pub uid: u32,
    /// The key rendezvous scores it and its slices by
    pub seat: u64,
    /// The host it is on, as an index into the shape's hosts
    pub host: u32,
    /// Its class
    pub class: &'static str,
    /// Its size
    pub gib: u64,
    /// Its weight: its size unless an operator set another
    pub weight: f64,
    /// How many slices it has
    pub slices: u16,
}

impl Device {
    /// The id of one of its slices, stable across every map the device is in
    ///
    /// # Arguments
    ///
    /// * `index` - The slice's index on the device
    #[must_use]
    pub fn slice_id(&self, index: u16) -> u32 {
        self.uid * MAX_SLICES + u32::from(index)
    }

    /// The key rendezvous scores one of its slices by
    ///
    /// # Arguments
    ///
    /// * `index` - The slice's index on the device
    #[must_use]
    pub fn slice_key(&self, index: u16) -> u64 {
        // derived from the seat, so a device that takes over a seat takes over its slices' keys
        mix64(self.seat ^ mix64(u64::from(index) + 1))
    }
}

/// A change to a shape, made to host zero's first device of the pool's class
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// A device like it added to its host
    Add,
    /// It removed
    Remove,
    /// Its weight halved
    Reweight,
    /// It removed and a new device of the same size added to its host, under a new seat
    Replace,
    /// The same, with the new device taking over the old one's seat
    ReplaceSeatKept,
    /// Every device of its host removed
    HostLost,
}

impl Change {
    /// Every change, in the order the tables print them
    pub const ALL: [Change; 6] = [
        Change::Add,
        Change::Remove,
        Change::Reweight,
        Change::Replace,
        Change::ReplaceSeatKept,
        Change::HostLost,
    ];

    /// The change as the tables print it
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Change::Add => "add",
            Change::Remove => "remove",
            Change::Reweight => "reweight ½",
            Change::Replace => "replace",
            Change::ReplaceSeatKept => "replace, seat kept",
            Change::HostLost => "host lost",
        }
    }
}

/// A pool map's devices and the pools over them
#[derive(Debug, Clone)]
pub struct Shape {
    /// The shape's name
    pub name: &'static str,
    /// What it stands for
    pub about: &'static str,
    /// The hosts
    pub hosts: Vec<Host>,
    /// The devices, host by host
    pub devices: Vec<Device>,
    /// The pools placed over them
    pub pools: Vec<Pool>,
    /// The uid the next device is given
    next_uid: u32,
    /// What mints seats
    rng: SplitMix,
}

impl Shape {
    /// An empty shape with named hosts
    ///
    /// # Arguments
    ///
    /// * `name` - The shape's name, which also seeds it
    /// * `about` - What it stands for
    /// * `hosts` - The hosts' names
    fn new(name: &'static str, about: &'static str, hosts: Vec<String>) -> Self {
        // the name seeds every seat, so a shape is the same on every run and host
        let seed = name
            .bytes()
            .fold(0u64, |acc, byte| mix64(acc ^ u64::from(byte)));
        let mut rng = SplitMix::new(seed);
        let hosts = hosts
            .into_iter()
            .map(|name| Host {
                name,
                seat: rng.next_u64(),
            })
            .collect();
        Shape {
            name,
            about,
            hosts,
            devices: Vec::new(),
            pools: Vec::new(),
            next_uid: 0,
            rng,
        }
    }

    /// Add a device to a host
    ///
    /// # Arguments
    ///
    /// * `host` - The host's index
    /// * `class` - The device's class
    /// * `gib` - Its size, which is also its weight
    /// * `slices` - How many slices it has
    fn device(&mut self, host: u32, class: &'static str, gib: u64, slices: u16) -> &mut Self {
        // a fresh uid and a fresh seat, as a device's first claim mints
        let device = Device {
            uid: self.next_uid,
            seat: self.rng.next_u64(),
            host,
            class,
            gib,
            weight: gib as f64,
            slices,
        };
        self.next_uid += 1;
        self.devices.push(device);
        self
    }

    /// Add a pool
    ///
    /// # Arguments
    ///
    /// * `name` - The pool's name
    /// * `class` - The class it is made of
    /// * `layout` - Its redundancy
    /// * `domain` - Its failure domain
    fn pool(
        &mut self,
        name: &'static str,
        class: &'static str,
        layout: Layout,
        domain: Domain,
    ) -> &mut Self {
        self.pools.push(Pool {
            name,
            class,
            layout,
            domain,
        });
        self
    }

    /// A shape of identical hosts of identical devices
    ///
    /// # Arguments
    ///
    /// * `name` - The shape's name
    /// * `about` - What it stands for
    /// * `hosts` - How many hosts
    /// * `devices` - Devices a host
    /// * `gib` - A device's size
    fn uniform(
        name: &'static str,
        about: &'static str,
        hosts: usize,
        devices: usize,
        gib: u64,
    ) -> Self {
        // hosts named by index, every one alike
        let mut shape = Shape::new(name, about, (0..hosts).map(|h| format!("h{h}")).collect());
        for host in 0..hosts {
            for _ in 0..devices {
                shape.device(host as u32, "ssd", gib, 1);
            }
        }
        shape
    }

    /// The lab's three hosts, named as they are
    ///
    /// # Arguments
    ///
    /// * `name` - The shape's name
    /// * `about` - What it stands for
    fn lab(name: &'static str, about: &'static str) -> Self {
        Shape::new(
            name,
            about,
            ["europa", "titan", "hyperion"]
                .iter()
                .map(|host| (*host).to_string())
                .collect(),
        )
    }

    /// Every shape X2 simulates, in the order the tables print them
    #[must_use]
    pub fn all() -> Vec<Shape> {
        let mut shapes = Vec::new();
        // the lab with one device a host
        let mut lab1 = Shape::lab("lab-1", "the lab, one 500 GB device a host");
        for host in 0..3 {
            lab1.device(host, "ssd", EVO_970, 1);
        }
        lab1.pool("p", "ssd", Layout::Replicas(3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(2, 1), Domain::Host);
        shapes.push(lab1);
        // the lab with two
        let mut lab2 = Shape::lab("lab-2", "the lab, two 500 GB devices a host");
        for host in 0..3 {
            lab2.device(host, "ssd", EVO_970, 1)
                .device(host, "ssd", EVO_970, 1);
        }
        lab2.pool("p", "ssd", Layout::Replicas(3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(2, 1), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(4, 2), Domain::Device);
        shapes.push(lab2);
        // the lab as it is fitted: europa's Optane and 990 PRO, a 970 EVO in each Zen1 host
        let mut fitted = Shape::lab(
            "lab-fitted",
            "the lab as fitted: 261 + 931 GiB on europa, 466 on titan and hyperion",
        );
        fitted
            .device(0, "ssd", OPTANE_900P, 1)
            .device(0, "ssd", PRO_990, 1)
            .device(1, "ssd", EVO_970, 1)
            .device(2, "ssd", EVO_970, 1);
        fitted
            .pool("p", "ssd", Layout::Replicas(3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(2, 1), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(3, 1), Domain::Device);
        shapes.push(fitted);
        // six hosts of twelve identical devices
        let mut six = Shape::uniform("6x12", "six hosts of twelve 8 TB devices", 6, 12, TB8);
        six.pool("p", "ssd", Layout::Replicas(3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(4, 2), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(8, 3), Domain::Device);
        shapes.push(six);
        // two sizes alternating inside every host, so the hosts weigh the same
        let mut mixed = Shape::new(
            "6x12-mixed",
            "six hosts, each of six 4 TB and six 16 TB devices",
            (0..6).map(|h| format!("h{h}")).collect(),
        );
        for host in 0..6 {
            for device in 0..12 {
                let gib = if device % 2 == 0 { TB4 } else { TB16 };
                mixed.device(host, "ssd", gib, 1);
            }
        }
        mixed
            .pool("p", "ssd", Layout::Erasure(4, 2), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(8, 3), Domain::Device);
        shapes.push(mixed);
        // two sizes split by host, so the hosts weigh one to four
        let mut uneven = Shape::new(
            "6x12-uneven",
            "three hosts of twelve 4 TB devices, three of twelve 16 TB",
            (0..6).map(|h| format!("h{h}")).collect(),
        );
        for host in 0..6 {
            let gib = if host < 3 { TB4 } else { TB16 };
            for _ in 0..12 {
                uneven.device(host, "ssd", gib, 1);
            }
        }
        uneven
            .pool("p", "ssd", Layout::Replicas(3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(4, 2), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(8, 3), Domain::Device);
        shapes.push(uneven);
        // devices of one slice beside devices of four
        let mut sliced = Shape::new(
            "6x12-slices",
            "six hosts of twelve 8 TB devices, every other one in four slices",
            (0..6).map(|h| format!("h{h}")).collect(),
        );
        for host in 0..6 {
            for device in 0..12 {
                let slices = if device % 2 == 0 { 1 } else { 4 };
                sliced.device(host, "ssd", TB8, slices);
            }
        }
        sliced
            .pool("p", "ssd", Layout::Erasure(4, 2), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(8, 3), Domain::Device);
        shapes.push(sliced);
        // fifty hosts of twenty-four
        let mut fifty = Shape::uniform("50x24", "fifty hosts of twenty-four 16 TB devices", 50, 24, TB16);
        fifty
            .pool("p", "ssd", Layout::Replicas(3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(8, 3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(10, 4), Domain::Host);
        shapes.push(fifty);
        // two classes in every host, and a pool on each
        let mut classes = Shape::new(
            "two-classes",
            "six hosts, each of eight 16 TB hdd and four 3.84 TB ssd in two slices",
            (0..6).map(|h| format!("h{h}")).collect(),
        );
        for host in 0..6 {
            for _ in 0..8 {
                classes.device(host, "hdd", TB16, 1);
            }
            for _ in 0..4 {
                classes.device(host, "ssd", TB3_84, 2);
            }
        }
        classes
            .pool("bulk", "hdd", Layout::Erasure(4, 2), Domain::Host)
            .pool("fast", "ssd", Layout::Replicas(3), Domain::Host);
        shapes.push(classes);
        shapes
    }

    /// The shapes the lookups are timed on, which add fifty hosts of devices in four slices
    #[must_use]
    pub fn timed() -> Vec<Shape> {
        // the lab, the fitted lab, six by twelve and fifty by twenty-four from the simulation
        let mut shapes: Vec<Shape> = Shape::all()
            .into_iter()
            .filter(|shape| matches!(shape.name, "lab-2" | "lab-fitted" | "6x12" | "50x24"))
            .collect();
        // and fifty by twenty-four again with every device in four slices
        let mut sliced = Shape::new(
            "50x24-slices",
            "fifty hosts of twenty-four 16 TB devices, each in four slices",
            (0..50).map(|h| format!("h{h}")).collect(),
        );
        for host in 0..50 {
            for _ in 0..24 {
                sliced.device(host, "ssd", TB16, 4);
            }
        }
        sliced
            .pool("p", "ssd", Layout::Erasure(8, 3), Domain::Host)
            .pool("p", "ssd", Layout::Erasure(10, 4), Domain::Host);
        shapes.push(sliced);
        shapes
    }

    /// How many distinct failure domains a pool's devices span
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool
    #[must_use]
    pub fn domains_of(&self, pool: &Pool) -> usize {
        // the distinct hosts, or the devices, of the pool's class
        let devices = self.devices.iter().filter(|device| device.class == pool.class);
        match pool.domain {
            Domain::Device => devices.count(),
            Domain::Host => {
                let mut hosts: Vec<u32> = devices.map(|device| device.host).collect();
                hosts.sort_unstable();
                hosts.dedup();
                hosts.len()
            }
        }
    }

    /// The shape after a change, made to host zero's first device of a class
    ///
    /// # Arguments
    ///
    /// * `change` - The change
    /// * `class` - The class of the device it is made to
    #[must_use]
    pub fn changed(&self, change: Change, class: &'static str) -> Shape {
        let mut shape = self.clone();
        // the device every change is made to
        let target = shape
            .devices
            .iter()
            .position(|device| device.host == 0 && device.class == class)
            .expect("host zero has a device of every class a pool names");
        let old = shape.devices[target].clone();
        match change {
            Change::Add => {
                // another like it, on the same host, under a fresh seat
                shape.device(old.host, old.class, old.gib, old.slices);
            }
            Change::Remove => {
                shape.devices.remove(target);
            }
            Change::Reweight => {
                shape.devices[target].weight = old.weight / 2.0;
            }
            Change::Replace => {
                // the old one gone and a new one in its host, with a uid and seat of its own
                shape.devices.remove(target);
                shape.device(old.host, old.class, old.gib, old.slices);
            }
            Change::ReplaceSeatKept => {
                // the same, but the new device sits in the old one's seat
                shape.devices.remove(target);
                shape.device(old.host, old.class, old.gib, old.slices);
                shape.devices.last_mut().expect("a device was just added").seat = old.seat;
            }
            Change::HostLost => {
                shape.devices.retain(|device| device.host != old.host);
            }
        }
        shape
    }
}
