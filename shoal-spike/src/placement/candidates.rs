//! The placement candidates of S5, each a function of a placement group and one pool map
//!
//! Every candidate here is stateless: its answer is a function of the placement group and the
//! [`View`] built from a map, and nothing else. The planner's table is not, and lives in
//! `table.rs`. The candidates:
//!
//! - **window**: the tablet rule extended. The pool's slices in one ordered list, interleaved
//!   host by host so that neighbours are on different hosts where they can be, and placement
//!   group `p` takes `list[(p + k) % N]` for position `k`. It repairs nothing: an answer that puts
//!   two positions in one domain is counted, which is what its feasibility column is
//! - **rendezvous**: S5 as written. Every slice is scored by weighted rendezvous, and position
//!   by position the best slice in a domain not yet used is taken. Taking the best slice in each
//!   new domain in turn is the same as ranking the domains by their best slice, which is how it
//!   is computed: one pass over the slices
//! - **rendezvous by position**: each position draws on its own, round `pos` first, and keeps
//!   its draw unless a position before it took that domain; a position that collides draws
//!   again a round later. It is the idea of CRUSH's `indep` mode, written for this map: when a
//!   slice leaves, only the positions that drew it draw again
//! - **rendezvous by domain**, and **by domain, by position**: the same two, drawn down the
//!   hierarchy, a host by its weight, then a device in it, then a slice of that device. They are
//!   what a lookup costs when a pool has thousands of slices
//! - **rendezvous, positions matched**, and **by domain, positions matched**: the set from
//!   `rendezvous` or `rendezvous by domain`, and the positions from a matching of the set's
//!   domains to positions by draws that no weight enters: every domain and position pair is
//!   drawn, and taken highest first while both are free. A reweight that leaves the set alone
//!   then moves no position, nor does a device that replaces another in the same host; a domain
//!   that leaves frees only its own position unless the one that replaces it outbids another
//!
//! A slice's weight is an equal share of its device's, and a domain's or device's weight is the
//! sum of its slices'. A draw is [`score::draw`] and its score [`Log2Table::score`].

use std::ops::Range;

use super::score::{draw, group_key, item_key, libm_score, log2_table, Log2Table};
use super::shape::{Device, Domain, Pool, Shape};

/// The widest answer any pool here asks for
pub const MAX_WIDTH: usize = 16;

/// Rounds a by-position candidate draws before it falls back to taking positions in turn
pub const ROUNDS: u32 = 32;

/// A position not yet filled
const EMPTY: u32 = u32::MAX;

/// What a slice's key is salted with for the draws that match it to a position
const POSITION_SALT: u64 = 0x706f_7369_7469_6f6e;

/// One placement group
#[derive(Debug, Clone, Copy)]
pub struct Pg {
    /// The key it is drawn by: its consumer, tablet and sub-range together
    pub key: u64,
    /// Its index among the consumer's placement groups, which only the window reads
    pub index: u64,
}

/// Every placement group of one consumer, at a number a tablet
///
/// # Arguments
///
/// * `consumer` - The consumer's id
/// * `per_tablet` - Placement groups a tablet
#[must_use]
pub fn pgs(consumer: u64, per_tablet: u32) -> Vec<Pg> {
    // a tablet is the top twelve bits of a stripe's key, and a placement group the next few
    let mut pgs = Vec::with_capacity(4096 * per_tablet as usize);
    for tablet in 0..4096u64 {
        for sub in 0..u64::from(per_tablet) {
            // the key names all three, so two consumers' groups never draw alike
            let key = super::score::mix64(consumer ^ super::score::mix64((tablet << 16) | sub));
            pgs.push(Pg {
                key,
                index: tablet * u64::from(per_tablet) + sub,
            });
        }
    }
    pgs
}

/// The candidates that are a function of the map alone
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Candidate {
    /// The tablet rule extended
    Window,
    /// Weighted rendezvous, the best slice in each new domain in turn, as S5 states it
    Rendezvous,
    /// The same with libm's logarithm, for comparing choices and cost
    RendezvousLibm,
    /// Weighted rendezvous, each position drawing on its own
    ByPosition,
    /// Weighted rendezvous down the hierarchy, domains in turn
    ByDomain,
    /// Weighted rendezvous down the hierarchy, each position drawing on its own
    ByDomainByPosition,
    /// The set from rendezvous, its positions by an unweighted matching
    Matched,
    /// The set from rendezvous by domain, its positions by an unweighted matching
    ByDomainMatched,
}

impl Candidate {
    /// The candidates whose fill differs: the matched two place the same sets as the two they match
    pub const FILLED: [Candidate; 5] = [
        Candidate::Window,
        Candidate::Rendezvous,
        Candidate::ByPosition,
        Candidate::ByDomain,
        Candidate::ByDomainByPosition,
    ];

    /// The candidates the simulation compares, the libm variant aside
    pub const SIMULATED: [Candidate; 7] = [
        Candidate::Window,
        Candidate::Rendezvous,
        Candidate::ByPosition,
        Candidate::ByDomain,
        Candidate::ByDomainByPosition,
        Candidate::Matched,
        Candidate::ByDomainMatched,
    ];

    /// The candidate as the tables print it
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Candidate::Window => "window",
            Candidate::Rendezvous => "rendezvous",
            Candidate::RendezvousLibm => "rendezvous, libm ln",
            Candidate::ByPosition => "rendezvous by position",
            Candidate::ByDomain => "rendezvous by domain",
            Candidate::ByDomainByPosition => "by domain, by position",
            Candidate::Matched => "rendezvous, positions matched",
            Candidate::ByDomainMatched => "by domain, positions matched",
        }
    }
}

/// What a lookup found out about its own answer
#[derive(Debug, Default, Clone, Copy)]
pub struct Flags {
    /// Two positions share a domain: only the window can answer so
    pub violation: bool,
    /// A by-position candidate ran out of rounds and took the rest in turn
    pub fallback: bool,
    /// Rounds a by-position candidate drew
    pub rounds: u32,
}

/// One slice a pool can place on
#[derive(Debug, Clone)]
pub struct SliceView {
    /// Its id, stable across maps
    pub id: u32,
    /// Its placement key, unmixed
    pub raw: u64,
    /// Its key mixed for round zero
    pub key: u64,
    /// One over its weight
    pub inv_w: f64,
    /// Its device, as an index into the view's devices
    pub device: u32,
    /// Its failure domain, as an index into the view's domains
    pub domain: u32,
}

/// One device a pool can place on
#[derive(Debug, Clone)]
pub struct DeviceView {
    /// Its uid
    pub uid: u32,
    /// Its seat, unmixed
    pub raw: u64,
    /// Its seat mixed for round zero
    pub key: u64,
    /// One over its weight
    pub inv_w: f64,
    /// Its weight
    pub weight: f64,
    /// Its host, as an index into the shape's hosts
    pub host: u32,
    /// Its failure domain, as an index into the view's domains
    pub domain: u32,
    /// Its slices, a range of the view's
    pub slices: Range<usize>,
}

/// One failure domain a pool can place in
#[derive(Debug, Clone)]
pub struct DomainView {
    /// Its key, unmixed: a host's seat, or a device's under a device domain
    pub raw: u64,
    /// Its key mixed for round zero
    pub key: u64,
    /// One over its weight, the sum of its devices'
    pub inv_w: f64,
    /// Its devices, a range of the view's
    pub devices: Range<usize>,
}

/// What a lookup needs from one pool map, built once each time the map changes
pub struct View {
    /// The pool's slices, device by device, domain by domain
    pub slices: Vec<SliceView>,
    /// The pool's devices, domain by domain
    pub devices: Vec<DeviceView>,
    /// The pool's failure domains
    pub domains: Vec<DomainView>,
    /// Slices a placement group's answer names
    pub width: usize,
    /// Whether a chunk's position matters
    pub positional: bool,
    /// The window's order: indices into `slices`, host by host in turn
    pub window: Vec<u32>,
    /// Slice keys mixed for the rounds below the width, round by round
    pub slice_rounds: Vec<u64>,
    /// Domain keys mixed for the rounds below the width, round by round
    pub domain_rounds: Vec<u64>,
    /// Domain keys for each position a matching draws, salted apart from every round's, position by position
    pub position_keys: Vec<u64>,
    /// The logarithm every score takes
    pub log: &'static Log2Table,
}

/// What one lookup reuses from the last
#[derive(Default)]
pub struct Scratch {
    /// Each domain's best score, or each slice's
    scores: Vec<f64>,
    /// Each domain's best slice
    best: Vec<u32>,
    /// Whether each domain is taken
    used: Vec<bool>,
}

impl View {
    /// Build the view of one pool over one map
    ///
    /// # Arguments
    ///
    /// * `shape` - The map
    /// * `pool` - The pool
    #[must_use]
    pub fn new(shape: &Shape, pool: &Pool) -> Self {
        // the pool's devices, held to their host order so a host's devices are contiguous
        let mut chosen: Vec<&Device> = shape
            .devices
            .iter()
            .filter(|device| device.class == pool.class)
            .collect();
        chosen.sort_by_key(|device| device.host);
        let mut slices = Vec::new();
        let mut devices = Vec::new();
        let mut domains: Vec<DomainView> = Vec::new();
        let mut domain_weights: Vec<f64> = Vec::new();
        let mut last_host = None;
        for device in chosen {
            // a new domain at every device under a device domain, or at every host
            let opens = match pool.domain {
                Domain::Device => true,
                Domain::Host => last_host != Some(device.host),
            };
            if opens {
                let raw = match pool.domain {
                    Domain::Device => device.seat,
                    Domain::Host => shape.hosts[device.host as usize].seat,
                };
                domains.push(DomainView {
                    raw,
                    key: item_key(raw, 0),
                    inv_w: 0.0,
                    devices: devices.len()..devices.len(),
                });
                domain_weights.push(0.0);
                last_host = Some(device.host);
            }
            let domain = domains.len() - 1;
            // the device, and each of its slices with an equal share of its weight
            let first = slices.len();
            let share = device.weight / f64::from(device.slices);
            for index in 0..device.slices {
                let raw = device.slice_key(index);
                slices.push(SliceView {
                    id: device.slice_id(index),
                    raw,
                    key: item_key(raw, 0),
                    inv_w: 1.0 / share,
                    device: devices.len() as u32,
                    domain: domain as u32,
                });
            }
            devices.push(DeviceView {
                uid: device.uid,
                raw: device.seat,
                key: item_key(device.seat, 0),
                inv_w: 1.0 / device.weight,
                weight: device.weight,
                host: device.host,
                domain: domain as u32,
                slices: first..slices.len(),
            });
            domains[domain].devices.end = devices.len();
            domain_weights[domain] += device.weight;
        }
        for (domain, weight) in domains.iter_mut().zip(&domain_weights) {
            domain.inv_w = 1.0 / weight;
        }
        // the window: each host's slices in device order, dealt one host at a time
        let mut by_host: Vec<Vec<u32>> = Vec::new();
        let mut host_of_list: Vec<u32> = Vec::new();
        for (index, slice) in slices.iter().enumerate() {
            let host = devices[slice.device as usize].host;
            match host_of_list.iter().position(|h| *h == host) {
                Some(at) => by_host[at].push(index as u32),
                None => {
                    host_of_list.push(host);
                    by_host.push(vec![index as u32]);
                }
            }
        }
        let mut window = Vec::with_capacity(slices.len());
        let deepest = by_host.iter().map(Vec::len).max().unwrap_or(0);
        for round in 0..deepest {
            for host in &by_host {
                if let Some(slice) = host.get(round) {
                    window.push(*slice);
                }
            }
        }
        // round keys for the first round of a by-position draw, mixed once with the map
        let width = pool.layout.width();
        let mut slice_rounds = Vec::with_capacity(width * slices.len());
        let mut domain_rounds = Vec::with_capacity(width * domains.len());
        let mut position_keys = Vec::with_capacity(width * domains.len());
        for round in 0..width as u32 {
            slice_rounds.extend(slices.iter().map(|slice| item_key(slice.raw, round)));
            domain_rounds.extend(domains.iter().map(|domain| item_key(domain.raw, round)));
            position_keys.extend(domains.iter().map(|domain| item_key(domain.raw ^ POSITION_SALT, round)));
        }
        View {
            slices,
            devices,
            domains,
            width,
            positional: pool.layout.positional(),
            window,
            slice_rounds,
            domain_rounds,
            position_keys,
            log: log2_table(),
        }
    }

    /// Whether the pool's devices span enough domains to place anything
    #[must_use]
    pub fn feasible(&self) -> bool {
        self.domains.len() >= self.width
    }

    /// A slice's key in one round, from the precomputed rounds where there is one
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice's index in the view
    /// * `round` - The round
    #[inline]
    fn slice_key(&self, slice: usize, round: u32) -> u64 {
        if (round as usize) < self.width {
            self.slice_rounds[round as usize * self.slices.len() + slice]
        } else {
            item_key(self.slices[slice].raw, round)
        }
    }

    /// A domain's key in one round, from the precomputed rounds where there is one
    ///
    /// # Arguments
    ///
    /// * `domain` - The domain's index in the view
    /// * `round` - The round
    #[inline]
    fn domain_key(&self, domain: usize, round: u32) -> u64 {
        if (round as usize) < self.width {
            self.domain_rounds[round as usize * self.domains.len() + domain]
        } else {
            item_key(self.domains[domain].raw, round)
        }
    }

    /// Place one placement group, writing a slice id for each position
    ///
    /// # Arguments
    ///
    /// * `candidate` - Which function
    /// * `pg` - The placement group
    /// * `scratch` - What the last lookup left to reuse
    /// * `out` - The answer, one slice id a position, `width` long
    pub fn place(&self, candidate: Candidate, pg: &Pg, scratch: &mut Scratch, out: &mut [u32]) -> Flags {
        // a pool short of domains places nothing, whatever the candidate
        debug_assert!(self.feasible() || candidate == Candidate::Window);
        scratch.used.clear();
        scratch.used.resize(self.domains.len(), false);
        match candidate {
            Candidate::Window => self.window(pg, out),
            Candidate::Rendezvous => {
                let members = self.rendezvous(pg, scratch, false);
                self.in_order(&members, out)
            }
            Candidate::RendezvousLibm => {
                let members = self.rendezvous(pg, scratch, true);
                self.in_order(&members, out)
            }
            Candidate::Matched => {
                let members = self.rendezvous(pg, scratch, false);
                self.matched(pg, &members, out)
            }
            Candidate::ByDomainMatched => {
                let members = self.by_domain(pg, scratch);
                self.matched(pg, &members, out)
            }
            Candidate::ByPosition => self.by_position(pg, scratch, out),
            Candidate::ByDomain => {
                let members = self.by_domain(pg, scratch);
                self.in_order(&members, out)
            }
            Candidate::ByDomainByPosition => self.by_domain_by_position(pg, scratch, out),
        }
    }

    /// The tablet rule extended: the next `width` slices of the window from the group's place
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `out` - The answer
    fn window(&self, pg: &Pg, out: &mut [u32]) -> Flags {
        let n = self.window.len() as u64;
        let mut flags = Flags::default();
        let mut domains = [0u32; MAX_WIDTH];
        for (k, slot) in out.iter_mut().enumerate().take(self.width) {
            // copy k of group p is on list[(p + k) % N], as copy k of tablet t is on a node
            let slice = &self.slices[self.window[((pg.index + k as u64) % n) as usize] as usize];
            *slot = slice.id;
            // an earlier position in the same domain is an answer the domain rule refuses
            flags.violation |= domains[..k].contains(&slice.domain);
            domains[k] = slice.domain;
        }
        flags
    }

    /// Weighted rendezvous, the best slice in each new domain in turn, as slice indices
    ///
    /// Taking position by position the best slice whose domain is unused picks the domains in
    /// the order of their best slices' scores, so one pass finds each domain's best and the
    /// lowest `width` of those are the answer in order.
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `scratch` - Reused buffers
    /// * `libm` - Whether to take libm's logarithm instead of the table's
    fn rendezvous(&self, pg: &Pg, scratch: &mut Scratch, libm: bool) -> [u32; MAX_WIDTH] {
        let group = group_key(pg.key);
        // each domain's best slice and its score, in one pass over the slices
        scratch.scores.clear();
        scratch.scores.resize(self.domains.len(), f64::INFINITY);
        scratch.best.clear();
        scratch.best.resize(self.domains.len(), 0);
        for (index, slice) in self.slices.iter().enumerate() {
            let u = draw(group, slice.key);
            let score = if libm {
                libm_score(u, slice.inv_w)
            } else {
                self.log.score(u, slice.inv_w)
            };
            let domain = slice.domain as usize;
            if score < scratch.scores[domain] {
                scratch.scores[domain] = score;
                scratch.best[domain] = index as u32;
            }
        }
        // the width lowest domains, lowest first, by insertion into a short sorted run
        let mut top: [(f64, u32); MAX_WIDTH] = [(f64::INFINITY, 0); MAX_WIDTH];
        let mut held = 0;
        for (domain, score) in scratch.scores.iter().enumerate() {
            if held == self.width && *score >= top[held - 1].0 {
                continue;
            }
            // shift the worse ones right and drop the last if the run is full
            let mut at = held.min(self.width - 1);
            if held < self.width {
                held += 1;
            }
            while at > 0 && top[at - 1].0 > *score {
                top[at] = top[at - 1];
                at -= 1;
            }
            top[at] = (*score, domain as u32);
        }
        // each chosen domain's best slice, in the domains' order
        let mut members = [0u32; MAX_WIDTH];
        for (member, (_, domain)) in members.iter_mut().zip(top.iter()).take(self.width) {
            *member = scratch.best[*domain as usize];
        }
        members
    }

    /// Write a set's members as the answer, in the order they were chosen
    ///
    /// # Arguments
    ///
    /// * `members` - Slice indices, `width` of them
    /// * `out` - The answer
    fn in_order(&self, members: &[u32; MAX_WIDTH], out: &mut [u32]) -> Flags {
        for (slot, member) in out.iter_mut().zip(members).take(self.width) {
            *slot = self.slices[*member as usize].id;
        }
        Flags::default()
    }

    /// Give a set's members positions by a matching of unweighted draws on their domains
    ///
    /// Every member and position pair is drawn from the key of the member's domain, and the
    /// pairs are taken highest first while both the member and the position are free. No weight
    /// and no score enters it, so the positions depend on which domains are in the set and
    /// nothing else about the map.
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `members` - Slice indices, `width` of them
    /// * `out` - The answer
    fn matched(&self, pg: &Pg, members: &[u32; MAX_WIDTH], out: &mut [u32]) -> Flags {
        let group = group_key(pg.key);
        let width = self.width;
        // every pair's draw, with the member and the position it pairs
        let mut pairs = [(0u64, 0u8, 0u8); MAX_WIDTH * MAX_WIDTH];
        let mut count = 0;
        for (index, member) in members.iter().enumerate().take(width) {
            for position in 0..width {
                let domain = self.slices[*member as usize].domain as usize;
                let key = self.position_keys[position * self.domains.len() + domain];
                pairs[count] = (draw(group, key), index as u8, position as u8);
                count += 1;
            }
        }
        // highest first, each taken while both sides are free
        pairs[..count].sort_unstable_by(|a, b| b.0.cmp(&a.0));
        let mut member_free = [true; MAX_WIDTH];
        let mut position_free = [true; MAX_WIDTH];
        let mut placed = 0;
        for (_, member, position) in &pairs[..count] {
            let (member, position) = (*member as usize, *position as usize);
            if member_free[member] && position_free[position] {
                member_free[member] = false;
                position_free[position] = false;
                out[position] = self.slices[members[member] as usize].id;
                placed += 1;
                if placed == width {
                    break;
                }
            }
        }
        Flags::default()
    }

    /// The best slice of all in one round, with no regard to what is taken
    ///
    /// # Arguments
    ///
    /// * `group` - The placement group's mixed key
    /// * `round` - The round
    fn best_slice(&self, group: u64, round: u32) -> usize {
        let mut best = 0;
        let mut best_score = f64::INFINITY;
        for (index, slice) in self.slices.iter().enumerate() {
            let score = self.log.score(draw(group, self.slice_key(index, round)), slice.inv_w);
            if score < best_score {
                best_score = score;
                best = index;
            }
        }
        best
    }

    /// Weighted rendezvous with each position drawing its own rounds
    ///
    /// Round `a` of position `pos` is draw `pos + a * width`. A position keeps its draw if no
    /// position filled before it holds that domain, and draws again next round if one does.
    /// After `ROUNDS` rounds the positions still empty take, in turn, the best slice of an unused
    /// domain at their own first draw, which always succeeds when the pool is feasible.
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `scratch` - Reused buffers
    /// * `out` - The answer
    fn by_position(&self, pg: &Pg, scratch: &mut Scratch, out: &mut [u32]) -> Flags {
        let group = group_key(pg.key);
        let width = self.width as u32;
        out[..self.width].fill(EMPTY);
        let mut filled = 0;
        let mut flags = Flags::default();
        // rounds of draws, each empty position in order
        for round in 0..ROUNDS {
            flags.rounds = round + 1;
            for pos in 0..self.width {
                if out[pos] != EMPTY {
                    continue;
                }
                let best = self.best_slice(group, pos as u32 + round * width);
                let domain = self.slices[best].domain as usize;
                // keep the draw unless an earlier fill holds its domain
                if !scratch.used[domain] {
                    scratch.used[domain] = true;
                    out[pos] = self.slices[best].id;
                    filled += 1;
                }
            }
            if filled == self.width {
                return flags;
            }
        }
        // out of rounds: each empty position takes the best of an unused domain at its first draw
        flags.fallback = true;
        for pos in 0..self.width {
            if out[pos] != EMPTY {
                continue;
            }
            let mut best = usize::MAX;
            let mut best_score = f64::INFINITY;
            for (index, slice) in self.slices.iter().enumerate() {
                if scratch.used[slice.domain as usize] {
                    continue;
                }
                let score = self
                    .log
                    .score(draw(group, self.slice_key(index, pos as u32)), slice.inv_w);
                if score < best_score {
                    best_score = score;
                    best = index;
                }
            }
            scratch.used[self.slices[best].domain as usize] = true;
            out[pos] = self.slices[best].id;
        }
        flags
    }

    /// Within one domain, the best device and then its best slice, in one round, as an id
    ///
    /// # Arguments
    ///
    /// * `group` - The placement group's mixed key
    /// * `domain` - The domain's index
    /// * `round` - The round
    fn within(&self, group: u64, domain: usize, round: u32) -> u32 {
        self.slices[self.within_index(group, domain, round) as usize].id
    }

    /// Within one domain, the best device and then its best slice, in one round, as an index
    ///
    /// # Arguments
    ///
    /// * `group` - The placement group's mixed key
    /// * `domain` - The domain's index
    /// * `round` - The round
    fn within_index(&self, group: u64, domain: usize, round: u32) -> u32 {
        let devices = self.domains[domain].devices.clone();
        // a device by its weight, unless the domain is the device
        let device = if devices.len() == 1 {
            devices.start
        } else {
            let mut best = devices.start;
            let mut best_score = f64::INFINITY;
            for index in devices {
                let device = &self.devices[index];
                let key = if round == 0 {
                    device.key
                } else {
                    item_key(device.raw, round)
                };
                let score = self.log.score(draw(group, key), device.inv_w);
                if score < best_score {
                    best_score = score;
                    best = index;
                }
            }
            best
        };
        // then a slice of it, every slice of a device weighing the same
        let slices = self.devices[device].slices.clone();
        if slices.len() == 1 {
            return slices.start as u32;
        }
        let mut best = slices.start;
        let mut best_draw = u64::MAX;
        for index in slices {
            let u = draw(group, self.slice_key(index, round));
            // equal weights, so the largest draw is the lowest score and no logarithm is needed
            let low = !u;
            if low < best_draw {
                best_draw = low;
                best = index;
            }
        }
        best as u32
    }

    /// Weighted rendezvous down the hierarchy, domains in the order of their draws, as indices
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `scratch` - Reused buffers
    fn by_domain(&self, pg: &Pg, scratch: &mut Scratch) -> [u32; MAX_WIDTH] {
        let group = group_key(pg.key);
        // every domain's score, once
        scratch.scores.clear();
        scratch
            .scores
            .extend(self.domains.iter().map(|domain| self.log.score(draw(group, domain.key), domain.inv_w)));
        // the lowest width of them, lowest first
        let mut top: [(f64, u32); MAX_WIDTH] = [(f64::INFINITY, 0); MAX_WIDTH];
        let mut held = 0;
        for (domain, score) in scratch.scores.iter().enumerate() {
            if held == self.width && *score >= top[held - 1].0 {
                continue;
            }
            let mut at = held.min(self.width - 1);
            if held < self.width {
                held += 1;
            }
            while at > 0 && top[at - 1].0 > *score {
                top[at] = top[at - 1];
                at -= 1;
            }
            top[at] = (*score, domain as u32);
        }
        // a device and a slice inside each, in round zero
        let mut members = [0u32; MAX_WIDTH];
        for (member, (_, domain)) in members.iter_mut().zip(top.iter()).take(self.width) {
            *member = self.within_index(group, *domain as usize, 0);
        }
        members
    }

    /// Weighted rendezvous down the hierarchy, each position drawing its own rounds
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group
    /// * `scratch` - Reused buffers
    /// * `out` - The answer
    fn by_domain_by_position(&self, pg: &Pg, scratch: &mut Scratch, out: &mut [u32]) -> Flags {
        let group = group_key(pg.key);
        let width = self.width as u32;
        out[..self.width].fill(EMPTY);
        let mut filled = 0;
        let mut flags = Flags::default();
        for round in 0..ROUNDS {
            flags.rounds = round + 1;
            for pos in 0..self.width {
                if out[pos] != EMPTY {
                    continue;
                }
                let r = pos as u32 + round * width;
                // the best domain of all in this round
                let mut best = 0;
                let mut best_score = f64::INFINITY;
                for (index, domain) in self.domains.iter().enumerate() {
                    let score = self.log.score(draw(group, self.domain_key(index, r)), domain.inv_w);
                    if score < best_score {
                        best_score = score;
                        best = index;
                    }
                }
                // kept unless taken, and then a device and slice in it by the same round
                if !scratch.used[best] {
                    scratch.used[best] = true;
                    out[pos] = self.within(group, best, r);
                    filled += 1;
                }
            }
            if filled == self.width {
                return flags;
            }
        }
        // out of rounds: positions in turn, each the best unused domain at its first draw
        flags.fallback = true;
        for pos in 0..self.width {
            if out[pos] != EMPTY {
                continue;
            }
            let mut best = usize::MAX;
            let mut best_score = f64::INFINITY;
            for (index, domain) in self.domains.iter().enumerate() {
                if scratch.used[index] {
                    continue;
                }
                let score = self
                    .log
                    .score(draw(group, self.domain_key(index, pos as u32)), domain.inv_w);
                if score < best_score {
                    best_score = score;
                    best = index;
                }
            }
            scratch.used[best] = true;
            out[pos] = self.within(group, best, pos as u32);
        }
        flags
    }

    /// Each slice id's index in the view, for reading answers back
    #[must_use]
    pub fn index(&self) -> std::collections::HashMap<u32, u32> {
        self.slices
            .iter()
            .enumerate()
            .map(|(index, slice)| (slice.id, index as u32))
            .collect()
    }
}
