//! What each group of workloads measures, and how to read it
//!
//! # The problem this exists to solve
//!
//! The results page used to draw one chart per workload identifier, alphabetically, with one
//! sentence of caption each. Eighty-eight charts arrived in a row with no grouping, so
//! `macro/encryption/depth/tls/65536/128` had the same weight as `macro/insert_unsorted`, and
//! nothing anywhere said what any of it meant. A reader who did not already know what a fan-out
//! curve was could not find out from the page that drew one.
//!
//! A **family** is the unit that fixes that. It is a group of workloads that answer one question,
//! and it carries four pieces of prose that a chart cannot: what the numbers measure, how to read
//! them, what would make them wrong, and what they cannot say. Those four appear at the top of
//! every generated page, before any chart.
//!
//! # Why the four blocks are mandatory rather than optional
//!
//! [`family_for`] must answer for every declared workload, and every family must fill in all four
//! blocks - both are tests. A workload added without a family fails the first; a family added
//! without its prose fails the second. That is the only mechanism that keeps a new sweep from
//! landing on the page as an unexplained chart, which is how the page got into the state this
//! replaces.
//!
//! **`what_it_cannot_say` is the one that earns its keep.** Every family here has a real limit -
//! the fan-out curve warms up as it runs, the grid's throughput is throughput at one depth, the
//! instrumented layers cannot produce a latency at all - and each of those limits has at some point
//! been read past in this repository.

use crate::model::macro_layer::TMDB_WORKLOAD;

/// Which generated page a family's charts and tables land on
///
/// The set is fixed rather than derived from the families, because every page needs an entry in
/// `docs/src/SUMMARY.md` and `create-missing = false` means a missing file fails the whole book
/// build. A page that appeared because a family was added would have to be registered by hand
/// anyway, so it is declared by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Surface {
    /// The index: what has been captured, and how to read any of it
    Overview,
    /// What a read/write mixture costs
    Grid,
    /// What row width costs
    RowSize,
    /// What kind of table costs what, and what storage costs
    TableTypes,
    /// What a skewed key space and a deeper queue do
    Access,
    /// What the client's transport modes and encryption cost
    Transport,
    /// What reading many partitions in one query costs
    Fanout,
    /// What each setting of the server configuration is worth
    Configuration,
    /// The criterion layer, its scaling curves and what it can resolve
    Micro,
    /// Where the time goes, from the two instrumented builds
    Attribution,
    /// Every workload's raw numbers, with no interpretation attached
    AllWorkloads,
}

impl Surface {
    /// Where this page is written, relative to the book's source root
    pub fn path(self) -> &'static str {
        match self {
            Surface::Overview => "docs/src/performance/overview.md",
            Surface::Grid => "docs/src/performance/grid.md",
            Surface::RowSize => "docs/src/performance/row-size.md",
            Surface::TableTypes => "docs/src/performance/table-types.md",
            Surface::Access => "docs/src/performance/access-patterns.md",
            Surface::Transport => "docs/src/performance/transport.md",
            Surface::Fanout => "docs/src/performance/fanout.md",
            Surface::Configuration => "docs/src/performance/configuration.md",
            Surface::Micro => "docs/src/performance/micro.md",
            Surface::Attribution => "docs/src/performance/attribution.md",
            Surface::AllWorkloads => "docs/src/performance/all-workloads.md",
        }
    }

    /// What this page is called, in its own heading and in the index
    pub fn title(self) -> &'static str {
        match self {
            Surface::Overview => "Performance",
            Surface::Grid => "Read/write mixtures",
            Surface::RowSize => "Row size",
            Surface::TableTypes => "Table types and what storage costs",
            Surface::Access => "Access patterns and load depth",
            Surface::Transport => "Transport and encryption",
            Surface::Fanout => "Reading many partitions at once",
            Surface::Configuration => "Configuration and what each setting is worth",
            Surface::Micro => "The micro layer",
            Surface::Attribution => "Where the time goes",
            Surface::AllWorkloads => "Every workload",
        }
    }

    /// This page's file name, for a link from another page in the same directory
    pub fn link(self) -> &'static str {
        // every page lives in one directory, so a sibling link is the bare file name
        self.path()
            .rsplit_once('/')
            .map(|(_, name)| name)
            .unwrap_or(self.path())
    }

    /// Every page, in the order they are registered in `SUMMARY.md`
    pub const ALL: [Surface; 11] = [
        Surface::Overview,
        Surface::Grid,
        Surface::RowSize,
        Surface::TableTypes,
        Surface::Access,
        Surface::Transport,
        Surface::Fanout,
        Surface::Configuration,
        Surface::Micro,
        Surface::Attribution,
        Surface::AllWorkloads,
    ];
}

/// One group of workloads that answer one question, and how to read their answer
#[derive(Debug, Clone, Copy)]
pub struct Family {
    /// A short slug, used in headings and anchors
    pub name: &'static str,
    /// What this group is called in prose
    pub title: &'static str,
    /// The page this family's charts and tables are drawn on
    pub surface: Surface,
    /// What the numbers are of
    pub what_it_measures: &'static str,
    /// How to turn a number here into a conclusion
    pub how_to_read_it: &'static str,
    /// What would make a number here mean something other than it appears to
    pub what_would_make_it_wrong: &'static str,
    /// The question a reader will want to ask that this family cannot answer
    pub what_it_cannot_say: &'static str,
}

impl Family {
    /// The four blocks, rendered as the opening of a page
    ///
    /// Emitted in one order everywhere, so a reader who has read one page knows where to look on
    /// the next.
    pub fn preamble(&self) -> String {
        format!(
            "**What this measures.** {}\n\n**How to read it.** {}\n\n**What would make it \
             wrong.** {}\n\n**What it cannot tell you.** {}\n\n",
            self.what_it_measures,
            self.how_to_read_it,
            self.what_would_make_it_wrong,
            self.what_it_cannot_say
        )
    }
}

/// Every family, in the order their pages appear
pub const FAMILIES: &[Family] = &[
    Family {
        name: "grid",
        title: "The workload grid",
        surface: Surface::Grid,
        what_it_measures:
            "One client sending a fixed mixture of reads and writes at a live server, over a table \
             of one kind, with rows of one width. `r50` is half reads and half writes, `r95` is \
             nineteen reads per write, `r0` is writes alone. Reads ask for rows that were seeded \
             before the run; writes insert rows into a key range no read ever asks for, so every \
             read is a hit and the read hit rate does not drift as the arm runs.",
        how_to_read_it:
            "Two numbers come off every arm and they answer different questions. The **latency** is \
             a service time - one timestamp either side of one query - and reads and writes are \
             summarised apart, because the p99 of a mixture is a number describing neither half. \
             The **throughput** is the whole arm's queries divided by its wall clock, which is a \
             throughput *at a load depth of 32* and not the most the server can do. Compare a cell \
             against the same cell in another capture, never against the cell beside it in another \
             sweep.",
        what_would_make_it_wrong:
            "Saturation. Past the knee of the throughput curve a p50 stops being a service time and \
             becomes a measure of how long the queue is, and nothing in the artifact says which \
             side of the knee an arm is on - that is [O31](../appendix/optimizations.md), still \
             open. The depth ladder on [Access patterns](access-patterns.md) is what places these \
             arms on that curve. The other one is the mixture itself: an arm is only comparable to \
             an arm with the same read share, because a write costs several times what a read does \
             and a mixture's average moves with the ratio rather than with the engine.",
        what_it_cannot_say:
            "Which half of the engine moved. A mixture blends the read path and the write path on \
             purpose, so a regression here is a signal to go and look at the isolating workloads on \
             [Table types](table-types.md), never an attribution in itself. That is what \
             [F8](../features/purpose-built-workloads.md) is about and this family does not repeal \
             it.",
    },
    Family {
        name: "row-size",
        title: "Row size",
        surface: Surface::RowSize,
        what_it_measures:
            "The same even mixture as the grid, swept across eleven row widths: eight fixed widths \
             from 64 bytes to 4 MiB, and three declared mixtures of widths. The mixtures are \
             named distributions rather than ranges - `mixed_small` is always the same four widths \
             in the same proportion - so two captures of one measure the same thing.",
        how_to_read_it:
            "The x axis is logarithmic, so the *shape* is what to read rather than the magnitude. \
             A cost that is flat across the narrow widths and rises past a kilobyte is a fixed \
             per-response cost giving way to a per-byte one; a straight rising line is a per-byte \
             cost the whole way. Throughput in **rows per second** and throughput in **bytes per \
             second** tell opposite-looking stories at the two ends of this axis, and both are \
             drawn, because a store that moves few large rows quickly is not slow.",
        what_would_make_it_wrong:
            "Sample count at the wide end. Every arm moves roughly the same number of *bytes*, so \
             the 4 MiB arm runs a few hundred queries where the 64 byte arm runs twenty thousand. \
             **Read the p50 at the wide end**: a p99 over two hundred samples is roughly its \
             third-worst observation and is not a percentile in any useful sense. The wide arms \
             also seed a small key space - sixty four partitions at 4 MiB - so every read there is \
             answered from a table that is entirely resident.",
        what_it_cannot_say:
            "What a wide row costs under a write-heavy mixture. This axis is swept at the reference \
             mixture only, so an interaction between width and mixture would be invisible to both \
             sweeps. The cross was chosen over the full cube deliberately; the gap is recorded in \
             [todos](../appendix/todos.md).",
    },
    Family {
        name: "tables",
        title: "Table types",
        surface: Surface::TableTypes,
        what_it_measures:
            "The same mixture and the same rows against each of the four tables Shoal has: sorted \
             and unsorted, persistent and ephemeral. An ephemeral table is the persistent one with \
             a storage engine that stores nothing, not a different implementation, so the gap \
             between a pair is the storage layer and nothing else. Beside them sit the isolating \
             workloads, which drive one path each rather than a mixture.",
        how_to_read_it:
            "Read this page in pairs. **Persistent against ephemeral** at the same table kind is \
             what durability costs. **Sorted against unsorted** at the same durability is what \
             holding rows in order costs. Every row here holds one row per partition, so a sorted \
             table's get is a keyed lookup rather than a range scan - the two kinds differ in how a \
             row is stored, not in how much of it is being asked for.",
        what_would_make_it_wrong:
            "Reading the ephemeral arms as a feature rather than as a control. Nothing an ephemeral \
             table holds is ever evicted and nothing survives a restart, so a write to one is not a \
             write anybody would ship - it exists to be subtracted from the persistent number. The \
             archived read arm is the other trap: it warms up as it runs, because a partition read \
             from disk stays resident afterwards.",
        what_it_cannot_say:
            "What a sorted table costs when a partition holds many rows. Every arm here writes one \
             row per partition so that the four tables stay comparable, which is exactly the case a \
             sorted table is not for. [Reading many partitions at once](fanout.md) has the closest \
             thing to that answer.",
    },
    Family {
        name: "skew",
        title: "Access patterns",
        surface: Surface::Access,
        what_it_measures:
            "The reference mixture with its reads drawn from three key distributions: uniform, \
             YCSB's scrambled Zipfian, and its recency-skewed `latest`. The generators are YCSB's \
             own, with YCSB's constant, so a number here is readable next to a published YCSB one.",
        how_to_read_it:
            "Uniform is the honest floor: it defeats every cache in the system, so a number under it \
             is a number the store can always produce. Zipfian is what most published figures \
             measure, and the gap between the two is the size of the caching effect on this \
             hardware at this working set. **The gap is a property of the working set as much as of \
             the store** - a table that fits in memory has nothing to be warmer than, which is why \
             only the persistent tables are swept here.",
        what_would_make_it_wrong:
            "Comparing a skewed number to a uniform one and calling the difference an improvement. \
             They are different measurements of different workloads. The artifact records which \
             distribution an arm used, and a comparison joins on the workload id, so the tool will \
             not do this - a reader quoting one number beside the other can.",
        what_it_cannot_say:
            "Where the knee of the throughput curve is for any arm but the reference cell. The depth \
             ladder below is measured on one table at one width, and every other arm in the grid is \
             assumed to sit at the same point on its own curve. That assumption has not been \
             checked at the wide end.",
    },
    Family {
        name: "depth",
        title: "The load depth ladder",
        surface: Surface::Access,
        what_it_measures:
            "The grid's reference cell at four load depths: one query outstanding, then eight, \
             thirty two - which is where every grid arm runs - and a hundred and twenty eight. \
             Everything else about the four is identical.",
        how_to_read_it:
            "This is a throughput-latency curve, and it is read from the shape of the two together. \
             While throughput rises roughly with depth, the server has capacity and a latency is a \
             service time. Where throughput flattens and latency keeps climbing is the **knee**, and \
             past it every extra query is queueing rather than being served. The grid runs at 32, so \
             where 32 sits on this curve is what says whether the grid's latencies are service times \
             or queue lengths.",
        what_would_make_it_wrong:
            "Nothing on this page is automatic. [O31](../appendix/optimizations.md) records that the \
             comparison rule cannot tell a result from a saturated workload, and that a capture \
             where more load bought *less* work is the signature to look for. Four points is enough \
             to see a knee and not enough to locate it precisely.",
        what_it_cannot_say:
            "What a second client would do. Depth and client count are separate axes - eight queries \
             outstanding on one client share a connection pool, a response map and one set of \
             handshakes, where one query on each of eight clients has eight of each. The client axis \
             is swept on [Transport and encryption](transport.md), not here.",
    },
    Family {
        name: "write",
        title: "The write path",
        surface: Surface::TableTypes,
        what_it_measures:
            "Inserting rows and nothing else, saturating the pipeline so the number worth reading is \
             the wall clock rather than any percentile. The persistent arm and the ephemeral arm \
             build byte-identical rows from the same seed and differ in the storage engine alone.",
        how_to_read_it:
            "This is a **per-batch** measurement: one timestamp covers a whole batch, so every query \
             in it is charged for the ones ahead of it and the percentiles are batch completion \
             times. Read the wall clock. The gap between the persistent arm and the ephemeral one is \
             what durability costs on this hardware, which is an Optane SSD and therefore a best \
             case.",
        what_would_make_it_wrong:
            "Comparing a per-batch percentile against a per-query one from anywhere else on this \
             site. They are not the same kind of number and the artifact records which is which \
             precisely so that a comparison can refuse to join them.",
        what_it_cannot_say:
            "What a write costs when the client is not saturating. A saturated pipeline is how \
             throughput is measured and it is not how most callers write; the grid's `r0` arms are \
             the same write path at a bounded depth.",
    },
    Family {
        name: "read",
        title: "The read path",
        surface: Surface::TableTypes,
        what_it_measures:
            "Getting one row by its partition key, at a bounded concurrency so that a sample is a \
             service time. Three arms: from memory, from disk after a restart has emptied memory, \
             and from an ephemeral table that has no disk under it at all.",
        how_to_read_it:
            "The resident arm and the archived arm are a control and its null: identical but for \
             where the row was when the get arrived. The archived arm reaches disk by restarting the \
             server rather than by squeezing the memory limit, because a partition cannot be evicted \
             until its generation has been compacted and a squeezed run would measure a different \
             mixture of resident and archived reads every time.",
        what_would_make_it_wrong:
            "The archived arm **warms up as it runs**. Every partition it reads from disk stays \
             resident afterwards, so the later queries in the run are increasingly resident reads. \
             Its number is a mixture weighted toward the cold case, not a cold number.",
        what_it_cannot_say:
            "What a read costs when the partition holds more than one row, which is the case a sorted \
             table exists for.",
    },
    Family {
        name: "fanout",
        title: "Fan-out",
        surface: Surface::Fanout,
        what_it_measures:
            "One get naming *n* partition keys, swept over six values of *n* from one to two hundred \
             and fifty six, on a resident table, an evicted one and an ephemeral one. Every point on \
             a curve reads the same total number of partitions, so the points cost roughly the same \
             and the curve is about the shape rather than the total.",
        how_to_read_it:
            "This is the curve that says whether the cost of naming *n* keys is proportional to *n* \
             or worse than proportional. A straight line on these axes is linear and is what a router \
             that splits a query once should produce; a bend upward is a per-partition term being \
             paid more than once, which is what [O13](../appendix/optimizations.md) claims from \
             reading the source and has never had a number.",
        what_would_make_it_wrong:
            "Sample count, again, and warming. The widest points run the fewest queries, so their p99 \
             is the worst few of a few hundred - **the curve is a curve in p50**. And at *n* = 256 \
             roughly ninety three percent of the reads in the evicted arm land on partitions an \
             earlier query already pulled into memory, so the evicted arm measures a progressively \
             warmer table as *n* rises.",
        what_it_cannot_say:
            "Where the cost is. This is an end-to-end measurement over a live server, so a bend in it \
             is evidence that something is superlinear and not evidence about which function. The \
             table-layer benchmark that would say lives in [todos](../appendix/todos.md).",
    },
    Family {
        name: "transport",
        title: "The client's transport modes",
        surface: Surface::Transport,
        what_it_measures:
            "The client's three ways of sending - one query at a time, a batch at a time, an ordered \
             stream and an unordered one - each at a narrow row and at a MiB one, over a plaintext \
             wire and over one the kernel encrypts.",
        how_to_read_it:
            "**The row width is the point of this family.** At 256 bytes the four modes spread nearly \
             fourfold, because at that width the fixed per-response costs are the whole cost and the \
             modes differ in exactly those. At a MiB they collapse into one number, because the wire \
             is the whole cost and every mode puts the same bytes on it. A conclusion drawn at one \
             width does not carry to the other.",
        what_would_make_it_wrong:
            "Reading the ordered stream's tail as a server property. An ordered stream buffers a \
             response until every earlier one has arrived, so one slow query holds back every sample \
             behind it - that is the mode's own behaviour and it is what the pair exists to show.",
        what_it_cannot_say:
            "What opening a connection costs. Every arm here opens its pool before it samples \
             anything, and `bb8` fills its idle connections inside `Pool::build`, so a handshake is \
             never inside a sample. [O30](../appendix/optimizations.md) is the entry that says so.",
    },
    Family {
        name: "encryption",
        title: "What encryption costs",
        surface: Surface::Transport,
        what_it_measures:
            "A get of one row, swept across four row widths, four load depths and four client counts, \
             with every arm having a twin that differs in the wire and in nothing else - same seed, \
             same rows, same width, same query count, same load.",
        how_to_read_it:
            "Every point is a pair, so the gap between the two halves is what encryption cost rather \
             than what else moved. The macro layer's rule applies: a difference counts only when the \
             two sides' observed run intervals are **disjoint**, and a pair whose runs overlapped has \
             not been shown to differ however far apart its medians look.",
        what_would_make_it_wrong:
            "The depth axis leaves the useful regime at its top end. At a depth of 128 throughput \
             *falls* against depth 32, which is the signature of a queue past its knee, and four \
             points of that sweep report encryption making queries measurably faster. They are \
             reliably weird rather than meaningfully different - disjointness is necessary and it is \
             not sufficient.",
        what_it_cannot_say:
            "What the handshake costs. The client sweep was built to see it and cannot, for the \
             reason the transport family gives.",
    },
    Family {
        name: "conf-storage",
        title: "The filesystem writer settings",
        surface: Surface::Configuration,
        what_it_measures:
            "The grid's reference cell - one client, an even read/write mixture, 1 KiB rows, the \
             persistent unsorted table, thirty two queries outstanding - run once for each value of \
             one storage setting, with every other setting left at what the committed `shoal.yml` \
             says. The settings are the ones on the two filesystem writers: the durability barrier a \
             write waits on, how many bytes the intent log buffers, how many writes it keeps in \
             flight, how large it grows before compaction, and the same buffer and queue depth on \
             the throughput writer.",
        how_to_read_it:
            "Down a column, never across one. Two arms of one sweep differ in exactly one field of \
             the configuration, so the difference between them is what that field is worth; two arms \
             of *different* sweeps differ in two and are never comparable. **A difference counts \
             only when the two arms' observed run intervals are disjoint** - the macro layer's rule, \
             for the reason [F7](../features/bench-runner.md) gives - which is what the *Real?* \
             column of the recommendation table reports. Each sweep also contains the value \
             `shoal.yml` is actually set to, so the arm to read every other one against is named \
             rather than implied.",
        what_would_make_it_wrong:
            "Reading a flat sweep as \"this setting does not matter\" rather than as \"this setting \
             did not reach the code\". The throughput writer's two sweeps are the live example: \
             [item 71](../appendix/known-issues.md) records that `throughput_sensitive` is applied \
             to the archive map's intent log and *not* to the archive writers themselves, so a flat \
             line there is evidence about the wiring and not about the device. The other one is the \
             mixture: these arms are swept at an even read/write share, so a setting that only bites \
             under sustained writing is being asked half a question.",
        what_it_cannot_say:
            "What two settings are worth together. One knob moves at a time against a fixed \
             reference of every other, which is the same cross-not-cube choice \
             [F17](../features/workload-grid.md) makes and has the same consequence: a write-behind \
             depth that only pays off at a large buffer would show here as two flat sweeps. It also \
             says nothing about a device other than the one the capture ran on - a buffer size is \
             rounded up to the O_DIRECT alignment of the disk underneath it, so the shape of that \
             sweep is a property of the pair.",
    },
    Family {
        name: "conf-resources",
        title: "Cores, memory and the frame bound",
        surface: Surface::Configuration,
        what_it_measures:
            "The same reference cell, swept across the shard count, the per-shard memory limit, and \
             the largest frame the server will accept. The first two are swept at the even mixture \
             *and* at a pure read share, because they are the two settings whose effect plausibly \
             differs between the halves of a mixture - more shards is more parallelism for reads and \
             more fsync contention for writes, and a memory limit only bites once a read has to \
             reach disk.",
        how_to_read_it:
            "The shard sweep is a scaling curve: read where it stops rising, not what it reaches. \
             The memory sweep is a cliff rather than a curve - it is flat while the working set fits \
             and steps once it does not - so the value worth taking off it is **where** the step is, \
             which is where this hardware's reads start being answered from disk. Note that \
             `resources.memory` is a **per-shard** budget, so a twelve shard server at 4Gi is holding \
             48 GiB, and the two axes of this page interact for that reason alone.",
        what_would_make_it_wrong:
            "The client and the server share this machine. A one shard arm leaves eleven cores idle \
             for a client that needs four, and a twelve shard arm does not, so the low end of the \
             shard sweep is measured under less contention than the high end and the curve flatters \
             the small configurations. The memory sweep has the matching hazard: an arm that evicts \
             is measuring compaction as well as the read path, because a partition cannot be evicted \
             until its generation has been compacted.",
        what_it_cannot_say:
            "How Shoal scales past this machine. Every arm here runs on one host with a fixed core \
             count and one device, so the shard curve is a curve in *this* box and not a statement \
             about scaling. It also cannot say what a frame bound costs a client that batches to it: \
             the seed bundles size themselves to whatever bound the server was started with, which \
             is what lets the narrow arms run at all, and it means the narrow arms send more, smaller \
             bundles rather than failing.",
    },
    Family {
        name: "retired",
        title: "The retired blended workload",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "Nothing that can be reproduced. `macro/tmdb` is the pre-[F8] workload: one blended \
             insert-and-get run over a 99,999 row CSV that was never in this repository and that no \
             script fetched.",
        how_to_read_it:
            "As history. Seven committed captures carry it and are never rewritten, so it appears in \
             the tables and joins with nothing taken since.",
        what_would_make_it_wrong:
            "Any comparison at all. The dataset is gone, so a number shaped like this one would look \
             comparable and would not be - which is why no replacement was built.",
        what_it_cannot_say:
            "Which path moved. It was one blended number, and that is the whole reason it was \
             replaced.",
    },
];

/// The family a workload identifier belongs to
///
/// Prefix matching, longest first, because `macro/grid/depth/32` is a rung of the ladder and
/// `macro/grid/unsorted/r50/1024` is a cell of the grid, and the shorter prefix matches both.
///
/// # Arguments
///
/// * `id` - The workload identifier to place
///
/// # Examples
///
/// ```
/// use shoal_bench::render::family::{Surface, family_for};
///
/// assert_eq!(family_for("macro/grid/unsorted/r50/1024").unwrap().name, "grid");
/// // the ladder is its own family, even though its ids sit under the grid's prefix
/// assert_eq!(family_for("macro/grid/depth/32").unwrap().name, "depth");
/// assert_eq!(family_for("macro/fanout/resident/16").unwrap().surface, Surface::Fanout);
/// ```
pub fn family_for(id: &str) -> Option<&'static Family> {
    // longest prefix first, so a more specific family wins over the one it sits inside
    let name = if id.starts_with("macro/grid/depth/") {
        "depth"
    } else if id.starts_with("macro/grid/") {
        // a cell is placed in the grid family whichever sweep drew it; the row size page selects
        // its own arms by their width rather than by their family
        "grid"
    } else if id.starts_with("macro/skew/") {
        "skew"
    } else if id.starts_with("macro/conf/storage/") {
        "conf-storage"
    } else if id.starts_with("macro/conf/resources/") {
        "conf-resources"
    } else if id.starts_with("macro/fanout/") {
        "fanout"
    } else if id.starts_with("macro/transport/") {
        "transport"
    } else if id.starts_with("macro/encryption/") {
        "encryption"
    } else if id.starts_with("macro/insert") {
        "write"
    } else if id.starts_with("macro/get") {
        "read"
    } else if id == TMDB_WORKLOAD {
        "retired"
    } else {
        return None;
    };
    by_name(name)
}

/// The family with a name
///
/// # Arguments
///
/// * `name` - The family's slug
pub fn by_name(name: &str) -> Option<&'static Family> {
    // a linear scan over eleven entries, which is not worth a map
    FAMILIES.iter().find(|family| family.name == name)
}

/// Every family drawn on one page, in declaration order
///
/// # Arguments
///
/// * `surface` - The page to collect families for
pub fn on(surface: Surface) -> Vec<&'static Family> {
    FAMILIES
        .iter()
        .filter(|family| family.surface == surface)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{FAMILIES, Surface, family_for, on};
    use crate::model::macro_layer::TMDB_WORKLOAD;
    use crate::workload_ids::IDS;

    /// Every declared workload belongs to exactly one family
    ///
    /// This is the guard that stopped the page being a wall of unexplained charts. A workload with
    /// no family has nowhere to be drawn and no prose describing it, and before this existed that
    /// was the default rather than an error.
    #[test]
    fn every_workload_has_a_family() {
        for id in IDS {
            assert!(
                family_for(id).is_some(),
                "{id} belongs to no family, so nothing on the site would explain it"
            );
        }
        // and the retired workload, which is in seven committed captures and in no id list
        assert!(family_for(TMDB_WORKLOAD).is_some());
    }

    /// A workload that belongs to no family is refused rather than guessed at
    #[test]
    fn an_unknown_workload_has_no_family() {
        assert!(family_for("macro/invented").is_none());
        assert!(family_for("partition_sorted/get_key/4096").is_none());
    }

    /// Every family fills in all four blocks
    ///
    /// The blocks are the whole point of the type. A family that shipped with an empty one would
    /// render a heading with nothing under it, which is worse than not having the heading.
    #[test]
    fn every_family_says_all_four_things() {
        for family in FAMILIES {
            for (label, block) in [
                ("what it measures", family.what_it_measures),
                ("how to read it", family.how_to_read_it),
                ("what would make it wrong", family.what_would_make_it_wrong),
                ("what it cannot say", family.what_it_cannot_say),
            ] {
                assert!(
                    block.len() > 60,
                    "{} says nothing useful about {label}",
                    family.name
                );
            }
        }
    }

    /// No two families share a name, since the name is an anchor on a page
    #[test]
    fn every_family_name_is_unique() {
        let mut names: Vec<&str> = FAMILIES.iter().map(|family| family.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "a family name is declared twice");
    }

    /// Every page that families are assigned to is a page that exists
    #[test]
    fn every_family_lands_on_a_declared_page() {
        for family in FAMILIES {
            assert!(
                Surface::ALL.contains(&family.surface),
                "{} renders on a page that is not declared",
                family.name
            );
        }
    }

    /// Every page's path is unique and lives under the performance directory
    #[test]
    fn every_page_has_its_own_path() {
        let mut paths: Vec<&str> = Surface::ALL.iter().map(|page| page.path()).collect();
        for path in &paths {
            assert!(path.starts_with("docs/src/performance/"), "{path}");
            assert!(path.ends_with(".md"), "{path}");
        }
        paths.sort_unstable();
        let before = paths.len();
        paths.dedup();
        assert_eq!(before, paths.len(), "two pages claim one path");
    }

    /// A sibling link is the bare file name, which is what an mdbook page needs
    #[test]
    fn a_page_links_to_its_sibling_by_name() {
        assert_eq!(Surface::Grid.link(), "grid.md");
        assert_eq!(Surface::AllWorkloads.link(), "all-workloads.md");
    }

    /// Every page that draws workloads has at least one family explaining it
    ///
    /// Three pages are exempt and each for the same reason: they do not draw workloads at all. The
    /// index draws the corpus, the micro page draws criterion benchmarks over functions, and the
    /// attribution page draws two instrumented builds' own reports. A family is a group of
    /// workloads, so those three have nothing to have one of.
    #[test]
    fn every_page_that_draws_workloads_explains_them() {
        for page in Surface::ALL {
            if matches!(
                page,
                Surface::Overview | Surface::Micro | Surface::Attribution
            ) {
                continue;
            }
            assert!(
                !on(page).is_empty(),
                "{} draws workloads that no family explains",
                page.title()
            );
        }
    }

    /// The preamble carries all four blocks in one order
    #[test]
    fn the_preamble_is_the_four_blocks() {
        let family = super::by_name("grid").expect("the grid family");
        let preamble = family.preamble();
        let order = [
            "**What this measures.**",
            "**How to read it.**",
            "**What would make it wrong.**",
            "**What it cannot tell you.**",
        ];
        let mut at = 0;
        for heading in order {
            let found = preamble[at..]
                .find(heading)
                .unwrap_or_else(|| panic!("{heading} is missing or out of order"));
            at += found + heading.len();
        }
    }
}
