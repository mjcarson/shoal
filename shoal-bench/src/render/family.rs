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
            "Row width, swept sixteen ways against all four tables: thirteen fixed widths from 64 \
             bytes to 4 MiB, and three declared mixtures of widths. The mixtures are named \
             distributions rather than ranges - `mixed_small` is always the same four widths in the \
             same proportion - so two captures of one measure the same thing. The whole axis is run \
             at three mixtures rather than one - an even split, a pure write and a pure read - and \
             again on one table with a single query outstanding.",
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
            "What a wide row costs at a *depth* other than the two measured. The mixture axis is \
             swept fully here, so an interaction between width and read share is now visible - but \
             the `r0` and `r100` sweeps run at the grid's depth of 32 and the depth-1 ladder runs at \
             an even mixture, so a cost that appears only at one query outstanding under a pure \
             write mixture is still invisible. The cross was chosen over the full cube \
             deliberately; what remains of the gap is recorded in \
             [todos](../appendix/todos.md).",
    },
    Family {
        name: "width-depth",
        title: "Row width at one query outstanding",
        surface: Surface::RowSize,
        what_it_measures:
            "The same width axis as above, on the persistent unsorted table, with **one** query \
             outstanding instead of thirty two. Everything else about these arms - the mixture, the \
             key distribution, the byte budget, the seed - is what the arms above use, so a rung \
             here and the arm above it at the same width differ in the load depth and in nothing \
             else.",
        how_to_read_it:
            "Read it against the curve above, width by width. Where the two lie close together, the \
             wider arm's latency is a **service time** and the throughput figure beside it is what \
             the server can do. Where they diverge, the depth-32 arm is queueing, and the gap is \
             how much of its latency was spent waiting rather than being served. The wide end is \
             where this bites: at 4 MiB a depth of 32 is 128 MiB outstanding on one connection \
             against a key space of sixty four partitions.",
        what_would_make_it_wrong:
            "Reading a depth-1 throughput as a capacity. One query outstanding leaves eleven of the \
             twelve shards idle for most of every round trip, so the throughput here is a floor and \
             not a measure of what the server can do - it is the *latency* that is the honest \
             number on this ladder. The two curves answer different halves of the same question and \
             neither answers it alone.",
        what_it_cannot_say:
            "Whether the ephemeral half behaves the same way. This ladder is one table, so the \
             pair subtraction that makes a width effect attributable to storage \
             ([F9](../features/ephemeral-tables.md)) does not exist for the depth axis. It also \
             says nothing about depths between 1 and 32; the four-rung ladder on [Access \
             patterns](access-patterns.md) covers those at the reference width alone.",
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
             assumed to sit at the same point on its own curve. That assumption is checked at one \
             other point and nowhere else: [Row size](row-size.md) runs the whole width axis at a \
             single outstanding query, which bounds how much of a wide arm's latency was queueing.",
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
             is swept on [Transport and encryption](transport.md), not here. It also says nothing \
             about any width but the reference one; the ladder at one query outstanding across the \
             whole width axis is on [Row size](row-size.md), and the two cross at the rung drawn \
             here at a depth of one.",
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
             under sustained writing is being asked half a question. And reading a sweep at one row \
             width as a fact about the setting: `latency_buffer` measured at 1 KiB alone reported a \
             confident small number, because at 1 KiB three records already share an aligned write \
             and the arm was on the flat side of the step. Its rows above are labelled with the \
             width they ran at for that reason.",
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
        name: "cluster-overhead",
        title: "The cost of being a cluster node",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "The grid's reference cell served by a server started with a `cluster:` block - a \
             cluster of one, bootstrapped on the default control core with a replication factor \
             of one - against the same cell served standalone (`macro/grid/unsorted/r50/1024`). \
             The two differ in the block and in nothing else, so the difference between them is \
             what M1's control plane costs a node that is alone: one shard candidate fewer on a \
             machine that has none to spare, a control thread that ticks and sends nothing, and a \
             marker rewrite before the run.",
        how_to_read_it:
            "Beside its twin, and only there. A number here means nothing on its own; the question \
             is whether this arm's range overlaps the reference cell's from the same capture, and \
             if it does not, by how much. The `cluster` record on the artifact says what the node \
             reported: one member, the desired factor beside the active one, which core the control \
             thread had and whether it shared it.",
        what_would_make_it_wrong:
            "A control core that took a shard's core. On a machine where `resources.cores` is the \
             whole box, a cluster node runs one shard fewer than its twin and the difference is a \
             shard's worth of throughput rather than the control plane's cost - the `cores` record \
             says which happened. A capture taken on a machine without the `performance` governor \
             makes the idle tick cost whatever the frequency scaling decides.",
        what_it_cannot_say:
            "Anything about a cluster. One node replicates to nobody, acknowledges its own writes, \
             and serves every read locally exactly as a standalone node does. What replication \
             costs is the `nodes/3` arm of this series, which needs the replication M4 delivers \
             over the transport M2 did.",
    },
    Family {
        name: "cluster-hop",
        title: "What a hop costs a read",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "One read - a get of one 1024 byte row from the ephemeral unsorted table, one \
             outstanding at a time - against one two-node static placement, three ways. \
             `same_shard` reads keys node zero owns on a node zero with one shard, so the shard \
             that accepted the connection is the one that answers. `local_shard` reads the same \
             keys on a node zero with four shards, so three queries in four cross the kanal mesh \
             to another shard. `remote_node` reads the other node's keys, so every query is \
             forwarded over the data lane, served there and answered back. `remote_node` less \
             `same_shard` is the peer hop, which is the M2 loopback budget's number.",
        how_to_read_it:
            "As three latency distributions of the same query, read against each other from one \
             capture and never against the grid: they are depth one and read-only, and nothing \
             else in the corpus is both. The medians and the tails together - the budget asks for \
             a p50 and says to report the tails too. The `cluster` record says what ran: the \
             placement, every node's cores, the `hop` the arm was built for with the mix its \
             construction implies, and the `transport` counters - frames sent and shed, dials, \
             queued bytes and the bounds they ran under.",
        what_would_make_it_wrong:
            "Taking `local_shard`'s median for a pure mesh hop. The kernel picks which shard a \
             connection lands on and nothing tells the client which, so on a node with four shards \
             a quarter of that arm's queries are served where they landed; its `hop.expected_mix` \
             says 25/75, its p50 and above are mesh hops and its lower quarter is not. A \
             `transport` record on `remote_node` with `shed_frames` above zero, or a data lane not \
             `up`, means queries were refused or timed out rather than forwarded, and the samples \
             are the failures' latencies. Two nodes that shared a physical core - the `cores` \
             record would say so, and the harness refuses to stage it.",
        what_it_cannot_say:
            "What a hop costs over a network. Both nodes are processes on one machine talking over \
             loopback, which has no bandwidth, no congestion and no independent failure; C10 says \
             never to multiply a loopback number by an RTT and call it a forecast. Nor what a hop \
             costs a write or a wide row: this is one narrow read, chosen so the hop is the \
             largest share of what is left. A page of its own comes with the first committed \
             capture on the benchmark host.",
    },
    Family {
        name: "cluster-replication",
        title: "What a quorum write costs",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "The grid's reference mixture - half reads, the reference width, the reference depth \
             - against one three-node placement of three shards a node, three ways. \
             `overhead/nodes/3` replicates to nobody: every tablet has one copy, and a write is \
             acknowledged by the shard's own WAL. `replication/durable` gives every tablet a copy \
             on every node and acknowledges a write once a majority holds it in a WAL that was \
             fsynced. `replication/volatile` does the same on the ephemeral table, where a \
             majority holds it in memory. `durable` less `nodes/3` is the durable quorum; \
             `durable` less `volatile` is the followers' fsync; `volatile` less `nodes/3` is the \
             lane and the round trip.",
        how_to_read_it:
            "As three distributions of the same mixture from one capture, read against each \
             other and against `overhead/nodes/3` alone - never against `overhead/nodes/1` or \
             the grid, which run twelve shards on one node where these run nine over three. The \
             `cluster` record says what ran: the desired factor beside the active one, every \
             node's cores, the `offered_load` the arm scheduled, every replica's `lag_end`, \
             `pending_bytes_end` and the writes it answered `unknown` or `rejected`, and the \
             `outcomes` summed over the nodes. A throughput beside a nonzero `outcomes.unknown` \
             is one the run could not stand behind.",
        what_would_make_it_wrong:
            "A run whose replicas ended behind. `lag_end` above a handful of entries on any \
             replica means the followers were applying after the client stopped, and the \
             latency the client saw was a leader ahead of its quorum's apply, not a quorum's \
             cost. Nonzero `rejected` means the pending bound shed writes and the driver's \
             closed loop measured refusals. Three nodes on one machine share a device, so \
             `durable`'s followers fsync into the same queue the leader does - the followers' \
             cost here is a lower bound on what separate devices would show for the fsync and \
             an upper bound for the contention.",
        what_it_cannot_say:
            "What replication costs over a network: three processes on loopback have no \
             bandwidth, no congestion and no independent failure, and C10 says never to multiply \
             a loopback number by an RTT. What a sustained load does: these are closed-loop arms \
             at one depth, which cannot expose an overload pause or a lag that grows; the \
             open-loop schedule C10 asks for is filed, not built. What a failover costs, which \
             is the `cluster-failover` family's arm.",
    },
    Family {
        name: "cluster-reads",
        title: "What a strong read and a fan-out read cost",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "Two sets of read-only arms. `reads/one`, `reads/barrier` and `reads/session` are one \
             get of one reference width row at the reference depth against the replication arms' \
             placement - three nodes of three shards, every tablet on every node - and differ only \
             in what the read asks for: the local replica's state, a `Quorum` read that obtains a \
             barrier from the group's leader and applies through it first, or a `One` read \
             carrying the session token of the write that seeded its tablet. `fanout/get`, \
             `fanout/filter`, `fanout/limit` and `fanout/empty` are one get of six keys, two on \
             each of three nodes at a factor of one, so every read is split three ways; they \
             return six rows, the three a filter passes, the three a limit keeps, and none.",
        how_to_read_it:
            "`barrier` less `one` is the barrier: the leader's heartbeat round, the hop two times \
             in three, and the application wait, which the `cluster.reads` record separates - \
             `barriers`, `barrier_hops`, `barrier_wait_mean_us`, `apply_wait_mean_us` and their \
             maxima, summed over the nodes and listed per node. `session` less `one` is the token \
             check and a wait that is almost always already satisfied. The fanout arms are read \
             against each other: `empty` against `get` is what the fan-out costs with no rows to \
             carry, and the `fanout` record on each says how many keys, whether filtered, and the \
             limit. `timeouts`, `late_shares` and `duplicate_shares` should be zero; a run with \
             any is a run that dropped something.",
        what_would_make_it_wrong:
            "A run whose replicas ended behind, which turns the apply wait into follower lag; \
             `lag_end` on the `replicas` record says so. A nonzero `timeouts` count, which means \
             reads were answered with an error the closed loop then measured. Comparing the read \
             arms against the grid or `overhead/nodes/1`, which run twelve shards on one node \
             where these run nine over three, or the fanout arms against the hop arms, whose \
             placement is two nodes.",
        what_it_cannot_say:
            "What a barrier costs over a network: three processes on loopback have no bandwidth, \
             no congestion and no independent failure, and C10 says never to multiply a loopback \
             number by an RTT. What a strong read costs under writes: these arms carry no write \
             background, on purpose, so the barrier's own cost is not inside a difference that \
             also holds follower lag; the read-under-writes arm is filed with the open-loop \
             schedule. What a strong read costs through a leader change: the failover arm drives \
             `One` reads, and the strong read through an election is the fixture's to test.",
    },
    Family {
        name: "cluster-failover",
        title: "What a client sees when a primary dies",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "One arm, `failover/kill`: the durable replication arm's placement and mixture - three \
             nodes of three shards, every tablet on every node, half reads at the reference width \
             and depth - driven for a fixed time with a client that does not retry, with node one, \
             which leads a third of the groups, killed a third of the way through and started again \
             from the same identity two thirds through. Every operation is stamped on a timeline, \
             and the `cluster.fault` record cuts it at what the client saw: `before`, up to its \
             first failed operation after the kill; `during`, until a sustained run of successes; \
             `after`, the rest, which holds the returning node's catch-up. The record carries each \
             window's own distribution, the outage between them in milliseconds, and a per second \
             series of operations, errors and percentiles.",
        how_to_read_it:
            "`outage_ms` is the number: from the client's first failure to the first operation \
             after which two seconds succeeded, which at the default failover base of five seconds \
             is the election and little else. Read `during` for what the outage looked like - how \
             many operations failed, and the service time of the ones that did not, which are the \
             reads and the writes to groups the dead node did not lead - and `after` against \
             `before` for what the returning node's catch-up cost the survivors. The series is \
             where the dip is; a `p99_us` that climbs in the seconds after `restarted_at_ms` is the \
             catch-up, and one that never comes back down is a run to read the replicas' `lag_end` \
             on. The distribution under `read` and `write` for the whole run holds successes \
             alone, so it is the three windows pooled and says nothing the windows do not.",
        what_would_make_it_wrong:
            "A `first_failure_ms` before `at_ms`, which means the run was failing before the fault \
             and the windows are cut at the wrong thing. A `recovered_ms` that is absent, which \
             means the run ended inside the outage and the `after` window is empty. A `failed` \
             count in the thousands, which means the client's pause after a failure was too short \
             for the machine and the error count is a count of refusals rather than attempts. \
             Comparing the outage between hosts whose failover base differs: it is the policy's \
             number before it is the code's.",
        what_it_cannot_say:
            "What a client with a retry sees: this client has none, on purpose, so the outage is \
             the cluster's and not the retry's. What the outage is over a network, where a kill is \
             a link that drops and not a process a kernel reaps at once. What a kill costs on a node \
             that is not node one, whose share of the primaries is what decides the size of the \
             outage. What a pause or a partition costs rather than a kill, which are the fixture's \
             to test and the open-loop schedule's to measure.",
    },
    Family {
        name: "cluster-catchup",
        title: "What a returning node's catch-up costs, by log and by snapshot",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "Two arms, `catchup/log` and `catchup/snapshot`: the kill arm's placement, mixture, \
             client and schedule - node one killed a third of the way through and started again \
             two thirds through - differing in how far the survivors' logs reach back when the \
             node returns. The log arm runs at the configuration's defaults and the node is fed \
             from the retained log; the snapshot arm shortens `checkpoint_entries` to sixteen and \
             `retained_entries` to thirty-two, so every group the node hosts \
             has purged past what it holds and it is fed a snapshot per group. Beside the fault \
             record, `cluster.catchup` is what the returning node's own report said each second \
             after it was placed: how it caught up, the seconds to converge, the bytes and entries \
             the snapshots moved, the entries the log fed, and a series of its lag.",
        how_to_read_it:
            "`seconds_to_converge` against `by` is the number: what the log costs a returning node \
             and what a snapshot costs it, on the same placement under the same load. \
             `snapshot_bytes` over the seconds is the transfer rate the bulk lane managed, and \
             `log_entries` over the same seconds the log's. The fault record's `after` window \
             against its `before` is what the catch-up cost the survivors, and the series is \
             where the lag fell - a lag that steps down is a snapshot landing, one that slopes \
             is the log. A `by` of `none` is a run that ended before the node converged, and the \
             series says what it was doing.",
        what_would_make_it_wrong:
            "A snapshot arm whose `by` is `log`: the node was inside the retained window after \
             all, which means the run wrote less than the retention in the third it was away, \
             and the arm is measuring nothing the log arm does not. A `converged_ms` before \
             `restarted_ms`, which means the sampler read a stale report. Comparing the two arms' \
             `seconds_to_converge` when the outage differed between them: the node returns at the \
             same mark, but what it has to catch up on is what the survivors wrote meanwhile, \
             which the outage decides.",
        what_it_cannot_say:
            "What a catch-up costs on a node that led nothing, or one that hosts a table other \
             than the reference one. What it costs over a network, where the bulk lane's rate is \
             the wire's. What a hot stream that cannot catch up inside its budget does over \
             minutes: the run is a minute and the retention test is the fixture's. The returning \
             node's own retry table, which only its leading would exercise.",
    },
    Family {
        name: "cluster-background",
        title: "What a scrub costs the foreground while it runs",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "One arm, `background/repair`: the kill arm's placement, mixture and client, driven \
             for the kill arm's time with nothing killed, and a `Repair` of the reference table \
             in verify mode asked for a third of the way through, its record polled each second \
             until every group is done. Every group's leader scrubs it while the mixture goes on: \
             the resident partitions hashed on the shard loop, the archived ones read off the disk \
             and hashed on a task, every member's report polled and judged. `cluster.background` \
             is when the repair was asked for and done, how many groups it covered and how many \
             were clean, what the scrubs hashed and read across every node, and the client's \
             distribution before, during and after it with a per second series.",
        how_to_read_it:
            "`during` against `before` is the number: what a scrub of the whole table costs the \
             foreground's median and tail while it runs, and `seconds` is how long it costs it \
             for. `bytes` over `seconds` is what the scrubs read off the archives per second, \
             which a scheduled scrub at an interval would spend that fraction of the time; \
             `partitions` says how much of that was resident and cost the loop rather than the \
             disk. A `clean` under `groups` is a verdict to read the record for, not a cost.",
        what_would_make_it_wrong:
            "A `finished_ms` that is absent, which means the run ended inside the scrub and the \
             `after` window is empty. A `bytes` of zero at full scale, which means every partition \
             was resident and the arm priced the loop's hashing alone. Reading `during` as an \
             outage: the client keeps its depth throughout and a slower window is the scrub's \
             share of the cores and the device, not a refusal.",
        what_it_cannot_say:
            "What a repair that installs costs, which is the snapshot arm's transfer on top of \
             this. What a scrub costs over a network, where the digests are a round trip each. \
             What it costs on a table whose archives are wider than memory, where the archived \
             pass is the whole of it. What a scheduled interval should be: this is the cost of one \
             pass, and the interval multiplies it.",
    },
    Family {
        name: "cluster-migration",
        title: "What a move costs the foreground, and how long it takes",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "One arm, `migration/move`: the kill arm's placement, mixture and client with a \
             fourth member staged beside the placement and placed on by nothing, driven for the \
             kill arm's time with nothing killed, and a `Move` of one set from node one to the \
             spare asked for a third of the way through, its record polled each second until it \
             is done. The destination is fed as a learner, made a voter through the group's own \
             joint transition once it is within the catch-up lag, activated by its own apply, \
             published as the set's configuration, and the source's copy retired after its grace, \
             all while the mixture goes on. `cluster.migration` is when the move was asked for and \
             done, how long each phase took, what the destination was fed - snapshot bytes and log \
             entries - and the client's distribution before, during and after it with a per \
             second series.",
        how_to_read_it:
            "`during` against `before` is the number: what a move of one set costs the \
             foreground's median and tail while it runs, and `seconds` is how long it costs it \
             for. `phase_ms` says where the time went: `catching_up` is the transfer, \
             `reconfiguring` the joint transition, `retiring` the source's grace, which \
             `cluster.migration.retire_after` sets and which is not a cost. `bytes` over the \
             catch-up is the transfer rate when the destination was fed a snapshot; `entries` \
             when it was fed the log.",
        what_would_make_it_wrong:
            "A `finished_ms` that is absent, which means the run ended inside the move and the \
             `after` window is empty; an `outcome` other than `moved`. A `bytes` of zero at full \
             scale, which means the set's rows fit the retained log and the arm priced a log feed \
             alone. Reading `during` as an outage: the client keeps its depth throughout, and a \
             slower window is the transfer's share of the cores and the device, not a refusal.",
        what_it_cannot_say:
            "What a move costs over a network, where the bulk lane's rate is the wire's. What \
             moving a set the client is not writing to costs, or moving one node's every set at \
             once, which is the rebalance family's. What a stale router pays, which the fixture \
             proves and the arm's single client never meets.",
    },
    Family {
        name: "cluster-rebalance",
        title: "What a plan costs the foreground: a spread onto a spare, a drain, an expiry, and one with nowhere to go",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "Four arms on the kill arm's placement, mixture and client, each with a plan the \
             control leader drives in the background from a third of the way through \
             ([F46](../features/capacity-rebalancing.md)). `rebalance/add` stages a fourth member \
             beside the placement and asks for a `Rebalance`, which is the one way data spreads onto \
             a new node. `rebalance/decommission` stages the same spare and asks for a `Decommission` \
             of node one, whose every set moves to the spare one at a time while node one keeps \
             serving. `rebalance/remove` stages the spare, kills node one and never starts it again, \
             under a five second grace: the leader counts the grace, records the expiry plan, and \
             rebuilds every set on the spare. `rebalance/capacity_blocked` stages no spare and asks \
             for a `Decommission` of node one at N = RF, which has nowhere to go. `cluster.rebalance` \
             is the kind, when the plan was asked for and done, the steps it derived and moved with \
             their bytes, the blocked reason if one, the client's distribution before, during and \
             after it with a per second series, and `p99_ratio_permille`: `during` over `before`, \
             in thousandths.",
        how_to_read_it:
            "`p99_ratio_permille` is the number: M9b's exit criterion is a healthy add or drain at \
             two times or under - `2000` here - with zero final errors, which is `during`'s `errors`. \
             `seconds` is how long the foreground paid it for, and `moved` over `seconds` is the \
             drain's pace under `cluster.rebalance.moves_per_node`, which these arms set to three \
             so nine sets fit the run. \
             `bytes` is what the sets held on their sources when planned; at full scale a step is \
             fed a snapshot under `stream_bytes_per_sec`, and the pace is the budget's. The blocked \
             arm reads differently: its `outcome` is `unfinished`, its `blocked` names the missing \
             member, its `steps` is zero, and its `during` window runs to the end of the run - what \
             it says is that a blocked plan costs the foreground nothing and stays visible. The \
             remove arm carries `cluster.fault` beside its plan, with no restart mark: the `during` \
             window there begins at the expiry, not at the kill, and the kill's own cost is the fault \
             record's.",
        what_would_make_it_wrong:
            "An `outcome` other than `completed` on the three arms with a spare, or other than \
             `unfinished` on the blocked one; a `finished_ms` that is absent on the three, which means \
             the run ended inside the plan and the `after` window is empty. A `p99_ratio_permille` \
             read without its `before` window's `ops`: a short `before` is a noisy denominator. A \
             `bytes` of zero at full scale, which means the sets' rows fit the retained log and the \
             arm priced a log feed alone. Reading a rebalance arm against the kill arm: the plan's \
             arms share its placement and mixture but stage a fourth member, and the control plane's \
             report and plan traffic is theirs alone.",
        what_it_cannot_say:
            "What a drain costs over a network, where the bulk lane's rate is the wire's and the \
             budget is the operator's. What a rebalance among many sets over many nodes costs: \
             three sets over four members is the smallest placement that has a move to make. \
             Whether the two-times budget holds on the benchmark host's hardware until a capture is \
             taken there: a smoke run on the development host is the shape, not the number. What a \
             plan costs a client the drained node is not serving, or a client on another node than \
             node zero.",
    },
    Family {
        name: "rehome",
        title: "What a restart at another executor count costs: the files a vanished executor left, moved before a shard starts",
        surface: Surface::AllWorkloads,
        what_it_measures:
            "One arm, the one node cluster arm seeded at twelve executors and measured at eight \
             ([F47](../features/local-rehome.md)). The start between the two runs a rehome: the \
             four vanished executors' archived records are copied onto the eight that remain, their \
             tablet groups' logs, votes, checkpoints and sidecars are moved, and their directories \
             are reclaimed, all before a shard starts. `cluster.rehome` is the pool's own report: the \
             counts it was between, the slots and groups moved, the records and bytes copied, the \
             steps a resumed rehome began again, and `millis`, how long the start was held for it. \
             The mixture after it is the reference mixture on eight executors hosting twelve slots.",
        how_to_read_it:
            "`millis` is the number: what an operator pays in start time for changing a node's core \
             count, at the seed's size. `bytes` over `millis` is the copy's pace, which is the \
             device's: every record is read verified and written as a fresh record, and the archives \
             are the bulk of it. `groups` is what the WAL half moved, and is small here - a group's \
             retained log is entries, not rows. `steps_redone` is zero on a capture; a nonzero one \
             means the start was interrupted and resumed, which the crash matrix does on purpose and \
             a capture never should. The mixture's distribution is not read against the reference \
             cell: eight executors hosting twelve slots is a different server, and the arm exists \
             for its record.",
        what_would_make_it_wrong:
            "A `from` and `to` other than twelve and eight, which means the arm's counts moved and \
             the capture is of another shrink. A `records` of zero at full scale, which means the seed \
             was not compacted before the stop and the rehome copied nothing - the mixture's writes \
             were still in the WAL, whose move is cheap, and `millis` priced the wrong thing. A \
             `millis` read on the development host: the copy is the device's pace, and this host's \
             is not the benchmark host's. A `steps_redone` above zero, which is a capture of a resume.",
        what_it_cannot_say:
            "What a growth costs, which leaves a donor's archives holding dead records until its own \
             compaction (O58) and copies less. What a rehome of a node holding a real share of a \
             large table costs: the seed is the reference cell's rows, and the copy is linear in \
             them. What the mixture pays afterwards for hosting twelve slots on eight executors \
             against eight on eight, which needs an arm at eight slots to read it against. What a \
             standalone node's rehome costs, whose fold of intent logs this arm never runs.",
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
    let name = if id.starts_with("macro/grid/depth/1/") {
        // longer than the ladder's own prefix, and checked first for that reason: these sweep the
        // width at one depth where the ladder sweeps the depth at one width
        "width-depth"
    } else if id.starts_with("macro/grid/depth/") {
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
    } else if id.starts_with("macro/cluster/hop/") {
        // before the wider cluster prefix, which would otherwise take these
        "cluster-hop"
    } else if id.starts_with("macro/cluster/reads/") || id.starts_with("macro/cluster/fanout/") {
        // the read arms, before the wider cluster prefix for the same reason
        "cluster-reads"
    } else if id.starts_with("macro/cluster/catchup/") {
        // the catch-up arms, before the wider cluster prefix for the same reason
        "cluster-catchup"
    } else if id.starts_with("macro/cluster/background/") {
        // the background arm, before the wider cluster prefix for the same reason
        "cluster-background"
    } else if id.starts_with("macro/cluster/migration/") {
        // the migration arm, before the wider cluster prefix for the same reason
        "cluster-migration"
    } else if id.starts_with("macro/cluster/rebalance/") {
        // the rebalance arms, before the wider cluster prefix for the same reason
        "cluster-rebalance"
    } else if id.starts_with("macro/cluster/failover/") {
        // the fault arms, before the wider cluster prefix for the same reason
        "cluster-failover"
    } else if id.starts_with("macro/rehome/") {
        // the rehome arm, whose server is a cluster of one under another prefix
        "rehome"
    } else if id.starts_with("macro/cluster/replication/") || id == "macro/cluster/overhead/nodes/3" {
        // the three node arms are read against each other and not against the one node one,
        // whose shard count they do not share
        "cluster-replication"
    } else if id.starts_with("macro/cluster/") {
        "cluster-overhead"
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
