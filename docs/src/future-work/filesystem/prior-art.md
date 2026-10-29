# W29. Lessons from CephFS, Lustre, JuiceFS, GlusterFS, SeaweedFS and InfiniFS

## Context

Every part of a distributed filesystem has been built before, several ways. CephFS is the closest to
Shoal in what it stands on: a namespace kept beside an object store that places its data. JuiceFS is
the closest in shape: a namespace in a replicated key-value store and the data elsewhere. Lustre is
the fastest at what HPC asks; GlusterFS is the one with no metadata service at all; SeaweedFS is the
inspiration the user named beside Ceph; and InfiniFS is the research that split a directory the way
[W31](namespace.md) does. This page records how each keeps its namespace, who serves an operation,
how a file's bytes travel, what a client may cache, and what a rename between directories costs, and
says for each what Shoal takes and what it does not. [W15](../volumes/prior-art.md) holds RBD, DRBD and
the block stores; [X14](../../object-storage/ceph-and-s3-sources.md) read Ceph's object path at the
source.

Every fact below was read on 2026-10-26 from the page or source file its link names; **(src)** is a
source file at the tag its link names. A claim that was not read is marked **recalled**, and one seen
only in a search engine's summary **(snippet)**. None of this has been run on the lab; reading the
namespace paths at pinned releases, and running CephFS and JuiceFS beside a file tree, is
[X36](spikes.md#x36-cephfs-juicefs-and-infinifs-read-at-the-source)'s, optional.

## The systems

### CephFS

**A journal a rank, and the metadata pool's latency is the filesystem's.** Each active metadata
server rank keeps a journal of events striped over objects in the metadata pool, replayed on
failover, 1,024 events a segment and trimming past 128 segments
([journaling](https://docs.ceph.com/en/latest/cephfs/mds-journaling/)). The metadata pool's latency
"will directly affect the latency of client file system operations", and it cannot be erasure coded,
since it needs omap ([createfs](https://docs.ceph.com/en/latest/cephfs/createfs/)). "Each Metadata
Server (MDS) cannot exploit many cores, so its latency is lowest with a high clock rate"
([hardware](https://docs.ceph.com/en/latest/start/hardware-recommendations/)).

**A directory is an object of omap keys, and a file's inode lives in its entry.** A directory fragment
is the RADOS object `file_object_t(ino, frag)`, named `"%llx.%08llx"`, in the metadata pool (src:
[CDir.h:477](https://github.com/ceph/ceph/blob/v20.2.0/src/mds/CDir.h#L477-L479),
[object.h:91-103](https://github.com/ceph/ceph/blob/v20.2.0/src/include/object.h#L91-L103)). Its entries
are omap keys: `'I'`, a primary entry with the inode embedded, and `'L'`, a remote entry for a hard link
holding only an inode number ([CDir.cc:1845, 1888](https://github.com/ceph/ceph/blob/v20.2.0/src/mds/CDir.cc#L1845));
a commit writes the fragment's header last, so a partial commit never looks complete
([CDir.cc:2432-2475](https://github.com/ceph/ceph/blob/v20.2.0/src/mds/CDir.cc#L2432-L2475)). Embedding
lets "whole directories ... load into cache in one I/O" ([kernel](https://docs.kernel.org/filesystems/ceph.html)),
and hard links carry "a particular performance cost"
([best practices](https://docs.ceph.com/en/latest/cephfs/app-best-practices/)). W31 keeps a file's inode
row in its entry's tablet for the same reason, without embedding it in the entry.

**Backtraces find an inode by its number.** The metadata server writes `parent`, the chain of ancestor
entries, and `layout` as xattrs on an inode's first data object (src:
[CInode.cc:60-90, 1432-1470](https://github.com/ceph/ceph/blob/v20.2.0/src/mds/CInode.cc#L60-L90)), so
every inode has an object in the default data pool, which hard links and recovery rely on
([createfs](https://docs.ceph.com/en/latest/cephfs/createfs/)). W31's inode numbers carry their home
tablet instead, so finding one needs no backtrace.

**Fragments split at ten thousand entries.** A fragment splits into eight at 10,000 entries after five
seconds, merges below 50, and refuses past 100,000 ([dirfrags](https://docs.ceph.com/en/latest/cephfs/dirfrags/)).
Several active ranks help "many clients working in separate directories"; `ceph.dir.pin` pins a subtree
to a rank and ephemeral pins place directories by a "consistent hash of its inode number"; the dynamic
balancer is off by default because it "can be slow or inefficient"
([multimds](https://docs.ceph.com/en/latest/cephfs/multimds/)). Shoal's placement by hash is the
ephemeral pin, on every node at once.

**Capabilities, and their recall.** Caps come in categories with bits for shared, exclusive, cache,
read, write, buffer, append and lazy I/O, and one "loner" client gets them all; with several readers
and writers an inode goes to a mixed state where "Fcb ... won't be granted ... clients will do sync
read/write" ([capabilities](https://docs.ceph.com/en/latest/cephfs/capabilities/)). The server recalls
caps when its cache is full, when a client holds more than a million, or when a client is idle, at most
30,000 a recall each second ([cache](https://docs.ceph.com/en/latest/cephfs/cache-configuration/)); a
client that does not release within `session_timeout`, 60 s, raises "failing to respond to capability
release" ([health](https://docs.ceph.com/en/latest/cephfs/health-messages/)), and past
`session_autoclose`, 300 s, it is evicted and blocklisted and "any un-flushed data will be lost"
([eviction](https://docs.ceph.com/en/latest/cephfs/eviction/)). LazyIO leaves coherence to the
application ([lazyio](https://docs.ceph.com/en/latest/cephfs/lazyio/)).

**A file's bytes go straight to the OSDs.** The default layout is a 4 MiB stripe unit, a stripe count of
one and 4 MiB objects, fixed once a file has data ([layouts](https://docs.ceph.com/en/latest/cephfs/file-layouts/)),
its objects `<ino hex>.%08llx` (src: [Striper.h:58-65](https://github.com/ceph/ceph/blob/v20.2.0/src/osdc/Striper.h#L58-L65));
clients talk to the OSDs directly ([architecture](https://docs.ceph.com/en/reef/architecture/)).

**The kernel client creates without waiting.** "The kernel client will always give better performance"
([mount prerequisites](https://docs.ceph.com/en/latest/cephfs/mount-prerequisites/)); holding a
directory's caps, it creates and unlinks without waiting for the server, since Linux 5.7 and by default
since 5.16, and a 2020 talk's figures had a directory's removal go from 10.3 s to 0.3 s and many creates
from 11.3 s to 5.5 s ([Phoronix](https://www.phoronix.com/news/Linux-5.16-Ceph)).

**Where it hurts.** `ls -l` waits for other clients' writers to flush their sizes; huge directories are
slow; workloads larger than the server's cache behave unlike small tests
([best practices](https://docs.ceph.com/en/latest/cephfs/app-best-practices/)).

### Lustre

**The metadata server leaves once a file is open.** A management server keeps configuration, the
metadata server and its targets hold the namespace and layouts and allocate the data servers' objects
at create, and "the MDS does not participate in file I/O once the file is opened"; a file may stripe
over up to 2,000 targets ([introduction](https://wiki.lustre.org/Introduction_to_Lustre)).

**Layouts grow with a file, and a small file can live with its metadata.** Progressive layouts add
components as a file grows, the client asking the metadata server to instantiate objects before it
writes past them ([PFL](https://wiki.lustre.org/Progressive_File_Layouts)); Data on MDT, since 2.11,
puts the first component on the metadata target, 1 MB by default and up to 1 GB
([manual](https://github.com/LiXi-storage/lustre_manual_markdown/blob/master/20-Data%20on%20MDT%20(DoM).md)).
W34's inline files are Data on MDT in Shoal's rows.

**Several metadata servers, by directory.** Striped directories, since 2.8, are "most useful for ...
50k entries or more", and remote directories hang off the first target by default
([manual](https://github.com/LiXi-storage/lustre_manual_markdown/blob/master/13-Lustre%20Operations.md)).

**Locks a client caches under, and intents.** The distributed lock manager lets clients "cache state
protected by a lock until a conflicting lock is requested", with extent, flock, inode-bits and plain
locks ([lustre_dlm.h](https://github.com/lustre/lustre-release/blob/master/lustre/include/lustre_dlm.h));
with an intent lock the metadata server "may execute the intent and return a different lock resource",
creating a file and returning a lock on it in one exchange ([arXiv, §7.5](https://arxiv.org/abs/1903.01955)).
W32's operations are intents in this sense: a client asks for the operation, and its tablet group does
it in one command.

**Why small files are slow.** A file's size lives on its data servers, so "the client must contact each
OST that the file is stored across" ([LSoM](https://github.com/LiXi-storage/lustre_manual_markdown/blob/master/21-Lazy%20Size%20on%20MDT%20(LSoM).md)),
and small random I/O on them "can cause device seeking and hurt the streaming IO performance"
([DoM](https://github.com/LiXi-storage/lustre_manual_markdown/blob/master/20-Data%20on%20MDT%20(DoM).md));
a client has only seven modifying metadata requests in flight by default
([parameters](https://github.com/LiXi-storage/lustre_manual_markdown/blob/master/39-Lustre%20Parameters.md)).

### JuiceFS

**Metadata in an engine, data as immutable slices.** A file is 64 MB chunks; each write makes a slice
inside one chunk, uploaded as blocks of up to 4 MB, and a read takes the newest slice at each offset
([architecture](https://juicefs.com/docs/community/architecture)). A flush uploads the blocks, then
updates the metadata; write-back mode answers once the data is in a local cache
([I/O](https://juicefs.com/docs/community/internals/io_processing)).

**Compaction, sometimes on the write's path.** A chunk read with five or more slices is compacted in the
background, and at 2,500 slices a write compacts synchronously (src:
[base.go:2246-2249](https://github.com/juicedata/juicefs/blob/main/pkg/meta/base.go#L2246-L2249));
replaced slices are kept for the trash period, a day by default
([format.go:195-197](https://github.com/juicedata/juicefs/blob/main/cmd/format.go#L195-L197)).

**Close-to-open, with one-second caches.** Close-to-open is the default, with attribute, entry and
directory caches of a second; `--open-cache` gives close-to-open up
([cache](https://juicefs.com/docs/community/guide/cache)).

**Sessions, sustained inodes and locks as rows.** A session heartbeats and other clients clean up one
that expires; a file unlinked while open is kept as "Sustained" until it is closed; BSD locks are keyed
by inode, session and owner, and POSIX range locks are 24-byte records
([internals](https://juicefs.com/docs/community/internals)). W35's held inodes and locks are these.

**Where the time goes.** A create took 570 µs on Redis, 844 on PostgreSQL, 1,209 on TiKV, 1,570 on MySQL
and 1,849 on etcd ([benchmark](https://juicefs.com/docs/community/metadata_engines_benchmark)): TiKV and
etcd keep three Raft copies, Shoal's shape, and cost two to three times Redis.

### GlusterFS

**No metadata service, and every directory everywhere.** File names hash into 32-bit ranges a brick,
recorded in xattrs on directories that exist on every brick
([architecture](https://docs.gluster.org/en/latest/Quick-Start-Guide/Architecture/)). A rename leaves a
zero-byte pointer behind, and a miss sends "a lookup to each distribute subvolume", twelve on a
four-by-three volume ([blog](https://www.gluster.org/dht-misses-are-expensive/)).

**The client replicates, in five phases.** AFR is synchronous and run by the client: lock, pre-op marking
the change dirty, the operation, post-op counting what each brick missed, unlock; healing runs every ten
minutes, by an index or a full crawl ([AFR](https://docs.gluster.org/en/latest/Administrator-Guide/Automatic-File-Replication/)).

**Why `ls` is slow.** `readdirp` runs brick after brick, and a recursive `ls -l` costs `(x+1)·m·n` of them
for `n` bricks ([RHBZ 1427096](https://bugzilla.redhat.com/show_bug.cgi?id=1427096)).

### SeaweedFS

**A filer over volumes of needles.** Masters form a Raft group and know where volumes are; volume
servers pack objects into volumes of up to 30 GB with "a 16-byte in-memory index per blob"
([components](https://github.com/seaweedfs/seaweedfs/wiki/Components), [README](https://github.com/seaweedfs/seaweedfs)).
The filer's stores are pluggable, LevelDB by default ([filer stores](https://github.com/seaweedfs/seaweedfs/wiki/Filer-Stores));
files split above 4 MB and small files are not inlined by default
([filer.go:124, 132](https://github.com/seaweedfs/seaweedfs/blob/master/weed/command/filer.go#L124)),
and where chunks overlap the later write wins
([filechunks.go:278-299](https://github.com/seaweedfs/seaweedfs/blob/master/weed/filer/filechunks.go#L278-L299)).

**The mount.** `weed mount` is FUSE: metadata through the filer, data to the volume servers, metadata
synced into a local database, and between mounts the last flush wins unless `-dlm` is given
([FUSE mount](https://github.com/seaweedfs/seaweedfs/wiki/FUSE-Mount)); its metadata cache lives 60 s and
FUSE's writeback cache is off by default, flagged "at risk of data loss on crash"
([mount.go](https://github.com/seaweedfs/seaweedfs/blob/master/weed/command/mount.go#L106)).

**A change feed of the namespace.** Changes are logged and `SubscribeMetadata(prefix, since_ns)` replays
from any point in time, feeding mounts, replication and backup
([events](https://github.com/seaweedfs/seaweedfs/wiki/Filer-Metadata-Events)).

**What the store comparison found.** SeaweedFS 4.48 synced nothing by default, its filer's entry in
LevelDB was never synced, and a stat was answered by the filer alone
([store comparison](../../object-storage/store-comparison.md)).

### InfiniFS

**A directory's access and content, apart.** InfiniFS "decouples the access and content metadata of
directories, so that the directory tree can be partitioned with both metadata locality and load
balancing", resolves a path speculatively "to traverse the path in parallel", keeps an "optimistic
access metadata cache on the client-side", and holds "stable performance for extremely large-scale
directory trees with up to 100 billion files"
([FAST '22](https://www.usenix.org/conference/fast22/presentation/lv)). A secondary write-up gives access
metadata as name, id and permissions and content metadata as the entry list and timestamps
([Blocks and Files](https://blocksandfiles.com/2022/03/10/infinifs-solves-the-100-billion-file-metadata-problem));
the paper's own definitions were not read. W31's access and content parts are this split.

### CFS

Baidu's metadata service, published at EuroSys 2023 (Wang et al., pp. 331–346, per
[dblp](https://dblp1.uni-trier.de/pid/221/2416-2.html)); only its bibliographic record was read. That it
makes most operations atomic primitives of one shard in a sharded store and serializes renames through a
renamer is **recalled**, and so is everything else about it; Baidu's own account of it is a blog's, not
the paper's.

## What Shoal takes, and what it does not

| From | Taken | Not taken, and why |
| --- | --- | --- |
| CephFS | A file's inode in its entry's place; hashed placement of directories, as ephemeral pins; a client writing bytes straight to the data's holders; caps, as write leases granted where the inode lives; a size committed into the row | A journal beside the namespace and a write-back of fragments after it: two durable writes an operation, where a tablet group's log already is the journal. A single-threaded cache as the truth, with recall storms. A size only a caps holder knows |
| Lustre | Intents: an operation asked for and done in one command; small files beside their metadata | A size on the data servers; seven modifying requests a client; a kernel module |
| JuiceFS | Close-to-open as the default contract; sustained inodes; locks as rows | A metadata engine a hop from the data; compaction on a write's path; an object put a small file |
| GlusterFS | An index of what to heal, not a crawl | Directories on every server; broadcast lookups; a client running replication |
| SeaweedFS | A change feed of the namespace, for later caches; the newest write winning an overlap | The last flush winning between mounts; metadata never synced |
| InfiniFS | A directory's access part with its parent and its content part with its children | Speculative path resolution, not needed while a path's parts are cached |
| CFS | Recalled only | — |

## Implementation reading list

| Source | What to read it for | Before |
| --- | --- | --- |
| `src/mds/CDir.cc` and `src/mds/Server.cc` at v20.2.0 | How a fragment is committed and how a rename between ranks is done | [M37](milestones.md#m37-across-groups) |
| `src/mds/Locker.cc` at v20.2.0, **recalled** as the caps' home | What a recall waits on, and how a client's eviction ends its caps | [M38](milestones.md#m38-file-data-on-the-lane) |
| JuiceFS's `pkg/meta/` | Sessions, sustained inodes and locks as rows | M37 |
| [InfiniFS](https://www.usenix.org/conference/fast22/presentation/lv), the paper | Its definitions of the two parts, and how it renames a directory | [M36](milestones.md#m36-the-namespace-on-one-node) |
| [FUSE over io_uring](https://github.com/torvalds/linux/blob/master/Documentation/filesystems/fuse/fuse-io-uring.rst) | Its queues and what still goes through `/dev/fuse` | [M39](milestones.md#m39-the-fuse-mount) |

## The comparison

| System | Where the namespace lives | Who serves an operation | A file's bytes | What a client may cache | A rename across directories |
| --- | --- | --- | --- | --- | --- |
| CephFS | Metadata servers' caches, journalled to the metadata pool | The rank owning the subtree | RADOS objects, written by the client | What its caps allow, recalled | Under the servers' locks; across ranks, **recalled** |
| Lustre | Metadata targets | The metadata server; several under DNE | Data servers' objects, by layout | What its locks protect | The metadata server |
| JuiceFS | Redis, TiKV or SQL | The client, against the engine | Slices in an object store | Close-to-open, a second | The engine's transaction |
| GlusterFS | Xattrs on every brick | The client, by hash | Files on bricks | **Recalled**: little | A pointer left behind |
| SeaweedFS | A filer's store | The filer | Needles in volumes | Its local copy of the metadata | The filer's store, **recalled** |
| InfiniFS | Partitioned metadata servers | Its servers | Not in its abstract | An optimistic access cache | Not in its abstract |
| **Shoal, proposed** | **The namespace table, on every node's table shards** | **The directory's tablet group** | **Lane stripes, under a write lease** | **Close-to-open and bounded caches; write leases called back** | **An operation record across two groups** |

## Sources

All read 2026-10-26.

- Ceph: at v20.2.0, `src/mds/CDir.h`, `src/mds/CDir.cc`, `src/mds/CInode.cc`, `src/include/object.h`,
  `src/osdc/Striper.h`; [journaling](https://docs.ceph.com/en/latest/cephfs/mds-journaling/),
  [createfs](https://docs.ceph.com/en/latest/cephfs/createfs/),
  [hardware](https://docs.ceph.com/en/latest/start/hardware-recommendations/),
  [dirfrags](https://docs.ceph.com/en/latest/cephfs/dirfrags/),
  [multimds](https://docs.ceph.com/en/latest/cephfs/multimds/),
  [capabilities](https://docs.ceph.com/en/latest/cephfs/capabilities/),
  [cache](https://docs.ceph.com/en/latest/cephfs/cache-configuration/),
  [health](https://docs.ceph.com/en/latest/cephfs/health-messages/),
  [eviction](https://docs.ceph.com/en/latest/cephfs/eviction/),
  [lazyio](https://docs.ceph.com/en/latest/cephfs/lazyio/),
  [layouts](https://docs.ceph.com/en/latest/cephfs/file-layouts/),
  [mount prerequisites](https://docs.ceph.com/en/latest/cephfs/mount-prerequisites/),
  [best practices](https://docs.ceph.com/en/latest/cephfs/app-best-practices/),
  [kernel](https://docs.kernel.org/filesystems/ceph.html),
  [Phoronix on 5.16](https://www.phoronix.com/news/Linux-5.16-Ceph).
- Lustre: [introduction](https://wiki.lustre.org/Introduction_to_Lustre),
  [PFL](https://wiki.lustre.org/Progressive_File_Layouts), the manual's chapters on DoM, operations, LSoM
  and parameters, [lustre_dlm.h](https://github.com/lustre/lustre-release/blob/master/lustre/include/lustre_dlm.h),
  [arXiv 1903.01955](https://arxiv.org/abs/1903.01955).
- JuiceFS: [architecture](https://juicefs.com/docs/community/architecture),
  [internals](https://juicefs.com/docs/community/internals),
  [I/O](https://juicefs.com/docs/community/internals/io_processing),
  [cache](https://juicefs.com/docs/community/guide/cache),
  [metadata engines](https://juicefs.com/docs/community/metadata_engines_benchmark), `pkg/meta/base.go`,
  `cmd/format.go`.
- GlusterFS: [architecture](https://docs.gluster.org/en/latest/Quick-Start-Guide/Architecture/),
  [DHT misses](https://www.gluster.org/dht-misses-are-expensive/),
  [AFR](https://docs.gluster.org/en/latest/Administrator-Guide/Automatic-File-Replication/),
  [RHBZ 1427096](https://bugzilla.redhat.com/show_bug.cgi?id=1427096).
- SeaweedFS: its wiki's components, filer stores, FUSE mount, filer metadata events, replication and
  erasure coding pages; `weed/command/filer.go`, `weed/command/mount.go`, `weed/filer/filechunks.go`.
- [InfiniFS at FAST '22](https://www.usenix.org/conference/fast22/presentation/lv);
  [Blocks and Files on InfiniFS](https://blocksandfiles.com/2022/03/10/infinifs-solves-the-100-billion-file-metadata-problem);
  [dblp for CFS](https://dblp1.uni-trier.de/pid/221/2416-2.html).

## Related

[W15](../volumes/prior-art.md) for the block stores; [S17](../../object-storage/prior-art.md) and
[X14](../../object-storage/ceph-and-s3-sources.md) for Ceph's object path; [W31](namespace.md) and
[W35](mounts-and-locks.md) for what these lessons shaped.
