### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 53.56 µs |
| fdatasync of a clean file | p50 15.40 µs, p99 24.33 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7755 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7557 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.29 µs, p99 20.42 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 4 (write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | overwrite-wt | 328.4 | 8376 | 75380 | 658839 | 0 | 4.01 | 0.962 |
| 4 | overwrite-wt | 249.2 | 8373 | 92069 | 866928 | 0 | 4.87 | 0.955 |
| 2 | overwrite-wt | 118.4 | 16686 | 33655 | 75073 | 0 | 4.01 | 0.974 |
| 1 | overwrite-wt | 119.4 | 8357 | 8722 | 8999 | 0 | 4.01 | 0.976 |

### 2. The journal-wt, round 4 (a 4 KiB header block on every record; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 117.5 | 117.9 | 1.00 | 8483 | 9092 | 9141 | 20.01 | 0 | 228.7 |
| 16K | 6 | written-ahead/each-wt | 4476 | 4426 | 1.01 | 919.0 | 9435 | 18376 | 20.00 | 0 | 33.63 |
| 4K | 1 | written-ahead/each-wt | 119.0 | 119.4 | 1.00 | 8393 | 8789 | 9027 | 8.01 | 0 | 247.5 |
| 4K | 6 | written-ahead/each-wt | 6813 | 6644 | 1.03 | 365.1 | 8915 | 17392 | 8.00 | 0 | 33.30 |

### 3. A partial write-wt, round 4 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-ssd-wt | 125.1 | 175.2 | 9160 | 18769 | 53.11 | 81.31 | 0 | 0 | 0 | 9249 | 18905 | 8.02 | 2.00 | 0 | 392.4 |
| 4K | 1 | J-wt | 11356 | 22537 | 14522 | 28607 | 53.29 | 89.32 | 0 | 0 | 0 | 25854 | 42898 | 16.37 | 4.09 | 0 | 597.0 |
| 4K | 6 | J-ssd-wt | 66.27 | 215.0 | 24981 | 119885 | 26.44 | 83.33 | 0 | 0 | 0 | 25055 | 119944 | 8.02 | 2.00 | 0 | 209.5 |
| 4K | 6 | J-wt | 21953 | 54260 | 31158 | 69701 | 42.74 | 70.13 | 0 | 0 | 0 | 55260 | 105947 | 16.02 | 4.00 | 0 | 309.0 |
| 16K | 1 | J-ssd-wt | 134.2 | 222.9 | 10372 | 24901 | 53.06 | 85.22 | 0 | 0 | 0 | 10530 | 25039 | 20.11 | 1.26 | 0 | 400.2 |
| 16K | 1 | J-wt | 11371 | 25956 | 14571 | 24380 | 53.25 | 86.28 | 0 | 0 | 0 | 26307 | 40148 | 40.12 | 2.51 | 0 | 579.9 |
| 16K | 6 | J-ssd-wt | 83.82 | 196.6 | 25207 | 120381 | 34.50 | 81.08 | 0 | 0 | 0 | 25298 | 120437 | 20.02 | 1.25 | 0 | 223.4 |
| 16K | 6 | J-wt | 23810 | 57164 | 32127 | 73410 | 51.89 | 89.97 | 0 | 0 | 0 | 57858 | 109144 | 40.02 | 2.50 | 0 | 339.0 |

