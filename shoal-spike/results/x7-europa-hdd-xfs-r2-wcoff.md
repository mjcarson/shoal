### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 54.03 µs |
| fdatasync of a clean file | p50 15.35 µs, p99 27.30 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7760 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7655 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.16 µs, p99 20.22 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 2 (write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | overwrite-wt | 351.4 | 8368 | 75115 | 617387 | 0 | 4.01 | 0.958 |
| 4 | overwrite-wt | 213.6 | 8479 | 116661 | 608252 | 0 | 4.01 | 0.966 |
| 2 | overwrite-wt | 219.2 | 8360 | 25045 | 191585 | 0 | 5.00 | 0.962 |
| 1 | overwrite-wt | 119.4 | 8355 | 8771 | 8995 | 0 | 4.01 | 0.977 |

### 2. The journal-wt, round 2 (a 4 KiB header block on every record; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 117.6 | 118.0 | 1.00 | 8487 | 9079 | 9398 | 20.48 | 0 | 211.8 |
| 16K | 6 | written-ahead/each-wt | 4950 | 4918 | 1.01 | 919.2 | 9294 | 10793 | 20.00 | 0 | 33.99 |
| 4K | 1 | written-ahead/each-wt | 119.0 | 119.3 | 1.00 | 8395 | 8903 | 9048 | 8.01 | 0 | 233.7 |
| 4K | 6 | written-ahead/each-wt | 11145 | 11056 | 1.01 | 365.1 | 8692 | 17043 | 8.00 | 0 | 32.36 |

### 3. A partial write-wt, round 2 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-ssd-wt | 125.4 | 190.2 | 10684 | 25161 | 54.30 | 86.26 | 0 | 0 | 0 | 10809 | 25289 | 8.38 | 2.10 | 0 | 411.0 |
| 4K | 1 | J-wt | 11176 | 21620 | 14826 | 26786 | 54.52 | 89.42 | 0 | 0 | 0 | 25675 | 43269 | 16.15 | 4.04 | 0 | 593.0 |
| 4K | 6 | J-ssd-wt | 69.78 | 180.3 | 24654 | 139865 | 28.39 | 84.04 | 0 | 0 | 0 | 24723 | 139922 | 8.02 | 2.00 | 0 | 212.2 |
| 4K | 6 | J-wt | 18198 | 42198 | 27849 | 65076 | 38.27 | 104.7 | 0 | 0 | 0 | 47515 | 86709 | 16.02 | 4.00 | 0 | 292.0 |
| 16K | 1 | J-ssd-wt | 137.2 | 213.4 | 12638 | 22364 | 54.15 | 86.10 | 0 | 0 | 0 | 12771 | 22503 | 20.10 | 1.26 | 0 | 412.1 |
| 16K | 1 | J-wt | 11232 | 19301 | 14833 | 25388 | 54.67 | 95.55 | 0 | 0 | 0 | 25728 | 41268 | 40.03 | 2.50 | 0 | 604.4 |
| 16K | 6 | J-ssd-wt | 82.40 | 186.0 | 25027 | 117344 | 29.27 | 81.94 | 0 | 0 | 0 | 25121 | 117473 | 20.12 | 1.26 | 0 | 221.8 |
| 16K | 6 | J-wt | 19984 | 57781 | 28763 | 75893 | 47.49 | 101.4 | 0 | 0 | 0 | 50430 | 108424 | 40.02 | 2.50 | 0 | 314.6 |

