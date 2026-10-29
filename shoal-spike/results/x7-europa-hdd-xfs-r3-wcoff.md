### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 69.31 µs |
| fdatasync of a clean file | p50 10.51 µs, p99 26.04 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7660 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7661 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.22 µs, p99 20.14 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 3 (write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite-wt | 119.4 | 8350 | 8727 | 8998 | 0 | 4.01 | 0.978 |
| 2 | overwrite-wt | 231.4 | 8351 | 17027 | 25565 | 0 | 4.01 | 0.965 |
| 4 | overwrite-wt | 127.4 | 24980 | 91530 | 1291365 | 0 | 4.03 | 0.975 |
| 6 | overwrite-wt | 237.4 | 16695 | 92069 | 1092107 | 0 | 4.02 | 0.963 |

### 2. The journal-wt, round 3 (a 4 KiB header block on every record; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 112.2 | 112.5 | 1.00 | 8485 | 25174 | 42149 | 20.39 | 0 | 216.7 |
| 16K | 6 | written-ahead/each-wt | 4483 | 4437 | 1.01 | 919.4 | 9431 | 17806 | 20.00 | 0 | 34.15 |
| 4K | 1 | written-ahead/each-wt | 118.7 | 119.1 | 1.00 | 8394 | 8830 | 9107 | 8.01 | 0 | 222.5 |
| 4K | 6 | written-ahead/each-wt | 6630 | 6428 | 1.03 | 365.6 | 8895 | 17381 | 8.00 | 0 | 34.29 |

### 3. A partial write-wt, round 3 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-wt | 11294 | 20182 | 14478 | 28262 | 53.80 | 97.44 | 0 | 0 | 0 | 26546 | 47104 | 16.49 | 4.12 | 0 | 597.9 |
| 4K | 1 | J-ssd-wt | 124.5 | 180.5 | 8559 | 20367 | 53.50 | 82.26 | 0 | 0 | 0 | 8673 | 20504 | 8.11 | 2.03 | 0 | 395.9 |
| 4K | 6 | J-wt | 22175 | 50151 | 29343 | 69097 | 41.95 | 86.62 | 0 | 0 | 0 | 53191 | 99176 | 16.02 | 4.00 | 0 | 310.0 |
| 4K | 6 | J-ssd-wt | 63.04 | 188.3 | 22483 | 173326 | 24.95 | 77.16 | 0 | 0 | 0 | 22544 | 173443 | 8.02 | 2.00 | 0 | 194.1 |
| 16K | 1 | J-wt | 11258 | 23754 | 14609 | 24612 | 54.16 | 87.61 | 0 | 0 | 0 | 26415 | 41657 | 40.12 | 2.51 | 0 | 606.6 |
| 16K | 1 | J-ssd-wt | 136.2 | 194.5 | 8976 | 18831 | 53.61 | 82.52 | 0 | 0 | 0 | 9122 | 18968 | 20.02 | 1.25 | 0 | 403.4 |
| 16K | 6 | J-wt | 23074 | 57706 | 29807 | 84768 | 42.97 | 85.19 | 0 | 0 | 0 | 55357 | 108480 | 40.10 | 2.51 | 0 | 330.8 |
| 16K | 6 | J-ssd-wt | 73.82 | 214.8 | 23135 | 134829 | 26.73 | 86.76 | 0 | 0 | 0 | 23207 | 134940 | 20.02 | 1.25 | 0 | 213.9 |

