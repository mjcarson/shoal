### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.05 µs |
| fdatasync of a clean file | p50 23.79 µs, p99 45.76 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 10 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fsync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.75 µs, p99 32.63 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 3 (write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite-wt | 119.6 | 8348 | 8383 | 8713 | 0 | 4.01 | 0.988 |
| 2 | overwrite-wt | 226.2 | 8350 | 25027 | 33359 | 0 | 4.01 | 0.981 |
| 4 | overwrite-wt | 259.8 | 8351 | 50009 | 908389 | 0 | 4.01 | 0.979 |
| 6 | overwrite-wt | 214.0 | 16686 | 66686 | 1767047 | 0 | 4.06 | 0.979 |

### 2. The journal-wt, round 3 (a 4 KiB header block on every record; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 118.2 | 118.6 | 1.00 | 8414 | 8794 | 17128 | 20.01 | 0 | 101.6 |
| 16K | 6 | written-ahead/each-wt | 9793 | 9794 | 1.00 | 472.3 | 9162 | 9214 | 20.00 | 0 | 56.44 |
| 4K | 1 | written-ahead/each-wt | 119.3 | 119.7 | 1.00 | 8365 | 8398 | 17057 | 8.01 | 0 | 105.8 |
| 4K | 6 | written-ahead/each-wt | 704.8 | 576.6 | 1.23 | 8522 | 8880 | 8917 | 8.00 | 0 | 45.58 |

### 3. A partial write-wt, round 3 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-wt | 9057 | 25969 | 16109 | 21929 | 40.97 | 52.19 | 0 | 0 | 0 | 25032 | 38733 | 16.37 | 4.09 | 0 | 255.3 |
| 4K | 1 | J-ssd-wt | 942.8 | 1947 | 7474 | 21954 | 41.37 | 52.06 | 0 | 0 | 0 | 8377 | 23901 | 8.02 | 2.00 | 0 | 282.6 |
| 4K | 6 | J-wt | 19300 | 43688 | 23404 | 47087 | 40.71 | 88.21 | 0 | 0 | 0 | 43275 | 77434 | 16.02 | 4.00 | 0 | 180.1 |
| 4K | 6 | J-ssd-wt | 993.7 | 2562 | 22658 | 130290 | 41.60 | 102.4 | 0 | 0 | 0 | 23680 | 133666 | 8.18 | 2.05 | 0 | 212.9 |
| 16K | 1 | J-wt | 9387 | 13958 | 13790 | 22113 | 41.22 | 50.55 | 0 | 0 | 0 | 25008 | 37915 | 40.02 | 2.50 | 0 | 268.6 |
| 16K | 1 | J-ssd-wt | 1035 | 3833 | 7682 | 24427 | 41.45 | 54.27 | 0 | 0 | 0 | 8735 | 25387 | 20.11 | 1.26 | 0 | 287.1 |
| 16K | 6 | J-wt | 19459 | 39949 | 23411 | 55862 | 34.92 | 80.49 | 0 | 0 | 0 | 43420 | 85703 | 40.02 | 2.50 | 0 | 202.4 |
| 16K | 6 | J-ssd-wt | 1054 | 3927 | 19803 | 240974 | 30.11 | 80.63 | 0 | 0 | 0 | 21168 | 242025 | 20.02 | 1.25 | 0 | 245.3 |

