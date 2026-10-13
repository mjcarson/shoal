### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 54.95 µs |
| fdatasync of a clean file | p50 15.36 µs, p99 24.16 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7666 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7670 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.23 µs, p99 19.98 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite-wt | 119.6 | 8351 | 8793 | 8983 | 0 | 4.01 | 0.980 |
| 2 | overwrite-wt | 118.8 | 16682 | 25296 | 42171 | 0 | 4.01 | 0.978 |
| 4 | overwrite-wt | 230.2 | 8731 | 66706 | 991923 | 0 | 4.01 | 0.964 |
| 6 | overwrite-wt | 220.0 | 16663 | 208252 | 975364 | 0 | 4.24 | 0.958 |

### 2. The journal-wt, round 1 (a 4 KiB header block on every record; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 117.6 | 118.0 | 1.00 | 8484 | 9074 | 9302 | 20.01 | 0 | 214.6 |
| 16K | 6 | written-ahead/each-wt | 4863 | 4834 | 1.01 | 918.9 | 9278 | 18849 | 20.01 | 0 | 33.72 |
| 4K | 1 | written-ahead/each-wt | 118.7 | 119.1 | 1.00 | 8396 | 8819 | 9007 | 8.01 | 0 | 218.2 |
| 4K | 6 | written-ahead/each-wt | 4760 | 4583 | 1.04 | 365.6 | 10036 | 17386 | 8.00 | 0 | 34.48 |

### 3. A partial write-wt, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-wt | 11441 | 15797 | 14384 | 25004 | 53.72 | 101.2 | 0 | 0 | 0 | 26578 | 44547 | 16.40 | 4.10 | 0 | 597.3 |
| 4K | 1 | J-ssd-wt | 123.7 | 179.9 | 9643 | 20381 | 53.38 | 82.15 | 0 | 0 | 0 | 9774 | 20504 | 8.02 | 2.00 | 0 | 397.7 |
| 4K | 6 | J-wt | 22121 | 62474 | 28680 | 61492 | 40.97 | 86.81 | 0 | 0 | 0 | 52075 | 97048 | 16.12 | 4.03 | 0 | 304.3 |
| 4K | 6 | J-ssd-wt | 67.25 | 184.2 | 24365 | 117734 | 26.86 | 81.09 | 0 | 0 | 0 | 24417 | 117856 | 8.02 | 2.00 | 0 | 208.4 |
| 16K | 1 | J-wt | 11298 | 21427 | 14436 | 26452 | 53.75 | 86.54 | 0 | 0 | 0 | 26196 | 41881 | 40.12 | 2.51 | 0 | 608.2 |
| 16K | 1 | J-ssd-wt | 135.7 | 190.3 | 9185 | 23088 | 53.61 | 81.77 | 0 | 0 | 0 | 9324 | 23228 | 20.02 | 1.25 | 0 | 402.5 |
| 16K | 6 | J-wt | 22295 | 58306 | 28969 | 76596 | 43.46 | 85.66 | 0 | 0 | 0 | 52295 | 106955 | 40.02 | 2.50 | 0 | 326.9 |
| 16K | 6 | J-ssd-wt | 73.37 | 231.3 | 24802 | 143001 | 26.24 | 88.58 | 0 | 0 | 0 | 24941 | 143095 | 20.18 | 1.26 | 0 | 211.8 |

