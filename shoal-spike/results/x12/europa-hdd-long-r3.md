### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs long

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1100 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 54.88 µs |
| fdatasync of a clean file | p50 10.26 µs, p99 20.40 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7743 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7532 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.28 µs, p99 15.82 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs long

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 55.33 | 61.96 | 0.656 | 0 | 0 | 0.333 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 19.00 | 0/0 | 0 | 62.73 | 72.86 | 0.643 | 66.69 | 17.79 | 0.413 |
| layout=4+2 piece=1M | fixed-10 | 10.00 | 1.000 | 38.00 | 0/0 | 0 | 65.87 | 78.85 | 0.636 | 62.88 | 17.43 | 0.445 |
| layout=4+2 piece=1M | idle | 58.82 | 0 | 226.0 | 2/2 | 0 | 69.28 | 80.04 | 0.610 | 39.99 | 16.05 | 0.847 |
| layout=4+2 piece=1M | idle-ceil-20 | 18.87 | 0 | 73.00 | 0/0 | 0 | 64.96 | 66.35 | 0.658 | 70.33 | 17.13 | 0.507 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs long

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 53.86 | 65.33 | 0.873 | 0.654 | 0 | 0.354 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.93 | 9.94 | 0.994 | 400.1 | 79.88 | 92.56 | 0.915 | 0.657 | 0 | 0.519 |
| role=dest layout=4+2 piece=1M | idle | 24.58 | 24.60 | 0 | 150.0 | 81.85 | 86.25 | 0.836 | 0.619 | 0 | 0.813 |

