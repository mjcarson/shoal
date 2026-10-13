### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1108 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 53.42 µs |
| fdatasync of a clean file | p50 10.63 µs, p99 26.50 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7866 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7678 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.22 µs, p99 20.91 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 51.21 | 76.27 | - | - | - |
| fold | 22.73 | 171.8 | - | - | - |
| crc+fold | 29.35 | 133.1 | - | - | - |
| copy | 28.76 | 135.8 | - | - | - |
| decode-21-data | 17.42 | 224.2 | - | - | - |
| decode-21-parity | 17.43 | 224.1 | - | - | - |
| decode-42-data | 9.27 | 421.5 | - | - | - |
| decode-42-parity | 9.30 | 419.9 | - | - | - |
| pipeline-copy | 21.82 | 179.0 | - | - | - |
| pipeline-21 | 8.81 | 443.2 | - | - | - |
| pipeline-42 | 4.56 | 855.9 | - | - | - |
| summary-42 | 4849 | 4.83 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 3 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 8.80 | 108.4 | 149.6 | 4151 | 8293 | 0 | 0 |
| layout=2+1 | unit | 30.70 | 32.11 | 50.69 | 68.21 | 128.0 | 0 | 0 |
| layout=4+2 | chunk | 5.90 | 158.4 | 266.8 | 4180 | 16765 | 0 | 0 |
| layout=4+2 | unit | 22.40 | 44.30 | 65.87 | 68.30 | 257.1 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 52.81 | 63.28 | 0.697 | 0 | 0 | 0.352 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 77.53 | 80.88 | 0.660 | 91.01 | 18.04 | 0.431 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 61.39 | 69.69 | 0.632 | 70.80 | 18.99 | 0.465 |
| layout=4+2 piece=1M | fixed-20 | 19.82 | 0.991 | 18.00 | 0/0 | 0 | 83.45 | 92.70 | 0.652 | 72.06 | 18.70 | 0.464 |
| layout=4+2 piece=1M | fixed-40 | 34.08 | 0.852 | 32.00 | 0/0 | 0 | 105.1 | 126.4 | 0.662 | 56.61 | 16.71 | 0.567 |
| layout=4+2 piece=1M | idle | 57.01 | 0 | 54.00 | 2/2 | 0 | 62.00 | 64.96 | 0.574 | 42.55 | 16.69 | 0.838 |
| layout=4+2 piece=1M | idle-ceil-20 | 18.62 | 0 | 17.00 | 2/2 | 0 | 60.10 | 63.51 | 0.639 | 67.12 | 18.73 | 0.519 |
| layout=4+2 piece=1M | unbounded | 76.07 | 0 | 73.00 | 0/0 | 0 | 240.3 | 266.4 | 0.574 | 31.82 | 16.20 | 0.880 |
| layout=4+2 piece=256K | idle | 54.90 | 0 | 51.00 | 0/0 | 0 | 76.94 | 120.3 | 0.613 | 34.18 | 14.55 | 0.824 |
| layout=4+2 piece=4M | idle | 67.67 | 0 | 65.00 | 0/0 | 0 | 92.67 | 115.4 | 0.573 | 37.21 | 15.42 | 0.853 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 45.55 | 51.53 | 0.885 | 0.630 | 0 | 0.338 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.95 | 9.96 | 0.996 | 400.5 | 71.28 | 86.32 | 0.868 | 0.604 | 0 | 0.510 |
| role=dest layout=4+2 piece=1M | fixed-20 | 17.80 | 17.82 | 0.891 | 216.9 | 97.53 | 97.22 | 0.838 | 0.605 | 0 | 0.638 |
| role=dest layout=4+2 piece=1M | fixed-40 | 24.80 | 24.82 | 0.621 | 158.3 | 83.94 | 151.7 | 1.16 | 0.619 | 0 | 0.748 |
| role=dest layout=4+2 piece=1M | fixed-80 | 28.25 | 28.28 | 0.353 | 133.5 | 114.2 | 101.6 | 0.853 | 0.636 | 0 | 0.829 |
| role=dest layout=4+2 piece=1M | idle | 26.00 | 26.03 | 0 | 149.9 | 60.87 | 81.18 | 0.744 | 0.571 | 0 | 0.826 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 22.15 | 22.17 | 0 | 158.5 | 80.83 | 95.86 | 0.825 | 0.601 | 0 | 0.755 |
| role=dest layout=4+2 piece=1M | unbounded | 60.95 | 61.01 | 0 | 125.0 | 209.4 | 288.2 | 0.833 | 0.577 | 0 | 0.880 |
| role=local layout=copy piece=1M | idle | 16.80 | 33.43 | 0 | 216.6 | 72.19 | 95.75 | 0.826 | 0.620 | 0 | 0.826 |
| role=local layout=copy piece=1M | unbounded | 35.60 | 71.42 | 0 | 208.5 | 298.6 | 296.0 | 0.838 | 0.563 | 0 | 0.883 |
| role=local layout=2+1 piece=1M | idle | 12.80 | 38.29 | 0 | 300.0 | 82.21 | 68.38 | 0.828 | 0.608 | 0 | 0.836 |
| role=local layout=2+1 piece=1M | unbounded | 24.40 | 73.47 | 0 | 308.5 | 297.6 | 438.3 | 0.722 | 0.607 | 0 | 0.900 |
| role=local layout=4+2 piece=1M | idle | 9.00 | 44.44 | 0 | 416.7 | 76.76 | 77.91 | 0.789 | 0.570 | 0 | 0.842 |
| role=local layout=4+2 piece=1M | unbounded | 16.20 | 80.08 | 0 | 483.2 | 386.5 | 483.3 | 0.741 | 0.593 | 0 | 0.892 |

