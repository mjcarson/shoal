### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 5.36 KiB a sync · sync p50 54.54 µs |
| fdatasync of a clean file | p50 10.28 µs, p99 22.24 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7693 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7659 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.12 µs, p99 16.49 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 46.07 | 84.79 | - | - | - |
| fold | 23.89 | 163.5 | - | - | - |
| crc+fold | 29.27 | 133.5 | - | - | - |
| copy | 28.57 | 136.7 | - | - | - |
| decode-21-data | 17.88 | 218.5 | - | - | - |
| decode-21-parity | 17.55 | 222.6 | - | - | - |
| decode-42-data | 9.31 | 419.6 | - | - | - |
| decode-42-parity | 9.33 | 418.8 | - | - | - |
| pipeline-copy | 21.43 | 182.3 | - | - | - |
| pipeline-21 | 8.82 | 443.0 | - | - | - |
| pipeline-42 | 4.59 | 850.9 | - | - | - |
| summary-42 | 5055 | 4.64 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 7.50 | 108.5 | 117.0 | 4854 | 9840 | 0 | 0 |
| layout=2+1 | unit | 28.50 | 34.14 | 55.35 | 71.58 | 134.7 | 0 | 0 |
| layout=4+2 | chunk | 4.50 | 166.7 | 199.6 | 5472 | 21867 | 0 | 0 |
| layout=4+2 | unit | 19.50 | 45.84 | 68.62 | 73.23 | 275.7 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 40.09 | 45.29 | 0.526 | 0 | 0 | 0.340 |
| layout=4+2 piece=1M | fixed-5 | 5.26 | 1.05 | 0 | 0/0 | 0 | 3.83 | 41.66 | 0.574 | 0 | 0 | 0.205 |
| layout=4+2 piece=1M | idle | 82.58 | 0 | 5.00 | 0/0 | 0 | 45.08 | 42.15 | 1.38 | 31.50 | 16.89 | 0.842 |
| layout=4+2 piece=1M | unbounded | 102.1 | 0 | 6.00 | 0/0 | 0 | 184.8 | 292.0 | 0.061 | 28.78 | 14.36 | 0.872 |
| layout=4+2 piece=256K | idle | 90.09 | 0 | 5.00 | 0/0 | 0 | 46.92 | 46.79 | 0.047 | 28.30 | 14.66 | 0.851 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 11.44 | 28.80 | 0.771 | 0.621 | 0 | 0.133 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.75 | 9.76 | 0.976 | 408.7 | 1.66 | 36.41 | 0.655 | 0.535 | 0 | 0.308 |
| role=dest layout=4+2 piece=1M | idle | 40.50 | 40.54 | 0 | 83.77 | 1.42 | 30.12 | 0.547 | 0.510 | 0 | 0.827 |
| role=dest layout=4+2 piece=1M | unbounded | 72.00 | 72.07 | 0 | 92.13 | 6.60 | 177.3 | 0.709 | 0.525 | 0 | 0.903 |
| role=local layout=copy piece=1M | unbounded | 42.00 | 87.08 | 0 | 175.0 | 35.24 | 170.5 | 0.645 | 0.504 | 0 | 0.874 |
| role=local layout=2+1 piece=1M | unbounded | 24.75 | 84.83 | 0 | 275.0 | 24.95 | 104.9 | 0.571 | 0.504 | 0 | 0.902 |
| role=local layout=4+2 piece=1M | unbounded | 15.00 | 90.84 | 0 | 557.9 | 200.0 | 253.1 | 0.655 | 0.668 | 0 | 0.916 |

