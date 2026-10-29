### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.60 KiB a sync · sync p50 29.32 µs |
| fdatasync of a clean file | p50 24.41 µs, p99 38.37 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8260 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.65 µs, p99 34.00 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.14 | 350.7 | - | - | - |
| fold | 13.72 | 284.7 | - | - | - |
| crc+fold | 7.70 | 507.2 | - | - | - |
| copy | 6.59 | 592.7 | - | - | - |
| decode-21-data | 5.17 | 755.8 | - | - | - |
| decode-21-parity | 5.15 | 759.2 | - | - | - |
| decode-42-data | 3.09 | 1263 | - | - | - |
| decode-42-parity | 3.06 | 1276 | - | - | - |
| pipeline-copy | 3.14 | 1244 | - | - | - |
| pipeline-21 | 2.23 | 1752 | - | - | - |
| pipeline-42 | 1.31 | 2985 | - | - | - |
| summary-42 | 816.2 | 28.72 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 6.00 | 141.7 | 150.1 | 4617 | 10250 | 0 | 0 |
| layout=2+1 | unit | 25.50 | 38.21 | 60.47 | 72.00 | 139.3 | 0 | 0 |
| layout=4+2 | chunk | 4.50 | 175.0 | 183.4 | 4788 | 21525 | 0 | 0 |
| layout=4+2 | unit | 21.00 | 45.63 | 54.92 | 72.86 | 260.6 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 99.67 | 61.24 | 0.102 | 0 | 0 | 0.496 |
| layout=4+2 piece=1M | fixed-5 | 5.26 | 1.05 | 0 | 0/0 | 0 | 10.26 | 49.59 | 0.058 | 0 | 0 | 0.268 |
| layout=4+2 piece=1M | idle | 61.56 | 0 | 4.00 | 0/0 | 0 | 124.3 | 62.16 | 0.225 | 116.6 | 48.09 | 0.734 |
| layout=4+2 piece=1M | unbounded | 102.1 | 0 | 6.00 | 0/0 | 0 | 567.3 | 485.9 | 0.129 | 114.8 | 48.37 | 0.829 |
| layout=4+2 piece=256K | idle | 97.03 | 0 | 5.00 | 0/0 | 0 | 17.70 | 51.29 | 0.083 | 115.1 | 48.27 | 0.860 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 41.01 | 47.57 | 1.86 | 0.049 | 0 | 0.373 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.75 | 9.76 | 0.976 | 466.8 | 46.74 | 64.34 | 2.00 | 0.072 | 0 | 0.368 |
| role=dest layout=4+2 piece=1M | idle | 26.25 | 26.27 | 0 | 141.7 | 66.14 | 52.77 | 4.73 | 0.154 | 0 | 0.776 |
| role=dest layout=4+2 piece=1M | unbounded | 54.00 | 54.05 | 0 | 141.7 | 209.9 | 146.2 | 4.42 | 0.223 | 0 | 0.815 |
| role=local layout=copy piece=1M | unbounded | 54.00 | 111.1 | 0 | 134.2 | 232.0 | 385.4 | 2.18 | 0.220 | 0 | 0.857 |
| role=local layout=2+1 piece=1M | unbounded | 24.00 | 81.08 | 0 | 291.7 | 345.7 | 323.2 | 2.12 | 0.187 | 0 | 0.888 |
| role=local layout=4+2 piece=1M | unbounded | 18.00 | 84.08 | 0 | 391.6 | 447.3 | 336.1 | 4.56 | 1.67 | 0 | 0.883 |

