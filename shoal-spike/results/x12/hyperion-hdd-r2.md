### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.64 KiB a sync · sync p50 28.35 µs |
| fdatasync of a clean file | p50 23.77 µs, p99 36.65 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.07 µs, p99 33.05 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 337.9 | - | - | - |
| fold | 14.00 | 279.0 | - | - | - |
| crc+fold | 7.75 | 503.9 | - | - | - |
| copy | 6.87 | 568.8 | - | - | - |
| decode-21-data | 5.26 | 742.7 | - | - | - |
| decode-21-parity | 5.27 | 741.7 | - | - | - |
| decode-42-data | 3.08 | 1270 | - | - | - |
| decode-42-parity | 3.07 | 1274 | - | - | - |
| pipeline-copy | 3.26 | 1200 | - | - | - |
| pipeline-21 | 2.25 | 1739 | - | - | - |
| pipeline-42 | 1.30 | 2994 | - | - | - |
| summary-42 | 833.0 | 28.13 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 2 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 20.50 | 48.21 | 77.68 | 68.02 | 257.2 | 0 | 0 |
| layout=4+2 | chunk | 5.00 | 183.4 | 633.5 | 4199 | 16564 | 0 | 0 |
| layout=2+1 | unit | 22.30 | 45.96 | 67.76 | 68.30 | 128.6 | 0 | 0 |
| layout=2+1 | chunk | 7.60 | 133.3 | 183.3 | 4158 | 8200 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=4M | idle | 54.25 | 0 | 50.00 | 0/0 | 0 | 119.1 | 162.1 | 0.146 | 115.8 | 48.69 | 0.782 |
| layout=4+2 piece=256K | idle | 41.45 | 0 | 39.00 | 0/0 | 0 | 111.2 | 168.2 | 0.079 | 116.0 | 48.26 | 0.795 |
| layout=4+2 piece=1M | unbounded | 85.03 | 0 | 79.00 | 2/2 | 0 | 579.7 | 817.5 | 0.188 | 116.8 | 48.25 | 0.837 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.32 | 0 | 16.00 | 0/0 | 0 | 98.47 | 102.9 | 0.089 | 117.8 | 48.76 | 0.522 |
| layout=4+2 piece=1M | idle | 44.54 | 0 | 46.00 | 0/0 | 0 | 114.9 | 107.9 | 0.123 | 115.6 | 48.35 | 0.784 |
| layout=4+2 piece=1M | fixed-40 | 32.53 | 0.813 | 31.00 | 0/0 | 0 | 147.7 | 152.2 | 0.144 | 116.1 | 48.53 | 0.544 |
| layout=4+2 piece=1M | fixed-20 | 19.42 | 0.971 | 18.00 | 0/0 | 0 | 122.1 | 108.6 | 0.055 | 118.8 | 48.67 | 0.461 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 107.3 | 89.99 | 0.056 | 116.9 | 48.60 | 0.441 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 100.1 | 108.2 | 0.090 | 125.4 | 48.36 | 0.405 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 82.59 | 82.89 | 0.058 | 0 | 0 | 0.395 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 16.20 | 81.13 | 0 | 441.7 | 742.0 | 710.1 | 14.86 | 0.221 | 0 | 0.852 |
| role=local layout=4+2 piece=1M | idle | 7.80 | 38.89 | 0 | 524.9 | 98.92 | 93.87 | 14.84 | 0.072 | 0 | 0.808 |
| role=local layout=2+1 piece=1M | unbounded | 24.80 | 74.87 | 0 | 300.0 | 497.2 | 680.6 | 14.86 | 0.191 | 0 | 0.855 |
| role=local layout=2+1 piece=1M | idle | 10.40 | 31.43 | 0 | 383.4 | 132.1 | 99.76 | 14.88 | 0.075 | 0 | 0.789 |
| role=local layout=copy piece=1M | unbounded | 32.80 | 65.66 | 0 | 225.0 | 520.6 | 537.0 | 14.97 | 0.173 | 0 | 0.862 |
| role=local layout=copy piece=1M | idle | 14.05 | 28.23 | 0 | 275.1 | 86.15 | 83.17 | 14.91 | 0.094 | 0 | 0.789 |
| role=dest layout=4+2 piece=1M | unbounded | 59.40 | 59.46 | 0 | 116.7 | 503.5 | 573.5 | 15.15 | 0.222 | 0 | 0.812 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 17.80 | 17.82 | 0 | 208.3 | 126.6 | 100.3 | 14.86 | 0.201 | 0 | 0.723 |
| role=dest layout=4+2 piece=1M | idle | 21.60 | 21.62 | 0 | 166.7 | 143.0 | 99.34 | 14.86 | 0.172 | 0 | 0.790 |
| role=dest layout=4+2 piece=1M | fixed-80 | 28.45 | 28.48 | 0.356 | 133.3 | 333.9 | 363.5 | 14.90 | 0.238 | 0 | 0.796 |
| role=dest layout=4+2 piece=1M | fixed-40 | 22.20 | 22.22 | 0.556 | 175.1 | 154.8 | 154.8 | 14.85 | 0.198 | 0 | 0.696 |
| role=dest layout=4+2 piece=1M | fixed-20 | 16.10 | 16.12 | 0.806 | 241.7 | 146.1 | 163.6 | 14.90 | 0.165 | 0 | 0.633 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.70 | 9.71 | 0.971 | 406.0 | 125.0 | 135.0 | 14.88 | 0.074 | 0 | 0.548 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 91.62 | 77.38 | 14.89 | 0.098 | 0 | 0.378 |

