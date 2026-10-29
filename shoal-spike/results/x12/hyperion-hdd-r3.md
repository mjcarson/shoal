### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.16 µs |
| fdatasync of a clean file | p50 23.71 µs, p99 44.94 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.12 µs, p99 32.92 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 337.9 | - | - | - |
| fold | 14.08 | 277.5 | - | - | - |
| crc+fold | 7.75 | 503.7 | - | - | - |
| copy | 6.72 | 581.2 | - | - | - |
| decode-21-data | 5.12 | 763.4 | - | - | - |
| decode-21-parity | 5.11 | 764.8 | - | - | - |
| decode-42-data | 3.03 | 1289 | - | - | - |
| decode-42-parity | 3.03 | 1289 | - | - | - |
| pipeline-copy | 3.21 | 1215 | - | - | - |
| pipeline-21 | 2.23 | 1750 | - | - | - |
| pipeline-42 | 1.29 | 3017 | - | - | - |
| summary-42 | 830.2 | 28.23 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 3 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 7.70 | 125.0 | 175.0 | 4211 | 8413 | 0 | 0 |
| layout=2+1 | unit | 21.40 | 46.63 | 68.42 | 71.46 | 128.9 | 0 | 0 |
| layout=4+2 | chunk | 5.40 | 175.0 | 241.7 | 4218 | 16912 | 0 | 0 |
| layout=4+2 | unit | 19.40 | 51.03 | 84.16 | 68.35 | 256.0 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 81.63 | 76.29 | 0.102 | 0 | 0 | 0.394 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 85.83 | 102.9 | 0.063 | 118.4 | 47.80 | 0.425 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 90.50 | 104.0 | 0.055 | 115.2 | 47.94 | 0.421 |
| layout=4+2 piece=1M | fixed-20 | 19.52 | 0.976 | 18.00 | 0/0 | 0 | 110.2 | 140.9 | 0.093 | 118.2 | 48.13 | 0.461 |
| layout=4+2 piece=1M | fixed-40 | 32.73 | 0.818 | 31.00 | 0/0 | 0 | 165.4 | 195.8 | 0.071 | 116.8 | 47.98 | 0.546 |
| layout=4+2 piece=1M | idle | 49.05 | 0 | 45.00 | 2/2 | 0 | 107.7 | 132.0 | 0.209 | 115.6 | 47.78 | 0.784 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.37 | 0 | 16.00 | 2/2 | 0 | 73.14 | 81.76 | 0.113 | 121.0 | 48.71 | 0.496 |
| layout=4+2 piece=1M | unbounded | 85.23 | 0 | 82.00 | 0/0 | 0 | 770.0 | 580.0 | 0.201 | 115.4 | 48.28 | 0.837 |
| layout=4+2 piece=256K | idle | 43.00 | 0 | 39.00 | 0/0 | 0 | 95.21 | 113.3 | 0.086 | 116.5 | 47.74 | 0.791 |
| layout=4+2 piece=4M | idle | 51.25 | 0 | 48.00 | 0/0 | 0 | 117.2 | 122.7 | 0.228 | 115.3 | 47.70 | 0.780 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 72.75 | 74.60 | 14.77 | 0.081 | 0 | 0.349 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.90 | 9.91 | 0.991 | 400.0 | 85.49 | 104.5 | 14.86 | 0.083 | 0 | 0.506 |
| role=dest layout=4+2 piece=1M | fixed-20 | 16.55 | 16.57 | 0.828 | 233.4 | 135.0 | 146.8 | 14.86 | 0.201 | 0 | 0.604 |
| role=dest layout=4+2 piece=1M | fixed-40 | 22.90 | 22.92 | 0.573 | 175.0 | 138.7 | 193.2 | 14.88 | 0.214 | 0 | 0.680 |
| role=dest layout=4+2 piece=1M | fixed-80 | 30.30 | 30.33 | 0.379 | 125.0 | 330.4 | 352.0 | 14.83 | 0.230 | 0 | 0.789 |
| role=dest layout=4+2 piece=1M | idle | 23.30 | 23.32 | 0 | 141.7 | 80.00 | 80.04 | 14.90 | 0.258 | 0 | 0.778 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 20.45 | 20.47 | 0 | 183.3 | 67.36 | 88.08 | 15.61 | 0.216 | 0 | 0.703 |
| role=dest layout=4+2 piece=1M | unbounded | 63.55 | 63.61 | 0 | 108.4 | 596.7 | 626.9 | 14.87 | 0.242 | 0 | 0.810 |
| role=local layout=copy piece=1M | idle | 15.00 | 30.18 | 0 | 241.6 | 85.17 | 90.71 | 14.82 | 0.090 | 0 | 0.786 |
| role=local layout=copy piece=1M | unbounded | 35.00 | 70.27 | 0 | 216.7 | 674.1 | 786.1 | 14.90 | 0.080 | 0 | 0.844 |
| role=local layout=2+1 piece=1M | idle | 11.90 | 35.53 | 0 | 291.7 | 84.99 | 98.62 | 14.86 | 0.190 | 0 | 0.789 |
| role=local layout=2+1 piece=1M | unbounded | 26.00 | 77.68 | 0 | 283.5 | 606.2 | 482.2 | 14.88 | 0.214 | 0 | 0.855 |
| role=local layout=4+2 piece=1M | idle | 8.20 | 41.49 | 0 | 416.8 | 109.1 | 99.31 | 14.81 | 0.082 | 0 | 0.786 |
| role=local layout=4+2 piece=1M | unbounded | 17.60 | 87.49 | 0 | 416.7 | 810.8 | 1220 | 14.82 | 0.217 | 0 | 0.849 |

