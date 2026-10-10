### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.60 KiB a sync · sync p50 28.27 µs |
| fdatasync of a clean file | p50 23.84 µs, p99 44.55 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8263 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.00 µs, p99 32.84 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.14 | 350.8 | - | - | - |
| fold | 13.78 | 283.5 | - | - | - |
| crc+fold | 7.78 | 502.1 | - | - | - |
| copy | 6.75 | 578.7 | - | - | - |
| decode-21-data | 5.17 | 755.7 | - | - | - |
| decode-21-parity | 5.16 | 757.1 | - | - | - |
| decode-42-data | 3.06 | 1276 | - | - | - |
| decode-42-parity | 3.06 | 1276 | - | - | - |
| pipeline-copy | 3.20 | 1222 | - | - | - |
| pipeline-21 | 2.23 | 1752 | - | - | - |
| pipeline-42 | 1.30 | 2998 | - | - | - |
| summary-42 | 837.9 | 27.97 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 6.00 | 141.7 | 141.7 | 5730 | 10250 | 0 | 0 |
| layout=2+1 | unit | 24.00 | 41.51 | 59.59 | 72.25 | 144.0 | 0 | 0 |
| layout=4+2 | chunk | 4.50 | 166.7 | 191.7 | 4447 | 21867 | 0 | 0 |
| layout=4+2 | unit | 19.50 | 48.33 | 57.74 | 73.23 | 285.5 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 77.94 | 55.05 | 0.050 | 0 | 0 | 0.460 |
| layout=4+2 piece=1M | fixed-5 | 5.26 | 1.05 | 0 | 0/0 | 0 | 3.79 | 45.28 | 0.050 | 0 | 0 | 0.262 |
| layout=4+2 piece=1M | idle | 60.06 | 0 | 4.00 | 0/0 | 0 | 95.38 | 67.76 | 0.068 | 115.4 | 47.89 | 0.749 |
| layout=4+2 piece=1M | unbounded | 108.1 | 0 | 7.00 | 1/1 | 0 | 23.41 | 387.0 | 0.200 | 115.5 | 48.09 | 0.839 |
| layout=4+2 piece=256K | idle | 88.21 | 0 | 5.00 | 0/0 | 0 | 11.33 | 66.25 | 0.185 | 120.7 | 48.90 | 0.811 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs · **quick: not a measurement**

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 36.63 | 43.41 | 2.01 | 0.048 | 0 | 0.373 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.75 | 9.76 | 0.976 | 441.7 | 0.683 | 53.42 | 2.01 | 0.084 | 0 | 0.281 |
| role=dest layout=4+2 piece=1M | idle | 27.00 | 27.03 | 0 | 166.7 | 54.08 | 62.32 | 4.12 | 0.167 | 0 | 0.754 |
| role=dest layout=4+2 piece=1M | unbounded | 51.00 | 51.05 | 0 | 141.6 | 270.5 | 220.4 | 2.98 | 0.220 | 0 | 0.818 |
| role=local layout=copy piece=1M | unbounded | 48.00 | 102.1 | 0 | 183.4 | 440.6 | 165.2 | 1.99 | 0.150 | 0 | 0.845 |
| role=local layout=2+1 piece=1M | unbounded | 30.00 | 87.08 | 0 | 283.3 | 434.8 | 237.8 | 2.08 | 0.274 | 0 | 0.878 |
| role=local layout=4+2 piece=1M | unbounded | 18.00 | 93.09 | 0 | 316.7 | 595.0 | 394.4 | 2.02 | 0.181 | 0 | 0.852 |

