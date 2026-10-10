### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.32 KiB a sync · sync p50 28.60 µs |
| fdatasync of a clean file | p50 24.05 µs, p99 38.64 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8241 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8260 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.06 µs, p99 33.26 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.54 | 338.5 | - | - | - |
| fold | 13.93 | 280.4 | - | - | - |
| crc+fold | 7.76 | 503.3 | - | - | - |
| copy | 6.61 | 591.4 | - | - | - |
| decode-21-data | 5.08 | 769.1 | - | - | - |
| decode-21-parity | 5.06 | 771.3 | - | - | - |
| decode-42-data | 3.01 | 1299 | - | - | - |
| decode-42-parity | 3.07 | 1272 | - | - | - |
| pipeline-copy | 3.20 | 1221 | - | - | - |
| pipeline-21 | 2.22 | 1758 | - | - | - |
| pipeline-42 | 1.30 | 3013 | - | - | - |
| summary-42 | 841.8 | 27.84 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 4 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 19.50 | 49.62 | 73.04 | 72.00 | 256.7 | 0 | 0 |
| layout=4+2 | chunk | 5.40 | 183.3 | 250.1 | 4211 | 16704 | 0 | 0 |
| layout=2+1 | unit | 21.90 | 45.94 | 65.29 | 69.63 | 129.2 | 0 | 0 |
| layout=2+1 | chunk | 7.70 | 125.0 | 183.3 | 4157 | 8253 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=4M | idle | 53.25 | 0 | 50.00 | 2/2 | 0 | 108.2 | 108.9 | 0.162 | 116.4 | 47.74 | 0.783 |
| layout=4+2 piece=256K | idle | 41.55 | 0 | 40.00 | 0/0 | 0 | 125.7 | 126.6 | 0.100 | 116.2 | 48.04 | 0.782 |
| layout=4+2 piece=1M | unbounded | 79.98 | 0 | 78.00 | 0/0 | 0 | 495.6 | 605.9 | 0.203 | 116.6 | 47.44 | 0.825 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.87 | 0 | 16.00 | 0/0 | 0 | 90.62 | 108.6 | 0.099 | 117.9 | 48.54 | 0.455 |
| layout=4+2 piece=1M | idle | 46.85 | 0 | 45.00 | 0/0 | 0 | 110.1 | 109.6 | 0.174 | 116.6 | 47.74 | 0.786 |
| layout=4+2 piece=1M | fixed-40 | 32.88 | 0.822 | 31.00 | 0/0 | 0 | 181.8 | 141.5 | 0.092 | 116.5 | 47.52 | 0.541 |
| layout=4+2 piece=1M | fixed-20 | 19.42 | 0.971 | 18.00 | 0/0 | 0 | 102.1 | 99.34 | 0.075 | 116.2 | 47.16 | 0.432 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 106.5 | 86.10 | 0.103 | 119.2 | 47.54 | 0.385 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 85.18 | 107.0 | 0.072 | 116.1 | 48.10 | 0.393 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 83.76 | 114.7 | 0.084 | 0 | 0 | 0.364 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 17.00 | 85.08 | 0 | 425.0 | 750.5 | 832.8 | 15.03 | 0.183 | 0 | 0.861 |
| role=local layout=4+2 piece=1M | idle | 8.30 | 41.94 | 0 | 416.6 | 151.0 | 265.2 | 14.78 | 0.179 | 0 | 0.795 |
| role=local layout=2+1 piece=1M | unbounded | 24.80 | 74.07 | 0 | 300.0 | 562.2 | 565.2 | 14.77 | 0.174 | 0 | 0.848 |
| role=local layout=2+1 piece=1M | idle | 11.40 | 34.03 | 0 | 308.3 | 115.8 | 97.25 | 14.72 | 0.081 | 0 | 0.792 |
| role=local layout=copy piece=1M | unbounded | 33.80 | 67.87 | 0 | 216.7 | 361.9 | 575.2 | 14.75 | 0.152 | 0 | 0.859 |
| role=local layout=copy piece=1M | idle | 14.85 | 29.68 | 0 | 241.6 | 76.25 | 96.99 | 14.77 | 0.197 | 0 | 0.798 |
| role=dest layout=4+2 piece=1M | unbounded | 58.80 | 58.86 | 0 | 125.1 | 397.6 | 360.6 | 14.77 | 0.245 | 0 | 0.801 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 19.30 | 19.32 | 0 | 199.9 | 152.2 | 81.66 | 14.77 | 0.210 | 0 | 0.726 |
| role=dest layout=4+2 piece=1M | idle | 22.35 | 22.37 | 0 | 166.6 | 76.97 | 83.82 | 14.70 | 0.207 | 0 | 0.785 |
| role=dest layout=4+2 piece=1M | fixed-80 | 29.00 | 29.03 | 0.363 | 133.4 | 312.8 | 340.1 | 14.90 | 0.252 | 0 | 0.799 |
| role=dest layout=4+2 piece=1M | fixed-40 | 22.25 | 22.27 | 0.557 | 175.0 | 169.8 | 216.0 | 14.73 | 0.223 | 0 | 0.693 |
| role=dest layout=4+2 piece=1M | fixed-20 | 16.60 | 16.62 | 0.831 | 233.4 | 161.4 | 118.0 | 15.21 | 0.204 | 0 | 0.609 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.75 | 9.76 | 0.976 | 408.3 | 142.7 | 140.8 | 14.78 | 0.201 | 0 | 0.504 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 48.29 | 61.90 | 14.69 | 0.104 | 0 | 0.340 |

