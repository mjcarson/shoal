### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.64 KiB a sync · sync p50 28.24 µs |
| fdatasync of a clean file | p50 24.07 µs, p99 37.83 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8243 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.15 µs, p99 33.20 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 338.0 | - | - | - |
| fold | 13.93 | 280.4 | - | - | - |
| crc+fold | 7.76 | 503.5 | - | - | - |
| copy | 6.64 | 588.4 | - | - | - |
| decode-21-data | 5.16 | 757.2 | - | - | - |
| decode-21-parity | 5.15 | 758.4 | - | - | - |
| decode-42-data | 3.09 | 1266 | - | - | - |
| decode-42-parity | 3.09 | 1264 | - | - | - |
| pipeline-copy | 3.21 | 1217 | - | - | - |
| pipeline-21 | 2.23 | 1754 | - | - | - |
| pipeline-42 | 1.31 | 2987 | - | - | - |
| summary-42 | 840.0 | 27.90 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 2 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 20.80 | 48.75 | 71.66 | 68.33 | 258.2 | 0 | 0 |
| layout=4+2 | chunk | 5.10 | 183.3 | 625.0 | 4124 | 16541 | 0 | 0 |
| layout=2+1 | unit | 22.30 | 45.41 | 65.35 | 68.30 | 128.0 | 0 | 0 |
| layout=2+1 | chunk | 7.60 | 133.3 | 216.6 | 4170 | 8308 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=4M | idle | 53.25 | 0 | 49.00 | 0/0 | 0 | 121.4 | 131.9 | 0.206 | 117.3 | 49.84 | 0.790 |
| layout=4+2 piece=256K | idle | 42.25 | 0 | 41.00 | 0/0 | 0 | 174.3 | 141.9 | 0.168 | 115.8 | 47.54 | 0.793 |
| layout=4+2 piece=1M | unbounded | 80.68 | 0 | 78.00 | 2/2 | 0 | 499.5 | 476.2 | 0.187 | 115.3 | 47.34 | 0.831 |
| layout=4+2 piece=1M | idle-ceil-20 | 17.62 | 0 | 16.00 | 0/0 | 0 | 97.21 | 105.0 | 0.091 | 116.6 | 47.21 | 0.504 |
| layout=4+2 piece=1M | idle | 45.29 | 0 | 46.00 | 0/0 | 0 | 94.33 | 95.09 | 0.192 | 116.2 | 47.70 | 0.775 |
| layout=4+2 piece=1M | fixed-40 | 30.93 | 0.773 | 29.00 | 0/0 | 0 | 147.8 | 141.3 | 0.091 | 116.4 | 47.95 | 0.552 |
| layout=4+2 piece=1M | fixed-20 | 19.42 | 0.971 | 18.00 | 0/0 | 0 | 151.1 | 155.1 | 0.101 | 116.2 | 47.91 | 0.448 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 135.6 | 106.1 | 0.071 | 115.6 | 47.81 | 0.435 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 88.90 | 96.62 | 0.251 | 117.8 | 48.60 | 0.402 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 76.90 | 79.76 | 0.069 | 0 | 0 | 0.387 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 15.80 | 79.08 | 0 | 466.7 | 687.7 | 746.7 | 14.83 | 0.178 | 0 | 0.849 |
| role=local layout=4+2 piece=1M | idle | 7.70 | 38.94 | 0 | 516.6 | 91.82 | 135.9 | 14.76 | 0.075 | 0 | 0.800 |
| role=local layout=2+1 piece=1M | unbounded | 25.60 | 77.13 | 0 | 283.4 | 640.1 | 624.4 | 14.73 | 0.176 | 0 | 0.853 |
| role=local layout=2+1 piece=1M | idle | 10.80 | 32.78 | 0 | 358.3 | 91.97 | 83.60 | 14.74 | 0.080 | 0 | 0.794 |
| role=local layout=copy piece=1M | unbounded | 32.80 | 65.86 | 0 | 225.1 | 604.5 | 540.0 | 14.86 | 0.176 | 0 | 0.846 |
| role=local layout=copy piece=1M | idle | 14.10 | 28.33 | 0 | 266.7 | 85.65 | 102.8 | 14.73 | 0.084 | 0 | 0.794 |
| role=dest layout=4+2 piece=1M | unbounded | 57.40 | 57.46 | 0 | 125.1 | 427.1 | 433.9 | 14.71 | 0.238 | 0 | 0.806 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 18.60 | 18.62 | 0 | 200.1 | 87.11 | 79.77 | 14.74 | 0.215 | 0 | 0.713 |
| role=dest layout=4+2 piece=1M | idle | 21.80 | 21.82 | 0 | 166.7 | 133.1 | 115.8 | 16.72 | 0.230 | 0 | 0.793 |
| role=dest layout=4+2 piece=1M | fixed-80 | 27.15 | 27.18 | 0.340 | 141.6 | 293.3 | 315.2 | 14.73 | 0.225 | 0 | 0.791 |
| role=dest layout=4+2 piece=1M | fixed-40 | 21.65 | 21.67 | 0.542 | 175.0 | 190.7 | 187.3 | 14.82 | 0.253 | 0 | 0.696 |
| role=dest layout=4+2 piece=1M | fixed-20 | 15.95 | 15.97 | 0.798 | 241.7 | 129.2 | 159.8 | 14.74 | 0.176 | 0 | 0.627 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.65 | 9.66 | 0.966 | 408.3 | 103.0 | 113.7 | 14.78 | 0.177 | 0 | 0.535 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 73.46 | 137.9 | 14.71 | 0.086 | 0 | 0.368 |

