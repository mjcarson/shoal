### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.07 µs |
| fdatasync of a clean file | p50 24.16 µs, p99 38.31 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8259 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.29 µs, p99 33.72 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 338.0 | - | - | - |
| fold | 13.95 | 280.0 | - | - | - |
| crc+fold | 7.76 | 503.6 | - | - | - |
| copy | 6.79 | 575.7 | - | - | - |
| decode-21-data | 5.23 | 747.3 | - | - | - |
| decode-21-parity | 5.18 | 754.0 | - | - | - |
| decode-42-data | 3.02 | 1294 | - | - | - |
| decode-42-parity | 3.01 | 1297 | - | - | - |
| pipeline-copy | 3.17 | 1231 | - | - | - |
| pipeline-21 | 2.25 | 1734 | - | - | - |
| pipeline-42 | 1.30 | 3013 | - | - | - |
| summary-42 | 834.9 | 28.07 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 7.80 | 125.0 | 175.1 | 4157 | 8253 | 0 | 0 |
| layout=2+1 | unit | 22.50 | 45.05 | 64.76 | 68.30 | 128.3 | 0 | 0 |
| layout=4+2 | chunk | 5.30 | 175.0 | 258.3 | 4181 | 16864 | 0 | 0 |
| layout=4+2 | unit | 19.80 | 49.71 | 77.29 | 70.00 | 258.6 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 112.2 | 117.6 | 0.089 | 0 | 0 | 0.404 |
| layout=4+2 piece=1M | fixed-5 | 5.00 | 1.00 | 4.00 | 0/0 | 0 | 99.83 | 118.4 | 0.084 | 124.5 | 50.70 | 0.415 |
| layout=4+2 piece=1M | fixed-10 | 10.01 | 1.00 | 9.00 | 0/0 | 0 | 138.6 | 135.2 | 0.068 | 115.8 | 47.67 | 0.428 |
| layout=4+2 piece=1M | fixed-20 | 19.32 | 0.966 | 18.00 | 0/0 | 0 | 133.9 | 135.0 | 0.083 | 117.8 | 48.74 | 0.492 |
| layout=4+2 piece=1M | fixed-40 | 32.53 | 0.813 | 31.00 | 0/0 | 0 | 158.1 | 165.7 | 0.166 | 118.7 | 48.72 | 0.547 |
| layout=4+2 piece=1M | idle | 47.55 | 0 | 45.00 | 0/0 | 0 | 124.7 | 112.0 | 0.076 | 115.0 | 47.50 | 0.788 |
| layout=4+2 piece=1M | idle-ceil-20 | 18.02 | 0 | 16.00 | 2/2 | 0 | 81.40 | 141.7 | 0.075 | 120.2 | 47.67 | 0.421 |
| layout=4+2 piece=1M | unbounded | 85.08 | 0 | 81.00 | 2/2 | 0 | 643.9 | 577.5 | 0.179 | 116.1 | 47.66 | 0.841 |
| layout=4+2 piece=256K | idle | 50.25 | 0 | 47.00 | 2/2 | 0 | 100.4 | 110.7 | 0.173 | 116.2 | 48.09 | 0.807 |
| layout=4+2 piece=4M | idle | 57.86 | 0 | 53.00 | 0/0 | 0 | 131.0 | 109.4 | 0.202 | 116.2 | 47.96 | 0.811 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 68.34 | 88.78 | 14.72 | 0.071 | 0 | 0.363 |
| role=dest layout=4+2 piece=1M | fixed-10 | 9.65 | 9.66 | 0.966 | 400.1 | 91.94 | 138.2 | 14.76 | 0.171 | 0 | 0.522 |
| role=dest layout=4+2 piece=1M | fixed-20 | 16.80 | 16.82 | 0.841 | 225.1 | 136.3 | 143.7 | 14.80 | 0.233 | 0 | 0.614 |
| role=dest layout=4+2 piece=1M | fixed-40 | 21.95 | 21.97 | 0.549 | 175.0 | 169.2 | 204.8 | 14.74 | 0.206 | 0 | 0.663 |
| role=dest layout=4+2 piece=1M | fixed-80 | 27.95 | 27.98 | 0.350 | 133.4 | 269.9 | 414.9 | 14.72 | 0.234 | 0 | 0.776 |
| role=dest layout=4+2 piece=1M | idle | 20.30 | 20.32 | 0 | 175.0 | 95.31 | 124.8 | 14.74 | 0.162 | 0 | 0.777 |
| role=dest layout=4+2 piece=1M | idle-ceil-40 | 18.00 | 18.02 | 0 | 191.8 | 107.7 | 135.9 | 14.79 | 0.206 | 0 | 0.703 |
| role=dest layout=4+2 piece=1M | unbounded | 58.40 | 58.46 | 0 | 125.1 | 522.5 | 588.5 | 15.05 | 0.252 | 0 | 0.799 |
| role=local layout=copy piece=1M | idle | 13.80 | 27.63 | 0 | 258.3 | 106.4 | 101.9 | 14.80 | 0.081 | 0 | 0.771 |
| role=local layout=copy piece=1M | unbounded | 32.60 | 65.26 | 0 | 233.4 | 450.2 | 475.9 | 14.76 | 0.248 | 0 | 0.842 |
| role=local layout=2+1 piece=1M | idle | 10.60 | 31.83 | 0 | 366.6 | 99.22 | 123.1 | 14.70 | 0.168 | 0 | 0.779 |
| role=local layout=2+1 piece=1M | unbounded | 24.70 | 74.57 | 0 | 300.1 | 514.2 | 784.3 | 14.81 | 0.201 | 0 | 0.846 |
| role=local layout=4+2 piece=1M | idle | 7.85 | 39.79 | 0 | 500.2 | 85.27 | 110.3 | 14.75 | 0.164 | 0 | 0.780 |
| role=local layout=4+2 piece=1M | unbounded | 16.00 | 80.68 | 0 | 466.7 | 774.2 | 731.3 | 14.72 | 0.232 | 0 | 0.844 |

