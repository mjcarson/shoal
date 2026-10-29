### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1118 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 2785 µs |
| fdatasync of a clean file | p50 112.7 µs, p99 125.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 4117 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 4180 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.29 µs, p99 33.73 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 337.8 | - | - | - |
| fold | 13.81 | 282.9 | - | - | - |
| crc+fold | 7.57 | 516.0 | - | - | - |
| copy | 6.53 | 598.2 | - | - | - |
| decode-21-data | 5.01 | 779.1 | - | - | - |
| decode-21-parity | 5.06 | 771.9 | - | - | - |
| decode-42-data | 3.00 | 1302 | - | - | - |
| decode-42-parity | 3.03 | 1291 | - | - | - |
| pipeline-copy | 2.81 | 1388 | - | - | - |
| pipeline-21 | 2.18 | 1795 | - | - | - |
| pipeline-42 | 1.29 | 3020 | - | - | - |
| summary-42 | 826.5 | 28.36 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 4 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 512.9 | 1.88 | 4.74 | 68.34 | 256.0 | 1.00 | 0 |
| layout=4+2 | chunk | 31.40 | 31.47 | 34.54 | 4115 | 16478 | 2.01 | 0 |
| layout=2+1 | unit | 574.5 | 1.68 | 4.54 | 68.01 | 128.0 | 1.00 | 0 |
| layout=2+1 | chunk | 48.70 | 20.25 | 23.33 | 4110 | 8217 | 2.01 | 0 |

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | cpu | 6154 | 0 | 5128 | 1474/1474 | 0 | 5.56 | 32.77 | 0.291 | 97.59 | 38.86 | 0.345 |
| layout=4+2 piece=4M | idle | 446.2 | 0 | 427.0 | 6/6 | 0 | 4.81 | 11.01 | 0.253 | 118.9 | 49.04 | 0.752 |
| layout=4+2 piece=256K | idle | 318.0 | 0 | 304.0 | 2/2 | 0 | 2.29 | 8.59 | 0.235 | 116.8 | 49.20 | 0.698 |
| layout=4+2 piece=1M | unbounded | 546.3 | 0 | 516.0 | 6/6 | 0 | 6.20 | 28.70 | 0.270 | 120.6 | 50.90 | 0.786 |
| layout=4+2 piece=1M | idle-ceil-200 | 182.1 | 0 | 174.0 | 2/2 | 0 | 2.06 | 8.29 | 0.218 | 117.9 | 49.68 | 0.519 |
| layout=4+2 piece=1M | idle | 387.1 | 0 | 370.0 | 4/4 | 0 | 2.16 | 8.39 | 0.242 | 117.1 | 49.39 | 0.717 |
| layout=4+2 piece=1M | fixed-400 | 280.7 | 0.702 | 268.0 | 2/2 | 0 | 5.39 | 15.24 | 0.242 | 119.4 | 51.01 | 0.594 |
| layout=4+2 piece=1M | fixed-200 | 184.2 | 0.921 | 176.0 | 2/2 | 0 | 5.14 | 12.83 | 0.225 | 119.0 | 50.12 | 0.522 |
| layout=4+2 piece=1M | fixed-100 | 99.70 | 0.997 | 95.00 | 2/2 | 0 | 4.90 | 11.27 | 0.196 | 119.1 | 50.03 | 0.461 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 4.40 | 11.25 | 0.121 | 119.6 | 50.19 | 0.422 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 2.28 | 9.30 | 0.064 | 0 | 0 | 0.394 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 113.6 | 568.6 | 0 | 140.6 | 18.38 | 116.4 | 74.51 | 0.664 | 0 | 0.673 |
| role=local layout=4+2 piece=1M | idle | 70.80 | 354.8 | 0 | 55.38 | 2.40 | 9.75 | 6.44 | 0.256 | 0 | 0.694 |
| role=local layout=2+1 piece=1M | unbounded | 176.0 | 527.9 | 0 | 94.08 | 17.03 | 93.68 | 60.04 | 0.454 | 0 | 0.607 |
| role=local layout=2+1 piece=1M | idle | 61.00 | 182.8 | 0 | 38.38 | 4.74 | 27.07 | 18.15 | 0.238 | 0 | 0.526 |
| role=local layout=copy piece=1M | unbounded | 223.0 | 446.2 | 0 | 72.51 | 14.08 | 87.18 | 61.59 | 0.415 | 0 | 0.603 |
| role=local layout=copy piece=1M | idle | 81.35 | 162.9 | 0 | 26.93 | 4.49 | 27.85 | 17.32 | 0.225 | 0 | 0.509 |
| role=dest layout=4+2 piece=1M | unbounded | 267.6 | 267.9 | 0 | 60.15 | 23.36 | 72.90 | 51.50 | 0.532 | 0 | 0.512 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 78.70 | 78.78 | 0 | 28.91 | 4.88 | 28.54 | 18.09 | 0.265 | 0 | 0.439 |
| role=dest layout=4+2 piece=1M | idle | 84.40 | 84.48 | 0 | 22.19 | 5.43 | 29.71 | 19.47 | 0.257 | 0 | 0.447 |
| role=dest layout=4+2 piece=1M | fixed-800 | 146.3 | 146.5 | 0.183 | 27.32 | 9.28 | 38.61 | 24.52 | 0.292 | 0 | 0.453 |
| role=dest layout=4+2 piece=1M | fixed-400 | 143.2 | 143.3 | 0.358 | 23.59 | 8.99 | 37.79 | 23.97 | 0.290 | 0 | 0.456 |
| role=dest layout=4+2 piece=1M | fixed-200 | 124.0 | 124.2 | 0.621 | 27.09 | 8.49 | 36.25 | 24.11 | 0.283 | 0 | 0.457 |
| role=dest layout=4+2 piece=1M | fixed-100 | 90.80 | 90.89 | 0.909 | 40.86 | 7.19 | 32.40 | 20.54 | 0.271 | 0 | 0.445 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 2.14 | 8.36 | 5.25 | 0.055 | 0 | 0.384 |

