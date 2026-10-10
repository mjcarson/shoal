### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1121 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 2727 µs |
| fdatasync of a clean file | p50 150.5 µs, p99 174.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 4170 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 4159 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.27 µs, p99 33.78 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.57 | 337.6 | - | - | - |
| fold | 13.78 | 283.5 | - | - | - |
| crc+fold | 7.55 | 517.3 | - | - | - |
| copy | 7.02 | 556.8 | - | - | - |
| decode-21-data | 5.23 | 746.3 | - | - | - |
| decode-21-parity | 5.20 | 751.8 | - | - | - |
| decode-42-data | 3.08 | 1267 | - | - | - |
| decode-42-parity | 3.08 | 1269 | - | - | - |
| pipeline-copy | 3.29 | 1188 | - | - | - |
| pipeline-21 | 2.26 | 1728 | - | - | - |
| pipeline-42 | 1.31 | 2985 | - | - | - |
| summary-42 | 831.2 | 28.20 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 2 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 512.3 | 1.88 | 4.77 | 68.01 | 256.0 | 1.00 | 0 |
| layout=4+2 | chunk | 31.40 | 31.44 | 35.09 | 4116 | 16465 | 2.04 | 0 |
| layout=2+1 | unit | 576.2 | 1.67 | 4.54 | 68.00 | 128.0 | 1.00 | 0 |
| layout=2+1 | chunk | 48.80 | 20.21 | 23.28 | 4110 | 8217 | 2.01 | 0 |

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | cpu | 6155 | 0 | 5129 | 1474/1474 | 0 | 6.19 | 37.91 | 0.290 | 97.69 | 38.72 | 0.341 |
| layout=4+2 piece=4M | idle | 446.2 | 0 | 427.0 | 4/4 | 0 | 4.83 | 11.30 | 0.255 | 119.3 | 50.00 | 0.743 |
| layout=4+2 piece=256K | idle | 320.0 | 0 | 305.0 | 4/4 | 0 | 2.20 | 9.00 | 0.237 | 116.9 | 48.91 | 0.699 |
| layout=4+2 piece=1M | unbounded | 546.2 | 0 | 522.0 | 6/6 | 0 | 6.31 | 28.31 | 0.277 | 120.7 | 50.20 | 0.779 |
| layout=4+2 piece=1M | idle-ceil-200 | 181.8 | 0 | 174.0 | 2/2 | 0 | 2.01 | 8.33 | 0.223 | 117.6 | 49.24 | 0.521 |
| layout=4+2 piece=1M | idle | 390.6 | 0 | 372.0 | 4/4 | 0 | 2.09 | 9.48 | 0.241 | 117.4 | 49.12 | 0.718 |
| layout=4+2 piece=1M | fixed-400 | 280.8 | 0.702 | 268.0 | 2/2 | 0 | 5.36 | 13.71 | 0.239 | 118.7 | 49.71 | 0.585 |
| layout=4+2 piece=1M | fixed-200 | 183.6 | 0.918 | 175.0 | 2/2 | 0 | 5.12 | 11.57 | 0.242 | 119.9 | 50.35 | 0.521 |
| layout=4+2 piece=1M | fixed-100 | 99.70 | 0.997 | 95.00 | 2/2 | 0 | 4.84 | 11.60 | 0.205 | 119.1 | 49.88 | 0.464 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 4.43 | 11.26 | 0.138 | 119.4 | 49.62 | 0.425 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 1.80 | 8.55 | 0.059 | 0 | 0 | 0.388 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 113.2 | 566.2 | 0 | 141.2 | 18.85 | 119.1 | 78.50 | 0.869 | 0 | 0.673 |
| role=local layout=4+2 piece=1M | idle | 69.20 | 346.0 | 0 | 55.12 | 3.05 | 11.31 | 6.83 | 0.256 | 0 | 0.682 |
| role=local layout=2+1 piece=1M | unbounded | 176.0 | 528.7 | 0 | 94.50 | 16.61 | 94.75 | 60.07 | 0.489 | 0 | 0.606 |
| role=local layout=2+1 piece=1M | idle | 71.20 | 213.6 | 0 | 36.78 | 4.49 | 27.62 | 17.85 | 0.234 | 0 | 0.558 |
| role=local layout=copy piece=1M | unbounded | 221.8 | 444.0 | 0 | 72.61 | 14.14 | 88.19 | 63.35 | 0.436 | 0 | 0.601 |
| role=local layout=copy piece=1M | idle | 86.40 | 173.1 | 0 | 27.33 | 4.49 | 26.87 | 17.34 | 0.230 | 0 | 0.522 |
| role=dest layout=4+2 piece=1M | unbounded | 274.8 | 275.1 | 0 | 59.55 | 23.20 | 68.14 | 49.79 | 0.590 | 0 | 0.520 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 91.60 | 91.69 | 0 | 23.65 | 4.70 | 29.55 | 18.60 | 0.262 | 0 | 0.444 |
| role=dest layout=4+2 piece=1M | idle | 88.40 | 88.49 | 0 | 22.16 | 5.24 | 29.86 | 19.29 | 0.259 | 0 | 0.440 |
| role=dest layout=4+2 piece=1M | fixed-800 | 148.2 | 148.3 | 0.185 | 27.23 | 9.18 | 37.31 | 23.42 | 0.292 | 0 | 0.450 |
| role=dest layout=4+2 piece=1M | fixed-400 | 144.2 | 144.3 | 0.361 | 22.28 | 8.88 | 35.85 | 22.76 | 0.292 | 0 | 0.455 |
| role=dest layout=4+2 piece=1M | fixed-200 | 125.5 | 125.7 | 0.628 | 26.62 | 8.63 | 36.97 | 24.53 | 0.282 | 0 | 0.452 |
| role=dest layout=4+2 piece=1M | fixed-100 | 90.00 | 90.09 | 0.901 | 40.99 | 7.47 | 31.95 | 20.27 | 0.272 | 0 | 0.439 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 2.27 | 10.15 | 6.77 | 0.065 | 0 | 0.385 |

