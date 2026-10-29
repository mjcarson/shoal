### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 858.8 µs |
| fdatasync of a clean file | p50 112.4 µs, p99 126.9 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2094 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2108 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.88 µs, p99 33.59 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs · **quick: not a measurement**

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.12 | 351.2 | - | - | - |
| fold | 13.77 | 283.6 | - | - | - |
| crc+fold | 7.71 | 506.7 | - | - | - |
| copy | 6.66 | 586.9 | - | - | - |
| decode-21-data | 5.17 | 755.3 | - | - | - |
| decode-21-parity | 5.12 | 762.6 | - | - | - |
| decode-42-data | 3.06 | 1278 | - | - | - |
| decode-42-parity | 3.05 | 1279 | - | - | - |
| pipeline-copy | 3.19 | 1223 | - | - | - |
| pipeline-21 | 2.25 | 1735 | - | - | - |
| pipeline-42 | 1.30 | 2995 | - | - | - |
| summary-42 | 771.0 | 30.40 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs · **quick: not a measurement**

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 48.00 | 20.21 | 23.05 | 4230 | 8328 | 2.06 | 0 |
| layout=2+1 | unit | 639.0 | 1.51 | 4.37 | 68.16 | 128.3 | 1.00 | 0 |
| layout=4+2 | chunk | 30.00 | 31.33 | 34.46 | 4307 | 17425 | 2.10 | 0 |
| layout=4+2 | unit | 564.0 | 1.70 | 4.50 | 68.18 | 256.7 | 1.00 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 2.7% to 90.8% of the device, 70 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs · **quick: not a measurement**

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 2.29 | 8.11 | 0.055 | 0 | 0 | 0.353 |
| layout=4+2 piece=1M | fixed-50 | 51.05 | 1.02 | 3.00 | 0/0 | 0 | 4.12 | 9.76 | 0.147 | 120.4 | 49.44 | 0.352 |
| layout=4+2 piece=1M | idle | 433.2 | 0 | 26.00 | 6/6 | 0 | 2.51 | 10.24 | 0.260 | 118.5 | 48.45 | 0.733 |
| layout=4+2 piece=1M | unbounded | 564.6 | 0 | 35.00 | 8/8 | 0 | 6.76 | 27.26 | 0.280 | 121.1 | 50.20 | 0.758 |
| layout=4+2 piece=256K | idle | 364.9 | 0 | 22.00 | 4/4 | 0 | 1.30 | 6.25 | 0.238 | 117.3 | 48.37 | 0.714 |
| layout=4+2 piece=1M | cpu | 6174 | 0 | 343.0 | 98/98 | 0 | 2.33 | 7.95 | 0.287 | 98.83 | 38.08 | 0.328 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 2.7% to 90.8% of the device, 70 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs · **quick: not a measurement**

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 1.36 | 5.63 | 3.99 | 0.062 | 0 | 0.334 |
| role=dest layout=4+2 piece=1M | fixed-100 | 96.00 | 96.10 | 0.961 | 40.66 | 4.16 | 11.26 | 6.33 | 0.271 | 0 | 0.429 |
| role=dest layout=4+2 piece=1M | idle | 210.0 | 210.2 | 0 | 17.74 | 3.67 | 14.11 | 8.63 | 0.295 | 0 | 0.569 |
| role=dest layout=4+2 piece=1M | unbounded | 444.8 | 445.2 | 0 | 38.24 | 15.89 | 30.09 | 23.84 | 0.560 | 0 | 0.696 |
| role=local layout=copy piece=1M | unbounded | 312.0 | 620.1 | 0 | 50.60 | 14.65 | 49.78 | 35.18 | 0.477 | 0 | 0.770 |
| role=local layout=2+1 piece=1M | unbounded | 240.8 | 724.5 | 0 | 65.54 | 10.63 | 60.73 | 39.77 | 0.565 | 0 | 0.788 |
| role=local layout=4+2 piece=1M | unbounded | 150.0 | 726.7 | 0 | 107.6 | 15.44 | 80.00 | 52.11 | 1.29 | 0 | 0.819 |

