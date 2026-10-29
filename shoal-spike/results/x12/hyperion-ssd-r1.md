### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 857.1 µs |
| fdatasync of a clean file | p50 112.9 µs, p99 127.2 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2080 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2078 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.85 µs, p99 33.05 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.55 | 338.3 | - | - | - |
| fold | 14.01 | 278.8 | - | - | - |
| crc+fold | 7.75 | 503.9 | - | - | - |
| copy | 6.86 | 569.4 | - | - | - |
| decode-21-data | 5.24 | 745.0 | - | - | - |
| decode-21-parity | 5.18 | 753.9 | - | - | - |
| decode-42-data | 3.05 | 1282 | - | - | - |
| decode-42-parity | 3.11 | 1256 | - | - | - |
| pipeline-copy | 3.23 | 1211 | - | - | - |
| pipeline-21 | 2.24 | 1742 | - | - | - |
| pipeline-42 | 1.27 | 3075 | - | - | - |
| summary-42 | 837.2 | 27.99 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 38.20 | 25.72 | 41.43 | 4106 | 8200 | 2.01 | 0 |
| layout=2+1 | unit | 251.3 | 3.89 | 6.95 | 68.03 | 128.1 | 1.00 | 0 |
| layout=4+2 | chunk | 26.40 | 36.99 | 50.15 | 4125 | 16513 | 2.02 | 0 |
| layout=4+2 | unit | 238.2 | 4.08 | 7.36 | 68.00 | 256.1 | 1.00 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 6.01 | 34.12 | 0.060 | 0 | 0 | 0.304 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 7.84 | 35.33 | 0.160 | 118.0 | 48.80 | 0.342 |
| layout=4+2 piece=1M | fixed-100 | 97.90 | 0.979 | 93.00 | 2/2 | 0 | 8.01 | 38.25 | 0.212 | 117.9 | 48.68 | 0.375 |
| layout=4+2 piece=1M | fixed-200 | 161.0 | 0.805 | 153.0 | 2/2 | 0 | 8.24 | 42.93 | 0.244 | 118.5 | 48.93 | 0.428 |
| layout=4+2 piece=1M | fixed-400 | 222.2 | 0.556 | 213.0 | 2/2 | 0 | 9.57 | 46.99 | 0.251 | 118.4 | 48.69 | 0.472 |
| layout=4+2 piece=1M | idle | 101.1 | 0 | 98.00 | 2/2 | 0 | 5.23 | 30.38 | 0.188 | 116.8 | 48.24 | 0.405 |
| layout=4+2 piece=1M | idle-ceil-200 | 166.4 | 0 | 161.0 | 2/2 | 0 | 3.33 | 16.54 | 0.220 | 118.1 | 48.64 | 0.481 |
| layout=4+2 piece=1M | unbounded | 394.0 | 0 | 381.0 | 4/4 | 0 | 8.44 | 47.09 | 0.289 | 119.1 | 49.19 | 0.603 |
| layout=4+2 piece=256K | idle | 70.14 | 0 | 67.00 | 2/2 | 0 | 5.79 | 32.62 | 0.174 | 117.7 | 48.31 | 0.381 |
| layout=4+2 piece=4M | idle | 136.9 | 0 | 131.0 | 0/0 | 0 | 5.41 | 31.30 | 0.234 | 117.2 | 48.41 | 0.436 |
| layout=4+2 piece=1M | cpu | 6181 | 0 | 5151 | 1480/1480 | 0 | 2.32 | 8.64 | 0.287 | 97.77 | 37.76 | 0.348 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 4.10 | 29.39 | 19.19 | 0.065 | 0 | 0.308 |
| role=dest layout=4+2 piece=1M | fixed-100 | 73.75 | 73.82 | 0.738 | 54.52 | 9.20 | 37.33 | 24.41 | 0.263 | 0 | 0.361 |
| role=dest layout=4+2 piece=1M | fixed-200 | 94.00 | 94.09 | 0.470 | 42.93 | 9.42 | 38.06 | 26.39 | 0.277 | 0 | 0.373 |
| role=dest layout=4+2 piece=1M | fixed-400 | 108.5 | 108.7 | 0.272 | 37.58 | 10.36 | 42.51 | 27.37 | 0.289 | 0 | 0.376 |
| role=dest layout=4+2 piece=1M | fixed-800 | 116.2 | 116.3 | 0.145 | 34.69 | 10.11 | 43.59 | 26.97 | 0.283 | 0 | 0.389 |
| role=dest layout=4+2 piece=1M | idle | 51.20 | 51.25 | 0 | 56.90 | 6.65 | 33.65 | 22.15 | 0.254 | 0 | 0.376 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 48.45 | 48.50 | 0 | 57.52 | 6.40 | 30.82 | 21.14 | 0.238 | 0 | 0.366 |
| role=dest layout=4+2 piece=1M | unbounded | 256.0 | 256.3 | 0 | 62.60 | 26.66 | 80.16 | 55.78 | 0.527 | 0 | 0.490 |
| role=local layout=copy piece=1M | idle | 43.95 | 87.84 | 0 | 73.90 | 5.29 | 28.66 | 18.60 | 0.196 | 0 | 0.411 |
| role=local layout=copy piece=1M | unbounded | 212.2 | 424.8 | 0 | 73.06 | 14.78 | 93.91 | 65.57 | 0.304 | 0 | 0.587 |
| role=local layout=2+1 piece=1M | idle | 24.20 | 73.02 | 0 | 153.1 | 4.87 | 27.08 | 17.09 | 0.188 | 0 | 0.385 |
| role=local layout=2+1 piece=1M | unbounded | 166.8 | 500.5 | 0 | 94.44 | 17.70 | 103.2 | 68.32 | 0.422 | 0 | 0.595 |
| role=local layout=4+2 piece=1M | idle | 21.05 | 105.8 | 0 | 173.9 | 4.35 | 25.84 | 16.92 | 0.203 | 0 | 0.420 |
| role=local layout=4+2 piece=1M | unbounded | 114.8 | 574.6 | 0 | 139.0 | 18.54 | 112.0 | 74.39 | 0.957 | 0 | 0.680 |

