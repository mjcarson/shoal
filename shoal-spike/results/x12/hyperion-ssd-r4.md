### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1111 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 2965 µs |
| fdatasync of a clean file | p50 112.4 µs, p99 124.3 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 4157 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 4181 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.92 µs, p99 33.22 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 4 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.55 | 338.3 | - | - | - |
| fold | 13.83 | 282.4 | - | - | - |
| crc+fold | 7.64 | 511.2 | - | - | - |
| copy | 6.68 | 585.1 | - | - | - |
| decode-21-data | 5.12 | 763.3 | - | - | - |
| decode-21-parity | 5.13 | 761.2 | - | - | - |
| decode-42-data | 3.01 | 1297 | - | - | - |
| decode-42-parity | 3.04 | 1283 | - | - | - |
| pipeline-copy | 3.17 | 1232 | - | - | - |
| pipeline-21 | 2.22 | 1758 | - | - | - |
| pipeline-42 | 1.29 | 3017 | - | - | - |
| summary-42 | 820.4 | 28.57 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 4 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 503.0 | 1.91 | 4.83 | 68.01 | 256.1 | 1.00 | 0 |
| layout=4+2 | chunk | 31.40 | 31.48 | 34.57 | 4115 | 16472 | 2.01 | 0 |
| layout=2+1 | unit | 572.2 | 1.68 | 4.54 | 68.09 | 128.0 | 1.00 | 0 |
| layout=2+1 | chunk | 48.60 | 20.24 | 25.63 | 4110 | 8223 | 2.01 | 0 |

### X12 · A deep scrub beside the foreground, round 4 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | cpu | 6194 | 0 | 5162 | 1484/1484 | 0 | 6.07 | 35.55 | 0.291 | 97.30 | 38.43 | 0.348 |
| layout=4+2 piece=4M | idle | 442.6 | 0 | 423.0 | 6/6 | 0 | 4.84 | 11.09 | 0.253 | 118.9 | 48.99 | 0.751 |
| layout=4+2 piece=256K | idle | 318.9 | 0 | 305.0 | 2/2 | 0 | 2.53 | 11.74 | 0.237 | 116.8 | 47.62 | 0.696 |
| layout=4+2 piece=1M | unbounded | 545.2 | 0 | 522.0 | 6/6 | 0 | 6.25 | 28.71 | 0.269 | 120.9 | 49.43 | 0.785 |
| layout=4+2 piece=1M | idle-ceil-200 | 181.3 | 0 | 173.0 | 2/2 | 0 | 2.18 | 8.74 | 0.222 | 117.4 | 47.76 | 0.530 |
| layout=4+2 piece=1M | idle | 385.9 | 0 | 368.0 | 4/4 | 0 | 2.17 | 9.93 | 0.243 | 117.2 | 47.77 | 0.714 |
| layout=4+2 piece=1M | fixed-400 | 279.9 | 0.700 | 268.0 | 2/2 | 0 | 5.46 | 16.46 | 0.245 | 119.0 | 48.54 | 0.589 |
| layout=4+2 piece=1M | fixed-200 | 183.8 | 0.919 | 176.0 | 2/2 | 0 | 5.23 | 13.56 | 0.221 | 118.9 | 48.55 | 0.521 |
| layout=4+2 piece=1M | fixed-100 | 99.90 | 0.999 | 95.00 | 2/2 | 0 | 4.85 | 11.08 | 0.189 | 118.5 | 48.76 | 0.466 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 4.44 | 10.43 | 0.156 | 118.5 | 49.23 | 0.423 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 2.34 | 9.69 | 0.056 | 0 | 0 | 0.388 |

### X12 · A rebuild beside the foreground, round 4 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 113.2 | 566.7 | 0 | 141.1 | 18.80 | 117.6 | 75.41 | 0.928 | 0 | 0.675 |
| role=local layout=4+2 piece=1M | idle | 70.40 | 352.5 | 0 | 55.08 | 2.31 | 8.87 | 5.84 | 0.256 | 0 | 0.697 |
| role=local layout=2+1 piece=1M | unbounded | 175.2 | 525.9 | 0 | 94.30 | 16.78 | 95.39 | 64.76 | 0.398 | 0 | 0.620 |
| role=local layout=2+1 piece=1M | idle | 62.65 | 188.3 | 0 | 37.12 | 4.73 | 28.34 | 17.56 | 0.233 | 0 | 0.532 |
| role=local layout=copy piece=1M | unbounded | 222.4 | 445.0 | 0 | 72.78 | 13.54 | 85.32 | 61.27 | 0.291 | 0 | 0.610 |
| role=local layout=copy piece=1M | idle | 77.25 | 154.8 | 0 | 27.78 | 4.76 | 27.85 | 18.28 | 0.227 | 0 | 0.509 |
| role=dest layout=4+2 piece=1M | unbounded | 264.6 | 264.9 | 0 | 60.72 | 25.95 | 76.49 | 55.24 | 0.441 | 0 | 0.513 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 83.35 | 83.43 | 0 | 23.76 | 4.80 | 26.75 | 17.47 | 0.259 | 0 | 0.444 |
| role=dest layout=4+2 piece=1M | idle | 91.80 | 91.89 | 0 | 22.89 | 5.03 | 27.72 | 17.79 | 0.266 | 0 | 0.454 |
| role=dest layout=4+2 piece=1M | fixed-800 | 145.6 | 145.7 | 0.182 | 26.41 | 9.48 | 38.52 | 25.57 | 0.281 | 0 | 0.454 |
| role=dest layout=4+2 piece=1M | fixed-400 | 144.8 | 144.9 | 0.362 | 22.27 | 8.90 | 38.86 | 23.83 | 0.281 | 0 | 0.463 |
| role=dest layout=4+2 piece=1M | fixed-200 | 126.2 | 126.3 | 0.631 | 26.79 | 8.45 | 34.40 | 22.85 | 0.275 | 0 | 0.461 |
| role=dest layout=4+2 piece=1M | fixed-100 | 90.20 | 90.29 | 0.903 | 40.83 | 7.33 | 32.47 | 21.12 | 0.259 | 0 | 0.448 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 1.91 | 9.36 | 5.70 | 0.058 | 0 | 0.381 |

