### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1111 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 3512 µs |
| fdatasync of a clean file | p50 137.5 µs, p99 145.7 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 4434 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 4235 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.33 µs, p99 33.14 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.56 | 338.0 | - | - | - |
| fold | 13.84 | 282.2 | - | - | - |
| crc+fold | 7.64 | 511.5 | - | - | - |
| copy | 6.83 | 571.6 | - | - | - |
| decode-21-data | 5.20 | 751.5 | - | - | - |
| decode-21-parity | 5.24 | 745.8 | - | - | - |
| decode-42-data | 3.06 | 1277 | - | - | - |
| decode-42-parity | 3.12 | 1253 | - | - | - |
| pipeline-copy | 3.26 | 1198 | - | - | - |
| pipeline-21 | 2.25 | 1739 | - | - | - |
| pipeline-42 | 1.30 | 2998 | - | - | - |
| summary-42 | 820.9 | 28.55 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 2 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 503.8 | 1.91 | 4.85 | 68.34 | 256.0 | 1.00 | 0 |
| layout=4+2 | chunk | 31.40 | 31.49 | 34.54 | 4115 | 16472 | 2.01 | 0 |
| layout=2+1 | unit | 572.0 | 1.68 | 4.55 | 68.02 | 128.0 | 1.00 | 0 |
| layout=2+1 | chunk | 48.80 | 20.29 | 23.30 | 4102 | 8200 | 2.01 | 0 |

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | cpu | 6181 | 0 | 5151 | 1480/1480 | 0 | 6.62 | 34.37 | 0.293 | 97.41 | 38.52 | 0.344 |
| layout=4+2 piece=4M | idle | 445.6 | 0 | 425.0 | 4/4 | 0 | 4.83 | 12.49 | 0.248 | 118.7 | 48.62 | 0.752 |
| layout=4+2 piece=256K | idle | 318.0 | 0 | 301.0 | 4/4 | 0 | 2.11 | 9.52 | 0.241 | 117.7 | 49.33 | 0.692 |
| layout=4+2 piece=1M | unbounded | 543.8 | 0 | 520.0 | 6/6 | 0 | 6.26 | 31.61 | 0.262 | 121.1 | 49.68 | 0.782 |
| layout=4+2 piece=1M | idle-ceil-200 | 181.0 | 0 | 172.0 | 2/2 | 0 | 2.02 | 7.83 | 0.214 | 117.6 | 48.69 | 0.524 |
| layout=4+2 piece=1M | idle | 387.9 | 0 | 369.0 | 4/4 | 0 | 2.11 | 8.84 | 0.244 | 117.9 | 50.40 | 0.717 |
| layout=4+2 piece=1M | fixed-400 | 280.4 | 0.701 | 268.0 | 2/2 | 0 | 5.47 | 16.59 | 0.238 | 119.4 | 50.51 | 0.592 |
| layout=4+2 piece=1M | fixed-200 | 183.8 | 0.919 | 176.0 | 2/2 | 0 | 5.13 | 16.62 | 0.229 | 119.6 | 49.82 | 0.527 |
| layout=4+2 piece=1M | fixed-100 | 99.70 | 0.997 | 95.00 | 2/2 | 0 | 4.88 | 11.92 | 0.193 | 119.7 | 50.39 | 0.463 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 4.48 | 11.21 | 0.119 | 119.9 | 50.78 | 0.427 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 2.04 | 9.73 | 0.059 | 0 | 0 | 0.392 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 112.8 | 564.2 | 0 | 141.3 | 18.45 | 117.6 | 76.54 | 0.828 | 0 | 0.669 |
| role=local layout=4+2 piece=1M | idle | 68.60 | 343.1 | 0 | 56.58 | 3.18 | 10.01 | 6.21 | 0.255 | 0 | 0.683 |
| role=local layout=2+1 piece=1M | unbounded | 175.2 | 526.0 | 0 | 94.47 | 16.77 | 99.49 | 66.00 | 0.478 | 0 | 0.610 |
| role=local layout=2+1 piece=1M | idle | 66.60 | 199.7 | 0 | 38.61 | 4.44 | 28.82 | 18.12 | 0.230 | 0 | 0.541 |
| role=local layout=copy piece=1M | unbounded | 222.8 | 446.0 | 0 | 72.67 | 13.67 | 85.18 | 60.34 | 0.437 | 0 | 0.603 |
| role=local layout=copy piece=1M | idle | 76.20 | 152.3 | 0 | 27.46 | 4.63 | 27.07 | 17.74 | 0.223 | 0 | 0.501 |
| role=dest layout=4+2 piece=1M | unbounded | 269.8 | 270.0 | 0 | 59.74 | 23.70 | 71.32 | 51.31 | 0.598 | 0 | 0.509 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 92.50 | 92.59 | 0 | 23.31 | 5.03 | 27.01 | 17.55 | 0.266 | 0 | 0.452 |
| role=dest layout=4+2 piece=1M | idle | 87.60 | 87.69 | 0 | 21.12 | 5.33 | 29.97 | 19.54 | 0.262 | 0 | 0.445 |
| role=dest layout=4+2 piece=1M | fixed-800 | 147.1 | 147.2 | 0.184 | 25.90 | 9.05 | 38.79 | 24.14 | 0.303 | 0 | 0.456 |
| role=dest layout=4+2 piece=1M | fixed-400 | 144.0 | 144.1 | 0.360 | 22.44 | 9.03 | 39.12 | 24.43 | 0.291 | 0 | 0.451 |
| role=dest layout=4+2 piece=1M | fixed-200 | 91.95 | 92.04 | 0.460 | 43.70 | 9.28 | 38.23 | 25.81 | 0.280 | 0 | 0.392 |
| role=dest layout=4+2 piece=1M | fixed-100 | 94.65 | 94.74 | 0.947 | 40.59 | 5.76 | 28.07 | 17.75 | 0.271 | 0 | 0.456 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 1.98 | 8.43 | 5.12 | 0.057 | 0 | 0.386 |

