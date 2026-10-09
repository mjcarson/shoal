### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 850.0 µs |
| fdatasync of a clean file | p50 112.0 µs, p99 122.7 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2088 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2107 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.03 µs, p99 32.83 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.55 | 338.3 | - | - | - |
| fold | 13.90 | 280.9 | - | - | - |
| crc+fold | 7.71 | 506.6 | - | - | - |
| copy | 6.68 | 584.4 | - | - | - |
| decode-21-data | 5.13 | 762.1 | - | - | - |
| decode-21-parity | 5.03 | 775.8 | - | - | - |
| decode-42-data | 3.01 | 1299 | - | - | - |
| decode-42-parity | 3.09 | 1266 | - | - | - |
| pipeline-copy | 3.22 | 1213 | - | - | - |
| pipeline-21 | 2.24 | 1748 | - | - | - |
| pipeline-42 | 1.30 | 3011 | - | - | - |
| summary-42 | 775.1 | 30.24 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 38.00 | 25.68 | 35.83 | 4102 | 8222 | 2.01 | 0 |
| layout=2+1 | unit | 251.2 | 3.87 | 7.07 | 68.03 | 128.1 | 1.00 | 0 |
| layout=4+2 | chunk | 26.20 | 36.84 | 48.08 | 4125 | 16517 | 2.02 | 0 |
| layout=4+2 | unit | 234.6 | 4.10 | 7.45 | 68.11 | 256.1 | 1.00 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 5.63 | 32.04 | 0.059 | 0 | 0 | 0.307 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 8.23 | 39.03 | 0.145 | 117.0 | 52.14 | 0.340 |
| layout=4+2 piece=1M | fixed-100 | 97.50 | 0.975 | 93.00 | 2/2 | 0 | 8.41 | 38.12 | 0.185 | 116.3 | 52.88 | 0.373 |
| layout=4+2 piece=1M | fixed-200 | 159.6 | 0.798 | 151.0 | 2/2 | 0 | 9.36 | 44.63 | 0.222 | 118.5 | 52.00 | 0.412 |
| layout=4+2 piece=1M | fixed-400 | 221.6 | 0.554 | 212.0 | 2/2 | 0 | 10.08 | 47.12 | 0.231 | 117.3 | 52.09 | 0.465 |
| layout=4+2 piece=1M | idle | 78.43 | 0 | 71.00 | 0/0 | 0 | 5.86 | 30.87 | 0.197 | 115.6 | 51.11 | 0.378 |
| layout=4+2 piece=1M | idle-ceil-200 | 44.89 | 0 | 42.00 | 2/2 | 0 | 6.40 | 35.98 | 0.093 | 116.8 | 51.62 | 0.345 |
| layout=4+2 piece=1M | unbounded | 341.3 | 0 | 329.0 | 4/4 | 0 | 10.72 | 56.49 | 0.240 | 117.5 | 52.24 | 0.550 |
| layout=4+2 piece=256K | idle | 344.8 | 0 | 329.0 | 4/4 | 0 | 1.57 | 8.50 | 0.239 | 116.5 | 50.32 | 0.696 |
| layout=4+2 piece=4M | idle | 280.7 | 0 | 292.0 | 2/2 | 0 | 4.82 | 24.68 | 0.243 | 118.7 | 51.49 | 0.574 |
| layout=4+2 piece=1M | cpu | 6181 | 0 | 5151 | 1480/1480 | 0 | 5.08 | 32.39 | 0.291 | 98.12 | 37.94 | 0.323 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 4.85 | 29.27 | 18.32 | 0.064 | 0 | 0.306 |
| role=dest layout=4+2 piece=1M | fixed-100 | 78.80 | 78.88 | 0.789 | 51.70 | 8.82 | 36.41 | 23.80 | 0.263 | 0 | 0.375 |
| role=dest layout=4+2 piece=1M | fixed-200 | 94.40 | 94.49 | 0.472 | 42.46 | 9.29 | 38.37 | 25.29 | 0.282 | 0 | 0.376 |
| role=dest layout=4+2 piece=1M | fixed-400 | 111.8 | 111.9 | 0.280 | 37.37 | 10.19 | 42.84 | 28.04 | 0.294 | 0 | 0.381 |
| role=dest layout=4+2 piece=1M | fixed-800 | 109.6 | 109.7 | 0.137 | 36.92 | 10.65 | 44.60 | 29.11 | 0.282 | 0 | 0.377 |
| role=dest layout=4+2 piece=1M | idle | 53.15 | 53.20 | 0 | 43.89 | 6.72 | 32.07 | 21.39 | 0.246 | 0 | 0.373 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 39.35 | 39.39 | 0 | 83.35 | 6.49 | 32.83 | 22.22 | 0.233 | 0 | 0.355 |
| role=dest layout=4+2 piece=1M | unbounded | 252.4 | 252.6 | 0 | 64.03 | 26.86 | 81.42 | 57.45 | 0.516 | 0 | 0.483 |
| role=local layout=copy piece=1M | idle | 37.70 | 75.37 | 0 | 91.08 | 5.35 | 29.63 | 19.33 | 0.173 | 0 | 0.395 |
| role=local layout=copy piece=1M | unbounded | 214.4 | 429.0 | 0 | 73.06 | 14.29 | 90.00 | 62.21 | 0.400 | 0 | 0.589 |
| role=local layout=2+1 piece=1M | idle | 36.85 | 111.0 | 0 | 89.68 | 4.47 | 24.78 | 16.35 | 0.202 | 0 | 0.435 |
| role=local layout=2+1 piece=1M | unbounded | 167.4 | 502.5 | 0 | 94.55 | 16.85 | 93.77 | 64.01 | 0.522 | 0 | 0.590 |
| role=local layout=4+2 piece=1M | idle | 29.20 | 146.1 | 0 | 123.9 | 4.49 | 22.66 | 15.56 | 0.221 | 0 | 0.468 |
| role=local layout=4+2 piece=1M | unbounded | 114.4 | 573.1 | 0 | 139.5 | 18.04 | 115.0 | 74.40 | 0.775 | 0 | 0.678 |

