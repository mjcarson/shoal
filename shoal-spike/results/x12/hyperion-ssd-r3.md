### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 915.0 µs |
| fdatasync of a clean file | p50 112.8 µs, p99 131.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2084 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2134 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.89 µs, p99 33.72 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.55 | 338.1 | - | - | - |
| fold | 14.00 | 278.9 | - | - | - |
| crc+fold | 7.76 | 503.3 | - | - | - |
| copy | 6.74 | 579.7 | - | - | - |
| decode-21-data | 5.15 | 758.7 | - | - | - |
| decode-21-parity | 5.09 | 766.9 | - | - | - |
| decode-42-data | 3.05 | 1283 | - | - | - |
| decode-42-parity | 3.04 | 1283 | - | - | - |
| pipeline-copy | 3.07 | 1273 | - | - | - |
| pipeline-21 | 2.20 | 1772 | - | - | - |
| pipeline-42 | 1.30 | 3002 | - | - | - |
| summary-42 | 842.2 | 27.83 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 3 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 49.00 | 20.20 | 23.14 | 4110 | 8213 | 2.01 | 0 |
| layout=2+1 | unit | 575.2 | 1.66 | 4.56 | 68.10 | 128.0 | 1.00 | 0 |
| layout=4+2 | chunk | 31.50 | 31.42 | 34.50 | 4115 | 16475 | 2.01 | 0 |
| layout=4+2 | unit | 502.5 | 1.91 | 4.81 | 68.03 | 256.1 | 1.00 | 0 |

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 2.09 | 10.05 | 0.058 | 0 | 0 | 0.386 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 4.55 | 10.76 | 0.128 | 119.1 | 49.61 | 0.415 |
| layout=4+2 piece=1M | fixed-100 | 99.70 | 0.997 | 95.00 | 0/0 | 0 | 4.87 | 11.36 | 0.202 | 119.8 | 49.27 | 0.463 |
| layout=4+2 piece=1M | fixed-200 | 183.8 | 0.919 | 176.0 | 2/2 | 0 | 5.18 | 15.11 | 0.226 | 119.4 | 49.33 | 0.521 |
| layout=4+2 piece=1M | fixed-400 | 280.3 | 0.701 | 268.0 | 2/2 | 0 | 5.47 | 17.45 | 0.240 | 119.2 | 49.32 | 0.593 |
| layout=4+2 piece=1M | idle | 383.1 | 0 | 369.0 | 4/4 | 0 | 2.36 | 8.96 | 0.240 | 117.9 | 49.10 | 0.706 |
| layout=4+2 piece=1M | idle-ceil-200 | 179.8 | 0 | 172.0 | 2/2 | 0 | 2.42 | 9.69 | 0.213 | 118.2 | 48.67 | 0.520 |
| layout=4+2 piece=1M | unbounded | 543.1 | 0 | 521.0 | 4/4 | 0 | 6.31 | 28.31 | 0.264 | 121.2 | 49.91 | 0.774 |
| layout=4+2 piece=256K | idle | 318.4 | 0 | 306.0 | 4/4 | 0 | 2.29 | 9.52 | 0.241 | 117.6 | 48.70 | 0.691 |
| layout=4+2 piece=4M | idle | 442.4 | 0 | 426.0 | 4/4 | 0 | 4.83 | 12.17 | 0.252 | 119.1 | 48.95 | 0.748 |
| layout=4+2 piece=1M | cpu | 6176 | 0 | 5147 | 1481/1481 | 0 | 1.97 | 9.18 | 0.287 | 97.77 | 37.85 | 0.386 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 5.10 | 31.35 | 19.05 | 0.060 | 0 | 0.339 |
| role=dest layout=4+2 piece=1M | fixed-100 | 97.45 | 97.55 | 0.975 | 40.30 | 4.37 | 20.47 | 12.82 | 0.258 | 0 | 0.468 |
| role=dest layout=4+2 piece=1M | fixed-200 | 108.0 | 108.1 | 0.541 | 38.50 | 8.91 | 36.64 | 23.79 | 0.275 | 0 | 0.428 |
| role=dest layout=4+2 piece=1M | fixed-400 | 137.8 | 137.9 | 0.345 | 28.55 | 9.19 | 38.69 | 24.56 | 0.278 | 0 | 0.450 |
| role=dest layout=4+2 piece=1M | fixed-800 | 149.6 | 149.7 | 0.187 | 26.30 | 9.06 | 38.30 | 23.24 | 0.280 | 0 | 0.456 |
| role=dest layout=4+2 piece=1M | idle | 108.3 | 108.4 | 0 | 21.08 | 4.82 | 25.80 | 16.35 | 0.264 | 0 | 0.473 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 100.5 | 100.6 | 0 | 23.22 | 4.91 | 26.55 | 17.34 | 0.258 | 0 | 0.464 |
| role=dest layout=4+2 piece=1M | unbounded | 284.2 | 284.5 | 0 | 57.91 | 23.14 | 71.75 | 51.72 | 0.462 | 0 | 0.536 |
| role=local layout=copy piece=1M | idle | 102.9 | 206.0 | 0 | 26.40 | 4.91 | 25.54 | 17.11 | 0.233 | 0 | 0.564 |
| role=local layout=copy piece=1M | unbounded | 226.8 | 453.8 | 0 | 71.97 | 14.84 | 87.74 | 60.99 | 0.319 | 0 | 0.624 |
| role=local layout=2+1 piece=1M | idle | 93.40 | 280.7 | 0 | 36.94 | 3.81 | 21.38 | 13.62 | 0.242 | 0 | 0.638 |
| role=local layout=2+1 piece=1M | unbounded | 179.8 | 539.8 | 0 | 93.71 | 16.46 | 95.22 | 63.42 | 0.471 | 0 | 0.624 |
| role=local layout=4+2 piece=1M | idle | 13.40 | 67.07 | 0 | 288.5 | 5.60 | 30.15 | 20.01 | 0.173 | 0 | 0.392 |
| role=local layout=4+2 piece=1M | unbounded | 134.4 | 672.3 | 0 | 112.6 | 16.95 | 110.2 | 72.88 | 0.748 | 0 | 0.761 |

