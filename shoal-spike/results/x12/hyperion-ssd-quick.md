### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 862.8 µs |
| fdatasync of a clean file | p50 112.5 µs, p99 124.2 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2079 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2114 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.53 µs, p99 32.69 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs · **quick: not a measurement**

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.12 | 351.4 | - | - | - |
| fold | 13.80 | 283.0 | - | - | - |
| crc+fold | 7.76 | 503.7 | - | - | - |
| copy | 6.64 | 588.3 | - | - | - |
| decode-21-data | 5.11 | 765.1 | - | - | - |
| decode-21-parity | 5.05 | 773.2 | - | - | - |
| decode-42-data | 3.00 | 1302 | - | - | - |
| decode-42-parity | 3.04 | 1284 | - | - | - |
| pipeline-copy | 3.17 | 1232 | - | - | - |
| pipeline-21 | 2.23 | 1748 | - | - | - |
| pipeline-42 | 1.29 | 3026 | - | - | - |
| summary-42 | 838.2 | 27.96 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs · **quick: not a measurement**

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 46.50 | 20.20 | 23.22 | 4366 | 8465 | 2.06 | 0 |
| layout=2+1 | unit | 624.0 | 1.56 | 2.45 | 68.16 | 128.3 | 1.00 | 0 |
| layout=4+2 | chunk | 30.00 | 31.41 | 34.35 | 4307 | 17322 | 2.10 | 0 |
| layout=4+2 | unit | 568.5 | 1.65 | 4.78 | 68.36 | 256.7 | 1.00 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 2.7% to 90.7% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs · **quick: not a measurement**

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 1.26 | 6.12 | 0.054 | 0 | 0 | 0.350 |
| layout=4+2 piece=1M | fixed-50 | 51.05 | 1.02 | 3.00 | 0/0 | 0 | 4.40 | 10.13 | 0.158 | 119.0 | 51.65 | 0.376 |
| layout=4+2 piece=1M | idle | 421.9 | 0 | 26.00 | 6/6 | 0 | 2.84 | 10.70 | 0.236 | 117.3 | 48.91 | 0.697 |
| layout=4+2 piece=1M | unbounded | 563.8 | 0 | 35.00 | 8/8 | 0 | 6.15 | 29.35 | 0.288 | 121.2 | 50.29 | 0.770 |
| layout=4+2 piece=256K | idle | 358.5 | 0 | 22.00 | 4/4 | 0 | 1.25 | 5.74 | 0.239 | 117.5 | 48.91 | 0.708 |
| layout=4+2 piece=1M | cpu | 6192 | 0 | 344.0 | 98/98 | 0 | 1.57 | 11.43 | 0.287 | 98.53 | 38.28 | 0.332 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 2.7% to 90.7% of the device, 70 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs · **quick: not a measurement**

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 1.22 | 4.82 | 3.61 | 0.054 | 0 | 0.335 |
| role=dest layout=4+2 piece=1M | fixed-100 | 96.00 | 96.10 | 0.961 | 40.58 | 3.86 | 9.94 | 6.78 | 0.279 | 0 | 0.428 |
| role=dest layout=4+2 piece=1M | idle | 211.5 | 211.7 | 0 | 17.77 | 3.40 | 13.56 | 7.83 | 0.310 | 0 | 0.595 |
| role=dest layout=4+2 piece=1M | unbounded | 445.5 | 445.9 | 0 | 39.11 | 15.72 | 30.82 | 23.21 | 0.452 | 0 | 0.721 |
| role=local layout=copy piece=1M | unbounded | 324.0 | 642.6 | 0 | 49.87 | 16.59 | 48.94 | 35.33 | 0.417 | 0 | 0.791 |
| role=local layout=2+1 piece=1M | unbounded | 237.0 | 714.7 | 0 | 65.23 | 10.73 | 70.85 | 47.76 | 0.616 | 0 | 0.788 |
| role=local layout=4+2 piece=1M | unbounded | 147.0 | 720.0 | 0 | 111.2 | 15.83 | 82.73 | 52.46 | 1.94 | 0 | 0.797 |

