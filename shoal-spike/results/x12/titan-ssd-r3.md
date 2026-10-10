### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 854.9 µs |
| fdatasync of a clean file | p50 112.3 µs, p99 121.2 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2085 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2107 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.26 µs, p99 33.76 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2; a summary is a 4+2 stripe's six)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 11.22 | 348.0 | - | - | - |
| fold | 13.90 | 280.9 | - | - | - |
| crc+fold | 7.71 | 506.9 | - | - | - |
| copy | 6.67 | 585.6 | - | - | - |
| decode-21-data | 5.12 | 762.6 | - | - | - |
| decode-21-parity | 5.03 | 776.8 | - | - | - |
| decode-42-data | 3.03 | 1288 | - | - | - |
| decode-42-parity | 3.04 | 1284 | - | - | - |
| pipeline-copy | 3.20 | 1220 | - | - | - |
| pipeline-21 | 2.24 | 1745 | - | - | - |
| pipeline-42 | 1.29 | 3019 | - | - | - |
| summary-42 | 772.6 | 30.34 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 3 (one at a time, nothing beside it)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 48.90 | 20.24 | 23.39 | 4104 | 8202 | 2.01 | 0 |
| layout=2+1 | unit | 579.0 | 1.66 | 4.54 | 68.02 | 128.0 | 1.00 | 0 |
| layout=4+2 | chunk | 31.40 | 31.50 | 34.41 | 4116 | 16501 | 2.01 | 0 |
| layout=4+2 | unit | 509.2 | 1.88 | 4.77 | 68.03 | 256.1 | 1.00 | 0 |

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 2.26 | 9.58 | 0.059 | 0 | 0 | 0.384 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 4.57 | 11.17 | 0.130 | 118.7 | 49.00 | 0.425 |
| layout=4+2 piece=1M | fixed-100 | 99.70 | 0.997 | 95.00 | 0/0 | 0 | 4.82 | 11.09 | 0.192 | 118.8 | 48.63 | 0.459 |
| layout=4+2 piece=1M | fixed-200 | 184.1 | 0.920 | 176.0 | 2/2 | 0 | 5.14 | 15.35 | 0.227 | 119.1 | 49.24 | 0.522 |
| layout=4+2 piece=1M | fixed-400 | 280.3 | 0.701 | 268.0 | 2/2 | 0 | 5.37 | 13.72 | 0.240 | 118.5 | 48.52 | 0.592 |
| layout=4+2 piece=1M | idle | 388.6 | 0 | 374.0 | 4/4 | 0 | 2.18 | 9.57 | 0.248 | 117.2 | 48.49 | 0.708 |
| layout=4+2 piece=1M | idle-ceil-200 | 180.0 | 0 | 172.0 | 2/2 | 0 | 2.26 | 9.41 | 0.219 | 117.8 | 48.32 | 0.520 |
| layout=4+2 piece=1M | unbounded | 546.3 | 0 | 523.0 | 4/4 | 0 | 6.28 | 27.84 | 0.264 | 120.7 | 49.94 | 0.781 |
| layout=4+2 piece=256K | idle | 318.4 | 0 | 307.0 | 4/4 | 0 | 2.06 | 8.72 | 0.242 | 116.8 | 50.18 | 0.691 |
| layout=4+2 piece=4M | idle | 447.2 | 0 | 430.0 | 4/4 | 0 | 4.84 | 11.94 | 0.254 | 118.3 | 48.91 | 0.753 |
| layout=4+2 piece=1M | cpu | 6186 | 0 | 5155 | 1482/1482 | 0 | 2.20 | 10.27 | 0.286 | 97.14 | 38.35 | 0.393 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2; population from 3.0% to 91.1% of the device, 71 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 4.77 | 30.21 | 18.51 | 0.060 | 0 | 0.337 |
| role=dest layout=4+2 piece=1M | fixed-100 | 98.40 | 98.50 | 0.985 | 40.23 | 3.89 | 12.46 | 7.65 | 0.269 | 0 | 0.472 |
| role=dest layout=4+2 piece=1M | fixed-200 | 111.4 | 111.5 | 0.558 | 36.96 | 8.77 | 34.38 | 22.70 | 0.279 | 0 | 0.430 |
| role=dest layout=4+2 piece=1M | fixed-400 | 108.0 | 108.1 | 0.270 | 37.71 | 9.27 | 41.43 | 25.43 | 0.276 | 0 | 0.389 |
| role=dest layout=4+2 piece=1M | fixed-800 | 150.2 | 150.3 | 0.188 | 25.58 | 9.08 | 35.41 | 22.55 | 0.292 | 0 | 0.459 |
| role=dest layout=4+2 piece=1M | idle | 38.85 | 38.89 | 0 | 84.91 | 5.73 | 31.40 | 20.56 | 0.243 | 0 | 0.378 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 109.2 | 109.3 | 0 | 22.62 | 4.52 | 26.99 | 16.94 | 0.264 | 0 | 0.474 |
| role=dest layout=4+2 piece=1M | unbounded | 285.6 | 285.9 | 0 | 57.58 | 23.38 | 72.26 | 53.58 | 0.476 | 0 | 0.536 |
| role=local layout=copy piece=1M | idle | 57.40 | 114.9 | 0 | 31.60 | 5.19 | 32.30 | 19.22 | 0.207 | 0 | 0.457 |
| role=local layout=copy piece=1M | unbounded | 238.1 | 476.5 | 0 | 70.76 | 13.98 | 86.48 | 59.82 | 0.336 | 0 | 0.645 |
| role=local layout=2+1 piece=1M | idle | 94.85 | 284.9 | 0 | 35.83 | 3.72 | 23.23 | 14.60 | 0.247 | 0 | 0.630 |
| role=local layout=2+1 piece=1M | unbounded | 176.6 | 529.9 | 0 | 94.31 | 16.04 | 89.11 | 58.76 | 0.471 | 0 | 0.616 |
| role=local layout=4+2 piece=1M | idle | 70.60 | 353.4 | 0 | 55.44 | 2.70 | 10.00 | 5.84 | 0.262 | 0 | 0.696 |
| role=local layout=4+2 piece=1M | unbounded | 113.2 | 567.2 | 0 | 140.9 | 18.43 | 114.5 | 74.60 | 0.736 | 0 | 0.683 |

