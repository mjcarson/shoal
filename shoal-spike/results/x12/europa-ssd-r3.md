### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.27 µs |
| fdatasync of a clean file | p50 10.83 µs, p99 16.47 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.49 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.45 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.34 µs, p99 16.20 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 3 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 51.48 | 75.87 | - | - | - |
| fold | 23.67 | 165.0 | - | - | - |
| crc+fold | 29.58 | 132.1 | - | - | - |
| copy | 28.78 | 135.7 | - | - | - |
| decode-21-data | 17.51 | 223.1 | - | - | - |
| decode-21-parity | 17.47 | 223.6 | - | - | - |
| decode-42-data | 9.34 | 418.2 | - | - | - |
| decode-42-parity | 9.33 | 418.9 | - | - | - |
| pipeline-copy | 21.54 | 181.4 | - | - | - |
| pipeline-21 | 8.79 | 444.3 | - | - | - |
| pipeline-42 | 4.62 | 846.4 | - | - | - |
| summary-42 | 4911 | 4.77 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 3 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 183.0 | 5.43 | 5.79 | 4104 | 8205 | 0 | 0 |
| layout=2+1 | unit | 5594 | 0.176 | 0.215 | 68.00 | 128.0 | 0 | 0 |
| layout=4+2 | chunk | 111.2 | 8.97 | 9.29 | 4105 | 16407 | 0 | 0 |
| layout=4+2 | unit | 4204 | 0.235 | 0.274 | 68.00 | 256.0 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 3 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 0.086 | 0.162 | 0.025 | 0 | 0 | 0.042 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 0.836 | 1.38 | 0.027 | 28.45 | 13.00 | 0.061 |
| layout=4+2 piece=1M | fixed-100 | 99.90 | 0.999 | 95.00 | 0/0 | 0 | 1.21 | 1.50 | 0.027 | 28.50 | 13.15 | 0.077 |
| layout=4+2 piece=1M | fixed-200 | 200.0 | 1.000 | 191.0 | 2/2 | 0 | 1.44 | 1.64 | 0.053 | 28.44 | 13.14 | 0.112 |
| layout=4+2 piece=1M | fixed-400 | 398.8 | 0.997 | 381.0 | 4/4 | 0 | 1.54 | 2.08 | 0.138 | 28.46 | 13.13 | 0.183 |
| layout=4+2 piece=1M | idle | 2122 | 0 | 2032 | 22/22 | 0 | 0.546 | 1.23 | 0.205 | 28.55 | 13.17 | 0.824 |
| layout=4+2 piece=1M | idle-ceil-200 | 200.0 | 0 | 191.0 | 2/2 | 0 | 0.430 | 0.514 | 0.035 | 28.44 | 13.56 | 0.113 |
| layout=4+2 piece=1M | unbounded | 2243 | 0 | 2147 | 22/22 | 0 | 1.66 | 2.96 | 0.204 | 28.31 | 13.31 | 0.838 |
| layout=4+2 piece=256K | idle | 1873 | 0 | 1793 | 18/18 | 0 | 0.518 | 1.03 | 0.204 | 28.24 | 13.20 | 0.809 |
| layout=4+2 piece=4M | idle | 2213 | 0 | 2118 | 22/22 | 0 | 1.69 | 2.37 | 0.209 | 28.21 | 13.18 | 0.835 |
| layout=4+2 piece=1M | cpu | 22313 | 0 | 18594 | 5346/5346 | 0 | 0.559 | 2.01 | 0.228 | 26.95 | 13.47 | 0.040 |

### X12 · A rebuild beside the foreground, round 3 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 0.086 | 0.163 | 0.096 | 0.025 | 0 | 0.042 |
| role=dest layout=4+2 piece=1M | fixed-100 | 99.80 | 99.90 | 0.999 | 40.04 | 0.914 | 1.09 | 1.04 | 0.162 | 0 | 0.077 |
| role=dest layout=4+2 piece=1M | fixed-200 | 199.7 | 199.8 | 0.999 | 20.02 | 1.13 | 1.21 | 1.12 | 0.198 | 0 | 0.114 |
| role=dest layout=4+2 piece=1M | fixed-400 | 399.6 | 400.0 | 1.000 | 10.01 | 1.23 | 1.32 | 1.25 | 0.210 | 0 | 0.191 |
| role=dest layout=4+2 piece=1M | fixed-800 | 733.6 | 734.4 | 0.918 | 5.42 | 1.27 | 1.73 | 1.27 | 0.217 | 0 | 0.319 |
| role=dest layout=4+2 piece=1M | idle | 1298 | 1299 | 0 | 3.05 | 0.555 | 1.13 | 1.07 | 0.220 | 0 | 0.575 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 399.6 | 400.0 | 0 | 10.01 | 0.538 | 1.07 | 1.02 | 0.211 | 0 | 0.204 |
| role=dest layout=4+2 piece=1M | unbounded | 1729 | 1731 | 0 | 8.49 | 6.28 | 8.46 | 6.44 | 0.332 | 0 | 0.704 |
| role=local layout=copy piece=1M | idle | 987.4 | 1977 | 0 | 4.03 | 0.604 | 0.709 | 0.653 | 0.175 | 0 | 0.811 |
| role=local layout=copy piece=1M | unbounded | 1188 | 2378 | 0 | 13.44 | 6.29 | 10.81 | 6.28 | 0.436 | 0 | 0.893 |
| role=local layout=2+1 piece=1M | idle | 677.3 | 2034 | 0 | 5.88 | 0.583 | 0.867 | 0.715 | 0.197 | 0 | 0.810 |
| role=local layout=2+1 piece=1M | unbounded | 801.8 | 2408 | 0 | 19.94 | 4.84 | 8.30 | 4.89 | 0.557 | 0 | 0.879 |
| role=local layout=4+2 piece=1M | idle | 408.6 | 2046 | 0 | 9.76 | 0.557 | 1.20 | 0.729 | 0.203 | 0 | 0.805 |
| role=local layout=4+2 piece=1M | unbounded | 487.6 | 2441 | 0 | 32.51 | 5.70 | 12.55 | 7.67 | 2.16 | 0 | 0.881 |

