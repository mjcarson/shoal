### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.29 µs |
| fdatasync of a clean file | p50 10.86 µs, p99 16.79 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.41 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.97 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.47 µs, p99 17.07 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 2 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 50.04 | 78.06 | - | - | - |
| fold | 23.34 | 167.3 | - | - | - |
| crc+fold | 29.21 | 133.7 | - | - | - |
| copy | 28.68 | 136.2 | - | - | - |
| decode-21-data | 17.46 | 223.7 | - | - | - |
| decode-21-parity | 17.32 | 225.5 | - | - | - |
| decode-42-data | 9.30 | 420.2 | - | - | - |
| decode-42-parity | 9.29 | 420.4 | - | - | - |
| pipeline-copy | 21.62 | 180.7 | - | - | - |
| pipeline-21 | 8.67 | 450.3 | - | - | - |
| pipeline-42 | 4.51 | 865.8 | - | - | - |
| summary-42 | 4942 | 4.74 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 2 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 | unit | 4201 | 0.235 | 0.275 | 68.06 | 256.0 | 0 | 0 |
| layout=4+2 | chunk | 112.0 | 8.86 | 9.25 | 4105 | 16426 | 0 | 0 |
| layout=2+1 | unit | 5597 | 0.176 | 0.211 | 68.00 | 128.0 | 0 | 0 |
| layout=2+1 | chunk | 185.3 | 5.36 | 5.68 | 4102 | 8202 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 2 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | cpu | 22162 | 0 | 18468 | 5306/5306 | 0 | 0.579 | 1.72 | 0.236 | 27.25 | 13.65 | 0.037 |
| layout=4+2 piece=4M | idle | 2216 | 0 | 2121 | 22/22 | 0 | 1.67 | 2.39 | 0.208 | 28.49 | 13.46 | 0.838 |
| layout=4+2 piece=256K | idle | 1883 | 0 | 1802 | 20/20 | 0 | 0.526 | 1.06 | 0.207 | 28.44 | 12.92 | 0.811 |
| layout=4+2 piece=1M | unbounded | 2243 | 0 | 2147 | 24/24 | 0 | 1.67 | 2.95 | 0.209 | 28.86 | 12.92 | 0.842 |
| layout=4+2 piece=1M | idle-ceil-200 | 200.0 | 0 | 191.0 | 2/2 | 0 | 0.424 | 0.480 | 0.037 | 28.23 | 13.50 | 0.115 |
| layout=4+2 piece=1M | idle | 2119 | 0 | 2028 | 20/20 | 0 | 0.548 | 1.28 | 0.207 | 28.33 | 12.95 | 0.832 |
| layout=4+2 piece=1M | fixed-400 | 398.8 | 0.997 | 381.0 | 4/4 | 0 | 1.54 | 1.94 | 0.132 | 28.74 | 12.93 | 0.182 |
| layout=4+2 piece=1M | fixed-200 | 200.0 | 1.000 | 191.0 | 2/2 | 0 | 1.43 | 1.64 | 0.050 | 28.65 | 12.91 | 0.114 |
| layout=4+2 piece=1M | fixed-100 | 99.90 | 0.999 | 95.00 | 2/2 | 0 | 1.24 | 1.39 | 0.028 | 28.84 | 12.92 | 0.079 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 0.877 | 1.08 | 0.026 | 28.45 | 13.07 | 0.061 |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 0.086 | 0.160 | 0.025 | 0 | 0 | 0.042 |

### X12 · A rebuild beside the foreground, round 2 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=local layout=4+2 piece=1M | unbounded | 490.6 | 2454 | 0 | 32.59 | 5.55 | 10.97 | 6.44 | 1.31 | 0 | 0.897 |
| role=local layout=4+2 piece=1M | idle | 408.5 | 2045 | 0 | 9.77 | 0.562 | 1.13 | 0.714 | 0.203 | 0 | 0.812 |
| role=local layout=2+1 piece=1M | unbounded | 790.4 | 2374 | 0 | 20.23 | 4.98 | 8.44 | 5.02 | 0.523 | 0 | 0.880 |
| role=local layout=2+1 piece=1M | idle | 671.1 | 2015 | 0 | 5.95 | 0.585 | 0.845 | 0.691 | 0.194 | 0 | 0.819 |
| role=local layout=copy piece=1M | unbounded | 1172 | 2346 | 0 | 13.64 | 6.45 | 11.10 | 6.42 | 0.485 | 0 | 0.889 |
| role=local layout=copy piece=1M | idle | 990.4 | 1982 | 0 | 4.01 | 0.604 | 0.702 | 0.648 | 0.188 | 0 | 0.808 |
| role=dest layout=4+2 piece=1M | unbounded | 1797 | 1799 | 0 | 7.38 | 6.03 | 8.80 | 6.25 | 0.324 | 0 | 0.714 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 399.6 | 400.0 | 0 | 10.01 | 0.537 | 1.09 | 1.03 | 0.212 | 0 | 0.203 |
| role=dest layout=4+2 piece=1M | idle | 1310 | 1312 | 0 | 3.03 | 0.553 | 1.10 | 1.05 | 0.220 | 0 | 0.577 |
| role=dest layout=4+2 piece=1M | fixed-800 | 730.2 | 731.0 | 0.914 | 5.44 | 1.31 | 1.77 | 1.31 | 0.216 | 0 | 0.322 |
| role=dest layout=4+2 piece=1M | fixed-400 | 399.6 | 400.0 | 1.000 | 10.01 | 1.26 | 1.36 | 1.25 | 0.209 | 0 | 0.198 |
| role=dest layout=4+2 piece=1M | fixed-200 | 199.7 | 199.8 | 0.999 | 20.02 | 1.13 | 1.27 | 1.18 | 0.190 | 0 | 0.120 |
| role=dest layout=4+2 piece=1M | fixed-100 | 99.80 | 99.90 | 0.999 | 40.04 | 0.935 | 1.15 | 1.08 | 0.176 | 0 | 0.080 |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 0.086 | 0.160 | 0.095 | 0.025 | 0 | 0.044 |

