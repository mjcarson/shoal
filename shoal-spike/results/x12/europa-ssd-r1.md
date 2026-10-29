### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.20 µs |
| fdatasync of a clean file | p50 10.78 µs, p99 16.46 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 73.27 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.07 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.30 µs, p99 15.44 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X12 · A rebuild's and a scrub's cpu on one core, round 1 (4 MiB chunks of 64 KiB units, out of cache; x86_64/avx2_gfni; a summary is a 4+2 stripe's six)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| op | GiB/s | µs a chunk | planted found | undetected | false alarms |
| --- | --- | --- | --- | --- | --- |
| crc | 50.38 | 77.54 | - | - | - |
| fold | 23.67 | 165.0 | - | - | - |
| crc+fold | 29.60 | 132.0 | - | - | - |
| copy | 28.61 | 136.6 | - | - | - |
| decode-21-data | 17.54 | 222.7 | - | - | - |
| decode-21-parity | 17.52 | 222.9 | - | - | - |
| decode-42-data | 9.33 | 418.7 | - | - | - |
| decode-42-parity | 9.34 | 418.2 | - | - | - |
| pipeline-copy | 21.81 | 179.1 | - | - | - |
| pipeline-21 | 8.81 | 443.6 | - | - | - |
| pipeline-42 | 4.60 | 848.5 | - | - | - |
| summary-42 | 5155 | 4.55 | - | - | - |
| detect | - | - | 2.00 | 0 | 0 |

### X12 · Q17: one missed unit rebuilt as its chunk and alone, round 1 (one at a time, nothing beside it)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilds/s | p50 ms | p99 ms | device KiB each | device read KiB each | flushes each | wrong |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=2+1 | chunk | 181.3 | 5.51 | 5.75 | 4105 | 8205 | 0 | 0 |
| layout=2+1 | unit | 5564 | 0.175 | 0.217 | 68.00 | 128.0 | 0 | 0 |
| layout=4+2 | chunk | 110.4 | 9.05 | 9.31 | 4106 | 16411 | 0 | 0 |
| layout=4+2 | unit | 4187 | 0.235 | 0.277 | 68.00 | 256.0 | 0 | 0 |

### X12 · A deep scrub beside the foreground, round 1 (4+2 stripes of 4 MiB chunks, CRC-64/NVME a 64 KiB unit, summaries encoded by x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | scrub MiB/s | achieved | stripes | planted found/checked | false alarms | read p99 ms | write p99 ms | lag p99 ms | crc µs/MiB | fold µs/MiB | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| layout=4+2 piece=1M | none | 0 | 0 | 0 | 0/0 | 0 | 0.086 | 0.164 | 0.025 | 0 | 0 | 0.042 |
| layout=4+2 piece=1M | fixed-50 | 50.05 | 1.00 | 47.00 | 0/0 | 0 | 0.831 | 1.07 | 0.027 | 28.31 | 13.25 | 0.061 |
| layout=4+2 piece=1M | fixed-100 | 99.90 | 0.999 | 95.00 | 2/2 | 0 | 1.20 | 1.38 | 0.027 | 28.62 | 13.09 | 0.080 |
| layout=4+2 piece=1M | fixed-200 | 200.0 | 1.000 | 191.0 | 2/2 | 0 | 1.43 | 1.64 | 0.043 | 28.35 | 13.03 | 0.114 |
| layout=4+2 piece=1M | fixed-400 | 398.8 | 0.997 | 381.0 | 4/4 | 0 | 1.53 | 2.02 | 0.140 | 28.76 | 13.05 | 0.184 |
| layout=4+2 piece=1M | idle | 2119 | 0 | 2028 | 22/22 | 0 | 0.545 | 1.25 | 0.207 | 28.42 | 13.08 | 0.837 |
| layout=4+2 piece=1M | idle-ceil-200 | 200.0 | 0 | 191.0 | 2/2 | 0 | 0.428 | 0.580 | 0.032 | 28.32 | 13.58 | 0.118 |
| layout=4+2 piece=1M | unbounded | 2245 | 0 | 2149 | 22/22 | 0 | 1.67 | 2.96 | 0.204 | 28.31 | 13.11 | 0.846 |
| layout=4+2 piece=256K | idle | 1879 | 0 | 1798 | 20/20 | 0 | 0.521 | 1.08 | 0.206 | 28.38 | 13.13 | 0.806 |
| layout=4+2 piece=4M | idle | 2214 | 0 | 2119 | 22/22 | 0 | 1.68 | 2.38 | 0.205 | 28.27 | 13.37 | 0.844 |
| layout=4+2 piece=1M | cpu | 22465 | 0 | 18721 | 5386/5386 | 0 | 0.558 | 1.76 | 0.228 | 26.54 | 13.61 | 0.044 |

### X12 · A rebuild beside the foreground, round 1 (4 MiB chunks of 64 KiB units, pieces of 1 MiB; x86_64/avx2_gfni; population from 3.2% to 94.6% of the device, 239 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| cell | side | rebuilt MiB/s | bg read+write MiB/s | achieved | chunk p50 ms | read p99 ms | write p99 ms | stage p99 ms | lag p99 ms | mismatches | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| role=dest layout=4+2 piece=1M | none | 0 | 0 | 0 | 0 | 0.085 | 0.162 | 0.097 | 0.025 | 0 | 0.043 |
| role=dest layout=4+2 piece=1M | fixed-100 | 99.80 | 99.90 | 0.999 | 40.04 | 1.00 | 1.13 | 1.06 | 0.164 | 0 | 0.079 |
| role=dest layout=4+2 piece=1M | fixed-200 | 199.7 | 199.8 | 0.999 | 20.02 | 1.18 | 1.30 | 1.16 | 0.192 | 0 | 0.119 |
| role=dest layout=4+2 piece=1M | fixed-400 | 399.6 | 400.0 | 1.000 | 10.01 | 1.27 | 1.34 | 1.24 | 0.211 | 0 | 0.201 |
| role=dest layout=4+2 piece=1M | fixed-800 | 732.6 | 733.3 | 0.917 | 5.42 | 1.28 | 1.76 | 1.28 | 0.217 | 0 | 0.323 |
| role=dest layout=4+2 piece=1M | idle | 1337 | 1338 | 0 | 2.98 | 0.549 | 1.11 | 1.06 | 0.220 | 0 | 0.582 |
| role=dest layout=4+2 piece=1M | idle-ceil-400 | 399.6 | 400.0 | 0 | 10.01 | 0.531 | 1.08 | 1.02 | 0.211 | 0 | 0.197 |
| role=dest layout=4+2 piece=1M | unbounded | 1749 | 1751 | 0 | 8.84 | 6.16 | 8.52 | 6.31 | 0.270 | 0 | 0.710 |
| role=local layout=copy piece=1M | idle | 981.8 | 1966 | 0 | 4.07 | 0.601 | 0.702 | 0.643 | 0.179 | 0 | 0.830 |
| role=local layout=copy piece=1M | unbounded | 1173 | 2349 | 0 | 13.62 | 6.45 | 11.12 | 6.53 | 0.350 | 0 | 0.911 |
| role=local layout=2+1 piece=1M | idle | 681.2 | 2046 | 0 | 5.85 | 0.580 | 0.851 | 0.710 | 0.198 | 0 | 0.831 |
| role=local layout=2+1 piece=1M | unbounded | 802.8 | 2411 | 0 | 19.91 | 4.82 | 8.26 | 4.84 | 0.532 | 0 | 0.890 |
| role=local layout=4+2 piece=1M | idle | 411.3 | 2059 | 0 | 9.71 | 0.554 | 1.18 | 0.722 | 0.203 | 0 | 0.823 |
| role=local layout=4+2 piece=1M | unbounded | 491.6 | 2459 | 0 | 32.55 | 5.46 | 10.71 | 6.13 | 0.872 | 0 | 0.898 |

